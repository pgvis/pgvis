//! # Pub/Sub Hub — in-process fan-out and REST SSE endpoints.
//!
//! The [`PubSubHub`] coordinates between the [`PubSubBackend`] (Postgres LISTEN/NOTIFY)
//! and local subscribers (REST SSE, MCP streaming, embedded). It manages:
//!
//! - Per-channel `tokio::sync::broadcast` for local fan-out
//! - Dynamic LISTEN/UNLISTEN: listens when first subscriber joins, unlistens when last leaves
//! - Subscriber count tracking (capped by config)
//! - SSE keepalive for idle connections
//!
//! ## Architecture
//!
//! ```text
//! PubSubBackend (Postgres)
//!       │
//!       ▼
//!  notification_stream()
//!       │
//!       ▼
//! PubSubHub.dispatch_task ──► per-channel broadcast::Sender
//!       │                              │
//!       │                    ┌─────────┼─────────┐
//!       │                    ▼         ▼         ▼
//!       │               SSE client  MCP tool  embedded
//!       │
//!       └── publish() ──► backend.publish()
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::StreamExt;
use pgvis_core::Config;
use pgvis_core::backend::Backend;
use pgvis_core::error::Error;
use pgvis_core::pubsub::{PubSubBackend, PubSubConfig, PubSubErrorCode, PubSubMessage};
use tokio::sync::{Mutex, broadcast};

// ---------------------------------------------------------------------------
// PubSubHub — the in-process coordinator
// ---------------------------------------------------------------------------

/// In-process pub/sub hub that bridges the database backend and local subscribers.
///
/// Created once at startup when pub/sub is enabled. Provides:
/// - `subscribe(channel)` — returns a stream for a specific channel
/// - `publish(channel, payload)` — publishes to the database (and hence all instances)
/// - Automatic LISTEN/UNLISTEN lifecycle management
pub struct PubSubHub {
    backend: Arc<dyn PubSubBackend>,
    config: Arc<PubSubConfig>,
    /// Per-channel broadcast senders. Channels are created on first subscribe.
    channels: Mutex<HashMap<String, ChannelState>>,
    /// Global subscriber count (across all channels).
    subscriber_count: AtomicUsize,
    /// Subscriber count per identity key, for `max_subscribers_per_identity`.
    identity_counts: std::sync::Mutex<HashMap<String, usize>>,
}

/// State for a single channel's local broadcast.
struct ChannelState {
    tx: broadcast::Sender<PubSubMessage>,
    /// Number of active local subscribers for this channel.
    local_subscribers: usize,
}

impl PubSubHub {
    /// Create a new hub and start the dispatch task.
    ///
    /// The dispatch task reads from the backend's notification stream and
    /// forwards messages to the appropriate per-channel broadcast.
    ///
    /// # Arguments
    ///
    /// * `backend` — The database pub/sub backend (e.g. `PgPubSub`)
    /// * `config` — Pub/sub configuration
    pub async fn new(
        backend: Arc<dyn PubSubBackend>,
        config: PubSubConfig,
    ) -> Result<Arc<Self>, Error> {
        let config = Arc::new(config);

        let hub = Arc::new(Self {
            backend: backend.clone(),
            config: config.clone(),
            channels: Mutex::new(HashMap::new()),
            subscriber_count: AtomicUsize::new(0),
            identity_counts: std::sync::Mutex::new(HashMap::new()),
        });

        // Start the dispatch task that reads from the backend notification stream
        let dispatch_hub = hub.clone();
        tokio::spawn(dispatch_loop(dispatch_hub));

        Ok(hub)
    }

    /// Subscribe to a channel.
    ///
    /// Returns a broadcast receiver that yields messages for this channel.
    /// Automatically issues LISTEN to the backend when this is the first
    /// subscriber for the channel.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The channel name is invalid or denied
    /// - Maximum subscribers exceeded
    /// - The backend LISTEN fails
    pub async fn subscribe(
        &self,
        channel: &str,
    ) -> Result<broadcast::Receiver<PubSubMessage>, Error> {
        self.subscribe_as(channel, None).await
    }

    /// Subscribe to a channel on behalf of `identity`, which is additionally
    /// capped at `max_subscribers_per_identity`. Release the slot with
    /// [`unsubscribe_as`](Self::unsubscribe_as) and the same identity.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe), plus the per-identity limit.
    pub async fn subscribe_as(
        &self,
        channel: &str,
        identity: Option<&str>,
    ) -> Result<broadcast::Receiver<PubSubMessage>, Error> {
        // Validate channel
        self.config.validate_channel(channel)?;

        // Reserve the global and per-identity slots atomically, so concurrent
        // subscribers can never overshoot either cap.
        let max = self.config.max_subscribers;
        if self
            .subscriber_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max).then_some(n + 1)
            })
            .is_err()
        {
            return Err(max_subscribers_error(format!(
                "maximum subscribers ({max}) exceeded"
            )));
        }
        if let Err(e) = self.reserve_identity(identity) {
            self.subscriber_count.fetch_sub(1, Ordering::AcqRel);
            return Err(e);
        }

        let mut channels = self.channels.lock().await;
        let rx = if let Some(state) = channels.get_mut(channel) {
            state.local_subscribers += 1;
            state.tx.subscribe()
        } else {
            // First subscriber for this channel — issue LISTEN
            if let Err(e) = self.backend.listen(channel).await {
                self.subscriber_count.fetch_sub(1, Ordering::AcqRel);
                self.release_identity(identity);
                return Err(e);
            }

            let (tx, rx) = broadcast::channel(self.config.channel_buffer_size.max(16));
            channels.insert(
                channel.to_string(),
                ChannelState {
                    tx,
                    local_subscribers: 1,
                },
            );
            rx
        };

        Ok(rx)
    }

    /// Take one of `identity`'s `max_subscribers_per_identity` slots.
    fn reserve_identity(&self, identity: Option<&str>) -> Result<(), Error> {
        let max = self.config.max_subscribers_per_identity;
        let Some(identity) = identity.filter(|_| max > 0) else {
            return Ok(());
        };
        let mut counts = self
            .identity_counts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let count = counts.entry(identity.to_string()).or_default();
        if *count >= max {
            return Err(max_subscribers_error(format!(
                "maximum subscribers per identity ({max}) exceeded"
            )));
        }
        *count += 1;
        Ok(())
    }

    /// Give back a slot taken by [`reserve_identity`](Self::reserve_identity).
    fn release_identity(&self, identity: Option<&str>) {
        let Some(identity) = identity.filter(|_| self.config.max_subscribers_per_identity > 0)
        else {
            return;
        };
        let mut counts = self
            .identity_counts
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(count) = counts.get_mut(identity) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(identity);
            }
        }
    }

    /// Unsubscribe from a channel (decrements subscriber count).
    ///
    /// When the last subscriber leaves, issues UNLISTEN to the backend and
    /// removes the channel's broadcast sender.
    pub async fn unsubscribe(&self, channel: &str) {
        self.unsubscribe_as(channel, None).await;
    }

    /// Unsubscribe a subscription made with [`subscribe_as`](Self::subscribe_as).
    pub async fn unsubscribe_as(&self, channel: &str, identity: Option<&str>) {
        let mut channels = self.channels.lock().await;
        let should_unlisten = if let Some(state) = channels.get_mut(channel) {
            state.local_subscribers = state.local_subscribers.saturating_sub(1);
            self.subscriber_count.fetch_sub(1, Ordering::AcqRel);
            self.release_identity(identity);
            if state.local_subscribers == 0 {
                channels.remove(channel);
                true
            } else {
                false
            }
        } else {
            false
        };

        if should_unlisten {
            if let Err(e) = self.backend.unlisten(channel).await {
                tracing::warn!(channel = %channel, error = %e, "UNLISTEN failed");
            }
        }
    }

    /// Publish a message to a channel.
    ///
    /// Trusted in-process API: runs as the pub/sub connection's role with no
    /// authorization check. The REST endpoint publishes as the caller instead
    /// (see [`pgvis_core::pubsub::publish`]).
    ///
    /// Validates payload size and channel name, then delegates to the backend.
    /// The message flows through Postgres NOTIFY and back through the dispatch
    /// task to all subscribers (including those on other pgvis instances).
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or the backend publish fails.
    pub async fn publish(&self, channel: &str, payload: &str) -> Result<(), Error> {
        self.config.validate_channel(channel)?;
        self.config.validate_payload(payload)?;
        self.backend.publish(channel, payload).await
    }

    /// Get pub/sub status information.
    pub async fn status(&self) -> PubSubStatus {
        let channels = self.channels.lock().await;
        let channel_info: Vec<ChannelInfo> = channels
            .iter()
            .map(|(name, state)| ChannelInfo {
                name: name.clone(),
                subscribers: state.local_subscribers,
            })
            .collect();
        PubSubStatus {
            total_subscribers: self.subscriber_count.load(Ordering::Acquire),
            channels: channel_info,
        }
    }

    /// Get the underlying config.
    pub fn config(&self) -> &PubSubConfig {
        &self.config
    }
}

fn max_subscribers_error(message: String) -> Error {
    Error::PubSub {
        message,
        code: PubSubErrorCode::MaxSubscribersExceeded,
    }
}

// ---------------------------------------------------------------------------
// Status types
// ---------------------------------------------------------------------------

/// Status information about the pub/sub system.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PubSubStatus {
    /// Total number of active subscribers across all channels.
    pub total_subscribers: usize,
    /// Per-channel information.
    pub channels: Vec<ChannelInfo>,
}

/// Information about a single pub/sub channel.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChannelInfo {
    /// The logical channel name.
    pub name: String,
    /// Number of active subscribers on this instance.
    pub subscribers: usize,
}

// ---------------------------------------------------------------------------
// Dispatch loop — reads from backend and fans out to local broadcasts
// ---------------------------------------------------------------------------

/// Reads messages from the backend notification stream and dispatches them
/// to the appropriate per-channel broadcast sender.
///
/// The stream normally lives for the whole process (the backend's broadcast
/// sender is stable across reconnects). If it ever ends or fails to open, the
/// loop re-opens it after a short delay so the hub never becomes a zombie that
/// holds subscribers but delivers nothing.
async fn dispatch_loop(hub: Arc<PubSubHub>) {
    loop {
        match hub.backend.notification_stream().await {
            Ok(mut stream) => {
                while let Some(msg) = stream.next().await {
                    let channels = hub.channels.lock().await;
                    if let Some(state) = channels.get(&msg.channel) {
                        // Broadcast to local subscribers — ignore "no receivers".
                        let _ = state.tx.send(msg);
                    }
                    // Messages for channels with no local subscribers are dropped
                    // (they can arrive briefly during UNLISTEN propagation).
                }
                tracing::warn!("pub/sub dispatch stream ended; re-opening");
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to open pub/sub notification stream; retrying");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

// ---------------------------------------------------------------------------
// REST SSE handlers
// ---------------------------------------------------------------------------

/// Router state for the pub/sub endpoints: the hub, the server config so
/// subscribe/publish can enforce the same JWT verification as the data API,
/// and the backend that authorizes and publishes as the caller's role.
#[derive(Clone)]
pub struct PubSubState {
    hub: Arc<PubSubHub>,
    config: Arc<Config>,
    backend: Arc<dyn Backend>,
}

/// The verified caller: the [`ExecContext`](pgvis_core::ExecContext) its
/// database work runs under, and its key for the per-identity subscriber cap.
struct Caller {
    ctx: pgvis_core::ExecContext,
    identity: String,
}

/// Verify the JWT exactly as the data API does and keep the identity.
///
/// Returns `Err(Response)` (401/500) when the request is unauthenticated and no
/// anonymous role is configured. When `jwt_secret` is unset, all requests pass
/// as the anonymous role (same as the data API's anonymous mode).
fn authorize_pubsub(
    state: &PubSubState,
    headers: &axum::http::HeaderMap,
) -> Result<Caller, axum::response::Response> {
    let auth = crate::routing::verify_jwt(headers, &state.config)?;
    // Role + `sub`; unauthenticated callers share their role's identity.
    let sub = auth.claims.as_ref().and_then(|c| c.get("sub"));
    let identity = format!(
        "{}\0{}",
        auth.role.as_deref().unwrap_or(""),
        sub.map(|v| v.to_string()).unwrap_or_default()
    );
    // Pub/sub statements may NOTIFY or call a volatile authorize function, so
    // they are mutations (never routed to a read replica).
    let ctx = crate::routing::build_exec_context(
        &state.config,
        &auth,
        &pgvis_core::Preferences::default(),
        true,
    );
    Ok(Caller { ctx, identity })
}

/// SSE subscribe handler: `GET /pubsub/{channel}`
///
/// Returns a Server-Sent Events stream that yields messages from the specified
/// channel. The connection stays open until the client disconnects.
///
/// Sends periodic `: keepalive` comments to prevent proxy timeouts.
pub async fn handle_subscribe(
    axum::extract::State(state): axum::extract::State<PubSubState>,
    axum::extract::Path(channel): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let caller = match authorize_pubsub(&state, &headers) {
        Ok(caller) => caller,
        Err(resp) => return resp,
    };
    let hub = state.hub;

    if let Err(e) = hub.config().validate_channel(&channel) {
        return error_response(&e);
    }
    if let Err(e) = pgvis_core::pubsub::authorize(
        &*state.backend,
        &caller.ctx,
        hub.config(),
        &channel,
        "subscribe",
    )
    .await
    {
        return error_response(&e);
    }

    // Validate and subscribe
    let rx = match hub.subscribe_as(&channel, Some(&caller.identity)).await {
        Ok(rx) => rx,
        Err(e) => {
            return error_response(&e);
        }
    };

    let keepalive_secs = hub.config().keepalive_interval_secs;
    let hub_clone = hub.clone();
    let channel_clone = channel.clone();

    // Build the SSE stream
    let stream = make_sse_stream(
        rx,
        keepalive_secs,
        hub_clone,
        channel_clone,
        caller.identity,
    );

    let headers = [
        (http::header::CONTENT_TYPE, "text/event-stream"),
        (http::header::CACHE_CONTROL, "no-cache"),
        (http::header::CONNECTION, "keep-alive"),
    ];

    (headers, axum::body::Body::from_stream(stream)).into_response()
}

/// Drop guard that unsubscribes from the hub when the SSE stream is dropped.
///
/// A client disconnect drops the response body (and hence the stream and this
/// guard); previously unsubscribe only ran on `RecvError::Closed`, which never
/// fires while the hub retains its `Sender`, so disconnected clients leaked
/// subscriber slots until `max_subscribers`. The guard runs on every drop.
struct SseSubscription {
    hub: Arc<PubSubHub>,
    channel: String,
    identity: String,
}

impl Drop for SseSubscription {
    fn drop(&mut self) {
        // `unsubscribe` is async; spawn it so Drop stays synchronous. This
        // decrements the subscriber count and issues UNLISTEN for the last one.
        let hub = self.hub.clone();
        let channel = std::mem::take(&mut self.channel);
        let identity = std::mem::take(&mut self.identity);
        tokio::spawn(async move {
            hub.unsubscribe_as(&channel, Some(&identity)).await;
        });
    }
}

/// Build an SSE byte stream from a broadcast receiver.
///
/// Emits `data: {...}\n\n` for messages and `: keepalive\n\n` comments on idle.
/// An [`SseSubscription`] guard owned by the stream calls `hub.unsubscribe()`
/// when the stream is dropped (client disconnect) or ends.
fn make_sse_stream(
    rx: broadcast::Receiver<PubSubMessage>,
    keepalive_secs: u64,
    hub: Arc<PubSubHub>,
    channel: String,
    identity: String,
) -> impl futures::Stream<Item = Result<bytes::Bytes, std::convert::Infallible>> {
    let keepalive_interval = std::time::Duration::from_secs(keepalive_secs);

    // The guard lives in the stream's state; dropping the stream drops it.
    let guard = SseSubscription {
        hub,
        channel: channel.clone(),
        identity,
    };

    futures::stream::unfold(
        (rx, guard, keepalive_interval),
        |(mut rx, guard, interval)| async move {
            loop {
                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Ok(msg) => {
                                let json = serde_json::to_string(&msg).unwrap_or_default();
                                let event = format!("event: message\ndata: {json}\n\n");
                                return Some((
                                    Ok(bytes::Bytes::from(event)),
                                    (rx, guard, interval),
                                ));
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::debug!(skipped = n, "SSE subscriber lagged");
                                let comment = format!(": lagged {n} messages\n\n");
                                return Some((
                                    Ok(bytes::Bytes::from(comment)),
                                    (rx, guard, interval),
                                ));
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                // Stream ends; `guard` drops here → unsubscribe.
                                return None;
                            }
                        }
                    }
                    _ = tokio::time::sleep(interval) => {
                        let comment = bytes::Bytes::from_static(b": keepalive\n\n");
                        return Some((
                            Ok(comment),
                            (rx, guard, interval),
                        ));
                    }
                }
            }
        },
    )
}

/// Publish handler: `POST /pubsub/{channel}`
///
/// Accepts a JSON or plain text body as the message payload and publishes it
/// to the specified channel, as the caller's role (see
/// [`pgvis_core::pubsub::publish`]).
///
/// Request body is the raw payload string (Content-Type: text/plain or application/json).
///
/// Returns 200 on success with `{"ok": true}`.
pub async fn handle_publish(
    axum::extract::State(state): axum::extract::State<PubSubState>,
    axum::extract::Path(channel): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let caller = match authorize_pubsub(&state, &headers) {
        Ok(caller) => caller,
        Err(resp) => return resp,
    };

    let payload = match std::str::from_utf8(&body) {
        Ok(s) => s.to_string(),
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({
                    "error": "payload must be valid UTF-8"
                })),
            )
                .into_response();
        }
    };

    match pgvis_core::pubsub::publish(
        &*state.backend,
        &caller.ctx,
        state.hub.config(),
        &channel,
        &payload,
    )
    .await
    {
        Ok(()) => axum::Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => error_response(&e),
    }
}

/// Status handler: `GET /pubsub`
///
/// Returns the current pub/sub status including active channels and subscriber counts.
pub async fn handle_status(
    axum::extract::State(state): axum::extract::State<PubSubState>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    // Status exposes channel names and subscriber counts, so it requires the same
    // authentication as subscribe/publish when JWT is configured.
    if let Err(resp) = authorize_pubsub(&state, &headers) {
        return resp;
    }
    let status = state.hub.status().await;
    axum::Json(status).into_response()
}

/// Build the pub/sub router.
///
/// Mounts the subscribe and publish handlers under the given prefix.
/// Typically mounted at `/{routing_prefix}/pubsub`. `config` supplies the JWT
/// settings so subscribe/publish enforce the same authentication as the data API;
/// `backend` runs the authorize function and publishes as the caller's role.
pub fn build_pubsub_router(
    hub: Arc<PubSubHub>,
    config: Arc<Config>,
    backend: Arc<dyn Backend>,
) -> axum::Router {
    use axum::routing::{get, post};

    let state = PubSubState {
        hub,
        config,
        backend,
    };

    axum::Router::new()
        .route("/", get(handle_status))
        .route("/{channel}", get(handle_subscribe))
        .route("/{channel}", post(handle_publish))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a pgvis Error into an HTTP error response.
fn error_response(err: &Error) -> axum::response::Response {
    use axum::response::IntoResponse;

    let (status_u16, code) = (err.http_status(), err.code().as_str());

    let status = axum::http::StatusCode::from_u16(status_u16)
        .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

    let body = serde_json::json!({
        "error": err.to_string(),
        "code": code,
    });

    (status, axum::Json(body)).into_response()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pubsub_status_serializes() {
        let status = PubSubStatus {
            total_subscribers: 5,
            channels: vec![
                ChannelInfo {
                    name: "orders.new".to_string(),
                    subscribers: 3,
                },
                ChannelInfo {
                    name: "chat.room1".to_string(),
                    subscribers: 2,
                },
            ],
        };
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["total_subscribers"], 5);
        assert_eq!(json["channels"][0]["name"], "orders.new");
        assert_eq!(json["channels"][1]["subscribers"], 2);
    }
}
