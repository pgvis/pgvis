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
        // Validate channel
        self.config.validate_channel(channel)?;

        // Check subscriber limit
        let current = self.subscriber_count.load(Ordering::Relaxed);
        if current >= self.config.max_subscribers {
            return Err(Error::PubSub {
                message: format!(
                    "maximum subscribers ({}) exceeded",
                    self.config.max_subscribers
                ),
                code: PubSubErrorCode::MaxSubscribersExceeded,
            });
        }

        let mut channels = self.channels.lock().await;
        let rx = if let Some(state) = channels.get_mut(channel) {
            state.local_subscribers += 1;
            self.subscriber_count.fetch_add(1, Ordering::Relaxed);
            state.tx.subscribe()
        } else {
            // First subscriber for this channel — issue LISTEN
            self.backend.listen(channel).await?;

            let (tx, rx) = broadcast::channel(self.config.channel_buffer_size.max(16));
            channels.insert(
                channel.to_string(),
                ChannelState {
                    tx,
                    local_subscribers: 1,
                },
            );
            self.subscriber_count.fetch_add(1, Ordering::Relaxed);
            rx
        };

        Ok(rx)
    }

    /// Unsubscribe from a channel (decrements subscriber count).
    ///
    /// When the last subscriber leaves, issues UNLISTEN to the backend and
    /// removes the channel's broadcast sender.
    pub async fn unsubscribe(&self, channel: &str) {
        let mut channels = self.channels.lock().await;
        let should_unlisten = if let Some(state) = channels.get_mut(channel) {
            state.local_subscribers = state.local_subscribers.saturating_sub(1);
            self.subscriber_count.fetch_sub(1, Ordering::Relaxed);
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
            total_subscribers: self.subscriber_count.load(Ordering::Relaxed),
            channels: channel_info,
        }
    }

    /// Get the underlying config.
    pub fn config(&self) -> &PubSubConfig {
        &self.config
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

/// Router state for the pub/sub endpoints: the hub plus the server config so
/// subscribe/publish can enforce the same JWT verification as the data API.
#[derive(Clone)]
pub struct PubSubState {
    hub: Arc<PubSubHub>,
    config: Arc<Config>,
}

/// Enforce JWT authentication for pub/sub endpoints, mirroring the data API.
///
/// Returns `Err(Response)` (401/500) when the request is unauthenticated and no
/// anonymous role is configured. When `jwt_secret` is unset, all requests pass
/// (same as the data API's anonymous mode).
fn authorize_pubsub(state: &PubSubState, headers: &axum::http::HeaderMap) -> Result<(), axum::response::Response> {
    // Reuse the exact JWT verification used by request dispatch. On success we
    // discard the identity (channels aren't RLS-scoped here) but reject
    // unauthenticated callers when auth is required.
    crate::routing::verify_jwt(headers, &state.config).map(|_| ())
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

    if let Err(resp) = authorize_pubsub(&state, &headers) {
        return resp;
    }
    let hub = state.hub;

    // Validate and subscribe
    let rx = match hub.subscribe(&channel).await {
        Ok(rx) => rx,
        Err(e) => {
            return error_response(&e);
        }
    };

    let keepalive_secs = hub.config().keepalive_interval_secs;
    let hub_clone = hub.clone();
    let channel_clone = channel.clone();

    // Build the SSE stream
    let stream = make_sse_stream(rx, keepalive_secs, hub_clone, channel_clone);

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
}

impl Drop for SseSubscription {
    fn drop(&mut self) {
        // `unsubscribe` is async; spawn it so Drop stays synchronous. This
        // decrements the subscriber count and issues UNLISTEN for the last one.
        let hub = self.hub.clone();
        let channel = std::mem::take(&mut self.channel);
        tokio::spawn(async move {
            hub.unsubscribe(&channel).await;
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
) -> impl futures::Stream<Item = Result<bytes::Bytes, std::convert::Infallible>> {
    let keepalive_interval = std::time::Duration::from_secs(keepalive_secs);

    // The guard lives in the stream's state; dropping the stream drops it.
    let guard = SseSubscription {
        hub,
        channel: channel.clone(),
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
/// to the specified channel.
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

    if let Err(resp) = authorize_pubsub(&state, &headers) {
        return resp;
    }
    let hub = state.hub;

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

    match hub.publish(&channel, &payload).await {
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
/// settings so subscribe/publish enforce the same authentication as the data API.
pub fn build_pubsub_router(hub: Arc<PubSubHub>, config: Arc<Config>) -> axum::Router {
    use axum::routing::{get, post};

    let state = PubSubState { hub, config };

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

    // For pub/sub errors, use the PubSubErrorCode's own string/status rather
    // than `Error::code()` (which reports Internal/PGV500 for the PubSub variant).
    let (status_u16, code) = match err {
        Error::PubSub { code, .. } => (code.http_status(), code.as_str().to_string()),
        other => (other.http_status(), other.code().as_str().to_string()),
    };

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
