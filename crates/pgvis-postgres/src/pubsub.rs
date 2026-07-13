//! # Postgres pub/sub implementation via LISTEN/NOTIFY.
//!
//! Implements [`PubSubBackend`] for PostgreSQL using a **dedicated non-pooled
//! connection** for LISTEN (LISTEN state is per-session and would be lost on
//! pool recycle) and **pooled connections** for NOTIFY (fire-and-forget).
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │              PgPubSub                        │
//! │                                             │
//! │  cmd_tx ──────► listener_task               │
//! │                   │                         │
//! │                   ├── connection.poll_message│
//! │                   │      ↓ Notification      │
//! │                   └── notify_tx.send(msg)   │
//! │                                             │
//! │  pool ──► publish() (SELECT pg_notify)      │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! ## Reconnection
//!
//! If the dedicated listener connection drops, the background task reconnects
//! with exponential backoff and re-issues LISTEN for all active channels.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use pgvis_core::error::Error;
use pgvis_core::pubsub::{PubSubBackend, PubSubConfig, PubSubErrorCode, PubSubMessage, PubSubStream};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio_postgres::tls::NoTlsStream;
use tokio_postgres::{AsyncMessage, Connection, NoTls, Socket};

use crate::create_pool;

// ---------------------------------------------------------------------------
// Commands sent to the listener task
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum PubSubCmd {
    Listen(String),
    Unlisten(String),
    Shutdown,
}

// ---------------------------------------------------------------------------
// PgPubSub — the public handle
// ---------------------------------------------------------------------------

/// Postgres pub/sub backend — implements [`PubSubBackend`].
///
/// Created by [`PgPubSub::new`], which spawns a background listener task.
/// The task maintains a dedicated connection and automatically reconnects
/// on failure.
pub struct PgPubSub {
    /// Channel for sending commands to the listener task.
    cmd_tx: mpsc::Sender<PubSubCmd>,
    /// Broadcast sender for outgoing notifications (hub subscribes to this).
    notify_tx: broadcast::Sender<PubSubMessage>,
    /// Pool for publish operations.
    pool: deadpool_postgres::Pool,
    /// Config (channel prefix, reconnect settings, etc.).
    config: Arc<PubSubConfig>,
    /// Set of channels currently being listened to.
    active: Arc<Mutex<HashSet<String>>>,
}

impl PgPubSub {
    /// Create a new Postgres pub/sub backend.
    ///
    /// Spawns a background task that maintains a dedicated LISTEN connection.
    /// The task automatically reconnects with exponential backoff on failure.
    ///
    /// # Arguments
    ///
    /// * `dsn` — PostgreSQL connection string
    /// * `config` — Pub/sub configuration (prefix, reconnect, buffer size)
    /// * `pool_config` — Pool config for publish connections
    ///
    /// # Errors
    ///
    /// Returns an error if the publish pool creation fails.
    pub fn new(
        dsn: &str,
        config: &PubSubConfig,
        pool_config: &pgvis_core::config::PoolConfig,
    ) -> Result<Self, Error> {
        let pool = create_pool(dsn, pool_config)?;
        let config = Arc::new(config.clone());
        let active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let (notify_tx, _) = broadcast::channel(config.channel_buffer_size.max(16));
        let (cmd_tx, cmd_rx) = mpsc::channel(64);

        // Spawn the listener task
        let task_dsn = dsn.to_string();
        let task_config = config.clone();
        let task_notify_tx = notify_tx.clone();
        let task_active = active.clone();

        tokio::spawn(listener_task(
            task_dsn,
            task_config,
            cmd_rx,
            task_notify_tx,
            task_active,
        ));

        Ok(Self {
            cmd_tx,
            notify_tx,
            pool,
            config,
            active,
        })
    }

}

impl PubSubBackend for PgPubSub {
    fn listen(&self, channel: &str) -> BoxFuture<'_, Result<(), Error>> {
        let channel = channel.to_string();
        Box::pin(async move {
            // Only record the channel as active once the command is enqueued, so a
            // failed send doesn't leave a phantom entry that reconnect would re-LISTEN.
            self.cmd_tx
                .send(PubSubCmd::Listen(channel.clone()))
                .await
                .map_err(|_| Error::PubSub {
                    message: "listener task is not running".to_string(),
                    code: PubSubErrorCode::ConnectionLost,
                })?;
            self.active.lock().await.insert(channel);
            Ok(())
        })
    }

    fn unlisten(&self, channel: &str) -> BoxFuture<'_, Result<(), Error>> {
        let channel = channel.to_string();
        Box::pin(async move {
            self.active.lock().await.remove(&channel);
            self.cmd_tx
                .send(PubSubCmd::Unlisten(channel))
                .await
                .map_err(|_| Error::PubSub {
                    message: "listener task is not running".to_string(),
                    code: PubSubErrorCode::ConnectionLost,
                })
        })
    }

    fn publish(&self, channel: &str, payload: &str) -> BoxFuture<'_, Result<(), Error>> {
        let pg_channel = self.config.pg_channel(channel);
        let payload = payload.to_string();
        Box::pin(async move {
            let client = self.pool.get().await.map_err(|e| Error::PubSub {
                message: format!("pool error: {e}"),
                code: PubSubErrorCode::ConnectionLost,
            })?;

            client
                .execute("SELECT pg_notify($1, $2)", &[&pg_channel, &payload])
                .await
                .map_err(|e| Error::PubSub {
                    message: format!("NOTIFY failed: {e}"),
                    code: PubSubErrorCode::ConnectionLost,
                })?;

            Ok(())
        })
    }

    fn notification_stream(&self) -> BoxFuture<'_, Result<PubSubStream, Error>> {
        let rx = self.notify_tx.subscribe();
        Box::pin(async move {
            let stream = futures::stream::unfold(rx, |mut rx| async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => return Some((msg, rx)),
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(skipped = n, "pub/sub subscriber lagged");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            });
            Ok(Box::pin(stream) as PubSubStream)
        })
    }

    fn active_channels(&self) -> BoxFuture<'_, Vec<String>> {
        Box::pin(async move {
            let active = self.active.lock().await;
            active.iter().cloned().collect()
        })
    }
}

impl Drop for PgPubSub {
    fn drop(&mut self) {
        let _ = self.cmd_tx.try_send(PubSubCmd::Shutdown);
    }
}

// ---------------------------------------------------------------------------
// Listener task — dedicated connection event loop
// ---------------------------------------------------------------------------

/// Background task maintaining the dedicated LISTEN connection.
///
/// Reconnects with exponential backoff on failure and re-issues all active
/// LISTENs after reconnection.
async fn listener_task(
    dsn: String,
    config: Arc<PubSubConfig>,
    mut cmd_rx: mpsc::Receiver<PubSubCmd>,
    notify_tx: broadcast::Sender<PubSubMessage>,
    active: Arc<Mutex<HashSet<String>>>,
) {
    let mut attempt: u32 = 0;

    loop {
        // Attempt to connect
        let connect_result = tokio_postgres::connect(&dsn, NoTls).await;

        let (client, connection) = match connect_result {
            Ok(pair) => {
                attempt = 0;
                tracing::info!("pub/sub listener connected");
                pair
            }
            Err(e) => {
                attempt = attempt.saturating_add(1);
                let delay = backoff_delay(attempt, config.reconnect_base_ms, config.reconnect_max_ms);
                tracing::warn!(
                    error = %e,
                    attempt,
                    delay_ms = delay.as_millis(),
                    "pub/sub listener connection failed, retrying"
                );
                tokio::time::sleep(delay).await;

                // Drain commands during sleep — honour Shutdown
                while let Ok(cmd) = cmd_rx.try_recv() {
                    if matches!(cmd, PubSubCmd::Shutdown) {
                        return;
                    }
                }
                continue;
            }
        };

        // Spawn a task to drive the `Connection` and forward async messages
        // (notifications) over an mpsc channel. This is essential: `Client`
        // requests (LISTEN/UNLISTEN below) only make progress while the
        // `Connection` future is being polled. Driving it in a separate task
        // means our LISTEN commands complete instead of deadlocking.
        let (async_tx, mut async_rx) = mpsc::unbounded_channel();
        let conn_task = tokio::spawn(drive_connection(connection, async_tx));

        // Re-issue LISTEN for all active channels. The connection is now being
        // polled by `conn_task`, so batch_execute makes progress.
        let channels: Vec<String> = active.lock().await.iter().cloned().collect();
        let mut listen_failed = false;
        for channel in &channels {
            let pg_ch = config.pg_channel(channel);
            let query = format!("LISTEN \"{}\"", escape_ident_inner(&pg_ch));
            if let Err(e) = client.batch_execute(&query).await {
                tracing::error!(channel = %channel, error = %e, "LISTEN failed on reconnect");
                listen_failed = true;
                break;
            }
        }

        if listen_failed {
            conn_task.abort();
            attempt = attempt.saturating_add(1);
            let delay = backoff_delay(attempt, config.reconnect_base_ms, config.reconnect_max_ms);
            tokio::time::sleep(delay).await;
            continue;
        }

        // Run the event loop on this connection
        let should_shutdown = connection_event_loop(
            &client,
            &mut async_rx,
            &config,
            &mut cmd_rx,
            &notify_tx,
        )
        .await;

        // Drop the client and stop driving the connection before reconnecting.
        drop(client);
        conn_task.abort();

        if should_shutdown {
            tracing::info!("pub/sub listener shutting down");
            return;
        }

        // Connection lost — backoff and retry
        attempt = attempt.saturating_add(1);
        let delay = backoff_delay(attempt, config.reconnect_base_ms, config.reconnect_max_ms);
        tracing::info!(attempt, delay_ms = delay.as_millis(), "pub/sub reconnecting");
        tokio::time::sleep(delay).await;

        // Honour shutdown during sleep
        if let Ok(PubSubCmd::Shutdown) = cmd_rx.try_recv() {
            return;
        }
    }
}

/// Drive a `Connection` future to completion, forwarding every `AsyncMessage`
/// (notifications, notices, …) over `async_tx`.
///
/// tokio-postgres only advances in-flight `Client` requests and delivers
/// notifications while this future is polled, so it runs in its own task.
/// When the connection ends (error or close), the channel is dropped, which
/// signals the event loop to reconnect.
async fn drive_connection(
    mut connection: Connection<Socket, NoTlsStream>,
    async_tx: mpsc::UnboundedSender<Result<AsyncMessage, tokio_postgres::Error>>,
) {
    loop {
        match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            Some(msg) => {
                // If the receiver is gone, the event loop has moved on — stop.
                if async_tx.send(msg).is_err() {
                    return;
                }
            }
            None => return, // Connection closed cleanly.
        }
    }
}

/// Drives command handling and consumes forwarded notifications.
///
/// Returns `true` if shutdown was requested, `false` if the connection broke.
async fn connection_event_loop(
    client: &tokio_postgres::Client,
    async_rx: &mut mpsc::UnboundedReceiver<Result<AsyncMessage, tokio_postgres::Error>>,
    config: &PubSubConfig,
    cmd_rx: &mut mpsc::Receiver<PubSubCmd>,
    notify_tx: &broadcast::Sender<PubSubMessage>,
) -> bool {
    loop {
        tokio::select! {
            // Consume async messages forwarded by the connection driver task.
            msg = async_rx.recv() => {
                match msg {
                    Some(Ok(AsyncMessage::Notification(n))) => {
                        let logical_channel = config
                            .strip_prefix(n.channel())
                            .unwrap_or(n.channel())
                            .to_string();

                        let msg = PubSubMessage {
                            channel: logical_channel,
                            payload: n.payload().to_string(),
                            timestamp: now_iso8601(),
                        };

                        // Broadcast — ignore "no receivers" error
                        let _ = notify_tx.send(msg);
                    }
                    Some(Ok(AsyncMessage::Notice(notice))) => {
                        tracing::debug!(message = %notice.message(), "pg notice on pub/sub connection");
                    }
                    Some(Ok(_)) => {
                        // Other async messages — ignore
                    }
                    Some(Err(e)) => {
                        tracing::error!(error = %e, "pub/sub connection error");
                        return false; // Reconnect
                    }
                    None => {
                        tracing::warn!("pub/sub connection closed");
                        return false; // Reconnect
                    }
                }
            }

            // Process commands from the hub
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(PubSubCmd::Listen(channel)) => {
                        let pg_ch = config.pg_channel(&channel);
                        let query = format!("LISTEN \"{}\"", escape_ident_inner(&pg_ch));
                        if let Err(e) = client.batch_execute(&query).await {
                            tracing::error!(channel = %channel, error = %e, "LISTEN failed");
                            return false; // Connection likely broken
                        }
                        tracing::debug!(channel = %channel, "LISTEN issued");
                    }
                    Some(PubSubCmd::Unlisten(channel)) => {
                        let pg_ch = config.pg_channel(&channel);
                        let query = format!("UNLISTEN \"{}\"", escape_ident_inner(&pg_ch));
                        if let Err(e) = client.batch_execute(&query).await {
                            tracing::error!(channel = %channel, error = %e, "UNLISTEN failed");
                            return false;
                        }
                        tracing::debug!(channel = %channel, "UNLISTEN issued");
                    }
                    Some(PubSubCmd::Shutdown) | None => {
                        return true; // Shutdown
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Deterministic exponential-backoff base (capped at `max_ms`), before jitter.
fn backoff_base(attempt: u32, base_ms: u64, max_ms: u64) -> u64 {
    let multiplier = 1u64.checked_shl(attempt.min(20)).unwrap_or(u64::MAX);
    base_ms.saturating_mul(multiplier).min(max_ms)
}

/// Exponential backoff with randomized jitter (up to ~20%).
///
/// The jitter is drawn from process/time entropy so that multiple instances
/// that disconnect together don't reconnect in lockstep (thundering herd).
fn backoff_delay(attempt: u32, base_ms: u64, max_ms: u64) -> Duration {
    let capped = backoff_base(attempt, base_ms, max_ms);
    let jitter_range = capped / 5;
    let jitter = if jitter_range == 0 {
        0
    } else {
        jitter_entropy() % (jitter_range + 1)
    };
    Duration::from_millis(capped.saturating_add(jitter))
}

/// A cheap, non-cryptographic entropy source for backoff jitter.
///
/// Mixes the current time's nanoseconds with the thread id — good enough to
/// desynchronize reconnects across instances without pulling in an RNG crate.
fn jitter_entropy() -> u64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut hasher = std::hash::DefaultHasher::new();
    std::hash::Hash::hash(&std::thread::current().id(), &mut hasher);
    nanos ^ std::hash::Hasher::finish(&hasher)
}

/// Escape a string for use inside double-quotes in SQL.
/// Doubles any embedded double-quote characters.
fn escape_ident_inner(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Current UTC time as ISO 8601 string (second precision).
fn now_iso8601() -> String {
    // Simple UTC timestamp without external time crate.
    // Format: seconds since epoch (compact). A full ISO format would
    // require chrono or manual calendar math — we use epoch seconds
    // which is still monotonic and machine-parseable.
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    // Produce a proper ISO 8601 format using basic arithmetic
    let total_secs = dur.as_secs();
    let days = total_secs / 86400;
    let day_secs = total_secs % 86400;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    // Days since 1970-01-01
    let (year, month, day) = days_to_date(days);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_date(days: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_base_deterministic() {
        assert_eq!(backoff_base(0, 500, 30_000), 500);
        assert_eq!(backoff_base(1, 500, 30_000), 1000);
        assert_eq!(backoff_base(2, 500, 30_000), 2000);
        // Caps at max_ms and never overflows for large attempts.
        assert_eq!(backoff_base(30, 500, 30_000), 30_000);
        assert_eq!(backoff_base(1000, 500, 30_000), 30_000);
    }

    #[test]
    fn test_backoff_delay_within_jittered_bounds() {
        for attempt in 0..5 {
            let base = backoff_base(attempt, 500, 30_000);
            let d = backoff_delay(attempt, 500, 30_000).as_millis() as u64;
            assert!(d >= base, "delay {d} below base {base}");
            assert!(d <= base + base / 5 + 1, "delay {d} exceeds base+20% {base}");
        }
    }

    #[test]
    fn test_escape_ident_inner() {
        assert_eq!(escape_ident_inner("hello"), "hello");
        assert_eq!(escape_ident_inner(r#"he"llo"#), r#"he""llo"#);
        assert_eq!(escape_ident_inner("pgvis:orders.new"), "pgvis:orders.new");
    }

    #[test]
    fn test_now_iso8601_format() {
        let ts = now_iso8601();
        assert!(ts.ends_with('Z'));
        assert!(ts.contains('T'));
        assert_eq!(ts.len(), 20); // "2025-06-01T12:00:00Z"
    }

    #[test]
    fn test_days_to_date() {
        // 1970-01-01 = day 0
        assert_eq!(days_to_date(0), (1970, 1, 1));
        // 2000-01-01 = day 10957
        assert_eq!(days_to_date(10957), (2000, 1, 1));
    }
}
