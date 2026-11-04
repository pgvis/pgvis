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
use pgvis_core::pubsub::{
    PubSubBackend, PubSubConfig, PubSubErrorCode, PubSubMessage, PubSubStream,
};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{AsyncMessage, Connection, Socket};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::{pool::ManagedPool, tls_connector};

// ---------------------------------------------------------------------------
// Commands sent to the listener task
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum PubSubCmd {
    Listen(String),
    Unlisten(String),
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
    pool: ManagedPool,
    listener: tokio::task::JoinHandle<()>,
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
        let pool = ManagedPool::new(dsn, pool_config)?;
        let config = Arc::new(config.clone());
        let active: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

        let (notify_tx, _) = broadcast::channel(config.channel_buffer_size.max(16));
        let (cmd_tx, cmd_rx) = mpsc::channel(64);

        // Spawn the listener task
        let task_dsn = dsn.to_string();
        let task_config = config.clone();
        let task_notify_tx = notify_tx.clone();
        let task_active = active.clone();

        let listener = tokio::spawn(listener_task(
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
            listener,
        })
    }
}

impl PgPubSub {
    async fn send_command(&self, command: PubSubCmd) -> Result<(), Error> {
        tokio::time::timeout(Duration::from_secs(5), self.cmd_tx.send(command))
            .await
            .map_err(|_| Error::PubSub {
                message: "listener command queue deadline exceeded".into(),
                code: PubSubErrorCode::ConnectionLost,
            })?
            .map_err(|_| Error::PubSub {
                message: "listener task is not running".into(),
                code: PubSubErrorCode::ConnectionLost,
            })
    }
}

impl PubSubBackend for PgPubSub {
    fn listen(&self, channel: &str) -> BoxFuture<'_, Result<(), Error>> {
        let channel = channel.to_string();
        Box::pin(async move {
            // Only record the channel as active once the command is enqueued, so a
            // failed send doesn't leave a phantom entry that reconnect would re-LISTEN.
            self.send_command(PubSubCmd::Listen(channel.clone()))
                .await?;
            self.active.lock().await.insert(channel);
            Ok(())
        })
    }

    fn unlisten(&self, channel: &str) -> BoxFuture<'_, Result<(), Error>> {
        let channel = channel.to_string();
        Box::pin(async move {
            self.active.lock().await.remove(&channel);
            self.send_command(PubSubCmd::Unlisten(channel)).await
        })
    }

    fn publish(&self, channel: &str, payload: &str) -> BoxFuture<'_, Result<(), Error>> {
        let pg_channel = self.config.pg_channel(channel);
        let payload = payload.to_string();
        Box::pin(async move {
            let checkout = self.pool.checkout().await?;
            self.pool
                .execute(
                    checkout,
                    &pgvis_core::backend::ExecContext {
                        is_mutation: true,
                        ..Default::default()
                    },
                    "SELECT pg_notify($1, $2), 'null'::json AS body",
                    &[
                        serde_json::Value::String(pg_channel),
                        serde_json::Value::String(payload),
                    ],
                )
                .await?;

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
        self.listener.abort();
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
        let connect_result = tokio::time::timeout(
            Duration::from_secs(5),
            tokio_postgres::connect(&dsn, tls_connector()),
        )
        .await
        .map_err(|_| "listener connection deadline exceeded".to_string())
        .and_then(|result| result.map_err(|e| e.to_string()));

        let (client, connection) = match connect_result {
            Ok(pair) => {
                attempt = 0;
                tracing::info!("pub/sub listener connected");
                pair
            }
            Err(e) => {
                attempt = attempt.saturating_add(1);
                let delay =
                    backoff_delay(attempt, config.reconnect_base_ms, config.reconnect_max_ms);
                tracing::warn!(
                    error = %e,
                    attempt,
                    delay_ms = delay.as_millis(),
                    "pub/sub listener connection failed, retrying"
                );
                tokio::time::sleep(delay).await;

                continue;
            }
        };

        // Drive notifications directly into the bounded broadcast ring. A
        // slow subscriber cannot accumulate an unbounded intermediate queue.
        // JoinSet aborts the driver even when this listener task is cancelled.
        let mut conn_tasks = tokio::task::JoinSet::new();
        conn_tasks.spawn(drive_connection(
            connection,
            config.clone(),
            notify_tx.clone(),
        ));

        // Re-issue LISTEN for all active channels. The connection is now being
        // polled by `conn_task`, so batch_execute makes progress.
        let channels: Vec<String> = active.lock().await.iter().cloned().collect();
        let mut listen_failed = false;
        for channel in &channels {
            let pg_ch = config.pg_channel(channel);
            let query = format!("LISTEN \"{}\"", escape_ident_inner(&pg_ch));
            if let Err(e) = listener_command(&client, &query).await {
                tracing::error!(channel = %channel, error = %e, "LISTEN failed on reconnect");
                listen_failed = true;
                break;
            }
        }

        if listen_failed {
            conn_tasks.abort_all();
            attempt = attempt.saturating_add(1);
            let delay = backoff_delay(attempt, config.reconnect_base_ms, config.reconnect_max_ms);
            tokio::time::sleep(delay).await;
            continue;
        }

        // Run the event loop on this connection
        let should_shutdown =
            connection_event_loop(&client, &mut conn_tasks, &config, &mut cmd_rx).await;

        // Drop the client and stop driving the connection before reconnecting.
        drop(client);
        conn_tasks.abort_all();

        if should_shutdown {
            tracing::info!("pub/sub listener shutting down");
            return;
        }

        // Connection lost — backoff and retry
        attempt = attempt.saturating_add(1);
        let delay = backoff_delay(attempt, config.reconnect_base_ms, config.reconnect_max_ms);
        tracing::info!(
            attempt,
            delay_ms = delay.as_millis(),
            "pub/sub reconnecting"
        );
        tokio::time::sleep(delay).await;
    }
}

/// Drive network progress without any unbounded intermediate message queue.
async fn drive_connection(
    mut connection: Connection<Socket, <MakeRustlsConnect as MakeTlsConnect<Socket>>::Stream>,
    config: Arc<PubSubConfig>,
    notify_tx: broadcast::Sender<PubSubMessage>,
) {
    while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
        match message {
            Ok(AsyncMessage::Notification(n)) => {
                let _ = notify_tx.send(PubSubMessage {
                    channel: config
                        .strip_prefix(n.channel())
                        .unwrap_or(n.channel())
                        .to_string(),
                    payload: n.payload().to_string(),
                    timestamp: now_iso8601(),
                });
            }
            Err(error) => {
                tracing::warn!(%error, "pub/sub connection lost");
                return;
            }
            _ => {}
        }
    }
}

async fn listener_command(client: &tokio_postgres::Client, query: &str) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(5), client.batch_execute(query))
        .await
        .map_err(|_| "listener command deadline exceeded".to_string())?
        .map_err(|e| e.to_string())
}

/// Returns true on shutdown, false when the driver exits or a command fails.
async fn connection_event_loop(
    client: &tokio_postgres::Client,
    conn_tasks: &mut tokio::task::JoinSet<()>,
    config: &PubSubConfig,
    cmd_rx: &mut mpsc::Receiver<PubSubCmd>,
) -> bool {
    loop {
        tokio::select! {
            _ = conn_tasks.join_next() => return false,
            command = cmd_rx.recv() => {
                let (verb, channel) = match command {
                    Some(PubSubCmd::Listen(channel)) => ("LISTEN", channel),
                    Some(PubSubCmd::Unlisten(channel)) => ("UNLISTEN", channel),
                    None => return true,
                };
                let pg_channel = config.pg_channel(&channel);
                let query = format!("{verb} \"{}\"", escape_ident_inner(&pg_channel));
                if let Err(error) = listener_command(client, &query).await {
                    tracing::warn!(%error, %verb, "pub/sub listener command failed");
                    return false;
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
            assert!(
                d <= base + base / 5 + 1,
                "delay {d} exceeds base+20% {base}"
            );
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
