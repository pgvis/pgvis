//! # Pub/Sub primitives — general-purpose messaging via Postgres LISTEN/NOTIFY.
//!
//! This module defines the types and trait for a Redis-style pub/sub system
//! backed by Postgres `LISTEN/NOTIFY`. pgvis relays messages between database
//! notification channels and connected clients (REST SSE, MCP streaming,
//! embedded Rust subscribers).
//!
//! ## Architecture
//!
//! ```text
//! Publisher ──► pgvis (NOTIFY) ──► Postgres ──► pgvis (LISTEN) ──► Subscribers
//!                                     ↕
//!                            Other pgvis instances
//! ```
//!
//! Multiple pgvis instances on the same database form a shared pub/sub bus
//! automatically — Postgres itself is the message broker.
//!
//! ## Design
//!
//! - **Dedicated listener connection** — LISTEN state is per-session; a pooled
//!   connection would lose subscriptions on recycle. A single non-pooled
//!   connection handles all LISTENs with reconnect/backoff.
//! - **Dynamic channel tracking** — `LISTEN` is issued when the first subscriber
//!   appears; `UNLISTEN` when the last disconnects.
//! - **Channel namespacing** — all channels are prefixed (default: `pgvis:`) to
//!   avoid collision with application-level NOTIFY usage.
//! - **8 KB payload limit** — Postgres caps NOTIFY payloads at ~8000 bytes.
//!   Validated at publish time.

use std::pin::Pin;

use futures::Stream;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::error::Error;

// ---------------------------------------------------------------------------
// PubSubMessage — the message unit
// ---------------------------------------------------------------------------

/// A message received from or sent to a pub/sub channel.
///
/// This is the unit of communication. Publishers send payloads to a named
/// channel; subscribers receive `PubSubMessage` instances on their stream.
///
/// # Example
///
/// ```rust
/// use pgvis_core::pubsub::PubSubMessage;
///
/// let msg = PubSubMessage {
///     channel: "orders.new".to_string(),
///     payload: r#"{"id": 42, "total": 99.99}"#.to_string(),
///     timestamp: "2025-06-01T12:00:00Z".to_string(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PubSubMessage {
    /// The logical channel name (without the namespace prefix).
    ///
    /// This is the user-facing name; the actual Postgres channel is
    /// `{prefix}{channel}` (e.g. `pgvis:orders.new`).
    pub channel: String,

    /// The message payload (UTF-8 string, max ~8000 bytes).
    ///
    /// Can be any UTF-8 content: JSON, plain text, etc. Postgres NOTIFY
    /// payloads are limited to approximately 8000 bytes.
    pub payload: String,

    /// ISO 8601 UTC timestamp of when pgvis processed this message.
    ///
    /// For received messages: when the notification arrived at the listener.
    /// For published messages: when the NOTIFY was issued.
    pub timestamp: String,
}

// ---------------------------------------------------------------------------
// PubSubStream — the subscriber stream type
// ---------------------------------------------------------------------------

/// A stream of incoming pub/sub messages from the database.
///
/// Yields [`PubSubMessage`] instances as they arrive on listened channels.
/// The stream is `Send` so it can be consumed across tasks.
pub type PubSubStream = Pin<Box<dyn Stream<Item = PubSubMessage> + Send>>;

// ---------------------------------------------------------------------------
// PubSubBackend — the trait for database pub/sub support
// ---------------------------------------------------------------------------

/// Backend extension for pub/sub support.
///
/// This is a **separate trait** from [`Backend`](crate::Backend) because:
/// 1. Not all backends support pub/sub (SQLite doesn't have LISTEN/NOTIFY).
/// 2. Pub/sub requires a dedicated non-pooled connection with different
///    lifecycle management.
/// 3. It keeps the core `Backend` trait focused on query execution.
///
/// # Implementors
///
/// - `pgvis-postgres::PgPubSub` — full implementation via LISTEN/NOTIFY
/// - SQLite — not supported (returns appropriate errors)
///
/// # Object Safety
///
/// Like [`Backend`](crate::Backend), this trait is object-safe via [`BoxFuture`].
/// Consumers hold `Arc<dyn PubSubBackend>`.
pub trait PubSubBackend: Send + Sync + 'static {
    /// Start listening on a channel.
    ///
    /// Issues `LISTEN "{prefix}{channel}"` on the dedicated connection.
    /// Idempotent — calling listen on an already-listened channel is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener connection is down and reconnection
    /// has not yet succeeded.
    fn listen(&self, channel: &str) -> BoxFuture<'_, Result<(), Error>>;

    /// Stop listening on a channel.
    ///
    /// Issues `UNLISTEN "{prefix}{channel}"` on the dedicated connection.
    /// Idempotent — calling unlisten on a non-listened channel is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener connection is down.
    fn unlisten(&self, channel: &str) -> BoxFuture<'_, Result<(), Error>>;

    /// Publish a message to a channel.
    ///
    /// Issues `SELECT pg_notify($1, $2)` on a pooled connection.
    /// The message will be received by all instances listening on this channel
    /// (including this instance, if it's also subscribed).
    ///
    /// # Payload size
    ///
    /// The caller should validate payload size before calling. If the payload
    /// exceeds Postgres's limit (~8000 bytes), the database will return an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the publish query fails (connection error, payload
    /// too large, etc.).
    fn publish(&self, channel: &str, payload: &str) -> BoxFuture<'_, Result<(), Error>>;

    /// Get the notification stream for all listened channels.
    ///
    /// Returns a single stream that yields messages from **all** channels
    /// that have been registered via [`listen()`](Self::listen). The
    /// `PubSubMessage::channel` field identifies which channel each message
    /// belongs to.
    ///
    /// This should only be called once at startup. The stream lives for the
    /// lifetime of the pub/sub backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the dedicated listener connection cannot be
    /// established.
    fn notification_stream(&self) -> BoxFuture<'_, Result<PubSubStream, Error>>;

    /// Get the list of channels currently being listened to.
    fn active_channels(&self) -> BoxFuture<'_, Vec<String>>;
}

// ---------------------------------------------------------------------------
// PubSubConfig — configuration for the pub/sub subsystem
// ---------------------------------------------------------------------------

/// Configuration for the pub/sub subsystem.
///
/// Controls whether pub/sub is enabled, channel naming, payload limits,
/// subscriber capacity, and reconnection behaviour.
///
/// ## Example (TOML)
///
/// ```toml
/// [pubsub]
/// enabled = true
/// channel_prefix = "pgvis:"
/// max_payload_bytes = 7500
/// max_subscribers = 1000
/// channel_buffer_size = 64
/// reconnect_base_ms = 500
/// reconnect_max_ms = 30000
/// ```
///
/// ## Environment Variables
///
/// | Env Var | Field |
/// |---------|-------|
/// | `PGVIS_PUBSUB_ENABLED` | `enabled` |
/// | `PGVIS_PUBSUB_CHANNEL_PREFIX` | `channel_prefix` |
/// | `PGVIS_PUBSUB_MAX_PAYLOAD_BYTES` | `max_payload_bytes` |
/// | `PGVIS_PUBSUB_MAX_SUBSCRIBERS` | `max_subscribers` |
/// | `PGVIS_PUBSUB_CHANNEL_BUFFER_SIZE` | `channel_buffer_size` |
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubConfig {
    /// Master switch. When false, all pub/sub endpoints return 404/501.
    ///
    /// No listener connection is created, no background tasks are spawned.
    ///
    /// Default: `false`.
    #[serde(default)]
    pub enabled: bool,

    /// Prefix prepended to all channel names in Postgres LISTEN/NOTIFY.
    ///
    /// Prevents collision with application-level NOTIFY usage. The prefix
    /// is stripped from incoming notifications and prepended to outgoing ones.
    ///
    /// Example: with prefix `"pgvis:"`, publishing to channel `"orders.new"`
    /// issues `NOTIFY "pgvis:orders.new"`.
    ///
    /// Default: `"pgvis:"`.
    #[serde(default = "default_channel_prefix")]
    pub channel_prefix: String,

    /// Maximum payload size in bytes.
    ///
    /// Postgres NOTIFY payloads are limited to ~8000 bytes. This limit is
    /// checked at publish time, returning a clear error before hitting the
    /// database. Set lower than 8000 to leave room for encoding overhead.
    ///
    /// Default: `7500`.
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: usize,

    /// Maximum total concurrent subscribers across all channels.
    ///
    /// Once this limit is reached, new subscribe requests receive a 503
    /// (Service Unavailable). This prevents unbounded memory growth from
    /// broadcast channel buffers.
    ///
    /// Default: `1000`.
    #[serde(default = "default_max_subscribers")]
    pub max_subscribers: usize,

    /// Per-channel broadcast buffer size.
    ///
    /// Number of messages buffered for slow subscribers before they start
    /// lagging (missing messages). Higher values use more memory but tolerate
    /// slower consumers. A subscriber that falls behind receives a "lagged"
    /// notification.
    ///
    /// Default: `64`.
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,

    /// Allowed channel name patterns (glob-style).
    ///
    /// When non-empty, only channels matching at least one pattern can be
    /// published to or subscribed from. Empty list means all channels are
    /// allowed.
    ///
    /// Default: `[]` (allow all).
    #[serde(default)]
    pub allowed_channels: Vec<String>,

    /// Reconnection backoff base in milliseconds.
    ///
    /// When the dedicated listener connection drops, pgvis reconnects with
    /// exponential backoff: `base * 2^attempt` (capped at `reconnect_max_ms`).
    ///
    /// Default: `500`.
    #[serde(default = "default_reconnect_base_ms")]
    pub reconnect_base_ms: u64,

    /// Reconnection backoff maximum in milliseconds.
    ///
    /// Upper bound on the backoff delay between reconnection attempts.
    ///
    /// Default: `30000` (30 seconds).
    #[serde(default = "default_reconnect_max_ms")]
    pub reconnect_max_ms: u64,

    /// SSE keepalive interval in seconds.
    ///
    /// The SSE endpoint sends a `: keepalive` comment at this interval to
    /// prevent proxy/load-balancer timeouts on idle connections.
    ///
    /// Default: `15`.
    #[serde(default = "default_keepalive_interval_secs")]
    pub keepalive_interval_secs: u64,
}

impl Default for PubSubConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            channel_prefix: default_channel_prefix(),
            max_payload_bytes: default_max_payload_bytes(),
            max_subscribers: default_max_subscribers(),
            channel_buffer_size: default_channel_buffer_size(),
            allowed_channels: Vec::new(),
            reconnect_base_ms: default_reconnect_base_ms(),
            reconnect_max_ms: default_reconnect_max_ms(),
            keepalive_interval_secs: default_keepalive_interval_secs(),
        }
    }
}

impl PubSubConfig {
    /// Check whether a channel name is allowed by the configuration.
    ///
    /// Returns `true` if `allowed_channels` is empty (all allowed) or if
    /// the channel matches at least one pattern.
    ///
    /// Patterns use simple glob matching: `*` matches any sequence of chars.
    pub fn is_channel_allowed(&self, channel: &str) -> bool {
        if self.allowed_channels.is_empty() {
            return true;
        }
        self.allowed_channels
            .iter()
            .any(|pattern| glob_match(pattern, channel))
    }

    /// Get the full Postgres channel name (with prefix).
    pub fn pg_channel(&self, channel: &str) -> String {
        format!("{}{}", self.channel_prefix, channel)
    }

    /// Strip the prefix from a Postgres channel name, returning the logical name.
    ///
    /// Returns `None` if the channel doesn't start with the configured prefix.
    pub fn strip_prefix<'a>(&self, pg_channel: &'a str) -> Option<&'a str> {
        pg_channel.strip_prefix(&self.channel_prefix)
    }

    /// Validate a payload before publishing.
    ///
    /// Returns `Ok(())` if the payload is within size limits, or an error
    /// describing the violation.
    pub fn validate_payload(&self, payload: &str) -> Result<(), Error> {
        if payload.len() > self.max_payload_bytes {
            return Err(Error::PubSub {
                message: format!(
                    "payload size {} bytes exceeds maximum of {} bytes",
                    payload.len(),
                    self.max_payload_bytes
                ),
                code: PubSubErrorCode::PayloadTooLarge,
            });
        }
        Ok(())
    }

    /// Validate a channel name.
    ///
    /// Channel names must be non-empty, not contain null bytes, and be
    /// allowed by the configuration.
    pub fn validate_channel(&self, channel: &str) -> Result<(), Error> {
        if channel.is_empty() {
            return Err(Error::PubSub {
                message: "channel name must not be empty".to_string(),
                code: PubSubErrorCode::InvalidChannel,
            });
        }
        if channel.contains('\0') {
            return Err(Error::PubSub {
                message: "channel name must not contain null bytes".to_string(),
                code: PubSubErrorCode::InvalidChannel,
            });
        }
        if !self.is_channel_allowed(channel) {
            return Err(Error::PubSub {
                message: format!("channel '{channel}' is not in the allowed list"),
                code: PubSubErrorCode::ChannelDenied,
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PubSubErrorCode — structured error codes for pub/sub operations
// ---------------------------------------------------------------------------

/// Error codes specific to pub/sub operations.
///
/// Used in [`Error::PubSub`] to provide machine-readable error classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PubSubErrorCode {
    /// The message payload exceeds the configured maximum size.
    PayloadTooLarge,
    /// The channel name is not in the allowed list.
    ChannelDenied,
    /// The channel name is invalid (empty, contains null bytes, etc.).
    InvalidChannel,
    /// The maximum number of concurrent subscribers has been reached.
    MaxSubscribersExceeded,
    /// The pub/sub backend is not available (e.g. SQLite, or disabled).
    NotAvailable,
    /// The listener connection is down and reconnecting.
    ConnectionLost,
}

impl PubSubErrorCode {
    /// The PGVIS error code string for HTTP/MCP error responses.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PayloadTooLarge => "PGVIS_PUBSUB_PAYLOAD_TOO_LARGE",
            Self::ChannelDenied => "PGVIS_PUBSUB_CHANNEL_DENIED",
            Self::InvalidChannel => "PGVIS_PUBSUB_INVALID_CHANNEL",
            Self::MaxSubscribersExceeded => "PGVIS_PUBSUB_MAX_SUBSCRIBERS",
            Self::NotAvailable => "PGVIS_PUBSUB_NOT_AVAILABLE",
            Self::ConnectionLost => "PGVIS_PUBSUB_CONNECTION_LOST",
        }
    }

    /// HTTP status code for this error.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::PayloadTooLarge => 400,
            Self::ChannelDenied => 403,
            Self::InvalidChannel => 400,
            Self::MaxSubscribersExceeded => 503,
            Self::NotAvailable => 501,
            Self::ConnectionLost => 503,
        }
    }
}

// ---------------------------------------------------------------------------
// Default value helpers
// ---------------------------------------------------------------------------

fn default_channel_prefix() -> String {
    "pgvis:".to_string()
}

fn default_max_payload_bytes() -> usize {
    7500
}

fn default_max_subscribers() -> usize {
    1000
}

fn default_channel_buffer_size() -> usize {
    64
}

fn default_reconnect_base_ms() -> u64 {
    500
}

fn default_reconnect_max_ms() -> u64 {
    30_000
}

fn default_keepalive_interval_secs() -> u64 {
    15
}

// ---------------------------------------------------------------------------
// Glob matching helper
// ---------------------------------------------------------------------------

/// Simple glob matching: `*` matches any sequence of characters.
///
/// This is intentionally minimal — no `?`, no `[...]` character classes.
/// Sufficient for channel allowlists like `"orders.*"`, `"chat.*"`.
fn glob_match(pattern: &str, input: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();

    if parts.len() == 1 {
        // No wildcard — exact match
        return pattern == input;
    }

    let mut pos = 0;

    // First part must be a prefix
    if let Some(first) = parts.first() {
        if !first.is_empty() {
            if !input.starts_with(*first) {
                return false;
            }
            pos = first.len();
        }
    }

    // Last part must be a suffix
    if let Some(last) = parts.last() {
        if !last.is_empty() && !input[pos..].ends_with(*last) {
            return false;
        }
    }

    // Middle parts must appear in order
    for part in &parts[1..parts.len().saturating_sub(1)] {
        if part.is_empty() {
            continue;
        }
        if let Some(idx) = input[pos..].find(*part) {
            pos += idx + part.len();
        } else {
            return false;
        }
    }

    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = PubSubConfig::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.channel_prefix, "pgvis:");
        assert_eq!(cfg.max_payload_bytes, 7500);
        assert_eq!(cfg.max_subscribers, 1000);
        assert_eq!(cfg.channel_buffer_size, 64);
        assert!(cfg.allowed_channels.is_empty());
    }

    #[test]
    fn test_pg_channel_name() {
        let cfg = PubSubConfig::default();
        assert_eq!(cfg.pg_channel("orders.new"), "pgvis:orders.new");
        assert_eq!(cfg.pg_channel("chat.room1"), "pgvis:chat.room1");
    }

    #[test]
    fn test_strip_prefix() {
        let cfg = PubSubConfig::default();
        assert_eq!(cfg.strip_prefix("pgvis:orders.new"), Some("orders.new"));
        assert_eq!(cfg.strip_prefix("other:foo"), None);
        assert_eq!(cfg.strip_prefix("pgvis:"), Some(""));
    }

    #[test]
    fn test_channel_allowed_empty_list() {
        let cfg = PubSubConfig::default();
        assert!(cfg.is_channel_allowed("anything"));
        assert!(cfg.is_channel_allowed("orders.new"));
    }

    #[test]
    fn test_channel_allowed_exact() {
        let cfg = PubSubConfig {
            allowed_channels: vec!["orders.new".to_string()],
            ..Default::default()
        };
        assert!(cfg.is_channel_allowed("orders.new"));
        assert!(!cfg.is_channel_allowed("orders.old"));
    }

    #[test]
    fn test_channel_allowed_glob() {
        let cfg = PubSubConfig {
            allowed_channels: vec!["orders.*".to_string(), "chat.*".to_string()],
            ..Default::default()
        };
        assert!(cfg.is_channel_allowed("orders.new"));
        assert!(cfg.is_channel_allowed("orders.cancelled"));
        assert!(cfg.is_channel_allowed("chat.room1"));
        assert!(!cfg.is_channel_allowed("admin.secret"));
    }

    #[test]
    fn test_validate_payload_ok() {
        let cfg = PubSubConfig::default();
        let payload = "x".repeat(7500);
        assert!(cfg.validate_payload(&payload).is_ok());
    }

    #[test]
    fn test_validate_payload_too_large() {
        let cfg = PubSubConfig::default();
        let payload = "x".repeat(7501);
        let err = cfg.validate_payload(&payload).unwrap_err();
        assert!(matches!(err, Error::PubSub { code: PubSubErrorCode::PayloadTooLarge, .. }));
    }

    #[test]
    fn test_validate_channel_empty() {
        let cfg = PubSubConfig::default();
        let err = cfg.validate_channel("").unwrap_err();
        assert!(matches!(err, Error::PubSub { code: PubSubErrorCode::InvalidChannel, .. }));
    }

    #[test]
    fn test_validate_channel_null_byte() {
        let cfg = PubSubConfig::default();
        let err = cfg.validate_channel("foo\0bar").unwrap_err();
        assert!(matches!(err, Error::PubSub { code: PubSubErrorCode::InvalidChannel, .. }));
    }

    #[test]
    fn test_validate_channel_denied() {
        let cfg = PubSubConfig {
            allowed_channels: vec!["orders.*".to_string()],
            ..Default::default()
        };
        let err = cfg.validate_channel("admin.secret").unwrap_err();
        assert!(matches!(err, Error::PubSub { code: PubSubErrorCode::ChannelDenied, .. }));
    }

    #[test]
    fn test_validate_channel_ok() {
        let cfg = PubSubConfig {
            allowed_channels: vec!["orders.*".to_string()],
            ..Default::default()
        };
        assert!(cfg.validate_channel("orders.new").is_ok());
    }

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
    }

    #[test]
    fn test_glob_match_star_suffix() {
        assert!(glob_match("orders.*", "orders.new"));
        assert!(glob_match("orders.*", "orders."));
        assert!(!glob_match("orders.*", "chat.room"));
    }

    #[test]
    fn test_glob_match_star_prefix() {
        assert!(glob_match("*.json", "data.json"));
        assert!(glob_match("*.json", ".json"));
        assert!(!glob_match("*.json", "data.xml"));
    }

    #[test]
    fn test_glob_match_star_middle() {
        assert!(glob_match("a*c", "abc"));
        assert!(glob_match("a*c", "aXXXc"));
        assert!(!glob_match("a*c", "aXXXd"));
    }

    #[test]
    fn test_glob_match_only_star() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
    }

    #[test]
    fn test_pubsub_error_code_str() {
        assert_eq!(PubSubErrorCode::PayloadTooLarge.as_str(), "PGVIS_PUBSUB_PAYLOAD_TOO_LARGE");
        assert_eq!(PubSubErrorCode::ChannelDenied.as_str(), "PGVIS_PUBSUB_CHANNEL_DENIED");
        assert_eq!(PubSubErrorCode::NotAvailable.http_status(), 501);
        assert_eq!(PubSubErrorCode::MaxSubscribersExceeded.http_status(), 503);
    }
}
