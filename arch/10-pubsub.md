# 10 — Pub/Sub: General-Purpose Messaging via Postgres LISTEN/NOTIFY

## Overview

pgvis exposes a **Redis-style pub/sub** messaging bus backed by Postgres `LISTEN/NOTIFY`. Multiple pgvis instances on the same database form a shared message bus automatically — Postgres itself is the broker.

This is **not** related to database mutations or change-data-capture. It is an independent, general-purpose messaging primitive.

## Architecture

```text
Publisher ──► pgvis (NOTIFY) ──► Postgres ──► pgvis (LISTEN) ──► Subscribers
                                    ↕
                           Other pgvis instances
```

### In-process layers

```text
┌─────────────────────────────────────────────────────────────────┐
│                         PgPubSub                                 │
│  Dedicated LISTEN connection (non-pooled, reconnect/backoff)    │
│  Pooled connections for NOTIFY (fire-and-forget)                │
└──────────────────────────────┬──────────────────────────────────┘
                               │ broadcast::Sender<PubSubMessage>
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                        PubSubHub                                  │
│  Per-channel tokio::broadcast for local fan-out                  │
│  Dynamic LISTEN/UNLISTEN as subscribers join/leave               │
│  Subscriber count tracking (capped by config)                    │
└───────┬──────────────────────┬────────────────────────┬──────────┘
        │                      │                        │
   REST SSE             MCP tools               Embedded API
  GET /pubsub/{ch}   pubsub_publish         hub.subscribe("ch")
  POST /pubsub/{ch}  pubsub_channels        hub.publish("ch", payload)
```

## Design Principles

| Principle | Implementation |
|-----------|---------------|
| Single dedicated connection | LISTEN state is per-session; pooled connections would lose subscriptions on recycle |
| Dynamic channel tracking | LISTEN issued on first subscriber, UNLISTEN on last leaving |
| Channel namespacing | All channels prefixed (default `pgvis:`) to avoid collision with application LISTEN/NOTIFY |
| 8 KB payload limit | Validated at publish time before hitting the database |
| Cross-instance | All instances on the same DB auto-share messages via Postgres |
| Exponential backoff | Listener reconnects with `base * 2^attempt` (capped), with jitter |

## Core Types (`pgvis-core/src/pubsub.rs`)

| Type | Role |
|------|------|
| `PubSubMessage` | Channel + payload + timestamp |
| `PubSubConfig` | Enabled, prefix, limits, reconnect params |
| `PubSubBackend` trait | Object-safe async trait: listen, unlisten, publish, notification_stream |
| `PubSubStream` | `Pin<Box<dyn Stream<Item = PubSubMessage> + Send>>` |
| `PubSubErrorCode` | PayloadTooLarge, ChannelDenied, InvalidChannel, MaxSubscribersExceeded, NotAvailable, ConnectionLost |

## Postgres Implementation (`pgvis-postgres/src/pubsub.rs`)

`PgPubSub` spawns a background task (`listener_task`) that:

1. Connects to Postgres with a **non-pooled** connection
2. Re-issues LISTEN for all active channels after reconnection
3. Drives `Connection::poll_message()` in a `tokio::select!` loop
4. Forwards `AsyncMessage::Notification` into a `broadcast::Sender`
5. Processes `PubSubCmd` (Listen/Unlisten/Shutdown) from the hub

Publishing uses a **pooled** connection: `SELECT pg_notify($1, $2)`.

## Hub (`pgvis-router/src/pubsub.rs`)

`PubSubHub` is the in-process coordinator:

- Holds `HashMap<String, ChannelState>` mapping channel → broadcast sender + subscriber count
- `subscribe(channel)` → validates, increments count, issues LISTEN on first subscriber
- `unsubscribe(channel)` → decrements count, issues UNLISTEN on last subscriber
- `publish(channel, payload)` → validates, delegates to backend
- Capped at `max_subscribers` total

## Surfaces

### REST

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/pubsub` | GET | Status: active channels, subscriber counts |
| `/pubsub/{channel}` | GET | SSE stream (subscribe). Sends `data: {...}\n\n` events and `: keepalive` comments |
| `/pubsub/{channel}` | POST | Publish: body is the payload string. Returns `{"ok": true}` |

SSE format:
```
data: {"channel":"orders.new","payload":"{\"id\":42}","timestamp":"2025-06-01T12:00:00Z"}

: keepalive
```

### MCP

| Tool | Description |
|------|-------------|
| `pubsub_publish` | Publish a message (channel + payload args) |
| `pubsub_channels` | List active channels on this instance |

MCP does not support persistent subscriptions (no streaming). Clients use REST SSE for subscriptions.

### Embedded Rust

```rust
let hub: Arc<PubSubHub> = components.pubsub.unwrap();

// Publish
hub.publish("orders.new", r#"{"id": 42}"#).await?;

// Subscribe
let mut rx = hub.subscribe("orders.new").await?;
while let Ok(msg) = rx.recv().await {
    println!("{}: {}", msg.channel, msg.payload);
}
```

## Configuration

```toml
[pubsub]
enabled = true                    # Master switch (default: false)
channel_prefix = "pgvis:"        # Postgres channel prefix
max_payload_bytes = 7500          # Pre-validation (Postgres limit ~8000)
max_subscribers = 1000            # Global cap across all channels
channel_buffer_size = 64          # Per-channel broadcast buffer
allowed_channels = ["orders.*"]   # Glob allowlist (empty = all)
reconnect_base_ms = 500           # Exponential backoff base
reconnect_max_ms = 30000          # Backoff cap
keepalive_interval_secs = 15      # SSE keepalive comment interval
```

Environment variables: `PGVIS_PUBSUB_ENABLED`, `PGVIS_PUBSUB_CHANNEL_PREFIX`.

CLI flags: `--pubsub-enabled`, `--pubsub-channel-prefix`.

## Error Codes

| Code | HTTP | Meaning |
|------|------|---------|
| `PGVIS_PUBSUB_PAYLOAD_TOO_LARGE` | 400 | Payload exceeds max_payload_bytes |
| `PGVIS_PUBSUB_CHANNEL_DENIED` | 403 | Channel not in allowed_channels |
| `PGVIS_PUBSUB_INVALID_CHANNEL` | 400 | Empty name or null bytes |
| `PGVIS_PUBSUB_MAX_SUBSCRIBERS` | 503 | Subscriber cap reached |
| `PGVIS_PUBSUB_NOT_AVAILABLE` | 501 | Disabled or unsupported backend |
| `PGVIS_PUBSUB_CONNECTION_LOST` | 503 | Listener connection down |

## Limitations

- **8 KB payload** — Postgres NOTIFY hard limit. For larger messages, publish a pointer/URL.
- **No persistence** — messages are fire-and-forget. Disconnected subscribers miss messages.
- **No acknowledgement** — subscribers don't ACK; at-most-once delivery.
- **SQLite not supported** — pub/sub requires Postgres LISTEN/NOTIFY.
- **Subscriber lag** — slow consumers miss messages (broadcast buffer overflow). A `: lagged N messages` SSE comment is sent.

## File Map

| File | Role |
|------|------|
| `crates/pgvis-core/src/pubsub.rs` | Types, trait, config, validation |
| `crates/pgvis-core/src/error.rs` | `Error::PubSub` variant |
| `crates/pgvis-core/src/config.rs` | `Config.pubsub` field |
| `crates/pgvis-postgres/src/pubsub.rs` | `PgPubSub` implementation |
| `crates/pgvis-router/src/pubsub.rs` | `PubSubHub`, REST handlers, SSE |
| `crates/pgvis-mcp/src/server.rs` | MCP `pubsub_publish`/`pubsub_channels` tools |
| `crates/pgvis-lib/src/lib.rs` | Wiring: creates hub, mounts router |
| `crates/pgvis-server/src/main.rs` | CLI flags: `--pubsub-enabled`, `--pubsub-channel-prefix` |
