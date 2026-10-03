//! Integration tests for pub/sub authorization: the configured authorize
//! function decides per caller role/claims, publish runs as the caller, MCP
//! pub/sub tools hit the auth guard, and subscriber caps hold under concurrency.

mod common;

use std::time::Duration;

use common::{free_port, setup_test_db, test_dsn};
use pgvis_core::Config;
use reqwest::{Client, Response, StatusCode};
use serde_json::{Value, json};

const SECRET: &str = "pubsub-test-secret-at-least-32-bytes-long";

/// Base config: JWT on, pub/sub on, decisions by `test_pubsub.authorize`.
fn config() -> Config {
    let mut config = Config {
        schemas: vec!["test".to_string()],
        jwt_secret: Some(SECRET.to_string()),
        anon_role: Some("pgvis_test_anon".to_string()),
        ..Config::default()
    };
    config.pubsub.enabled = true;
    config.pubsub.authorize_function = Some("test_pubsub.authorize".to_string());
    config
}

/// Start an in-process server (with MCP over HTTP at `/mcp`); returns its base URL.
async fn start(config: Config) -> String {
    setup_test_db(&test_dsn()).await;
    let router = pgvis_lib::Builder::new(test_dsn())
        .config(config)
        .with_mcp_http()
        .build()
        .await
        .expect("failed to build pgvis router");
    let port = free_port();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("failed to bind");
    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    format!("http://127.0.0.1:{port}")
}

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

/// An HS256 token for `pgvis_test_user` with the given `sub`.
fn token(sub: &str) -> String {
    let claims = json!({"role": "pgvis_test_user", "sub": sub, "exp": 4_000_000_000u64});
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

/// A per-test channel suffix so parallel tests never share a channel.
fn unique(name: &str) -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(0);
    format!(
        "{name}{}x{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

async fn subscribe(base: &str, channel: &str, sub: Option<&str>) -> Response {
    let mut req = client().get(format!("{base}/pubsub/{channel}"));
    if let Some(sub) = sub {
        req = req.bearer_auth(token(sub));
    }
    req.send().await.expect("subscribe request failed")
}

async fn publish(base: &str, channel: &str, sub: Option<&str>, payload: &str) -> Response {
    let mut req = client()
        .post(format!("{base}/pubsub/{channel}"))
        .body(payload.to_string());
    if let Some(sub) = sub {
        req = req.bearer_auth(token(sub));
    }
    req.send().await.expect("publish request failed")
}

/// Read SSE chunks until one carries `needle` (or `wait` elapses).
async fn wait_for_event(resp: &mut Response, needle: &str, wait: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + wait;
    let mut seen = String::new();
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, resp.chunk()).await {
            Ok(Ok(Some(chunk))) => {
                seen.push_str(&String::from_utf8_lossy(&chunk));
                if seen.contains(needle) {
                    return true;
                }
            }
            _ => return false,
        }
    }
    false
}

/// `(op, role, sub)` rows the authorize function recorded for `channel`.
async fn audit(channel: &str) -> Vec<(String, String, Option<String>)> {
    let (db, conn) = tokio_postgres::connect(&test_dsn(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    db.query(
        "SELECT op, role::text, sub FROM test_pubsub.audit WHERE channel = $1",
        &[&channel],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2)))
    .collect()
}

#[tokio::test]
async fn authorize_function_decides_subscribe_and_publish() {
    let base = start(config()).await;
    let own = format!("private.alice.{}", unique("c"));

    // Allowed: alice on her own private channel, end to end.
    let mut sse = subscribe(&base, &own, Some("alice")).await;
    assert_eq!(sse.status(), StatusCode::OK);
    // LISTEN is issued asynchronously after subscribe returns, so republish
    // until the first message arrives.
    let mut delivered = false;
    for _ in 0..20 {
        let resp = publish(&base, &own, Some("alice"), "hello-alice").await;
        assert_eq!(resp.status(), StatusCode::OK);
        if wait_for_event(&mut sse, "hello-alice", Duration::from_millis(250)).await {
            delivered = true;
            break;
        }
    }
    assert!(delivered, "message not delivered");

    // Denied (false): bob may neither read nor write alice's channel.
    assert_eq!(
        subscribe(&base, &own, Some("bob")).await.status(),
        StatusCode::FORBIDDEN
    );
    let resp = publish(&base, &own, Some("bob"), "spoofed").await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "PGVIS_PUBSUB_CHANNEL_DENIED");

    // Denied (NULL and raised error).
    let other = format!("other.{}", unique("c"));
    assert_eq!(
        subscribe(&base, &other, Some("alice")).await.status(),
        StatusCode::FORBIDDEN
    );
    let raise = format!("raise.{}", unique("c"));
    assert_eq!(
        subscribe(&base, &raise, Some("alice")).await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        publish(&base, &raise, Some("alice"), "x").await.status(),
        StatusCode::FORBIDDEN
    );

    // Anonymous callers run as anon_role: may subscribe to public.*, not publish.
    let public = format!("public.{}", unique("c"));
    assert_eq!(
        subscribe(&base, &public, None).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        publish(&base, &public, None, "x").await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        publish(&base, &public, Some("alice"), "x").await.status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn publish_runs_as_the_callers_role_and_claims() {
    let base = start(config()).await;
    let channel = format!("private.carol.{}", unique("c"));

    assert_eq!(
        publish(&base, &channel, Some("carol"), "x").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        publish(&base, &channel, None, "x").await.status(),
        StatusCode::FORBIDDEN
    );

    // The check (in the same statement as pg_notify) ran as the caller, not
    // the DSN superuser.
    let rows = audit(&channel).await;
    assert!(
        rows.contains(&(
            "publish".into(),
            "pgvis_test_user".into(),
            Some("carol".into())
        )),
        "{rows:?}"
    );
    assert!(
        rows.contains(&("publish".into(), "pgvis_test_anon".into(), None)),
        "{rows:?}"
    );
}

#[tokio::test]
async fn publish_without_authorize_function_still_runs_as_caller() {
    let mut config = config();
    config.pubsub.authorize_function = None;
    // A role that does not exist: if publish ran as the pool's DSN role it
    // would succeed; as the caller it must fail.
    config.anon_role = Some("pgvis_test_missing_role".to_string());
    let base = start(config).await;
    let channel = format!("any.{}", unique("c"));

    assert_eq!(
        publish(&base, &channel, Some("dave"), "x").await.status(),
        StatusCode::OK
    );
    assert!(
        !publish(&base, &channel, None, "x")
            .await
            .status()
            .is_success()
    );
}

/// Minimal MCP Streamable HTTP client: initialize, then call one tool.
async fn mcp_call(base: &str, tool: &str, arguments: Value) -> Value {
    let http = client();
    let post = |body: Value, session: Option<String>| {
        let mut req = http
            .post(format!("{base}/mcp"))
            .header("accept", "application/json, text/event-stream")
            .json(&body);
        if let Some(s) = session {
            req = req.header("mcp-session-id", s);
        }
        req.send()
    };
    let init = post(
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "pubsub-test", "version": "0"}
        }}),
        None,
    )
    .await
    .unwrap();
    assert!(init.status().is_success(), "initialize: {}", init.status());
    let session = init
        .headers()
        .get("mcp-session-id")
        .map(|v| v.to_str().unwrap().to_string());
    init.text().await.unwrap();
    post(
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        session.clone(),
    )
    .await
    .unwrap();
    let resp = post(
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
               "params": {"name": tool, "arguments": arguments}}),
        session,
    )
    .await
    .unwrap();
    let text = resp.text().await.unwrap();
    text.lines()
        .filter_map(|l| l.strip_prefix("data:"))
        .filter_map(|d| serde_json::from_str::<Value>(d.trim()).ok())
        .find(|v| v["id"] == 2)
        .unwrap_or_else(|| panic!("no tools/call response in {text}"))
}

#[tokio::test]
async fn mcp_pubsub_tools_hit_the_auth_guard() {
    // Auth required and no anon role: the pub/sub tools must refuse like every
    // other tool, before reaching the database.
    let mut config = config();
    config.anon_role = None;
    let base = start(config).await;
    let channel = format!("public.{}", unique("c"));

    for (tool, args) in [
        (
            "pubsub_publish",
            json!({"channel": channel, "payload": "x"}),
        ),
        ("pubsub_channels", json!({})),
    ] {
        let resp = mcp_call(&base, tool, args).await;
        assert_eq!(resp["result"]["isError"], true, "{tool}: {resp}");
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("Anonymous access is disabled"),
            "{tool}: {text}"
        );
    }
    assert!(audit(&channel).await.is_empty());
}

#[tokio::test]
async fn mcp_publish_goes_through_the_authorize_function() {
    let base = start(config()).await;
    let channel = format!("public.{}", unique("c"));

    // MCP runs as anon_role, which the function does not let publish.
    let resp = mcp_call(
        &base,
        "pubsub_publish",
        json!({"channel": channel, "payload": "x"}),
    )
    .await;
    assert_eq!(resp["result"]["isError"], true, "{resp}");
    assert_eq!(
        audit(&channel).await,
        vec![("publish".into(), "pgvis_test_anon".into(), None)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscriber_caps_hold_under_concurrency() {
    let mut config = config();
    config.pubsub.max_subscribers = 5;
    config.pubsub.max_subscribers_per_identity = 2;
    config.pubsub.keepalive_interval_secs = 1;
    let base = start(config).await;
    let channel = format!("public.{}", unique("c"));

    // Per identity: 10 concurrent subscribes as one caller → exactly 2 slots.
    let attempts = (0..10).map(|_| {
        let base = base.clone();
        let channel = channel.clone();
        async move { subscribe(&base, &channel, Some("erin")).await }
    });
    let responses = futures_join_all(attempts).await;
    let (ok, rest): (Vec<_>, Vec<_>) = responses
        .into_iter()
        .partition(|r| r.status() == StatusCode::OK);
    assert_eq!(ok.len(), 2);
    assert!(
        rest.iter()
            .all(|r| r.status() == StatusCode::SERVICE_UNAVAILABLE)
    );

    // Global: 20 concurrent distinct callers → only the 3 remaining slots.
    let attempts = (0..20).map(|i| {
        let base = base.clone();
        let channel = channel.clone();
        async move { subscribe(&base, &channel, Some(&format!("user{i}"))).await }
    });
    let others = futures_join_all(attempts).await;
    let others_ok: Vec<_> = others
        .into_iter()
        .filter(|r| r.status() == StatusCode::OK)
        .collect();
    assert_eq!(others_ok.len(), 3);

    // Dropping a stream releases its global and per-identity slot.
    drop(ok);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let resp = subscribe(&base, &channel, Some("erin")).await;
        if resp.status() == StatusCode::OK {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "slot never released"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Run futures concurrently on the runtime and collect their outputs in order.
async fn futures_join_all<F>(futures: impl Iterator<Item = F>) -> Vec<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handles: Vec<_> = futures.map(tokio::spawn).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await.unwrap());
    }
    out
}
