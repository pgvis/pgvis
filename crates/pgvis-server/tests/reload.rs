//! Schema-cache reload: NOTIFY pgrst, explicit reload, data-cache invalidation.

mod common;

use std::time::{Duration, Instant};

use common::{free_port, test_dsn};
use reqwest::StatusCode;
use serde_json::Value;

async fn psql(sql: &str) {
    let (client, conn) = tokio_postgres::connect(&test_dsn(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    client.batch_execute(sql).await.unwrap();
}

/// Start a server exposing only `test_reload`, with list caching on.
async fn start() -> (String, pgvis_lib::SchemaReloader) {
    psql(
        "DROP SCHEMA IF EXISTS test_reload CASCADE;
         CREATE SCHEMA test_reload;
         CREATE TABLE test_reload.t1 (id int PRIMARY KEY);
         INSERT INTO test_reload.t1 VALUES (1);",
    )
    .await;
    let mut config = pgvis_core::Config {
        schemas: vec!["test_reload".to_string()],
        ..Default::default()
    };
    config.cache.enabled = true;
    config.cache.cache_lists = true;
    let components = pgvis_lib::Builder::new(test_dsn())
        .config(config)
        .build_components()
        .await
        .unwrap();
    let port = free_port();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let router = components.router;
    tokio::spawn(async move { axum::serve(listener, router).await.ok() });
    (format!("http://127.0.0.1:{port}"), components.reloader)
}

/// Poll until `check` holds or 10 s pass.
async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn schema_changes_are_picked_up_without_a_restart() {
    let (base, reloader) = start().await;
    let get = |path: &str| reqwest::get(format!("{base}/api/test_reload/{path}"));

    // A table created after startup is unknown...
    psql("CREATE TABLE test_reload.t2 (id int)").await;
    assert_eq!(get("t2").await.unwrap().status(), StatusCode::NOT_FOUND);
    // ...until the database announces the change.
    psql("NOTIFY pgrst, 'reload schema'").await;
    eventually("t2 routable after NOTIFY", async || {
        get("t2").await.unwrap().status() == StatusCode::OK
    })
    .await;

    // An explicit reload takes effect when it returns.
    psql("CREATE TABLE test_reload.t3 (id int)").await;
    reloader.reload_now().await.unwrap();
    assert_eq!(get("t3").await.unwrap().status(), StatusCode::OK);

    // A cached list response is not served against the new schema.
    let first: Value = get("t1").await.unwrap().json().await.unwrap();
    assert_eq!(first, serde_json::json!([{ "id": 1 }]));
    psql("ALTER TABLE test_reload.t1 ADD COLUMN note text DEFAULT 'new'").await;
    psql("NOTIFY pgrst, 'reload schema'").await;
    eventually("cached t1 refreshed with the new column", async || {
        let body: Value = get("t1").await.unwrap().json().await.unwrap();
        body[0]["note"] == "new"
    })
    .await;

    // Unrelated payloads don't trigger a reload.
    psql("CREATE TABLE test_reload.t4 (id int); NOTIFY pgrst, 'reload config'").await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(get("t4").await.unwrap().status(), StatusCode::NOT_FOUND);

    psql("DROP SCHEMA test_reload CASCADE").await;
}
