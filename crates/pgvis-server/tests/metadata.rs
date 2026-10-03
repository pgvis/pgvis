//! Metadata endpoints: authentication and pool metrics.

mod common;

use common::{PgvisServer, setup_test_db, test_dsn};
use jsonwebtoken::{EncodingKey, Header, encode};
use reqwest::StatusCode;
use serde_json::{Value, json};

const SECRET: &str = "metadata-test-secret-metadata-test";

fn token(role: &str) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    encode(
        &Header::default(),
        &json!({ "role": role, "exp": exp }),
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

#[tokio::test]
async fn the_cache_endpoint_reports_pool_occupancy() {
    setup_test_db(&test_dsn()).await;
    let server = PgvisServer::start(&test_dsn(), "test").await;
    // Open at least one pooled connection.
    assert!(server.get("/api/test/items?limit=1").await.status().is_success());
    let body: Value = server.get("/pgvis/cache").await.json().await.unwrap();
    let pool = &body["pool"];
    assert!(pool["max_size"].as_u64().unwrap() > 0, "{body}");
    assert!(pool["size"].as_u64().unwrap() >= 1, "{body}");
    assert!(pool["waiting"].is_u64(), "{body}");
}

#[tokio::test]
async fn metadata_needs_auth_when_the_data_api_does() {
    setup_test_db(&test_dsn()).await;
    // JWT required and no anonymous role: the data API rejects anonymous
    // callers, so the schema listing, OpenAPI spec and cache stats must too.
    let config = pgvis_core::Config {
        schemas: vec!["test".to_string()],
        jwt_secret: Some(SECRET.to_string()),
        ..Default::default()
    };
    let server = PgvisServer::start_with_config(&test_dsn(), config).await;
    let client = reqwest::Client::new();
    for (path, accept) in [
        ("/api/", "application/json"),
        ("/api/", "application/openapi+json"),
        ("/pgvis/cache", "application/json"),
    ] {
        let url = format!("{}{path}", server.base_url);
        let anon = client.get(&url).header("accept", accept).send().await.unwrap();
        assert_eq!(anon.status(), StatusCode::UNAUTHORIZED, "{path} ({accept})");

        let user = std::env::var("USER").unwrap_or_else(|_| "postgres".into());
        let authed = client
            .get(&url)
            .header("accept", accept)
            .bearer_auth(token(&user))
            .send()
            .await
            .unwrap();
        assert_eq!(authed.status(), StatusCode::OK, "{path} ({accept})");
    }
}
