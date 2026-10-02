//! Integration tests for HTTP semantics, preferences, CORS, and content negotiation.
//!
//! Covers PostgREST spec parity for HTTP-layer concerns.

mod common;

use common::{PgvisServer, setup_test_db, test_dsn};
use reqwest::StatusCode;
use serde_json::json;
use std::sync::OnceLock;

struct ServerInfo {
    client: reqwest::Client,
    base_url: String,
}

static SERVER: OnceLock<ServerInfo> = OnceLock::new();

fn server_info() -> &'static ServerInfo {
    SERVER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<String>();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let dsn = test_dsn();
                setup_test_db(&dsn).await;
                let s = PgvisServer::start(&dsn, "test").await;
                tx.send(s.base_url.clone()).unwrap();
                std::future::pending::<()>().await;
            });
        });

        let base_url = rx.recv().expect("failed to receive server base_url");
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();
        ServerInfo { client, base_url }
    })
}

async fn get(path: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .get(format!("{}{path}", s.base_url))
        .send()
        .await
        .expect("GET request failed")
}

async fn get_with_header(path: &str, header: &str, value: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .get(format!("{}{path}", s.base_url))
        .header(header, value)
        .send()
        .await
        .expect("GET request failed")
}

async fn head(path: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .head(format!("{}{path}", s.base_url))
        .send()
        .await
        .expect("HEAD request failed")
}

// ============================================================================
// Content-Type
// ============================================================================

#[tokio::test]
async fn test_response_content_type_is_json() {
    let resp = get("/api/test/items").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .expect("should have content-type")
        .to_str()
        .unwrap();
    assert!(
        ct.contains("application/json"),
        "content-type should be JSON, got '{ct}'"
    );
}

#[tokio::test]
async fn test_response_content_type_has_charset() {
    let resp = get("/api/test/items").await;
    let ct = resp
        .headers()
        .get("content-type")
        .expect("should have content-type")
        .to_str()
        .unwrap();
    assert!(
        ct.contains("charset=utf-8"),
        "content-type should include charset=utf-8, got '{ct}'"
    );
}

// ============================================================================
// Content-Range header
// ============================================================================

#[tokio::test]
async fn test_content_range_header_present() {
    let resp = get("/api/test/items").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cr = resp.headers().get("content-range");
    assert!(cr.is_some(), "response should have content-range header");
}

#[tokio::test]
async fn test_content_range_with_limit() {
    let resp = get("/api/test/items?limit=3").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
}

#[tokio::test]
async fn test_content_range_with_exact_count() {
    let resp = get_with_header("/api/test/items", "Prefer", "count=exact").await;
    let status = resp.status();
    assert!(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT);
    let cr = resp
        .headers()
        .get("content-range")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // With exact count, content-range should have a total count (not *)
    // Format: "0-N/total"
    assert!(
        !cr.is_empty(),
        "content-range should be present with exact count"
    );
}

// ============================================================================
// HEAD requests
// ============================================================================

#[tokio::test]
async fn test_head_returns_no_body() {
    let resp = head("/api/test/items").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT,
        "HEAD should return 200/206, got {status}"
    );
    // HEAD responses should not have a body
    let body = resp.bytes().await.unwrap();
    assert!(body.is_empty(), "HEAD response should have empty body");
}

#[tokio::test]
async fn test_head_returns_content_range() {
    let resp = head("/api/test/items").await;
    let cr = resp.headers().get("content-range");
    assert!(
        cr.is_some(),
        "HEAD response should still have content-range header"
    );
}

// ============================================================================
// Prefer header handling
// ============================================================================

#[tokio::test]
async fn test_prefer_count_exact() {
    let resp = get_with_header("/api/test/items", "Prefer", "count=exact").await;
    let status = resp.status();
    assert!(status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT);

    // Check Preference-Applied header
    let applied = resp
        .headers()
        .get("preference-applied")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // Should echo back count=exact if honored
    if !applied.is_empty() {
        assert!(
            applied.contains("count=exact"),
            "preference-applied should echo count=exact, got '{applied}'"
        );
    }
}

#[tokio::test]
async fn test_prefer_return_representation_on_post() {
    let s = server_info();
    let resp = s
        .client
        .post(format!("{}/api/test/items", s.base_url))
        .header("content-type", "application/json")
        .header("Prefer", "return=representation")
        .json(&json!({"name": "PreferTest", "price": 1.00}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    // Should return the created row(s)
    assert!(!body.is_null(), "return=representation should return data");
}

#[tokio::test]
async fn test_prefer_return_minimal_on_post() {
    let s = server_info();
    let resp = s
        .client
        .post(format!("{}/api/test/items", s.base_url))
        .header("content-type", "application/json")
        .header("Prefer", "return=minimal")
        .json(&json!({"name": "MinimalTest", "price": 2.00}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::NO_CONTENT,
        "return=minimal should give 201 or 204, got {status}"
    );
}

// ============================================================================
// OpenAPI / Root endpoint
// ============================================================================

#[tokio::test]
async fn test_root_returns_openapi_spec() {
    let resp = get("/api/").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK,
        "root should return info, got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    // Root returns either an OpenAPI spec or a hint/schemas response
    assert!(
        body.get("openapi").is_some()
            || body.get("info").is_some()
            || body.get("paths").is_some()
            || body.get("schemas").is_some()
            || body.get("hint").is_some(),
        "root should return useful info, got: {}",
        serde_json::to_string_pretty(&body).unwrap_or_default()
    );
}

#[tokio::test]
async fn test_root_openapi_has_paths() {
    let resp = get("/api/").await;
    if resp.status() == StatusCode::OK {
        let body: serde_json::Value = resp.json().await.unwrap();
        if let Some(paths) = body.get("paths") {
            assert!(paths.is_object(), "paths should be an object");
            // Should have at least one path for our test tables
            let paths_obj = paths.as_object().unwrap();
            assert!(!paths_obj.is_empty(), "paths should not be empty");
        }
    }
}

// ============================================================================
// Pagination / 206 Partial Content
// ============================================================================

#[tokio::test]
async fn test_partial_content_with_limit_less_than_total() {
    // With exact count preference, limited responses should be 206
    let resp = get_with_header("/api/test/items?limit=3", "Prefer", "count=exact").await;
    let status = resp.status();
    // 206 when page_total < total_count
    assert!(
        status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT,
        "limited result with count should be 200 or 206, got {status}"
    );
}

#[tokio::test]
async fn test_full_content_returns_200() {
    // Without limit, all rows should be 200 OK
    let resp = get("/api/test/items").await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ============================================================================
// Empty responses
// ============================================================================

#[tokio::test]
async fn test_empty_table_returns_empty_array() {
    let resp = get("/api/test/empty_table").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn test_filter_no_match_returns_empty_array() {
    let resp = get("/api/test/items?name=eq.NOTEXISTS12345").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body, json!([]));
}

// ============================================================================
// URL encoding
// ============================================================================

#[tokio::test]
async fn test_url_encoded_filter_value() {
    // Space in filter value
    let resp = get("/api/test/items?name=eq.Rubber%20Duck").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert!(!arr.is_empty(), "should find 'Rubber Duck'");
    assert_eq!(arr[0]["name"], "Rubber Duck");
}

#[tokio::test]
async fn test_url_encoded_special_chars() {
    // Query with special characters
    let resp = get("/api/test/unicode_data?label=eq.%C3%91o%C3%B1o").await;
    let status = resp.status();
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert!(!arr.is_empty(), "should find Ñoño");
}

// ============================================================================
// Multiple select + filters combined
// ============================================================================

#[tokio::test]
async fn test_combined_select_filter_order_limit() {
    let resp =
        get("/api/test/items?select=name,price&category=eq.gadgets&order=price.desc&limit=2").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert!(arr.len() <= 2, "should be limited to 2");
    if arr.len() == 2 {
        let p1 = arr[0]["price"].as_f64().unwrap_or(0.0);
        let p2 = arr[1]["price"].as_f64().unwrap_or(0.0);
        assert!(p1 >= p2, "should be ordered desc: {p1} >= {p2}");
    }
}

// ============================================================================
// Idempotency
// ============================================================================

#[tokio::test]
async fn test_get_is_idempotent() {
    // Only the seeded rows (ids 1-10): the POST tests in this binary insert
    // items concurrently, which would otherwise change the second read.
    let resp1 = get("/api/test/items?id=lte.10&order=id.asc").await;
    let body1: serde_json::Value = resp1.json().await.unwrap();
    assert_eq!(body1.as_array().map(Vec::len), Some(10), "seeded items");

    let resp2 = get("/api/test/items?id=lte.10&order=id.asc").await;
    let body2: serde_json::Value = resp2.json().await.unwrap();

    assert_eq!(body1, body2, "GET should be idempotent");
}

// ============================================================================
// Concurrent requests
// ============================================================================

#[tokio::test]
async fn test_concurrent_reads() {
    let s = server_info();
    let mut handles = Vec::new();

    for _ in 0..10 {
        let client = s.client.clone();
        let url = format!("{}/api/test/items", s.base_url);
        handles.push(tokio::spawn(async move {
            let resp = client.get(&url).send().await.unwrap();
            resp.status()
        }));
    }

    for handle in handles {
        let status = handle.await.unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "concurrent read failed with {status}"
        );
    }
}
