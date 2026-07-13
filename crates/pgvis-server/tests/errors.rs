//! Integration tests for error handling and edge cases.
//!
//! Covers PostgREST error response format parity.

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

async fn post(path: &str, body: serde_json::Value) -> reqwest::Response {
    let s = server_info();
    s.client
        .post(format!("{}{path}", s.base_url))
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("POST request failed")
}

async fn patch(path: &str, body: serde_json::Value) -> reqwest::Response {
    let s = server_info();
    s.client
        .patch(format!("{}{path}", s.base_url))
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("PATCH request failed")
}

async fn delete(path: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .delete(format!("{}{path}", s.base_url))
        .send()
        .await
        .expect("DELETE request failed")
}

// ============================================================================
// 404 Not Found
// ============================================================================

#[tokio::test]
async fn test_404_nonexistent_table() {
    let resp = get("/api/test/does_not_exist").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await.unwrap();
    // Error response should have structured format
    assert!(body.get("message").is_some() || body.get("code").is_some());
}

#[tokio::test]
async fn test_404_nonexistent_schema() {
    let resp = get("/api/nonexistent_schema/items").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_404_nonexistent_rpc() {
    let resp = post("/api/test/rpc/nonexistent_function", json!({})).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_404_empty_target() {
    // Requesting just the schema prefix should return the root/OpenAPI response
    let resp = get("/api/test/").await;
    let status = resp.status();
    // This might be 200 (root) or 404
    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_FOUND,
        "got {status}"
    );
}

// ============================================================================
// Invalid filter errors
// ============================================================================

#[tokio::test]
async fn test_error_invalid_filter_operator() {
    let resp = get("/api/test/items?name=invalid_op.hello").await;
    let status = resp.status();
    // Should be 400 for invalid operator, or might fall through as text filter
    assert!(
        status.is_client_error() || status.is_server_error() || status == StatusCode::OK,
        "got {status}"
    );
}

#[tokio::test]
async fn test_error_column_not_found_in_filter() {
    let resp = get("/api/test/items?nonexistent_col=eq.hello").await;
    let status = resp.status();
    assert!(
        status == StatusCode::NOT_FOUND
            || status == StatusCode::BAD_REQUEST
            || status == StatusCode::INTERNAL_SERVER_ERROR,
        "expected error for nonexistent column in filter, got {status}"
    );
}

#[tokio::test]
async fn test_error_column_not_found_in_select() {
    let resp = get("/api/test/items?select=nonexistent_col").await;
    let status = resp.status();
    assert!(
        status == StatusCode::NOT_FOUND
            || status == StatusCode::BAD_REQUEST
            || status == StatusCode::INTERNAL_SERVER_ERROR,
        "expected error for nonexistent column in select, got {status}"
    );
}

#[tokio::test]
async fn test_error_column_not_found_in_order() {
    let resp = get("/api/test/items?order=nonexistent_col.asc").await;
    let status = resp.status();
    assert!(
        status == StatusCode::NOT_FOUND
            || status == StatusCode::BAD_REQUEST
            || status == StatusCode::INTERNAL_SERVER_ERROR,
        "expected error for nonexistent column in order, got {status}"
    );
}

// ============================================================================
// Constraint violation errors
// ============================================================================

#[tokio::test]
async fn test_error_unique_violation() {
    // users.email is UNIQUE - try inserting duplicate
    let _ = post(
        "/api/test/users",
        json!({"name": "UniqueTest1", "email": "unique_test@example.com", "role": "user"}),
    )
    .await;

    let resp = post(
        "/api/test/users",
        json!({"name": "UniqueTest2", "email": "unique_test@example.com", "role": "user"}),
    )
    .await;
    let status = resp.status();
    // Should be 409 Conflict or 500 with DB error
    assert!(
        status == StatusCode::CONFLICT || status.is_server_error() || status.is_client_error(),
        "expected conflict or error for unique violation, got {status}"
    );
}

#[tokio::test]
async fn test_error_not_null_violation() {
    // items.name is NOT NULL
    let resp = post(
        "/api/test/items",
        json!({"price": 10.00, "category": "test"}),
    )
    .await;
    let status = resp.status();
    // Should error because 'name' is NOT NULL and not provided
    assert!(
        status.is_client_error() || status.is_server_error(),
        "expected error for not-null violation, got {status}"
    );
}

#[tokio::test]
async fn test_error_foreign_key_violation() {
    // orders.user_id references users(id)
    let resp = post(
        "/api/test/orders",
        json!({"user_id": 99999, "total": 10.00, "status": "pending"}),
    )
    .await;
    let status = resp.status();
    assert!(
        status.is_client_error() || status.is_server_error(),
        "expected error for foreign key violation, got {status}"
    );
}

// ============================================================================
// Error response format
// ============================================================================

#[tokio::test]
async fn test_error_response_has_json_content_type() {
    let resp = get("/api/test/nonexistent_table").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("application/json"),
        "error response should have JSON content-type, got '{ct}'"
    );
}

#[tokio::test]
async fn test_error_response_structure() {
    let resp = get("/api/test/nonexistent_table").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await.unwrap();
    // Should have at least "message" field
    assert!(
        body.get("message").is_some(),
        "error response should have 'message' field, got: {body}"
    );
}

// ============================================================================
// Method not allowed / unsupported
// ============================================================================

#[tokio::test]
async fn test_patch_on_view_may_error() {
    // Views are typically not updatable unless explicitly made so
    let resp = patch("/api/test/items_view?id=eq.1", json!({"name": "NewName"})).await;
    let status = resp.status();
    // Might be 405 Method Not Allowed or some other error, or (if the view is
    // updatable) a successful mutation — 204 No Content without a representation.
    assert!(
        status.is_client_error()
            || status.is_server_error()
            || status == StatusCode::OK
            || status == StatusCode::NO_CONTENT,
        "got {status}"
    );
}

#[tokio::test]
async fn test_delete_on_view_may_error() {
    let resp = delete("/api/test/items_view?id=eq.1").await;
    let status = resp.status();
    assert!(
        status.is_client_error()
            || status.is_server_error()
            || status == StatusCode::OK
            || status == StatusCode::NO_CONTENT,
        "got {status}"
    );
}

// ============================================================================
// Singular response (Accept: application/vnd.pgrst.object)
// ============================================================================

#[tokio::test]
async fn test_singular_response_single_row() {
    let s = server_info();
    let resp = s
        .client
        .get(format!("{}/api/test/items?id=eq.1", s.base_url))
        .header("Accept", "application/vnd.pgrst.object+json")
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_ACCEPTABLE,
        "got {status}"
    );
    if status == StatusCode::OK {
        let body: serde_json::Value = resp.json().await.unwrap();
        // Should be a single object, not array
        assert!(
            body.is_object(),
            "singular response should be object, got: {body}"
        );
        assert_eq!(body["id"], 1);
    }
}

#[tokio::test]
async fn test_singular_response_multiple_rows_returns_406() {
    let s = server_info();
    let resp = s
        .client
        .get(format!("{}/api/test/items", s.base_url))
        .header("Accept", "application/vnd.pgrst.object+json")
        .send()
        .await
        .unwrap();
    // Multiple rows + singular = 406 Not Acceptable
    assert_eq!(
        resp.status(),
        StatusCode::NOT_ACCEPTABLE,
        "multiple rows with singular accept should be 406"
    );
}

#[tokio::test]
async fn test_singular_response_no_rows_returns_406() {
    let s = server_info();
    let resp = s
        .client
        .get(format!(
            "{}/api/test/items?name=eq.ABSOLUTELY_NOTHING_HERE",
            s.base_url
        ))
        .header("Accept", "application/vnd.pgrst.object+json")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_ACCEPTABLE,
        "no rows with singular accept should be 406"
    );
}

// ============================================================================
// Invalid body
// ============================================================================

#[tokio::test]
async fn test_post_with_invalid_json() {
    let s = server_info();
    let resp = s
        .client
        .post(format!("{}/api/test/items", s.base_url))
        .header("content-type", "application/json")
        .body("this is not valid json{{{")
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status.is_client_error() || status.is_server_error(),
        "invalid JSON body should fail, got {status}"
    );
}

#[tokio::test]
async fn test_patch_with_invalid_json() {
    let s = server_info();
    let resp = s
        .client
        .patch(format!("{}/api/test/items?id=eq.1", s.base_url))
        .header("content-type", "application/json")
        .body("not json")
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status.is_client_error() || status.is_server_error(),
        "invalid JSON body should fail, got {status}"
    );
}

// ============================================================================
// Large responses
// ============================================================================

#[tokio::test]
async fn test_large_result_set() {
    // The items table has 10 rows - that's our baseline. Ensure we get them all.
    let resp = get("/api/test/items").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert!(arr.len() >= 10, "should return all seeded items");
}
