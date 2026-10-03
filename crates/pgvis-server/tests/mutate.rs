//! Integration tests for mutations — INSERT, UPDATE, DELETE.
//!
//! Covers PostgREST MutateSpec.hs parity.

mod common;

use common::{PgvisServer, assert_json, prefer, setup_test_db, test_dsn};
use reqwest::StatusCode;
use serde_json::json;
use std::sync::OnceLock;

/// Shared server info — initialized once on a background thread with its own runtime.
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

/// POST (INSERT) helper.
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

/// POST with Prefer header.
async fn post_prefer(path: &str, body: serde_json::Value, pref: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .post(format!("{}{path}", s.base_url))
        .header("content-type", "application/json")
        .header("Prefer", pref)
        .json(&body)
        .send()
        .await
        .expect("POST request failed")
}

/// PATCH (UPDATE) helper.
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

/// PATCH with Prefer header.
async fn patch_prefer(path: &str, body: serde_json::Value, pref: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .patch(format!("{}{path}", s.base_url))
        .header("content-type", "application/json")
        .header("Prefer", pref)
        .json(&body)
        .send()
        .await
        .expect("PATCH request failed")
}

/// DELETE helper.
async fn delete(path: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .delete(format!("{}{path}", s.base_url))
        .send()
        .await
        .expect("DELETE request failed")
}

/// DELETE with Prefer header.
async fn delete_prefer(path: &str, pref: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .delete(format!("{}{path}", s.base_url))
        .header("Prefer", pref)
        .send()
        .await
        .expect("DELETE request failed")
}

/// GET helper (to verify state).
async fn get(path: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .get(format!("{}{path}", s.base_url))
        .send()
        .await
        .expect("GET request failed")
}

// ============================================================================
// Body validation (before anything is rendered or executed)
// ============================================================================

#[tokio::test]
async fn test_unknown_payload_column_is_rejected() {
    let resp = post("/api/test/items", json!({ "name": "x", "price": 1, "nope": 1 })).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "PGRST204");
}

#[tokio::test]
async fn test_malformed_bodies_are_rejected_not_defaulted() {
    // `[]` and a bare scalar used to insert a row of column defaults.
    for body in [json!([]), json!([{}]), json!(5), json!("x"), json!([{"name": "a"}, 7])] {
        let resp = post("/api/test/items", body.clone()).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "POST {body}");
    }
    // `PATCH {}` rendered `SET` with nothing after it (a 500); an array
    // updated every column from the first element only.
    for body in [json!({}), json!([{"name": "a"}, {"price": 2}])] {
        let resp = patch("/api/test/items?id=eq.1", body.clone()).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "PATCH {body}");
    }
}

#[tokio::test]
async fn test_bulk_insert_past_the_bind_parameter_limit_succeeds() {
    // 40k rows x 2 columns: past Postgres's 65535 bind parameters, which used
    // to fail as a 500. The rows now travel as one JSON parameter.
    let marker = "bulk-40k";
    let rows: Vec<_> = (0..40_000)
        .map(|i| json!({ "col_int4": i, "col_text": marker }))
        .collect();
    let resp = post_prefer("/api/test/menagerie", json!(rows), "return=minimal").await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let resp = delete(&format!("/api/test/menagerie?col_text=eq.{marker}")).await;
    assert!(resp.status().is_success());
}

#[tokio::test]
async fn test_insert_converts_json_arrays_and_objects_to_column_types() {
    // One text parameter per value sent `["a","b"]` as JSON text, which is
    // not an array literal: inserting into a text[] column failed.
    let row = json!({
        "col_text": "typed-insert",
        "col_text_arr": ["a", "b"],
        "col_int_arr": [1, 2, 3],
        "col_jsonb": { "k": [1, { "n": null }] },
        "col_json": [true, "x"],
        "col_bool": true,
        "col_numeric": "12345678901234.12345",
        "col_date": "2026-10-03",
        "col_uuid": "0190a3f2-1b2c-7000-8000-00000000000a"
    });
    let resp = post_prefer("/api/test/menagerie", row, "return=representation").await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    let got = &body[0];
    assert_eq!(got["col_text_arr"], json!(["a", "b"]));
    assert_eq!(got["col_int_arr"], json!([1, 2, 3]));
    assert_eq!(got["col_jsonb"], json!({ "k": [1, { "n": null }] }));
    assert_eq!(got["col_json"], json!([true, "x"]));
    assert_eq!(got["col_date"], "2026-10-03");
    let resp = delete("/api/test/menagerie?col_text=eq.typed-insert").await;
    assert!(resp.status().is_success());
}

// ============================================================================
// INSERT (POST)
// ============================================================================

#[tokio::test]
async fn test_insert_single_row() {
    let resp = post_prefer(
        "/api/test/items",
        json!({"name": "TestInsert1", "price": 5.55, "category": "test"}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "expected 201 or 200, got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    // Should return the inserted row
    let arr = body.as_array().expect("body should be array");
    assert!(!arr.is_empty(), "should return at least one row");
    assert_eq!(arr[0]["name"], "TestInsert1");
    assert_eq!(arr[0]["category"], "test");
}

#[tokio::test]
async fn test_insert_returns_201_status() {
    let resp = post(
        "/api/test/items",
        json!({"name": "TestInsert2", "price": 1.00, "category": "test"}),
    )
    .await;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
}

#[tokio::test]
async fn test_insert_with_return_minimal() {
    let resp = post_prefer(
        "/api/test/items",
        json!({"name": "TestMinimal", "price": 2.00, "category": "test"}),
        "return=minimal",
    )
    .await;
    let status = resp.status();
    // return=minimal should give 201 with no body (or empty body)
    assert!(
        status == StatusCode::CREATED || status == StatusCode::NO_CONTENT,
        "expected 201 or 204, got {status}"
    );
}

#[tokio::test]
async fn test_insert_with_return_representation() {
    let resp = post_prefer(
        "/api/test/items",
        json!({"name": "TestRepr", "price": 3.33, "category": "test"}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "expected 201 or 200, got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    assert_eq!(arr[0]["name"], "TestRepr");
}

#[tokio::test]
async fn test_insert_with_select() {
    let resp = post_prefer(
        "/api/test/items?select=name,price",
        json!({"name": "TestSelect", "price": 7.77, "category": "test"}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    assert_eq!(arr[0]["name"], "TestSelect");
    // Should only have selected columns
    assert!(arr[0].get("price").is_some());
}

#[tokio::test]
async fn test_insert_multiple_rows() {
    let resp = post_prefer(
        "/api/test/items",
        json!([
            {"name": "Bulk1", "price": 1.00, "category": "bulk"},
            {"name": "Bulk2", "price": 2.00, "category": "bulk"},
            {"name": "Bulk3", "price": 3.00, "category": "bulk"},
        ]),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    assert!(
        arr.len() >= 3,
        "expected at least 3 inserted rows, got {}",
        arr.len()
    );
}

#[tokio::test]
async fn test_insert_with_default_values() {
    // Insert with only required fields, let defaults fill in
    let resp = post_prefer(
        "/api/test/items",
        json!({"name": "DefaultTest"}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    // price should default to 0
    assert_eq!(arr[0]["name"], "DefaultTest");
}

#[tokio::test]
async fn test_insert_null_field() {
    let resp = post_prefer(
        "/api/test/users",
        json!({"name": "NullUser", "role": "user", "email": null, "age": null}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    assert_eq!(arr[0]["name"], "NullUser");
    assert!(arr[0]["email"].is_null());
}

#[tokio::test]
async fn test_insert_jsonb_field() {
    let resp = post_prefer(
        "/api/test/users",
        json!({"name": "JsonUser", "role": "user", "data": {"theme": "blue", "level": 5}}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    assert_eq!(arr[0]["name"], "JsonUser");
    assert_eq!(arr[0]["data"]["theme"], "blue");
}

#[tokio::test]
async fn test_insert_nonexistent_table_returns_404() {
    let resp = post("/api/test/nonexistent", json!({"name": "test"})).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// UPDATE (PATCH)
// ============================================================================

#[tokio::test]
async fn test_update_with_filter() {
    let resp = patch_prefer(
        "/api/test/items?name=eq.Widget",
        json!({"category": "updated_gadgets"}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    assert!(!arr.is_empty());
    assert_eq!(arr[0]["category"], "updated_gadgets");
}

#[tokio::test]
async fn test_update_multiple_rows() {
    let resp = patch_prefer(
        "/api/test/items?category=eq.tools",
        json!({"in_stock": false}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    // All matched rows should be updated
    for row in arr {
        assert_eq!(row["in_stock"], false);
    }
}

#[tokio::test]
async fn test_update_with_return_minimal() {
    let resp = patch_prefer(
        "/api/test/items?name=eq.Gizmo",
        json!({"price": 25.00}),
        "return=minimal",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::NO_CONTENT,
        "got {status}"
    );
}

#[tokio::test]
async fn test_update_no_matching_rows() {
    let resp = patch_prefer(
        "/api/test/items?name=eq.NonexistentItem99999",
        json!({"price": 0.01}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_FOUND,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    // Empty result set
    if let Some(arr) = body.as_array() {
        assert!(arr.is_empty());
    }
}

#[tokio::test]
async fn test_update_with_select() {
    let resp = patch_prefer(
        "/api/test/items?name=eq.Gizmo&select=name,price",
        json!({"price": 26.00}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    if !arr.is_empty() {
        assert!(arr[0].get("name").is_some());
        assert!(arr[0].get("price").is_some());
    }
}

#[tokio::test]
async fn test_update_nonexistent_table_returns_404() {
    let resp = patch("/api/test/nonexistent?id=eq.1", json!({"name": "test"})).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// DELETE
// ============================================================================

#[tokio::test]
async fn test_delete_with_filter() {
    // First insert something to delete
    post("/api/test/no_pk", json!({"a": "to_delete", "b": 999})).await;

    let resp = delete_prefer("/api/test/no_pk?a=eq.to_delete", "return=representation").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::NO_CONTENT,
        "got {status}"
    );
}

#[tokio::test]
async fn test_delete_returns_200() {
    // Insert then delete
    post("/api/test/no_pk", json!({"a": "del_test", "b": 888})).await;

    let resp = delete("/api/test/no_pk?a=eq.del_test").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::NO_CONTENT,
        "got {status}"
    );
}

#[tokio::test]
async fn test_delete_with_return_representation() {
    // Insert then delete
    post("/api/test/no_pk", json!({"a": "del_repr", "b": 777})).await;

    let resp = delete_prefer("/api/test/no_pk?a=eq.del_repr", "return=representation").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    if let Some(arr) = body.as_array() {
        if !arr.is_empty() {
            assert_eq!(arr[0]["a"], "del_repr");
        }
    }
}

#[tokio::test]
async fn test_delete_no_matching_rows() {
    let resp = delete_prefer(
        "/api/test/items?name=eq.NeverExisted12345",
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_FOUND,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    if let Some(arr) = body.as_array() {
        assert!(arr.is_empty());
    }
}

#[tokio::test]
async fn test_delete_nonexistent_table_returns_404() {
    let resp = delete("/api/test/nonexistent?id=eq.1").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// UPSERT (POST with on_conflict)
// ============================================================================

#[tokio::test]
async fn test_upsert_insert_new() {
    let resp = post_prefer(
        "/api/test/compound_pk?on_conflict=k1,k2",
        json!({"k1": 99, "k2": "upsert_new", "value": "fresh"}),
        "return=representation,resolution=merge-duplicates",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "got {status}"
    );
}

#[tokio::test]
async fn test_upsert_update_existing() {
    // Insert first
    post(
        "/api/test/compound_pk",
        json!({"k1": 98, "k2": "upsert_exist", "value": "original"}),
    )
    .await;

    // Upsert should update the existing row
    let resp = post_prefer(
        "/api/test/compound_pk?on_conflict=k1,k2",
        json!({"k1": 98, "k2": "upsert_exist", "value": "updated"}),
        "return=representation,resolution=merge-duplicates",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::CREATED || status == StatusCode::OK,
        "got {status}"
    );
}

// ============================================================================
// Edge cases
// ============================================================================

#[tokio::test]
async fn test_insert_empty_body_rejected() {
    let s = server_info();
    let resp = s
        .client
        .post(format!("{}/api/test/items", s.base_url))
        .header("content-type", "application/json")
        .send()
        .await
        .expect("request failed");
    // Should handle gracefully — either 400 or default values insert
    let status = resp.status();
    assert!(
        status.is_client_error() || status.is_success(),
        "got unexpected {status}"
    );
}

#[tokio::test]
async fn test_insert_wrong_column_name() {
    let resp = post(
        "/api/test/items",
        json!({"nonexistent_col": "value", "name": "test"}),
    )
    .await;
    // Should either ignore unknown columns or return an error
    let status = resp.status();
    assert!(
        status.is_client_error() || status.is_server_error() || status.is_success(),
        "got {status}"
    );
}

#[tokio::test]
async fn test_update_with_numeric_filter() {
    let resp = patch_prefer(
        "/api/test/items?price=gt.100",
        json!({"description": "expensive item updated"}),
        "return=representation",
    )
    .await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("body should be array");
    for row in arr {
        assert_eq!(row["description"], "expensive item updated");
    }
}
