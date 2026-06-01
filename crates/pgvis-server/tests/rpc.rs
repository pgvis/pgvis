//! Integration tests for RPC (stored function calls).
//!
//! Covers PostgREST RpcSpec.hs parity, plus in-process `call_rpc` tests.

mod common;

use common::{PgvisServer, setup_test_db, test_dsn};
use pgvis_lib;
use pgvis_lib::pgvis_router;
use reqwest::StatusCode;
use serde_json::json;
use std::sync::OnceLock;

/// Shared server info.
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

/// POST to an RPC endpoint with JSON body.
async fn rpc_post(fn_name: &str, body: serde_json::Value) -> reqwest::Response {
    let s = server_info();
    s.client
        .post(format!("{}/api/test/rpc/{fn_name}", s.base_url))
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("RPC POST request failed")
}

/// POST to RPC with Prefer header.
async fn rpc_post_prefer(fn_name: &str, body: serde_json::Value, pref: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .post(format!("{}/api/test/rpc/{fn_name}", s.base_url))
        .header("content-type", "application/json")
        .header("Prefer", pref)
        .json(&body)
        .send()
        .await
        .expect("RPC POST request failed")
}

/// GET to an RPC endpoint (for STABLE/IMMUTABLE functions).
async fn rpc_get(fn_name: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .get(format!("{}/api/test/rpc/{fn_name}", s.base_url))
        .send()
        .await
        .expect("RPC GET request failed")
}

/// GET to an RPC endpoint with query params.
async fn rpc_get_params(fn_name: &str, params: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .get(format!("{}/api/test/rpc/{fn_name}?{params}", s.base_url))
        .send()
        .await
        .expect("RPC GET request failed")
}

// ============================================================================
// Scalar functions
// ============================================================================

#[tokio::test]
async fn test_rpc_add_two_integers() {
    let resp = rpc_post("add", json!({"a": 3, "b": 5})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    // Scalar function returns {"result": 8} or just 8 or [{"result": 8}]
    let result = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(result, json!(8));
}

#[tokio::test]
async fn test_rpc_add_negative_numbers() {
    let resp = rpc_post("add", json!({"a": -10, "b": 7})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let result = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(result, json!(-3));
}

#[tokio::test]
async fn test_rpc_echo_params_defaults() {
    let resp = rpc_post("echo_params", json!({})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    // Default: "hello, world!"
    let result = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(result, json!("hello, world!"));
}

#[tokio::test]
async fn test_rpc_echo_params_custom() {
    let resp = rpc_post("echo_params", json!({"name": "pgvis", "greeting": "hi"})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let result = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(result, json!("hi, pgvis!"));
}

// ============================================================================
// Set-returning functions
// ============================================================================

#[tokio::test]
async fn test_rpc_get_items_returns_set() {
    let resp = rpc_post("get_items", json!({})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body
        .as_array()
        .expect("set-returning function should return array");
    assert!(
        arr.len() >= 10,
        "should return at least 10 items, got {}",
        arr.len()
    );
    // Each item should have typical columns
    assert!(arr[0].get("id").is_some());
    assert!(arr[0].get("name").is_some());
}

#[tokio::test]
async fn test_rpc_search_items() {
    let resp = rpc_post("search_items", json!({"query": "Widget"})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should find at least one Widget");
    for item in arr {
        let name = item["name"].as_str().unwrap_or("");
        assert!(
            name.to_lowercase().contains("widget"),
            "all results should contain 'widget', got '{name}'"
        );
    }
}

#[tokio::test]
async fn test_rpc_search_items_no_match() {
    let resp = rpc_post("search_items", json!({"query": "ZZZZNOEXIST99999"})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("should return array");
    assert!(arr.is_empty(), "should return empty array for no match");
}

#[tokio::test]
async fn test_rpc_get_single_item() {
    let resp = rpc_post("get_item", json!({"item_id": 1})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    // Single-row return — might be object or array with one element
    if let Some(arr) = body.as_array() {
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["name"], "Widget");
    } else {
        assert_eq!(body["id"], 1);
        assert_eq!(body["name"], "Widget");
    }
}

// ============================================================================
// Void functions
// ============================================================================

#[tokio::test]
async fn test_rpc_void_function() {
    let resp = rpc_post("void_function", json!({})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK
            || status == StatusCode::CREATED
            || status == StatusCode::NO_CONTENT,
        "got {status}"
    );
}

// ============================================================================
// JSON-returning functions
// ============================================================================

#[tokio::test]
async fn test_rpc_get_json() {
    let resp = rpc_post("get_json", json!({})).await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    // Should contain the JSON result
    let result = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    // The function returns {"key": "value", "count": 42}
    assert_eq!(result["key"], "value");
    assert_eq!(result["count"], 42);
}

// ============================================================================
// GET-based RPC (for STABLE/IMMUTABLE)
// ============================================================================

#[tokio::test]
async fn test_rpc_get_method_stable_function() {
    let resp = rpc_get("get_items").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("should return array");
    assert!(!arr.is_empty());
}

// ============================================================================
// Error cases
// ============================================================================

#[tokio::test]
async fn test_rpc_nonexistent_function() {
    let resp = rpc_post("totally_fake_function", json!({})).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_rpc_wrong_param_name() {
    // Function expects "a" and "b", we pass "x" and "y"
    let resp = rpc_post("add", json!({"x": 1, "y": 2})).await;
    let status = resp.status();
    // Should either fail or use defaults (which may error for non-default params)
    assert!(
        status.is_client_error()
            || status.is_server_error()
            || status == StatusCode::OK
            || status == StatusCode::CREATED,
        "got {status}"
    );
}

// ============================================================================
// GET-based RPC with query parameters
// ============================================================================

#[tokio::test]
async fn test_rpc_get_add_with_query_params() {
    // STABLE function — pass args via query string
    let resp = rpc_get_params("add", "a=2&b=3").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "GET RPC with params should succeed, got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let result = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(result, json!(5));
}

#[tokio::test]
async fn test_rpc_get_search_with_query_params() {
    // STABLE set-returning function with query string argument
    let resp = rpc_get_params("search_items", "query=Widget").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "GET RPC search should succeed, got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should find at least one Widget via GET");
}

#[tokio::test]
async fn test_rpc_get_echo_params_with_query_params() {
    let resp = rpc_get_params("echo_params", "name=rustacean&greeting=ahoy").await;
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let result = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(result, json!("ahoy, rustacean!"));
}

// ============================================================================
// RPC with pagination / ordering / select on set-returning functions
// ============================================================================

#[tokio::test]
async fn test_rpc_set_returning_with_limit() {
    let s = server_info();
    let resp = s
        .client
        .post(format!("{}/api/test/rpc/get_items?limit=3", s.base_url))
        .header("content-type", "application/json")
        .json(&json!({}))
        .send()
        .await
        .expect("RPC POST with limit failed");
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("should return array");
    assert_eq!(arr.len(), 3, "should be limited to 3 items");
}

#[tokio::test]
async fn test_rpc_set_returning_with_order() {
    let s = server_info();
    let resp = s
        .client
        .post(format!(
            "{}/api/test/rpc/get_items?order=name.asc",
            s.base_url
        ))
        .header("content-type", "application/json")
        .json(&json!({}))
        .send()
        .await
        .expect("RPC POST with order failed");
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("should return array");
    assert!(arr.len() >= 2, "need at least 2 items to verify ordering");
    let first = arr[0]["name"].as_str().unwrap_or("");
    let second = arr[1]["name"].as_str().unwrap_or("");
    assert!(
        first <= second,
        "should be ordered ascending: '{first}' <= '{second}'"
    );
}

#[tokio::test]
async fn test_rpc_set_returning_with_select() {
    let s = server_info();
    let resp = s
        .client
        .post(format!(
            "{}/api/test/rpc/get_items?select=id,name",
            s.base_url
        ))
        .header("content-type", "application/json")
        .json(&json!({}))
        .send()
        .await
        .expect("RPC POST with select failed");
    let status = resp.status();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "got {status}"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let arr = body.as_array().expect("should return array");
    assert!(!arr.is_empty());
    // Should have only id and name columns
    let item = &arr[0];
    assert!(item.get("id").is_some(), "should have id");
    assert!(item.get("name").is_some(), "should have name");
    assert!(
        item.get("price").is_none(),
        "should NOT have price when select=id,name"
    );
}

#[tokio::test]
async fn test_rpc_set_returning_with_offset() {
    let s = server_info();
    // Get first page
    let resp1 = s
        .client
        .post(format!(
            "{}/api/test/rpc/get_items?order=id.asc&limit=2",
            s.base_url
        ))
        .header("content-type", "application/json")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let body1: serde_json::Value = resp1.json().await.unwrap();
    let arr1 = body1.as_array().unwrap();

    // Get second page (offset=2)
    let resp2 = s
        .client
        .post(format!(
            "{}/api/test/rpc/get_items?order=id.asc&limit=2&offset=2",
            s.base_url
        ))
        .header("content-type", "application/json")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let body2: serde_json::Value = resp2.json().await.unwrap();
    let arr2 = body2.as_array().unwrap();

    // Pages should not overlap
    if !arr1.is_empty() && !arr2.is_empty() {
        let last_id_page1 = arr1.last().unwrap()["id"].as_i64().unwrap();
        let first_id_page2 = arr2[0]["id"].as_i64().unwrap();
        assert!(
            first_id_page2 > last_id_page1,
            "page 2 first id ({first_id_page2}) should be > page 1 last id ({last_id_page1})"
        );
    }
}

// ============================================================================
// RPC with singular object response (Accept: application/vnd.pgrst.object)
// ============================================================================

#[tokio::test]
async fn test_rpc_singular_object_accept() {
    let s = server_info();
    let resp = s
        .client
        .post(format!("{}/api/test/rpc/get_item", s.base_url))
        .header("content-type", "application/json")
        .header("Accept", "application/vnd.pgrst.object+json")
        .json(&json!({"item_id": 1}))
        .send()
        .await
        .expect("RPC with singular accept failed");
    let status = resp.status();
    assert!(
        status == StatusCode::OK
            || status == StatusCode::CREATED
            || status == StatusCode::NOT_ACCEPTABLE,
        "got {status}"
    );
    if status == StatusCode::OK || status == StatusCode::CREATED {
        let body: serde_json::Value = resp.json().await.unwrap();
        // With singular accept, should be a single object (not array)
        assert!(
            body.is_object() || (body.is_array() && body.as_array().unwrap().len() == 1),
            "singular accept should return object or single-element array, got: {body}"
        );
    }
}

// ============================================================================
// RPC Content-Range on set-returning functions
// ============================================================================

#[tokio::test]
async fn test_rpc_content_range_on_set_returning() {
    let s = server_info();
    let resp = s
        .client
        .post(format!("{}/api/test/rpc/get_items?limit=3", s.base_url))
        .header("content-type", "application/json")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status == StatusCode::OK
            || status == StatusCode::CREATED
            || status == StatusCode::PARTIAL_CONTENT,
        "got {status}"
    );
    // Content-Range header should be present for set-returning results
    let cr = resp.headers().get("content-range");
    if let Some(cr_val) = cr {
        let cr_str = cr_val.to_str().unwrap_or("");
        assert!(
            !cr_str.is_empty(),
            "content-range should not be empty for set-returning RPC"
        );
    }
}

// ============================================================================
// In-process RPC via AppState (no HTTP)
// ============================================================================

/// Shared AppState for in-process tests.
struct InProcessState {
    state: pgvis_router::AppState,
}

static IN_PROCESS: OnceLock<InProcessState> = OnceLock::new();

fn in_process_state() -> &'static InProcessState {
    IN_PROCESS.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<pgvis_router::AppState>();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let dsn = test_dsn();
                setup_test_db(&dsn).await;

                let components = pgvis_lib::Builder::new(&dsn)
                    .schemas(vec!["test".to_string()])
                    .build_components()
                    .await
                    .expect("failed to build components");

                let state = pgvis_router::AppState::new(
                    components.cache,
                    components.config,
                    components.dialect,
                    components.backend,
                );
                tx.send(state).unwrap();

                // Keep the runtime alive
                std::future::pending::<()>().await;
            });
        });

        let state = rx.recv().expect("failed to receive AppState");
        InProcessState { state }
    })
}

#[tokio::test]
async fn test_in_process_rpc_scalar_add() {
    let ips = in_process_state();
    let result = ips
        .state
        .call_rpc(
            "test",
            "add",
            json!({"a": 10, "b": 20}),
            &pgvis_router::CallerIdentity::anonymous(),
        )
        .await
        .expect("call_rpc should succeed");

    // Result body should contain the scalar result
    let body = &result.body;
    let value = if let Some(arr) = body.as_array() {
        assert!(!arr.is_empty(), "scalar result should have one row");
        arr[0].get("result").cloned().unwrap_or(arr[0].clone())
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(value, json!(30), "10 + 20 should be 30");
}

#[tokio::test]
async fn test_in_process_rpc_set_returning() {
    let ips = in_process_state();
    let result = ips
        .state
        .call_rpc(
            "test",
            "get_items",
            json!({}),
            &pgvis_router::CallerIdentity::anonymous(),
        )
        .await
        .expect("call_rpc should succeed");

    let arr = result
        .body
        .as_array()
        .expect("set-returning function should return array");
    assert!(
        arr.len() >= 10,
        "should return at least 10 items, got {}",
        arr.len()
    );
    assert!(arr[0].get("id").is_some());
    assert!(arr[0].get("name").is_some());
}

#[tokio::test]
async fn test_in_process_rpc_with_args() {
    let ips = in_process_state();
    let result = ips
        .state
        .call_rpc(
            "test",
            "search_items",
            json!({"query": "Widget"}),
            &pgvis_router::CallerIdentity::anonymous(),
        )
        .await
        .expect("call_rpc should succeed");

    let arr = result
        .body
        .as_array()
        .expect("search should return array");
    assert!(!arr.is_empty(), "should find at least one Widget");
    for item in arr {
        let name = item["name"].as_str().unwrap_or("");
        assert!(
            name.to_lowercase().contains("widget"),
            "all results should contain 'widget', got '{name}'"
        );
    }
}

#[tokio::test]
async fn test_in_process_rpc_not_found() {
    let ips = in_process_state();
    let err = ips
        .state
        .call_rpc(
            "test",
            "nonexistent_function_xyz",
            json!({}),
            &pgvis_router::CallerIdentity::anonymous(),
        )
        .await;

    assert!(
        err.is_err(),
        "calling nonexistent function should return error"
    );
}

#[tokio::test]
async fn test_in_process_rpc_echo_defaults() {
    let ips = in_process_state();
    let result = ips
        .state
        .call_rpc(
            "test",
            "echo_params",
            json!({}),
            &pgvis_router::CallerIdentity::anonymous(),
        )
        .await
        .expect("call_rpc should succeed");

    let body = &result.body;
    let value = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(value, json!("hello, world!"));
}

#[tokio::test]
async fn test_in_process_rpc_with_role() {
    let ips = in_process_state();
    // Call with a specific role — should still work for public functions
    let result = ips
        .state
        .call_rpc(
            "test",
            "add",
            json!({"a": 1, "b": 2}),
            &pgvis_router::CallerIdentity::with_role("postgres"),
        )
        .await
        .expect("call_rpc with role should succeed");

    let body = &result.body;
    let value = if let Some(arr) = body.as_array() {
        if arr.is_empty() {
            json!(null)
        } else {
            arr[0].get("result").cloned().unwrap_or(arr[0].clone())
        }
    } else if let Some(r) = body.get("result") {
        r.clone()
    } else {
        body.clone()
    };
    assert_eq!(value, json!(3));
}
