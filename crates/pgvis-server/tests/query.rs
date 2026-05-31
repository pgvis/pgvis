//! Integration tests for GET queries — filters, select, order, limit, range.
//!
//! Covers PostgREST QuerySpec.hs parity.

mod common;

use common::{PgvisServer, assert_json, setup_test_db, test_dsn};
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

        // Spawn a background thread with its own tokio runtime to host the server.
        // The thread runs forever (detached) keeping the server alive.
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
                // Keep the server alive forever (until process exits)
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

/// Convenience: GET against the shared server.
async fn get(path: &str) -> reqwest::Response {
    let s = server_info();
    s.client
        .get(format!("{}{path}", s.base_url))
        .send()
        .await
        .expect("GET request failed")
}

// ============================================================================
// Basic SELECT
// ============================================================================

#[tokio::test]
async fn test_get_all_items() {
    let body = assert_json(get("/api/test/items").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 10, "should return all 10 items");
}

#[tokio::test]
async fn test_get_all_users() {
    let body = assert_json(get("/api/test/users").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 5, "should return all 5 users");
}

#[tokio::test]
async fn test_get_returns_json_content_type() {
    let resp = get("/api/test/items").await;
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_json(resp, StatusCode::OK).await;
    assert!(
        ct.contains("application/json"),
        "content-type should be JSON, got: {ct}"
    );
}

#[tokio::test]
async fn test_get_returns_content_range() {
    let resp = get("/api/test/items").await;
    let cr = resp
        .headers()
        .get("content-range")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_json(resp, StatusCode::OK).await;
    assert!(
        cr.starts_with("0-9/"),
        "content-range should start with 0-9/, got: {cr}"
    );
}

// ============================================================================
// Column Selection (?select=...)
// ============================================================================

#[tokio::test]
async fn test_select_single_column() {
    let body = assert_json(get("/api/test/items?select=name").await, StatusCode::OK).await;
    let first = &body[0];
    assert!(first.get("name").is_some(), "should have 'name' field");
    assert!(first.get("id").is_none(), "should NOT have 'id' field");
    assert!(
        first.get("price").is_none(),
        "should NOT have 'price' field"
    );
}

#[tokio::test]
async fn test_select_multiple_columns() {
    let body = assert_json(
        get("/api/test/items?select=id,name,price").await,
        StatusCode::OK,
    )
    .await;
    let first = &body[0];
    assert!(first.get("id").is_some());
    assert!(first.get("name").is_some());
    assert!(first.get("price").is_some());
    assert!(first.get("category").is_none());
    assert!(first.get("description").is_none());
}

#[tokio::test]
async fn test_select_star() {
    let body = assert_json(get("/api/test/items?select=*").await, StatusCode::OK).await;
    let first = &body[0];
    // Star should return all columns
    assert!(first.get("id").is_some());
    assert!(first.get("name").is_some());
    assert!(first.get("price").is_some());
    assert!(first.get("category").is_some());
}

// ============================================================================
// Equality Filters
// ============================================================================

#[tokio::test]
async fn test_filter_eq_text() {
    let body = assert_json(get("/api/test/items?name=eq.Widget").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "Widget");
}

#[tokio::test]
async fn test_filter_eq_integer() {
    let body = assert_json(get("/api/test/items?id=eq.3").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"], 3);
    assert_eq!(arr[0]["name"], "Doohickey");
}

#[tokio::test]
async fn test_filter_eq_no_match() {
    let body = assert_json(
        get("/api/test/items?name=eq.NonExistent").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 0, "no match should return empty array");
}

#[tokio::test]
async fn test_filter_neq() {
    let body = assert_json(get("/api/test/items?name=neq.Widget").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 9, "neq should exclude Widget, leaving 9");
}

// ============================================================================
// Comparison Filters
// ============================================================================

#[tokio::test]
async fn test_filter_gt() {
    let body = assert_json(get("/api/test/items?price=gt.100").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    for item in arr {
        let price = item["price"].as_f64().unwrap();
        assert!(price > 100.0, "price {price} should be > 100");
    }
    assert!(!arr.is_empty(), "should have some items > 100");
}

#[tokio::test]
async fn test_filter_gte() {
    let body = assert_json(get("/api/test/items?price=gte.99.99").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    for item in arr {
        let price = item["price"].as_f64().unwrap();
        assert!(price >= 99.99, "price {price} should be >= 99.99");
    }
}

#[tokio::test]
async fn test_filter_lt() {
    let body = assert_json(get("/api/test/items?price=lt.5").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    for item in arr {
        let price = item["price"].as_f64().unwrap();
        assert!(price < 5.0, "price {price} should be < 5");
    }
}

#[tokio::test]
async fn test_filter_lte() {
    let body = assert_json(get("/api/test/items?price=lte.9.99").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    for item in arr {
        let price = item["price"].as_f64().unwrap();
        assert!(price <= 9.99, "price {price} should be <= 9.99");
    }
}

// ============================================================================
// Pattern Matching (LIKE / ILIKE)
// ============================================================================

#[tokio::test]
async fn test_filter_like() {
    let body = assert_json(
        get("/api/test/items?name=like.*ocket*").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "Sprocket");
}

#[tokio::test]
async fn test_filter_like_prefix() {
    let body = assert_json(get("/api/test/items?name=like.Wid*").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "Widget");
}

#[tokio::test]
async fn test_filter_ilike() {
    let body = assert_json(
        get("/api/test/items?name=ilike.*WIDGET*").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    assert!(!arr.is_empty(), "ilike should be case-insensitive");
    assert!(arr.iter().any(|i| i["name"] == "Widget"));
}

// ============================================================================
// IS NULL / IS NOT NULL
// ============================================================================

#[tokio::test]
async fn test_filter_is_null() {
    let body = assert_json(
        get("/api/test/items?description=is.null").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    for item in arr {
        assert!(item["description"].is_null(), "description should be NULL");
    }
    assert!(!arr.is_empty());
}

#[tokio::test]
async fn test_filter_is_not_null() {
    let body = assert_json(
        get("/api/test/users?email=not.is.null").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    for user in arr {
        assert!(!user["email"].is_null(), "email should NOT be NULL");
    }
    // Eve has no email
    assert_eq!(arr.len(), 4);
}

// ============================================================================
// IN filter
// ============================================================================

#[tokio::test]
async fn test_filter_in() {
    let body = assert_json(
        get("/api/test/items?category=in.(gadgets,toys)").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    for item in arr {
        let cat = item["category"].as_str().unwrap();
        assert!(
            cat == "gadgets" || cat == "toys",
            "category should be gadgets or toys, got: {cat}"
        );
    }
}

#[tokio::test]
async fn test_filter_not_in() {
    let body = assert_json(
        get("/api/test/items?category=not.in.(premium,toys)").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    for item in arr {
        let cat = item["category"].as_str().unwrap_or("");
        assert!(
            cat != "premium" && cat != "toys",
            "category should NOT be premium or toys, got: {cat}"
        );
    }
}

// ============================================================================
// Negation (not.)
// ============================================================================

#[tokio::test]
async fn test_filter_not_eq() {
    let body = assert_json(
        get("/api/test/users?role=not.eq.user").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    for user in arr {
        assert_ne!(user["role"], "user");
    }
}

// ============================================================================
// Boolean filters
// ============================================================================

#[tokio::test]
async fn test_filter_boolean_true() {
    let body = assert_json(
        get("/api/test/items?in_stock=eq.true").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    for item in arr {
        assert_eq!(item["in_stock"], true);
    }
}

#[tokio::test]
async fn test_filter_boolean_false() {
    let body = assert_json(
        get("/api/test/items?in_stock=eq.false").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2); // Thingamajig and Quantum Flux Capacitor
    for item in arr {
        assert_eq!(item["in_stock"], false);
    }
}

// ============================================================================
// ORDER BY
// ============================================================================

#[tokio::test]
async fn test_order_asc() {
    let body = assert_json(
        get("/api/test/items?select=name,price&order=price.asc").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    let prices: Vec<f64> = arr.iter().map(|i| i["price"].as_f64().unwrap()).collect();
    let mut sorted = prices.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(prices, sorted, "prices should be in ascending order");
}

#[tokio::test]
async fn test_order_desc() {
    let body = assert_json(
        get("/api/test/items?select=name,price&order=price.desc").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    let prices: Vec<f64> = arr.iter().map(|i| i["price"].as_f64().unwrap()).collect();
    let mut sorted = prices.clone();
    sorted.sort_by(|a, b| b.partial_cmp(a).unwrap());
    assert_eq!(prices, sorted, "prices should be in descending order");
}

#[tokio::test]
async fn test_order_multiple_columns() {
    let body = assert_json(
        get("/api/test/items?select=category,price&order=category.asc,price.desc").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    // Verify categories are ordered, and within same category prices are desc
    for i in 1..arr.len() {
        let cat_prev = arr[i - 1]["category"].as_str().unwrap_or("");
        let cat_curr = arr[i]["category"].as_str().unwrap_or("");
        if cat_prev == cat_curr {
            let price_prev = arr[i - 1]["price"].as_f64().unwrap();
            let price_curr = arr[i]["price"].as_f64().unwrap();
            assert!(
                price_prev >= price_curr,
                "within same category, prices should be desc"
            );
        }
    }
}

// ============================================================================
// LIMIT / OFFSET
// ============================================================================

#[tokio::test]
async fn test_limit() {
    let body = assert_json(get("/api/test/items?limit=3").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
}

#[tokio::test]
async fn test_limit_and_offset() {
    let all = assert_json(get("/api/test/items?order=id.asc").await, StatusCode::OK).await;
    let page2 = assert_json(
        get("/api/test/items?order=id.asc&limit=3&offset=3").await,
        StatusCode::OK,
    )
    .await;

    let all_arr = all.as_array().unwrap();
    let page2_arr = page2.as_array().unwrap();
    assert_eq!(page2_arr.len(), 3);
    assert_eq!(page2_arr[0]["id"], all_arr[3]["id"]);
    assert_eq!(page2_arr[1]["id"], all_arr[4]["id"]);
    assert_eq!(page2_arr[2]["id"], all_arr[5]["id"]);
}

#[tokio::test]
async fn test_offset_only() {
    let body = assert_json(
        get("/api/test/items?order=id.asc&offset=8").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2, "offset 8 of 10 items = 2 remaining");
}

// ============================================================================
// Combined filters
// ============================================================================

#[tokio::test]
async fn test_multiple_filters() {
    let body = assert_json(
        get("/api/test/items?category=eq.tools&price=gt.5").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    for item in arr {
        assert_eq!(item["category"], "tools");
        assert!(item["price"].as_f64().unwrap() > 5.0);
    }
}

#[tokio::test]
async fn test_select_with_filter_and_order() {
    let body = assert_json(
        get("/api/test/items?select=name,price&category=eq.gadgets&order=price.desc").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    assert!(!arr.is_empty());
    // Check ordering
    let prices: Vec<f64> = arr.iter().map(|i| i["price"].as_f64().unwrap()).collect();
    let mut sorted = prices.clone();
    sorted.sort_by(|a, b| b.partial_cmp(a).unwrap());
    assert_eq!(prices, sorted);
}

// ============================================================================
// Empty table
// ============================================================================

#[tokio::test]
async fn test_empty_table() {
    let body = assert_json(get("/api/test/empty_table").await, StatusCode::OK).await;
    assert_eq!(body, json!([]));
}

// ============================================================================
// Unicode
// ============================================================================

#[tokio::test]
async fn test_unicode_data() {
    let body = assert_json(get("/api/test/unicode_data").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 4);
    assert!(arr.iter().any(|r| r["label"] == "日本語テスト"));
    assert!(arr.iter().any(|r| r["label"] == "🎉 Party"));
}

#[tokio::test]
async fn test_unicode_filter() {
    let body = assert_json(
        get("/api/test/unicode_data?label=eq.Ñoño").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["description"], "Spanish special chars");
}

// ============================================================================
// NULL handling
// ============================================================================

#[tokio::test]
async fn test_null_values_included_in_response() {
    let body = assert_json(get("/api/test/nullable_cols?id=eq.3").await, StatusCode::OK).await;
    let row = &body[0];
    assert_eq!(row["required_col"], "all_null");
    assert!(row["optional_col"].is_null());
    assert!(row["optional_int"].is_null());
    assert!(row["optional_bool"].is_null());
}

// ============================================================================
// Views
// ============================================================================

#[tokio::test]
async fn test_view_query() {
    let body = assert_json(get("/api/test/items_view").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 10);
    // View only has id, name, price, category, in_stock
    let first = &arr[0];
    assert!(first.get("id").is_some());
    assert!(first.get("name").is_some());
    assert!(first.get("description").is_none()); // not in view
}

#[tokio::test]
async fn test_view_with_filter() {
    let body = assert_json(get("/api/test/expensive_items").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    for item in arr {
        assert!(item["price"].as_f64().unwrap() > 50.0);
    }
}

// ============================================================================
// Content-Range header
// ============================================================================

#[tokio::test]
async fn test_content_range_all_rows() {
    let resp = get("/api/test/items").await;
    let cr = resp
        .headers()
        .get("content-range")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_json(resp, StatusCode::OK).await;
    // Should be "0-9/*" (10 items, 0-indexed)
    assert!(cr.starts_with("0-9/"), "got content-range: {cr}");
}

#[tokio::test]
async fn test_content_range_limited() {
    let resp = get("/api/test/items?limit=3").await;
    let cr = resp
        .headers()
        .get("content-range")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_json(resp, StatusCode::OK).await;
    assert!(cr.starts_with("0-2/"), "got content-range: {cr}");
}

#[tokio::test]
async fn test_content_range_empty() {
    let resp = get("/api/test/empty_table").await;
    let cr = resp
        .headers()
        .get("content-range")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_json(resp, StatusCode::OK).await;
    assert_eq!(cr, "*/*", "empty result should have */* range, got: {cr}");
}

// ============================================================================
// Compound primary key
// ============================================================================

#[tokio::test]
async fn test_compound_pk_filter() {
    let body = assert_json(
        get("/api/test/compound_pk?k1=eq.1&k2=eq.a").await,
        StatusCode::OK,
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["value"], "first");
}

// ============================================================================
// Table without primary key
// ============================================================================

#[tokio::test]
async fn test_no_pk_table() {
    let body = assert_json(get("/api/test/no_pk").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
}

// ============================================================================
// JSON/JSONB columns
// ============================================================================

#[tokio::test]
async fn test_jsonb_column_in_response() {
    let body = assert_json(
        get("/api/test/users?id=eq.1&select=name,data").await,
        StatusCode::OK,
    )
    .await;
    let user = &body[0];
    assert_eq!(user["name"], "Alice");
    assert_eq!(user["data"]["theme"], "dark");
    assert_eq!(user["data"]["lang"], "en");
}

// ============================================================================
// Error cases
// ============================================================================

#[tokio::test]
async fn test_nonexistent_table_returns_404() {
    let resp = get("/api/test/nonexistent_table").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_nonexistent_column_filter() {
    let resp = get("/api/test/items?nonexistent_col=eq.foo").await;
    // pgvis returns an error for unknown column (could be 400, 404, or 500 depending on impl)
    let status = resp.status();
    assert!(
        status.is_client_error() || status.is_server_error(),
        "unknown column should error, got: {status}",
    );
}

// ============================================================================
// Cursor Pagination
// ============================================================================

#[tokio::test]
async fn test_cursor_basic() {
    // cursor_value=3 with order=id.asc means WHERE id > 3, limit 3 → ids 4, 5, 6
    let resp = get("/api/test/items?cursor_column=id&cursor_value=3&limit=3&order=id.asc").await;
    let next_cursor = resp
        .headers()
        .get("x-next-cursor")
        .map(|v| v.to_str().unwrap().to_string());
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["id"], 4);
    assert_eq!(arr[1]["id"], 5);
    assert_eq!(arr[2]["id"], 6);
    assert_eq!(next_cursor.as_deref(), Some("6"), "X-Next-Cursor should be last row's id");
}

#[tokio::test]
async fn test_cursor_first_page_no_value() {
    // No cursor_value means first page — no WHERE cursor condition
    let resp = get("/api/test/items?cursor_column=id&limit=3&order=id.asc").await;
    let next_cursor = resp
        .headers()
        .get("x-next-cursor")
        .map(|v| v.to_str().unwrap().to_string());
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["id"], 1);
    assert_eq!(arr[1]["id"], 2);
    assert_eq!(arr[2]["id"], 3);
    assert_eq!(next_cursor.as_deref(), Some("3"));
}

#[tokio::test]
async fn test_cursor_pk_default() {
    // cursor_column omitted → uses primary key (id) as default
    let resp = get("/api/test/items?cursor_value=3&limit=3&order=id.asc").await;
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["id"], 4);
    assert_eq!(arr[1]["id"], 5);
    assert_eq!(arr[2]["id"], 6);
}

#[tokio::test]
async fn test_cursor_desc_order() {
    // With DESC order, cursor uses < operator: WHERE id < 8
    let resp = get("/api/test/items?cursor_column=id&cursor_value=8&order=id.desc&limit=3").await;
    let next_cursor = resp
        .headers()
        .get("x-next-cursor")
        .map(|v| v.to_str().unwrap().to_string());
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["id"], 7);
    assert_eq!(arr[1]["id"], 6);
    assert_eq!(arr[2]["id"], 5);
    assert_eq!(next_cursor.as_deref(), Some("5"));
}

#[tokio::test]
async fn test_cursor_with_filter() {
    // Cursor combined with a regular filter: tools with id > 2
    // Tools: id 3 (Doohickey), 4 (Thingamajig), 6 (Sprocket), 7 (Flanged Bracket)
    let resp = get("/api/test/items?cursor_column=id&cursor_value=2&category=eq.tools&order=id.asc&limit=5").await;
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert!(!arr.is_empty());
    for item in arr {
        assert_eq!(item["category"], "tools");
        assert!(item["id"].as_i64().unwrap() > 2);
    }
}

#[tokio::test]
async fn test_cursor_invalid_column() {
    // Invalid cursor_column should return an error
    let resp = get("/api/test/items?cursor_column=nonexistent&cursor_value=3").await;
    let status = resp.status();
    assert!(
        status.is_client_error(),
        "invalid cursor_column should return client error, got: {status}"
    );
}

#[tokio::test]
async fn test_cursor_no_pk_no_column_error() {
    // Table with no PK + no cursor_column specified → error
    let resp = get("/api/test/no_pk?cursor_value=3").await;
    let status = resp.status();
    assert!(
        status.is_client_error(),
        "no PK + no cursor_column should error, got: {status}"
    );
}

#[tokio::test]
async fn test_cursor_ignores_offset() {
    // When cursor is active, offset should be ignored
    let resp = get("/api/test/items?cursor_column=id&cursor_value=3&offset=5&limit=3&order=id.asc").await;
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // Should start after cursor_value=3, not at offset=5
    assert_eq!(arr[0]["id"], 4);
    assert_eq!(arr[1]["id"], 5);
    assert_eq!(arr[2]["id"], 6);
}

#[tokio::test]
async fn test_cursor_empty_result_no_header() {
    // Cursor past all data → empty result, no X-Next-Cursor header
    let resp = get("/api/test/items?cursor_column=id&cursor_value=100&limit=3&order=id.asc").await;
    let next_cursor = resp.headers().get("x-next-cursor").cloned();
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 0);
    assert!(
        next_cursor.is_none(),
        "empty result should NOT have X-Next-Cursor header"
    );
}

#[tokio::test]
async fn test_cursor_last_page_partial() {
    // Cursor near the end: cursor_value=8, only 2 items remain (9, 10)
    let resp = get("/api/test/items?cursor_column=id&cursor_value=8&limit=5&order=id.asc").await;
    let next_cursor = resp
        .headers()
        .get("x-next-cursor")
        .map(|v| v.to_str().unwrap().to_string());
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], 9);
    assert_eq!(arr[1]["id"], 10);
    assert_eq!(next_cursor.as_deref(), Some("10"));
}

#[tokio::test]
async fn test_cursor_non_pk_column() {
    // Use price as cursor column (non-PK)
    // Prices: 2.99, 3.75, 7.50, 9.99, 12.00, 24.50, 42.00, 99.99, 149.95, 999.00
    let resp = get("/api/test/items?cursor_column=price&cursor_value=12.00&order=price.asc&limit=3").await;
    let body = assert_json(resp, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // Should be items with price > 12.00: 24.50, 42.00, 99.99
    for item in arr {
        assert!(item["price"].as_f64().unwrap() > 12.0);
    }
}

// ============================================================================
// Numeric type coercion
// ============================================================================

#[tokio::test]
async fn test_numeric_filter_float() {
    let body = assert_json(get("/api/test/items?price=eq.9.99").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "Widget");
}

#[tokio::test]
async fn test_integer_filter_range() {
    let body = assert_json(get("/api/test/users?age=gte.30").await, StatusCode::OK).await;
    let arr = body.as_array().unwrap();
    for user in arr {
        assert!(user["age"].as_i64().unwrap() >= 30);
    }
}
