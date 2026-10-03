//! MCP tool calls against a real database: write tools must fail closed.

mod common;

use common::{setup_test_db, test_dsn};
use pgvis_lib::pgvis_mcp::{McpToolCall, build_mcp_tools, handle_tool_call};
use serde_json::json;

async fn components() -> pgvis_lib::Components {
    let dsn = test_dsn();
    setup_test_db(&dsn).await;
    pgvis_lib::Builder::new(&dsn)
        .schemas(vec!["test".to_string()])
        .build_components()
        .await
        .expect("failed to build components")
}

fn tool_name(c: &pgvis_lib::Components, suffix: &str) -> String {
    build_mcp_tools(&c.cache.load(), &c.config)
        .into_iter()
        .map(|t| t.name)
        .find(|n| n.ends_with(suffix))
        .unwrap_or_else(|| panic!("no tool ending in {suffix}"))
}

async fn call(c: &pgvis_lib::Components, name: &str, arguments: serde_json::Value) -> bool {
    let call = McpToolCall {
        name: name.to_string(),
        arguments,
    };
    let result = handle_tool_call(&call, &c.cache.load(), &c.dialect, &c.config, &*c.backend).await;
    result.is_error
}

async fn item_count() -> usize {
    let (client, conn) = tokio_postgres::connect(&test_dsn(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    let row = client
        .query_one("SELECT count(*) FROM test.items", &[])
        .await
        .unwrap();
    row.get::<_, i64>(0) as usize
}

#[tokio::test]
async fn unfiltered_or_malformed_delete_is_refused_and_deletes_nothing() {
    let c = components().await;
    let delete = tool_name(&c, "delete_items");
    let before = item_count().await;

    for args in [
        json!({}),
        json!({ "filters": {} }),
        json!({ "filters": "id.eq.5" }),
        json!({ "filters": {}, "or": "(id.eq.5,id.eq.6" }),
    ] {
        assert!(call(&c, &delete, args.clone()).await, "should refuse {args}");
    }
    assert_eq!(item_count().await, before, "no row may be deleted");

    // A real filter still works (matching nothing, so the fixture is intact).
    assert!(!call(&c, &delete, json!({ "filters": { "id": "eq.-1" } })).await);
}
