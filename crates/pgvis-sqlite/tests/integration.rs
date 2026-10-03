//! Integration tests for the SQLite backend.
//!
//! Tests introspection, query execution, mutations, type coercion, and relationships.
//! Uses an in-memory database seeded with test fixtures.

use pgvis_core::backend::{Backend, ExecContext, IntrospectConfig, TxEnd};
use pgvis_core::cache::Cardinality;
use pgvis_sqlite::SqliteBackend;
use serde_json::{Value, json};

/// Schema SQL loaded at compile time.
const SCHEMA_SQL: &str = include_str!("fixtures/schema.sql");
/// Seed data SQL loaded at compile time.
const SEED_SQL: &str = include_str!("fixtures/seed.sql");

/// Create a fresh in-memory backend with schema and seed data.
async fn setup_backend() -> SqliteBackend {
    let backend = SqliteBackend::open(":memory:").await.unwrap();
    backend.execute_raw(SCHEMA_SQL).await.unwrap();
    backend.execute_raw(SEED_SQL).await.unwrap();
    backend
}

/// Helper to execute a query and return the body as a JSON array.
async fn query(backend: &SqliteBackend, sql: &str, params: &[Value]) -> Value {
    let ctx = ExecContext::default();
    let result = backend.execute(&ctx, sql, params).await.unwrap();
    result.body
}

/// Helper to execute a mutation and return the body.
async fn mutate(backend: &SqliteBackend, sql: &str, params: &[Value]) -> Value {
    let ctx = ExecContext {
        is_mutation: true,
        ..Default::default()
    };
    let result = backend.execute(&ctx, sql, params).await.unwrap();
    result.body
}

// ===========================================================================
// Introspection tests
// ===========================================================================

#[tokio::test]
async fn test_introspect_discovers_tables() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    // Should find all tables (not sqlite_ internal ones)
    let table_names: Vec<&str> = cache.tables.keys().map(|k| k.name.as_str()).collect();
    assert!(table_names.contains(&"users"));
    assert!(table_names.contains(&"items"));
    assert!(table_names.contains(&"orders"));
    assert!(table_names.contains(&"order_items"));
    assert!(table_names.contains(&"tags"));
    assert!(table_names.contains(&"categories"));
    assert!(table_names.contains(&"logs"));
    assert!(!table_names.contains(&"sqlite_master"));
}

#[tokio::test]
async fn test_introspect_discovers_views() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    let active_users = cache.find_table("main", "active_users").unwrap();
    assert!(active_users.is_view);
    assert!(!active_users.insertable);
}

#[tokio::test]
async fn test_introspect_columns() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    let users = cache.find_table("main", "users").unwrap();
    assert!(users.columns.contains_key("id"));
    assert!(users.columns.contains_key("username"));
    assert!(users.columns.contains_key("email"));
    assert!(users.columns.contains_key("is_active"));

    let id_col = &users.columns["id"];
    assert!(id_col.is_pk);
    assert_eq!(id_col.typ, "INTEGER");

    let username_col = &users.columns["username"];
    assert!(!username_col.nullable);

    let email_col = &users.columns["email"];
    assert!(email_col.nullable);
}

#[tokio::test]
async fn test_introspect_primary_key() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    let users = cache.find_table("main", "users").unwrap();
    assert_eq!(users.pk_cols, vec!["id"]);

    // Composite PK
    let order_items = cache.find_table("main", "order_items").unwrap();
    assert!(order_items.pk_cols.contains(&"order_id".to_string()));
    assert!(order_items.pk_cols.contains(&"item_id".to_string()));
    assert_eq!(order_items.pk_cols.len(), 2);
}

#[tokio::test]
async fn test_introspect_no_pk_table() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    let logs = cache.find_table("main", "logs").unwrap();
    assert!(logs.pk_cols.is_empty());
}

#[tokio::test]
async fn test_introspect_generated_column() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    let products = cache.find_table("main", "products").unwrap();
    let total_value = &products.columns["total_value"];
    assert!(total_value.is_generated);
    assert!(!total_value.updatable);
}

#[tokio::test]
async fn test_introspect_relationships() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    // items.user_id → users.id should create M2O
    let items_ident = pgvis_core::QualifiedIdentifier::new("main", "items");
    let user_rel = cache.find_relationships(&items_ident).find(|r| {
        r.source_table.name == "items"
            && r.target_table.name == "users"
            && matches!(r.cardinality, Cardinality::M2O)
    });
    assert!(user_rel.is_some(), "Should find M2O from items to users");
}

#[tokio::test]
async fn test_introspect_inverse_relationships() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    // Should have O2M from users to items (inverse of items→users M2O)
    let users_ident = pgvis_core::QualifiedIdentifier::new("main", "users");
    let o2m_rel = cache.find_relationships(&users_ident).find(|r| {
        r.source_table.name == "users"
            && r.target_table.name == "items"
            && matches!(r.cardinality, Cardinality::O2M)
    });
    assert!(o2m_rel.is_some(), "Should find O2M from users to items");
}

#[tokio::test]
async fn test_introspect_m2m_relationships() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    // items ←→ tags via item_tags should create M2M (direction may vary)
    let m2m_rel = cache.relationships.iter().find(|r| {
        let involves_items = r.source_table.name == "items" || r.target_table.name == "items";
        let involves_tags = r.source_table.name == "tags" || r.target_table.name == "tags";
        involves_items && involves_tags && matches!(r.cardinality, Cardinality::M2M { .. })
    });
    assert!(
        m2m_rel.is_some(),
        "Should find M2M between items and tags via item_tags. Rels: {:?}",
        cache
            .relationships
            .iter()
            .filter(|r| matches!(r.cardinality, Cardinality::M2M { .. }))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_introspect_self_referential() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    let cat_ident = pgvis_core::QualifiedIdentifier::new("main", "categories");
    let self_rel = cache.find_relationships(&cat_ident).find(|r| r.is_self);
    assert!(
        self_rel.is_some(),
        "categories should have a self-referential FK"
    );
}

#[tokio::test]
async fn test_introspect_fk_columns_marked() {
    let backend = setup_backend().await;
    let cache = backend
        .introspect(&IntrospectConfig::default())
        .await
        .unwrap();

    let items = cache.find_table("main", "items").unwrap();
    assert!(items.columns["user_id"].is_fk);
    assert!(!items.columns["name"].is_fk);
}

// ===========================================================================
// Query execution tests
// ===========================================================================

#[tokio::test]
async fn test_select_all_rows() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT * FROM users", &[]).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
}

#[tokio::test]
async fn test_select_specific_columns() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT id, username FROM users", &[]).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // Each row should only have id and username
    let row = &arr[0];
    assert!(row.get("id").is_some());
    assert!(row.get("username").is_some());
    assert!(row.get("email").is_none());
}

#[tokio::test]
async fn test_filter_with_parameter() {
    let backend = setup_backend().await;
    let body = query(
        &backend,
        "SELECT * FROM users WHERE username = ?1",
        &[json!("alice")],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["username"], "alice");
}

#[tokio::test]
async fn test_filter_integer_parameter() {
    let backend = setup_backend().await;
    let body = query(
        &backend,
        "SELECT * FROM items WHERE price > ?1",
        &[json!(10.0)],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2); // Gadget (19.99) and Thingamajig (29.99)
}

#[tokio::test]
async fn test_order_by() {
    let backend = setup_backend().await;
    let body = query(
        &backend,
        "SELECT name, price FROM items ORDER BY price DESC",
        &[],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr[0]["name"], "Thingamajig");
    assert_eq!(arr[3]["name"], "Doohickey");
}

#[tokio::test]
async fn test_limit_and_offset() {
    let backend = setup_backend().await;
    let body = query(
        &backend,
        "SELECT * FROM users ORDER BY id LIMIT 2 OFFSET 1",
        &[],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["username"], "bob");
    assert_eq!(arr[1]["username"], "charlie");
}

#[tokio::test]
async fn test_empty_result() {
    let backend = setup_backend().await;
    let body = query(
        &backend,
        "SELECT * FROM users WHERE username = ?1",
        &[json!("nobody")],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 0);
}

// ===========================================================================
// Type coercion tests
// ===========================================================================

#[tokio::test]
async fn test_boolean_coercion() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT is_active FROM users WHERE id = 1", &[]).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr[0]["is_active"], Value::Bool(true));
}

#[tokio::test]
async fn test_boolean_false_coercion() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT is_active FROM users WHERE id = 3", &[]).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr[0]["is_active"], Value::Bool(false));
}

#[tokio::test]
async fn test_json_column_parsing() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT metadata FROM items WHERE id = 1", &[]).await;
    let arr = body.as_array().unwrap();
    let metadata = &arr[0]["metadata"];
    assert_eq!(metadata["color"], "blue");
    assert_eq!(metadata["weight"], 0.5);
}

#[tokio::test]
async fn test_null_values() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT * FROM nullable_test WHERE id = 2", &[]).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr[0]["optional_col"], Value::Null);
    assert_eq!(arr[0]["optional_int"], Value::Null);
    assert_eq!(arr[0]["optional_bool"], Value::Null);
}

#[tokio::test]
async fn test_real_type() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT price FROM items WHERE id = 1", &[]).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr[0]["price"].as_f64().unwrap(), 9.99);
}

#[tokio::test]
async fn test_unicode_data() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT * FROM unicode_data ORDER BY id", &[]).await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr[0]["label"], "Ünïcödé");
    assert!(
        arr[0]["description"]
            .as_str()
            .unwrap()
            .contains("こんにちは")
    );
}

#[tokio::test]
async fn test_generated_column_value() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT * FROM products WHERE id = 1", &[]).await;
    let arr = body.as_array().unwrap();
    // unit_price=0.10, quantity=1000 → total_value=100.0
    assert_eq!(arr[0]["total_value"].as_f64().unwrap(), 100.0);
}

// ===========================================================================
// Mutation tests
// ===========================================================================

#[tokio::test]
async fn test_insert() {
    let backend = setup_backend().await;
    let body = mutate(
        &backend,
        "INSERT INTO users (id, username, email, is_active) VALUES (?1, ?2, ?3, ?4) RETURNING *",
        &[
            json!(10),
            json!("dave"),
            json!("dave@example.com"),
            json!(true),
        ],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["username"], "dave");
    assert_eq!(arr[0]["is_active"], true);
}

#[tokio::test]
async fn test_update() {
    let backend = setup_backend().await;
    let body = mutate(
        &backend,
        "UPDATE users SET email = ?1 WHERE id = ?2 RETURNING id, email",
        &[json!("new@example.com"), json!(1)],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["email"], "new@example.com");
}

#[tokio::test]
async fn test_delete() {
    let backend = setup_backend().await;
    let body = mutate(
        &backend,
        "DELETE FROM users WHERE id = ?1 RETURNING id, username",
        &[json!(3)],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["username"], "charlie");

    // Verify it's gone
    let body = query(&backend, "SELECT * FROM users WHERE id = 3", &[]).await;
    assert_eq!(body.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_upsert() {
    let backend = setup_backend().await;
    let body = mutate(
        &backend,
        "INSERT INTO users (id, username, email, is_active) VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT(id) DO UPDATE SET email = excluded.email \
         RETURNING *",
        &[
            json!(1),
            json!("alice"),
            json!("updated@example.com"),
            json!(true),
        ],
    )
    .await;
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["email"], "updated@example.com");
}

// ===========================================================================
// Transaction management tests
// ===========================================================================

#[tokio::test]
async fn test_transaction_rollback() {
    let backend = setup_backend().await;

    // Execute a mutation with rollback preference
    let ctx = ExecContext {
        is_mutation: true,
        tx_end: Some(TxEnd::Rollback),
        ..Default::default()
    };
    let result = backend
        .execute(
            &ctx,
            "INSERT INTO users (id, username, is_active) VALUES (99, 'temp', 1) RETURNING *",
            &[],
        )
        .await
        .unwrap();

    // The result should show the inserted row
    let arr = result.body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["username"], "temp");

    // But it should NOT persist (was rolled back)
    let body = query(&backend, "SELECT * FROM users WHERE id = 99", &[]).await;
    assert_eq!(body.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_mutation_guards_roll_back_before_commit() {
    let backend = setup_backend().await;
    let before = query(&backend, "SELECT * FROM users", &[]).await;
    let all = before.as_array().unwrap().len();
    assert!(all > 1, "fixture needs several users");

    for ctx in [
        ExecContext {
            is_mutation: true,
            max_affected: Some(1),
            ..Default::default()
        },
        ExecContext {
            is_mutation: true,
            single_row: true,
            ..Default::default()
        },
    ] {
        let err = backend
            .execute(&ctx, "UPDATE users SET is_active = 9 RETURNING *", &[])
            .await
            .unwrap_err();
        assert!(matches!(err.http_status(), 400 | 406), "{err}");
        // Rolled back: no row changed.
        let after = query(&backend, "SELECT * FROM users WHERE is_active = 9", &[]).await;
        assert_eq!(after.as_array().unwrap().len(), 0);
    }
}

// ===========================================================================
// View query tests
// ===========================================================================

#[tokio::test]
async fn test_view_query() {
    let backend = setup_backend().await;
    let body = query(&backend, "SELECT * FROM active_users ORDER BY id", &[]).await;
    let arr = body.as_array().unwrap();
    // Only alice and bob are active
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["username"], "alice");
    assert_eq!(arr[1]["username"], "bob");
}

// ===========================================================================
// Concurrent access tests
// ===========================================================================

#[tokio::test]
async fn test_concurrent_reads() {
    let backend = std::sync::Arc::new(setup_backend().await);

    let mut handles = Vec::new();
    for _ in 0..10 {
        let backend = backend.clone();
        handles.push(tokio::spawn(async move {
            let ctx = ExecContext::default();
            let result = backend
                .execute(&ctx, "SELECT COUNT(*) as cnt FROM users", &[])
                .await
                .unwrap();
            let arr = result.body.as_array().unwrap();
            arr[0]["cnt"].as_i64().unwrap()
        }));
    }

    for handle in handles {
        let count = handle.await.unwrap();
        assert_eq!(count, 3);
    }
}

// ===========================================================================
// Dialect tests
// ===========================================================================

#[tokio::test]
async fn test_dialect_is_sqlite() {
    let backend = setup_backend().await;
    let dialect = backend.dialect();
    assert!(!dialect.has_routines);
    assert!(!dialect.supports_array_ops);
    assert!(!dialect.supports_regex_match);
    assert!(!dialect.supports_range_ops);
}
