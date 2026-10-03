//! Introspection of PostgreSQL 18 catalog features. Skips on older servers.

mod common;

use common::{setup_test_db, test_dsn};

async fn server_version_num() -> i32 {
    let (client, conn) = tokio_postgres::connect(&test_dsn(), tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    let row = client
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .unwrap();
    row.get(0)
}

#[tokio::test]
async fn pg18_catalog_features_are_introspected_correctly() {
    if server_version_num().await < 180000 {
        eprintln!("skipped: needs PostgreSQL 18+");
        return;
    }
    let dsn = test_dsn();
    setup_test_db(&dsn).await;
    let components = pgvis_lib::Builder::new(&dsn)
        .schemas(vec!["test".to_string()])
        .build_components()
        .await
        .expect("failed to build components");
    let cache = components.cache.load();

    let items = cache.find_table("test", "pg18_items").expect("pg18_items");
    // A virtual generated column is read-only and has no default, like a
    // stored one; it used to be reported as a writable column.
    let doubled = &items.columns["doubled"];
    assert!(doubled.is_generated);
    assert!(!doubled.updatable);
    assert_eq!(doubled.default, None);
    // A NOT VALID not-null constraint doesn't make the column non-nullable.
    assert!(items.columns["note"].nullable);
    assert!(!items.columns["price"].nullable);

    // A temporal (WITHOUT OVERLAPS) key isn't an equality key.
    let bookings = cache
        .find_table("test", "pg18_bookings")
        .expect("pg18_bookings");
    assert!(bookings.pk_cols.is_empty(), "got {:?}", bookings.pk_cols);
}
