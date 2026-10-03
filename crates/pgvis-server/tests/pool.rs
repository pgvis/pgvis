//! Connection checkout: cancellation safety and pool exhaustion.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{setup_test_db, test_dsn};
use pgvis_lib::pgvis_core::backend::{Backend, ExecContext};
use pgvis_lib::pgvis_core::config::PoolConfig;
use pgvis_lib::pgvis_postgres::PgBackend;

/// A one-connection pool, so a reused connection can't hide.
fn backend(timeout_ms: u64) -> PgBackend {
    let cfg = PoolConfig {
        size: 1,
        timeout_ms,
        ..Default::default()
    };
    PgBackend::new(&test_dsn(), &cfg).unwrap()
}

async fn current_user(b: &PgBackend) -> String {
    let result = b
        .execute(&ExecContext::default(), "SELECT to_json(current_user::text) AS body", &[])
        .await
        .unwrap();
    result.body.as_str().unwrap().to_string()
}

#[tokio::test]
async fn a_request_dropped_mid_transaction_never_leaks_its_session() {
    setup_test_db(&test_dsn()).await; // creates the pgvis_test_anon role
    let b = backend(5000);
    let dsn_user = current_user(&b).await;

    // Dropped while the server is still inside its transaction, role set.
    let as_role = ExecContext {
        role: Some("pgvis_test_anon".into()),
        ..Default::default()
    };
    let slow = b.execute(&as_role, "SELECT pg_sleep(5)::text AS x, '[]'::json AS body", &[]);
    assert!(tokio::time::timeout(Duration::from_millis(300), slow).await.is_err());

    // The only pooled connection was mid-transaction: the next request must
    // get a fresh one, not wait for it or inherit its role.
    let started = Instant::now();
    assert_eq!(current_user(&b).await, dsn_user);
    assert!(started.elapsed() < Duration::from_secs(3), "reused the abandoned connection");
}

#[tokio::test]
async fn an_exhausted_pool_is_a_503_not_a_500() {
    let b = Arc::new(backend(100));
    let holder = {
        let b = b.clone();
        tokio::spawn(async move {
            let ctx = ExecContext::default();
            let _ = b
                .execute(&ctx, "SELECT pg_sleep(1)::text AS x, '[]'::json AS body", &[])
                .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    let err = b
        .execute(&ExecContext::default(), "SELECT '[]'::json AS body", &[])
        .await
        .unwrap_err();
    assert_eq!(err.http_status(), 503, "{err}");
    holder.await.unwrap();
}
