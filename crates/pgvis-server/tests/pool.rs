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
        .execute(
            &ExecContext::default(),
            "SELECT to_json(current_user::text) AS body",
            &[],
        )
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
    let slow = b.execute(
        &as_role,
        "SELECT pg_sleep(5)::text AS x, '[]'::json AS body",
        &[],
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(300), slow)
            .await
            .is_err()
    );

    // The only pooled connection was mid-transaction: the next request must
    // get a fresh one, not wait for it or inherit its role.
    let started = Instant::now();
    assert_eq!(current_user(&b).await, dsn_user);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "reused the abandoned connection"
    );
}

#[tokio::test]
async fn an_exhausted_pool_is_a_503_not_a_500() {
    let b = Arc::new(backend(100));
    let holder = {
        let b = b.clone();
        tokio::spawn(async move {
            let ctx = ExecContext::default();
            let _ = b
                .execute(
                    &ctx,
                    "SELECT pg_sleep(1)::text AS x, '[]'::json AS body",
                    &[],
                )
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

#[tokio::test]
async fn checkout_queue_is_bounded_and_cancelled_waiters_release_admission() {
    let config = PoolConfig {
        size: 1,
        max_waiters: 1,
        timeout_ms: 5000,
        ..Default::default()
    };
    let backend = Arc::new(PgBackend::new(&test_dsn(), &config).unwrap());
    let held = backend.pool().get().await.unwrap();
    let pending = {
        let backend = backend.clone();
        tokio::spawn(async move { current_user(&backend).await })
    };
    while backend.pool_status().unwrap().waiting != 1 {
        tokio::task::yield_now().await;
    }
    let started = Instant::now();
    let error = backend
        .execute(&ExecContext::default(), "SELECT '[]'::json AS body", &[])
        .await
        .unwrap_err();
    assert_eq!(error.http_status(), 503);
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "full queue did not refuse promptly"
    );
    pending.abort();
    let _ = pending.await;
    assert_eq!(backend.pool_status().unwrap().waiting, 0);
    drop(held);
    current_user(&backend).await;
    let metrics = backend.pool_status().unwrap().metrics;
    assert_eq!(metrics.checkout_rejected, 1);
    assert_eq!(metrics.checkout_attempts, 3);
}

#[tokio::test]
async fn network_query_deadline_discards_the_connection_without_replaying_work() {
    let config = PoolConfig {
        size: 1,
        query_timeout_ms: 100,
        ..Default::default()
    };
    let backend = PgBackend::new(&test_dsn(), &config).unwrap();
    let before = Instant::now();
    let error = backend
        .execute(
            &ExecContext::default(),
            "SELECT pg_sleep(5)::text AS x, '[]'::json AS body",
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(error.http_status(), 504);
    assert!(before.elapsed() < Duration::from_secs(1));
    current_user(&backend).await;
    let metrics = backend.pool_status().unwrap().metrics;
    assert_eq!(metrics.queries, 2);
    assert_eq!(metrics.query_errors, 1);
    assert_eq!(metrics.query_cancelled, 0);
}

#[tokio::test]
async fn adaptive_pool_grows_under_load_then_reduces_after_slow_work() {
    use pgvis_lib::pgvis_core::config::AdaptivePoolConfig;
    let config = PoolConfig {
        size: 4,
        adaptive: Some(AdaptivePoolConfig {
            min_size: 1,
            target_query_time_ms: 100,
            adjustment_interval_ms: 20,
        }),
        ..Default::default()
    };
    let backend = Arc::new(PgBackend::new(&test_dsn(), &config).unwrap());
    assert_eq!(backend.pool_status().unwrap().max_size, 1);
    let start = Instant::now();
    let mut work = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let backend = backend.clone();
        work.spawn(async move {
            for _ in 0..25 {
                backend
                    .execute(
                        &ExecContext::default(),
                        "SELECT pg_sleep(0.002)::text AS x, '[]'::json AS body",
                        &[],
                    )
                    .await
                    .unwrap();
                let status = backend.pool_status().unwrap();
                assert!(status.size <= 4 && status.max_size <= 4 && status.hard_max_size == 4);
            }
        });
    }
    while let Some(result) = work.join_next().await {
        result.unwrap();
    }
    eprintln!(
        "400 short queries with adaptive 1..4 connections: {:?}",
        start.elapsed()
    );
    assert_eq!(backend.pool_status().unwrap().max_size, 4);
    let mut held = Vec::new();
    for _ in 0..3 {
        held.push(backend.pool().get().await.unwrap());
    }
    backend
        .execute(
            &ExecContext::default(),
            "SELECT pg_sleep(0.2)::text AS x, '[]'::json AS body",
            &[],
        )
        .await
        .unwrap();
    let status = backend.pool_status().unwrap();
    assert_eq!(status.max_size, 2);
    assert!(status.metrics.adaptive_increases >= 3 && status.metrics.adaptive_decreases >= 1);
    assert_eq!(status.metrics.queries, 401);
    assert_eq!(status.metrics.query_errors, 0);
    assert!(status.size >= 3 && status.size <= 4);
    for connection in &held {
        connection
            .query_one("SELECT 1", &[])
            .await
            .expect("shrinking cancelled active work");
    }
    drop(held);
    assert!(backend.pool_status().unwrap().size <= 2);
}

/// Local comparative load test, not a PgBouncer benchmark or production SLO.
#[tokio::test]
#[ignore = "runs 16384 queries with 2048 simultaneous callers"]
async fn compare_fixed_and_adaptive_pools_under_2048_callers() {
    use pgvis_lib::pgvis_core::config::AdaptivePoolConfig;
    for adaptive in [false, true] {
        let config = PoolConfig {
            size: 16,
            max_waiters: 4096,
            timeout_ms: 10000,
            adaptive: adaptive.then_some(AdaptivePoolConfig {
                min_size: 2,
                target_query_time_ms: 100,
                adjustment_interval_ms: 20,
            }),
            ..Default::default()
        };
        let backend = Arc::new(PgBackend::new(&test_dsn(), &config).unwrap());
        let mut tasks = tokio::task::JoinSet::new();
        let started = Instant::now();
        for _ in 0..2048 {
            let backend = backend.clone();
            tasks.spawn(async move {
                let mut times = Vec::new();
                for _ in 0..4 {
                    let started = Instant::now();
                    backend
                        .execute(
                            &ExecContext::default(),
                            "SELECT pg_sleep(0.002)::text AS x, '[]'::json AS body",
                            &[],
                        )
                        .await
                        .unwrap();
                    times.push(started.elapsed().as_micros());
                    assert!(backend.pool_status().unwrap().size <= 16);
                }
                times
            });
        }
        let mut times = Vec::new();
        while let Some(result) = tasks.join_next().await {
            times.extend(result.unwrap());
        }
        times.sort_unstable();
        let status = backend.pool_status().unwrap();
        assert_eq!(status.metrics.queries, 8192);
        assert_eq!(status.metrics.query_errors, 0);
        assert_eq!(status.metrics.checkout_errors, 0);
        assert_eq!(status.metrics.checkout_rejected, 0);
        eprintln!(
            "adaptive={adaptive}: queries=8192 callers=2048 elapsed={:?} request_p50_us={} request_p95_us={} request_p99_us={} final_limit={} increases={} decreases={} checkout_wait_us={}",
            started.elapsed(),
            times[times.len() / 2],
            times[times.len() * 95 / 100],
            times[times.len() * 99 / 100],
            status.max_size,
            status.metrics.adaptive_increases,
            status.metrics.adaptive_decreases,
            status.metrics.checkout_duration_us
        );
    }
}
