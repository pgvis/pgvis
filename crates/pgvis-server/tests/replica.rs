//! Read-replica health monitoring.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::test_dsn;
use pgvis_lib::pgvis_core::backend::{Backend, ExecContext};
use pgvis_lib::pgvis_core::config::{PoolConfig, ReplicaConfig};
use pgvis_lib::pgvis_postgres::PgReplicaBackend;

#[tokio::test]
async fn a_saturated_replica_pool_does_not_get_the_replica_excluded() {
    // The local database stands in for both primary and replica; with lag
    // checking off, the monitor only checks the replica is reachable.
    let pool = PoolConfig {
        size: 1,
        timeout_ms: 1000,
        ..Default::default()
    };
    let replicas = ReplicaConfig {
        replica_dsns: vec![test_dsn()],
        max_replication_lag_bytes: 0,
        health_check_interval_ms: 200,
        primary_reads: false,
    };
    let backend = Arc::new(PgReplicaBackend::new(&test_dsn(), &pool, &replicas).unwrap());
    assert_eq!(backend.eligible_readers(), 1);

    // A long read takes the replica's only request connection.
    let busy = {
        let backend = backend.clone();
        tokio::spawn(async move {
            let ctx = ExecContext::default();
            let sql = "SELECT pg_sleep(4)::text AS x, '[]'::json AS body";
            let _ = backend.execute(&ctx, sql, &[]).await;
        })
    };

    // Probing through the request pool, the monitor's checkout would time out
    // after 1 s and exclude the (perfectly healthy) replica.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        backend.eligible_readers(),
        1,
        "healthy replica was excluded"
    );
    busy.await.unwrap();
}

#[tokio::test]
async fn configured_lag_checks_do_not_accept_a_primary_as_a_replica() {
    // A primary has no replay position. Previously the primary probe decoded
    // NUMERIC as i64 and silently disabled lag checking for every replica.
    let config = ReplicaConfig {
        replica_dsns: vec![test_dsn()],
        health_check_interval_ms: 50,
        primary_reads: false,
        ..Default::default()
    };
    let backend = PgReplicaBackend::new(&test_dsn(), &PoolConfig::default(), &config).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        backend.eligible_readers(),
        0,
        "unknown replay position must not bypass the lag limit"
    );
}

#[tokio::test]
async fn dropping_the_replica_backend_stops_its_monitor() {
    let application = "pgvis_monitor_drop_regression";
    let dsn = format!(
        "{}{}application_name={application}",
        test_dsn(),
        if test_dsn().contains('?') { "&" } else { "?" }
    );
    let config = ReplicaConfig {
        replica_dsns: vec![dsn.clone()],
        max_replication_lag_bytes: 0,
        health_check_interval_ms: 50,
        primary_reads: false,
    };
    let backend = PgReplicaBackend::new(&dsn, &PoolConfig::default(), &config).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(backend);
    let (client, connection) = tokio_postgres::connect(&test_dsn(), tokio_postgres::NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(connection);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let count: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
                &[&application],
            )
            .await
            .unwrap()
            .get(0);
        if count == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "monitor retained {count} connections after backend drop"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    task.abort();
}
