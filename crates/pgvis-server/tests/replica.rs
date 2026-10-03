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
    assert_eq!(backend.eligible_readers(), 1, "healthy replica was excluded");
    busy.await.unwrap();
}
