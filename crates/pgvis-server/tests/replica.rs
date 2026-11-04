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
        discovery: None,
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
        discovery: None,
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
        discovery: None,
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

#[tokio::test]
async fn reader_transactions_block_nested_volatile_writes_without_replaying() {
    let (client, connection) = tokio_postgres::connect(&test_dsn(), tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    client.batch_execute("DROP SCHEMA IF EXISTS replica_read_guard CASCADE; CREATE SCHEMA replica_read_guard; CREATE SEQUENCE replica_read_guard.attempts; CREATE FUNCTION replica_read_guard.volatile_write() RETURNS bigint LANGUAGE SQL VOLATILE AS 'SELECT nextval(''replica_read_guard.attempts'')'; CREATE FUNCTION replica_read_guard.stable_wrapper() RETURNS bigint LANGUAGE SQL STABLE AS 'SELECT replica_read_guard.volatile_write()';").await.unwrap();
    let config = ReplicaConfig {
        replica_dsns: vec![test_dsn()],
        primary_reads: false,
        max_replication_lag_bytes: 0,
        ..Default::default()
    };
    let backend = PgReplicaBackend::new(&test_dsn(), &PoolConfig::default(), &config).unwrap();
    let error = backend
        .execute(
            &ExecContext::default(),
            "SELECT json_build_array(replica_read_guard.stable_wrapper()) AS body",
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, pgvis_core::error::Error::Execution { db_code: Some(ref code), .. } if code == "25006")
    );
    assert!(
        !client
            .query_one("SELECT is_called FROM replica_read_guard.attempts", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    // The same read-only contract holds for a primary reader and primary
    // fallback after connection failures; neither may replay as a writable call.
    for replica in [
        ReplicaConfig::default(),
        ReplicaConfig {
            replica_dsns: vec![
                "host=127.0.0.1 port=1 user=unused dbname=unused sslmode=disable".into(),
            ],
            primary_reads: false,
            max_replication_lag_bytes: 0,
            ..Default::default()
        },
    ] {
        let fallback = PgReplicaBackend::new(
            &test_dsn(),
            &PoolConfig {
                timeout_ms: 200,
                create_timeout_ms: 200,
                ..Default::default()
            },
            &replica,
        )
        .unwrap();
        let error = fallback
            .execute(
                &ExecContext::default(),
                "SELECT json_build_array(replica_read_guard.stable_wrapper()) AS body",
                &[],
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, pgvis_core::error::Error::Execution { db_code: Some(ref code), .. } if code == "25006")
        );
    }
    assert!(
        !client
            .query_one("SELECT is_called FROM replica_read_guard.attempts", &[])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let forced = ExecContext {
        require_primary: true,
        ..Default::default()
    };
    let result = backend
        .execute(
            &forced,
            "SELECT json_build_array(replica_read_guard.stable_wrapper()) AS body",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.body, serde_json::json!([1]));
    client
        .batch_execute("DROP SCHEMA replica_read_guard CASCADE")
        .await
        .unwrap();
    driver.abort();
}

#[tokio::test]
async fn table_routing_rejects_unsafe_and_nonpermanent_relations() {
    let (client, connection) = tokio_postgres::connect(&test_dsn(), tokio_postgres::NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    client.batch_execute("DROP SCHEMA IF EXISTS replica_table_routing CASCADE; CREATE SCHEMA replica_table_routing;
        CREATE TABLE replica_table_routing.plain(id int);
        CREATE UNLOGGED TABLE replica_table_routing.unlogged(id int);
        CREATE TABLE replica_table_routing.parent(id int);
        CREATE TABLE replica_table_routing.child() INHERITS(replica_table_routing.parent);
        CREATE VIEW replica_table_routing.viewed AS SELECT * FROM replica_table_routing.plain;
        CREATE TABLE replica_table_routing.restricted(id int);
        ALTER TABLE replica_table_routing.restricted ENABLE ROW LEVEL SECURITY;
        CREATE TABLE replica_table_routing.partitioned(id int) PARTITION BY RANGE(id);
        CREATE TABLE replica_table_routing.leaf PARTITION OF replica_table_routing.partitioned FOR VALUES FROM (0) TO (100);").await.unwrap();
    let backend =
        pgvis_lib::pgvis_postgres::PgBackend::new(&test_dsn(), &PoolConfig::default()).unwrap();
    let cfg = pgvis_core::backend::IntrospectConfig {
        schemas: vec!["replica_table_routing".into()],
        ..Default::default()
    };
    let cache = backend.introspect(&cfg).await.unwrap();
    for (name, expected) in [
        ("plain", true),
        ("unlogged", false),
        ("parent", false),
        ("child", true),
        ("viewed", false),
        ("restricted", false),
        ("partitioned", true),
    ] {
        let id = pgvis_core::cache::QualifiedIdentifier::new("replica_table_routing", name);
        assert_eq!(cache.tables[&id].replica_readable, expected, "{name}");
    }
    client
        .batch_execute("ALTER TABLE replica_table_routing.leaf ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    let cache = backend.introspect(&cfg).await.unwrap();
    assert!(
        !cache.tables
            [&pgvis_core::cache::QualifiedIdentifier::new("replica_table_routing", "partitioned")]
            .replica_readable
    );

    client.batch_execute("ALTER TABLE replica_table_routing.leaf DISABLE ROW LEVEL SECURITY; ALTER TABLE replica_table_routing.leaf SET UNLOGGED").await.unwrap();
    let cache = backend.introspect(&cfg).await.unwrap();
    assert!(
        !cache.tables
            [&pgvis_core::cache::QualifiedIdentifier::new("replica_table_routing", "partitioned")]
            .replica_readable,
        "permanent parent with an unlogged partition must stay primary"
    );
    let components = pgvis_lib::Builder::new(test_dsn())
        .config(pgvis_core::Config {
            schemas: cfg.schemas.clone(),
            replica: ReplicaConfig {
                replica_dsns: vec![test_dsn()],
                primary_reads: false,
                max_replication_lag_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        })
        .build_components()
        .await
        .unwrap();
    let state = pgvis_lib::pgvis_router::AppState::new(
        components.cache.clone(),
        components.config.clone(),
        components.dialect.clone(),
        components.backend.clone(),
    );
    for table in ["unlogged", "partitioned", "parent"] {
        state
            .call_read(
                "replica_table_routing",
                table,
                &std::collections::HashMap::new(),
                &pgvis_lib::pgvis_router::CallerIdentity::anonymous(),
            )
            .await
            .unwrap();
    }
    assert_eq!(components.backend.pool_status().unwrap().metrics.queries, 3);
    assert_eq!(
        components.backend.reader_pool_status()[0]
            .pool
            .metrics
            .queries,
        0
    );
    state
        .call_read(
            "replica_table_routing",
            "plain",
            &std::collections::HashMap::new(),
            &pgvis_lib::pgvis_router::CallerIdentity::anonymous(),
        )
        .await
        .unwrap();
    assert_eq!(
        components.backend.reader_pool_status()[0]
            .pool
            .metrics
            .queries,
        1
    );
    client
        .batch_execute("DROP SCHEMA replica_table_routing CASCADE")
        .await
        .unwrap();
    driver.abort();
}
