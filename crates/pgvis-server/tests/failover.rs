//! Opt-in isolated streaming-replication fixture. Requires PostgreSQL tools in PATH.
//! Run: cargo test -p pgvis-server --test failover -- --ignored --nocapture

use pgvis_core::{
    Backend,
    backend::ExecContext,
    config::{PoolConfig, ReplicaConfig},
};
use pgvis_lib::pgvis_postgres::PgReplicaBackend;
use std::{path::PathBuf, process::Command, time::Duration};

struct Cluster {
    root: PathBuf,
    primary_port: u16,
    standby_port: u16,
}
impl Cluster {
    fn command(program: &str, args: &[&str]) {
        let output = Command::new(program)
            .args(args)
            .output()
            .expect("PostgreSQL test tool missing");
        assert!(
            output.status.success(),
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    fn start(&self, name: &str, port: u16) {
        let data = self.root.join(name);
        Self::command(
            "pg_ctl",
            &[
                "-D",
                data.to_str().unwrap(),
                "-l",
                self.root.join(format!("{name}.log")).to_str().unwrap(),
                "-o",
                &format!("-p {port} -h 127.0.0.1 -k {}", self.root.display()),
                "-w",
                "-t",
                "15",
                "start",
            ],
        );
    }
    fn control(&self, name: &str, action: &str) {
        Self::command(
            "pg_ctl",
            &[
                "-D",
                self.root.join(name).to_str().unwrap(),
                "-w",
                "-t",
                "15",
                "-m",
                "fast",
                action,
            ],
        );
    }
    fn new() -> Self {
        fn port() -> u16 {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        }
        let root = std::env::temp_dir().join(format!("pgvis-ha-{}", std::process::id()));
        std::fs::create_dir(&root).expect("fixture directory must be new");
        let c = Self {
            root,
            primary_port: port(),
            standby_port: port(),
        };
        let primary = c.root.join("primary");
        Self::command(
            "initdb",
            &[
                "-D",
                primary.to_str().unwrap(),
                "-U",
                "pgvis_ha",
                "-A",
                "trust",
                "--no-locale",
            ],
        );
        std::fs::write(
            primary.join("postgresql.auto.conf"),
            "wal_level=replica\nmax_wal_senders=4\nmax_connections=40\nhot_standby=on\n",
        )
        .unwrap();
        c.start("primary", c.primary_port);
        Self::command(
            "pg_basebackup",
            &[
                "-D",
                c.root.join("standby").to_str().unwrap(),
                "-h",
                "127.0.0.1",
                "-p",
                &c.primary_port.to_string(),
                "-U",
                "pgvis_ha",
                "-X",
                "stream",
                "-R",
            ],
        );
        c.start("standby", c.standby_port);
        c
    }
    fn dsn(&self, port: u16) -> String {
        format!("host=127.0.0.1 port={port} user=pgvis_ha dbname=postgres sslmode=disable")
    }
    fn writer_dsn(&self) -> String {
        format!(
            "host=127.0.0.1,127.0.0.1 port={},{} user=pgvis_ha dbname=postgres sslmode=disable target_session_attrs=read-write",
            self.primary_port, self.standby_port
        )
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for name in ["standby", "primary"] {
            let _ = Command::new("pg_ctl")
                .args([
                    "-D",
                    self.root.join(name).to_str().unwrap(),
                    "-w",
                    "-t",
                    "10",
                    "-m",
                    "immediate",
                    "stop",
                ])
                .output();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, driver) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    client
}
async fn eventually(label: &str, mut check: impl AsyncFnMut() -> bool) {
    let end = tokio::time::Instant::now() + Duration::from_secs(10);
    while !check().await {
        assert!(tokio::time::Instant::now() < end, "timed out: {label}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "starts disposable PostgreSQL primary and streaming standby"]
async fn replica_lag_recovery_primary_loss_and_external_promotion() {
    let cluster = Cluster::new();
    let primary = connect(&cluster.dsn(cluster.primary_port)).await;
    let standby = connect(&cluster.dsn(cluster.standby_port)).await;
    primary.batch_execute("CREATE TABLE ha_rows(id int primary key, payload text); CREATE SEQUENCE attempts; CREATE FUNCTION attempted_write() RETURNS json LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('attempts'); PERFORM pg_sleep(2); RETURN '[1]'::json; END $$;").await.unwrap();
    let pool = PoolConfig {
        size: 3,
        timeout_ms: 500,
        query_timeout_ms: 300,
        ..Default::default()
    };
    let replicas = ReplicaConfig {
        replica_dsns: vec![cluster.dsn(cluster.standby_port)],
        primary_reads: false,
        health_check_interval_ms: 50,
        max_replication_lag_bytes: 4096,
    };
    let backend = PgReplicaBackend::new(&cluster.writer_dsn(), &pool, &replicas).unwrap();
    eventually("streaming standby eligible", async || {
        backend.eligible_readers() == 1
    })
    .await;
    let reader = backend.reader_pool_status();
    assert_eq!(reader.len(), 1);
    assert!(reader[0].eligible && !reader[0].is_primary);
    assert_eq!(reader[0].pool.hard_max_size, 3);
    standby
        .batch_execute("SELECT pg_wal_replay_pause()")
        .await
        .unwrap();
    primary
        .batch_execute(
            "INSERT INTO ha_rows SELECT n, repeat(md5(n::text), 50) FROM generate_series(1,1000) n",
        )
        .await
        .unwrap();
    eventually("paused standby excluded", async || {
        backend.eligible_readers() == 0
    })
    .await;
    assert!(!backend.reader_pool_status()[0].eligible);
    let result = backend
        .execute(
            &ExecContext::default(),
            "SELECT json_build_array(count(*)) AS body FROM ha_rows",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.body, serde_json::json!([1000]));
    standby
        .batch_execute("SELECT pg_wal_replay_resume()")
        .await
        .unwrap();
    eventually("recovered standby eligible", async || {
        backend.eligible_readers() == 1
    })
    .await;
    let mutation = ExecContext {
        is_mutation: true,
        ..Default::default()
    };
    let failure = backend
        .execute(&mutation, "SELECT attempted_write() AS body", &[])
        .await
        .unwrap_err();
    assert_eq!(failure.http_status(), 504);
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(
        primary
            .query_one("SELECT last_value FROM attempts", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1,
        "timed-out mutation was replayed"
    );
    cluster.control("primary", "stop");
    eventually("unknown lag after writer outage excluded", async || {
        backend.eligible_readers() == 0
    })
    .await;
    let failure = backend
        .execute(&mutation, "SELECT '[]'::json AS body", &[])
        .await
        .unwrap_err();
    assert_eq!(failure.http_status(), 503);
    cluster.control("standby", "promote");
    eventually("external promotion available to writer DSN", async || {
        backend
            .execute(
                &mutation,
                "SELECT json_build_array(pg_is_in_recovery()) AS body",
                &[],
            )
            .await
            .is_ok_and(|r| r.body == serde_json::json!([false]))
    })
    .await;
    // Promotion leaves pg_last_wal_replay_lsn non-NULL; it is no longer a
    // replaying standby and must not remain in the replica routing set.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        backend.eligible_readers(),
        0,
        "promoted endpoint still accepted as a standby"
    );
    let result = backend.execute(&mutation, "WITH inserted AS (INSERT INTO ha_rows VALUES (1001, 'after promotion') RETURNING id) SELECT json_agg(id) AS body FROM inserted", &[]).await.unwrap();
    assert_eq!(result.body, serde_json::json!([1001]));
    assert_eq!(
        standby
            .query_one("SELECT count(*) FROM ha_rows", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1001
    );
    drop(backend);
    drop(primary);
    drop(standby);
}
