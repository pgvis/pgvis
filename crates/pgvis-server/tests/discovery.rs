//! Opt-in Linux fixture with real primary + two direct physical standbys.
//! Requires passwordless sudo, iproute2, PostgreSQL tools and an isolated host.
//! PGVIS_DISCOVERY_NETNS=1 cargo test -p pgvis-server --test discovery -- --ignored --nocapture
//! Network namespaces give WAL receivers the same source IP as their query endpoint.
#![cfg(target_os = "linux")]

use pgvis_core::{
    Backend,
    backend::ExecContext,
    config::{PoolConfig, ReplicaConfig, ReplicaDiscoveryConfig},
};
use pgvis_lib::{
    pgvis_postgres::PgReplicaBackend,
    pgvis_router::{AppState, CallerIdentity},
};
use serde_json::json;
use std::{collections::HashMap, path::PathBuf, process::Command, time::Duration};

fn command(program: &str, args: &[&str]) {
    let output = Command::new(program)
        .args(args)
        .output()
        .expect("fixture tool missing");
    assert!(
        output.status.success(),
        "{program} {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
struct Cluster {
    root: PathBuf,
    prefix: String,
    subnet: String,
    port: u16,
    pg_ctl: String,
    root_created: bool,
    bridge_created: bool,
    namespaces_created: Vec<usize>,
    links_created: Vec<usize>,
}
impl Cluster {
    fn ip(&self, node: usize) -> String {
        format!("{}.{}", self.subnet, node)
    }
    fn namespace(&self, node: usize) -> String {
        format!("{}n{node}", self.prefix)
    }
    fn pg(&self, node: usize, action: &str) {
        let data = self.root.join(node.to_string());
        let log = self.root.join(format!("{node}.log"));
        let sockets = self.root.join(format!("sockets{node}"));
        std::fs::create_dir_all(&sockets).unwrap();
        let opts = format!(
            "-p {} -h {} -k {}",
            self.port,
            self.ip(node),
            sockets.display()
        );
        let args = [
            "-D",
            data.to_str().unwrap(),
            "-l",
            log.to_str().unwrap(),
            "-o",
            &opts,
            "-w",
            "-t",
            "20",
            "-m",
            "fast",
            action,
        ];
        let output = if node == 1 {
            Command::new(&self.pg_ctl).args(args).output().unwrap()
        } else {
            let ns = self.namespace(node);
            let mut all = vec![
                "ip",
                "netns",
                "exec",
                &ns,
                "runuser",
                "-u",
                "ubuntu",
                "--",
                &self.pg_ctl,
            ];
            all.extend(args);
            Command::new("sudo").args(all).output().unwrap()
        };
        assert!(
            output.status.success(),
            "pg_ctl {node} {action}: {}\n{}",
            String::from_utf8_lossy(&output.stderr),
            std::fs::read_to_string(log).unwrap_or_default()
        );
    }
    fn new() -> Self {
        assert_eq!(
            std::env::var("PGVIS_DISCOVERY_NETNS").as_deref(),
            Ok("1"),
            "explicit isolated-host opt-in required"
        );
        assert_eq!(
            std::env::var("USER").as_deref(),
            Ok("ubuntu"),
            "fixture runuser expects disposable Ubuntu host"
        );
        let pid = std::process::id();
        let pg_ctl = String::from_utf8(
            Command::new("sh")
                .args(["-c", "command -v pg_ctl"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        let mut c = Self {
            root: std::env::temp_dir().join(format!("pgvis-discovery-{pid}")),
            prefix: format!("pvd{}", pid % 100000),
            subnet: format!("10.231.{}", pid % 200 + 20),
            port: std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port(),
            pg_ctl,
            root_created: false,
            bridge_created: false,
            namespaces_created: Vec::new(),
            links_created: Vec::new(),
        };
        std::fs::create_dir(&c.root).unwrap();
        c.root_created = true;
        let bridge = format!("{}b", c.prefix);
        command("sudo", &["ip", "link", "add", &bridge, "type", "bridge"]);
        c.bridge_created = true;
        command(
            "sudo",
            &[
                "ip",
                "addr",
                "add",
                &format!("{}/24", c.ip(1)),
                "dev",
                &bridge,
            ],
        );
        command("sudo", &["ip", "link", "set", &bridge, "up"]);
        for node in [2, 3] {
            let ns = c.namespace(node);
            let host = format!("{}h{node}", c.prefix);
            let peer = format!("{}p{node}", c.prefix);
            command("sudo", &["ip", "netns", "add", &ns]);
            c.namespaces_created.push(node);
            command(
                "sudo",
                &[
                    "ip", "link", "add", &host, "type", "veth", "peer", "name", &peer,
                ],
            );
            c.links_created.push(node);
            command("sudo", &["ip", "link", "set", &host, "master", &bridge]);
            command("sudo", &["ip", "link", "set", &host, "up"]);
            command("sudo", &["ip", "link", "set", &peer, "netns", &ns]);
            command(
                "sudo",
                &[
                    "ip",
                    "-n",
                    &ns,
                    "addr",
                    "add",
                    &format!("{}/24", c.ip(node)),
                    "dev",
                    &peer,
                ],
            );
            command("sudo", &["ip", "-n", &ns, "link", "set", &peer, "up"]);
            command("sudo", &["ip", "-n", &ns, "link", "set", "lo", "up"]);
        }
        let primary = c.root.join("1");
        command(
            "initdb",
            &[
                "-D",
                primary.to_str().unwrap(),
                "-U",
                "pgvis_discovery",
                "-A",
                "trust",
                "--no-locale",
            ],
        );
        std::fs::write(primary.join("postgresql.auto.conf"), "wal_level=logical\nmax_wal_senders=8\nmax_connections=80\nhot_standby=on\nwal_sender_timeout=1000\nwal_retrieve_retry_interval=100\n").unwrap();
        use std::io::Write;
        writeln!(
            std::fs::OpenOptions::new()
                .append(true)
                .open(primary.join("pg_hba.conf"))
                .unwrap(),
            "host all all {}.0/24 trust\nhost replication all {}.0/24 trust",
            c.subnet,
            c.subnet
        )
        .unwrap();
        c.pg(1, "start");
        for node in [2, 3] {
            command(
                "pg_basebackup",
                &[
                    "-D",
                    c.root.join(node.to_string()).to_str().unwrap(),
                    "-h",
                    &c.ip(1),
                    "-p",
                    &c.port.to_string(),
                    "-U",
                    "pgvis_discovery",
                    "-X",
                    "stream",
                    "-R",
                ],
            );
            c.pg(node, "start");
        }
        c
    }
    fn dsn(&self, node: usize) -> String {
        format!(
            "host={} port={} user=pgvis_discovery dbname=postgres sslmode=disable application_name=pgvis_discovery_test",
            self.ip(node),
            self.port
        )
    }
    fn config(&self) -> ReplicaConfig {
        ReplicaConfig {
            primary_reads: false,
            health_check_interval_ms: 100,
            max_replication_lag_bytes: 16384,
            discovery: Some(ReplicaDiscoveryConfig {
                trusted_cidrs: vec![format!("{}.0/24", self.subnet)],
                allow_insecure: true,
                max_nodes: 2,
                refresh_interval_ms: 100,
                stale_after_ms: 600,
                port: None,
            }),
            ..Default::default()
        }
    }
    async fn receiver(&self, client: &tokio_postgres::Client, enabled: bool) {
        let port = if enabled { self.port } else { 1 };
        client.batch_execute(&format!("ALTER SYSTEM SET primary_conninfo = 'host={} port={port} user=pgvis_discovery connect_timeout=1'", self.ip(1))).await.unwrap();
        client
            .batch_execute("SELECT pg_reload_conf()")
            .await
            .unwrap();
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        if !self.root_created {
            return;
        }
        for node in [3, 2, 1] {
            let data = self.root.join(node.to_string());
            if node == 1 {
                let _ = Command::new(&self.pg_ctl)
                    .args([
                        "-D",
                        data.to_str().unwrap(),
                        "-m",
                        "immediate",
                        "-w",
                        "stop",
                    ])
                    .output();
            } else if self.namespaces_created.contains(&node) {
                let ns = self.namespace(node);
                let _ = Command::new("sudo")
                    .args([
                        "ip",
                        "netns",
                        "exec",
                        &ns,
                        "runuser",
                        "-u",
                        "ubuntu",
                        "--",
                        &self.pg_ctl,
                        "-D",
                        data.to_str().unwrap(),
                        "-m",
                        "immediate",
                        "-w",
                        "stop",
                    ])
                    .output();
                let _ = Command::new("sudo")
                    .args(["ip", "netns", "delete", &ns])
                    .output();
                if self.links_created.contains(&node) {
                    let _ = Command::new("sudo")
                        .args(["ip", "link", "delete", &format!("{}h{node}", self.prefix)])
                        .output();
                }
            }
        }
        if self.bridge_created {
            let _ = Command::new("sudo")
                .args(["ip", "link", "delete", &format!("{}b", self.prefix)])
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
    let end = tokio::time::Instant::now() + Duration::from_secs(20);
    while !check().await {
        assert!(tokio::time::Instant::now() < end, "timed out: {label}");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}
async fn rpc(state: &AppState, name: &str) -> String {
    state
        .call_rpc_scalar::<String>(
            "discovery_fixture",
            name,
            json!({}),
            &CallerIdentity::anonymous(),
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "creates isolated Linux network namespaces and three PostgreSQL clusters"]
async fn streaming_discovery_routes_drains_recovers_and_fails_closed() {
    let c = Cluster::new();
    let primary = connect(&c.dsn(1)).await;
    let standby2 = connect(&c.dsn(2)).await;
    let standby3 = connect(&c.dsn(3)).await;
    primary.batch_execute(r#"
        CREATE SCHEMA discovery_fixture;
        CREATE TABLE discovery_fixture.items(id int primary key);
        INSERT INTO discovery_fixture.items VALUES (1);
        CREATE TABLE discovery_fixture.restricted(id int);
        CREATE UNLOGGED TABLE discovery_fixture.unlogged(id int);
        INSERT INTO discovery_fixture.unlogged VALUES (1);
        ALTER TABLE discovery_fixture.restricted ENABLE ROW LEVEL SECURITY;
        CREATE VIEW discovery_fixture.location AS SELECT host(inet_server_addr()) AS address;
        CREATE FUNCTION discovery_fixture.stable_address() RETURNS text LANGUAGE sql STABLE AS 'SELECT host(inet_server_addr())';
        CREATE FUNCTION discovery_fixture.immutable_address() RETURNS text LANGUAGE sql IMMUTABLE AS 'SELECT host(inet_server_addr())';
        CREATE FUNCTION discovery_fixture.volatile_address() RETURNS text LANGUAGE sql VOLATILE AS 'SELECT host(inet_server_addr())';
        CREATE ROLE discovery_limited LOGIN;
        GRANT pg_read_all_stats TO discovery_limited;
        GRANT EXECUTE ON FUNCTION pg_control_system(), pg_control_checkpoint() TO discovery_limited;
    "#).await.unwrap();
    primary
        .batch_execute("CREATE PUBLICATION discovery_publication")
        .await
        .unwrap();
    primary
        .query_one(
            "SELECT * FROM pg_create_logical_replication_slot('discovery_logical', 'pgoutput')",
            &[],
        )
        .await
        .unwrap();
    let logical_dsn = c.dsn(1).replace(
        "application_name=pgvis_discovery_test",
        "application_name=pgvis_discovery_logical",
    );
    let _logical = tokio::process::Command::new("pg_recvlogical")
        .args([
            "--dbname",
            &logical_dsn,
            "--slot",
            "discovery_logical",
            "--start",
            "--file",
            "/dev/null",
            "--no-loop",
            "-o",
            "proto_version=1",
            "-o",
            "publication_names=discovery_publication",
        ])
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    eventually("logical sender streaming", async || primary.query_one(
        "SELECT count(*) FROM pg_stat_replication r JOIN pg_replication_slots s ON s.active_pid=r.pid WHERE s.slot_type='logical' AND r.state='streaming'", &[])
        .await.unwrap().get::<_,i64>(0) == 1).await;
    let pool = PoolConfig {
        size: 1,
        timeout_ms: 500,
        query_timeout_ms: 10000,
        ..Default::default()
    };
    let config = pgvis_core::Config {
        schemas: vec!["discovery_fixture".into()],
        replica: c.config(),
        pool: pool.clone(),
        ..Default::default()
    };
    let components = pgvis_lib::Builder::new(c.dsn(1))
        .config(config)
        .build_components()
        .await
        .unwrap();
    let backend = components.backend.clone();
    let state = AppState::new(
        components.cache.clone(),
        components.config.clone(),
        components.dialect.clone(),
        backend.clone(),
    );
    eventually("two discovered standbys", async || {
        backend
            .reader_pool_status()
            .iter()
            .filter(|r| r.discovered && r.eligible)
            .count()
            == 2
    })
    .await;
    let original_ids: Vec<_> = backend
        .reader_pool_status()
        .iter()
        .map(|r| r.id.clone())
        .collect();
    for name in ["stable_address", "immutable_address"] {
        let address = rpc(&state, name).await;
        assert!(
            [c.ip(2), c.ip(3)].contains(&address),
            "{name} routed to {address}"
        );
    }
    assert_eq!(rpc(&state, "volatile_address").await, c.ip(1));
    let reader_queries = || {
        backend
            .reader_pool_status()
            .iter()
            .map(|r| r.pool.metrics.queries)
            .sum::<u64>()
    };
    let before = reader_queries();
    state
        .call_read(
            "discovery_fixture",
            "items",
            &HashMap::new(),
            &CallerIdentity::anonymous(),
        )
        .await
        .unwrap();
    assert_eq!(
        reader_queries(),
        before + 1,
        "table read must execute on a discovered pool"
    );
    let view = state
        .call_read(
            "discovery_fixture",
            "location",
            &HashMap::new(),
            &CallerIdentity::anonymous(),
        )
        .await
        .unwrap();
    assert_eq!(view.body[0]["address"], c.ip(1));
    let before = reader_queries();
    state
        .call_read(
            "discovery_fixture",
            "restricted",
            &HashMap::new(),
            &CallerIdentity::anonymous(),
        )
        .await
        .unwrap();
    let unlogged = state
        .call_read(
            "discovery_fixture",
            "unlogged",
            &HashMap::new(),
            &CallerIdentity::anonymous(),
        )
        .await
        .unwrap();
    assert_eq!(unlogged.body[0]["id"], 1);
    assert_eq!(
        reader_queries(),
        before,
        "RLS and unlogged tables stay primary"
    );
    primary
        .batch_execute("ALTER FUNCTION discovery_fixture.stable_address() VOLATILE")
        .await
        .unwrap();
    components.reloader.reload_now().await.unwrap();
    assert_eq!(
        rpc(&state, "stable_address").await,
        c.ip(1),
        "routing must follow reloaded volatility"
    );
    let primary_only = ExecContext {
        require_primary: true,
        ..Default::default()
    };
    assert_eq!(
        backend
            .execute(
                &primary_only,
                "SELECT json_build_array(host(inet_server_addr())) AS body",
                &[]
            )
            .await
            .unwrap()
            .body,
        json!([c.ip(1)])
    );
    // A hook is not classified by the main plan's volatility and stays primary.
    primary.batch_execute("CREATE SEQUENCE discovery_fixture.hook_calls; CREATE FUNCTION discovery_fixture.hook() RETURNS void LANGUAGE sql AS 'SELECT nextval(''discovery_fixture.hook_calls'')'").await.unwrap();
    let hook = ExecContext {
        pre_request: Some("discovery_fixture.hook".into()),
        ..Default::default()
    };
    assert_eq!(
        backend
            .execute(
                &hook,
                "SELECT json_build_array(host(inet_server_addr())) AS body",
                &[]
            )
            .await
            .unwrap()
            .body,
        json!([c.ip(1)])
    );
    assert_eq!(
        backend
            .reader_pool_status()
            .iter()
            .map(|r| r.id.clone())
            .collect::<Vec<_>>(),
        original_ids,
        "refresh reuses pool identities"
    );
    eprintln!(
        "routing: table/STABLE/IMMUTABLE -> standbys; VOLATILE/view/RLS/unlogged/hook/forced -> primary; schema reload verified"
    );

    standby2
        .batch_execute("SELECT pg_wal_replay_pause()")
        .await
        .unwrap();
    primary
        .batch_execute("INSERT INTO discovery_fixture.items SELECT generate_series(2,10000)")
        .await
        .unwrap();
    eventually("discovered standby replay lag excluded", async || {
        backend.reader_pool_status().iter().any(|r| {
            r.id.ends_with(&format!("{}:{}", c.ip(2), c.port))
                && r.status == "lagging"
                && !r.eligible
        })
    })
    .await;
    let result = backend
        .execute(
            &ExecContext::default(),
            "SELECT json_build_array(host(inet_server_addr())) AS body",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.body, json!([c.ip(3)]));
    standby2
        .batch_execute("SELECT pg_wal_replay_resume()")
        .await
        .unwrap();
    eventually("discovered standby replay caught up", async || {
        backend
            .reader_pool_status()
            .iter()
            .filter(|r| r.eligible)
            .count()
            == 2
    })
    .await;
    eprintln!("lag: paused discovered standby excluded; resumed replay restored eligibility");

    let busy = {
        let backend = backend.clone();
        tokio::spawn(async move {
            backend
                .execute(
                    &ExecContext::default(),
                    "SELECT pg_sleep(6)::text, json_build_array(host(inet_server_addr())) AS body",
                    &[],
                )
                .await
        })
    };
    eventually("in-flight reader acquired", async || {
        backend
            .reader_pool_status()
            .iter()
            .any(|r| r.pool.size == 1 && r.pool.available == 0)
    })
    .await;
    let held = backend
        .reader_pool_status()
        .into_iter()
        .find(|r| r.pool.size == 1 && r.pool.available == 0)
        .unwrap();
    let removed_node = if held.id.ends_with(&format!("{}:{}", c.ip(2), c.port)) {
        2
    } else {
        3
    };
    let removed = if removed_node == 2 {
        &standby2
    } else {
        &standby3
    };
    c.receiver(removed, false).await;
    eventually("removed pool retires while read continues", async || {
        backend
            .reader_pool_status()
            .iter()
            .any(|r| r.id == held.id && r.retiring && r.pool.size == 1)
    })
    .await;
    assert!(
        !busy.is_finished(),
        "removal cancelled an already checked-out read"
    );
    let attempts = backend
        .reader_pool_status()
        .iter()
        .find(|r| r.id == held.id)
        .unwrap()
        .pool
        .metrics
        .checkout_attempts;
    for _ in 0..8 {
        let result = backend
            .execute(
                &ExecContext::default(),
                "SELECT json_build_array(host(inet_server_addr())) AS body",
                &[],
            )
            .await
            .unwrap();
        assert_ne!(result.body, json!([c.ip(removed_node)]));
    }
    assert_eq!(
        backend
            .reader_pool_status()
            .iter()
            .find(|r| r.id == held.id)
            .unwrap()
            .pool
            .metrics
            .checkout_attempts,
        attempts
    );
    for _ in 0..2 {
        c.receiver(removed, true).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let metrics = backend.replica_discovery_status().unwrap();
        assert_eq!(metrics.active_nodes + metrics.retiring_nodes, 2);
        assert_eq!(
            metrics.retiring_nodes, 1,
            "flapping must not revive retired pool"
        );
        c.receiver(removed, false).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    c.receiver(removed, true).await;
    assert_eq!(
        busy.await.unwrap().unwrap().body,
        json!([c.ip(removed_node)])
    );
    eventually("drained slot reusable", async || {
        backend
            .reader_pool_status()
            .iter()
            .filter(|r| r.eligible)
            .count()
            == 2
    })
    .await;
    assert!(
        backend
            .reader_pool_status()
            .iter()
            .all(|r| r.pool.size <= 1 && !r.retiring)
    );
    eprintln!(
        "lifecycle: removal stopped new checkout; in-flight read completed; flapping retained bounded draining slots; re-add recovered"
    );

    let limited_dsn = c
        .dsn(1)
        .replace("user=pgvis_discovery ", "user=discovery_limited ");
    let limited = PgReplicaBackend::new(&limited_dsn, &pool, &c.config()).unwrap();
    eventually("limited monitor granted metadata", async || {
        limited.eligible_readers() == 2
    })
    .await;
    primary
        .batch_execute("REVOKE pg_read_all_stats FROM discovery_limited")
        .await
        .unwrap();
    eventually(
        "masked metadata fails closed and stale pools retire",
        async || {
            let d = limited.replica_discovery_status().unwrap();
            d.failed_refreshes > 0
                && d.active_nodes == 0
                && d.retiring_nodes == 0
                && limited.eligible_readers() == 0
        },
    )
    .await;
    primary
        .batch_execute("GRANT pg_read_all_stats TO discovery_limited")
        .await
        .unwrap();
    eventually("metadata privilege recovery", async || {
        limited.eligible_readers() == 2
    })
    .await;
    drop(limited);
    eprintln!(
        "metadata: privilege loss disabled routing, stale pools retired, restored privileges rediscovered standbys"
    );

    let mut denied_config = c.config();
    denied_config.discovery.as_mut().unwrap().trusted_cidrs = vec!["127.0.0.1/32".into()];
    let denied_dsn = c.dsn(1).replace(
        "application_name=pgvis_discovery_test",
        "application_name=pgvis_discovery_denied",
    );
    let denied = PgReplicaBackend::new(&denied_dsn, &pool, &denied_config).unwrap();
    eventually(
        "untrusted destination rejected before pool creation",
        async || denied.replica_discovery_status().unwrap().status == "replica_address_not_trusted",
    )
    .await;
    assert!(denied.reader_pool_status().is_empty());
    for standby in [&standby2, &standby3] {
        assert_eq!(standby.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name='pgvis_discovery_denied'", &[]).await.unwrap().get::<_,i64>(0), 0);
    }
    drop(denied);
    eprintln!(
        "trust: denied addresses received no connection; logical streaming sender excluded from discovery"
    );

    // Keep monitor stale deliberately: the transaction-level guard must catch promotion.
    let race_config = ReplicaConfig {
        replica_dsns: vec![c.dsn(2)],
        primary_reads: false,
        health_check_interval_ms: 60000,
        ..Default::default()
    };
    let race = PgReplicaBackend::new(&c.dsn(1), &pool, &race_config).unwrap();
    eventually("promotion race reader eligible", async || {
        race.eligible_readers() == 1
    })
    .await;
    race.execute(&ExecContext::default(), "SELECT '[]'::json AS body", &[])
        .await
        .unwrap();
    c.pg(2, "promote");
    let error = race
        .execute(&ExecContext::default(), "SELECT '[]'::json AS body", &[])
        .await
        .unwrap_err();
    assert!(
        matches!(error, pgvis_core::Error::Execution { db_code: Some(ref code), .. } if code == "22012")
    );
    assert_eq!(
        race.pool_status().unwrap().metrics.queries,
        0,
        "promoted-reader error must not replay on primary"
    );
    drop(race);
    drop(state);
    drop(components);
    drop(backend);
    eventually("monitor and pool cleanup", async || {
        primary.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name='pgvis_discovery_test' AND pid <> pg_backend_pid()", &[]).await.unwrap().get::<_,i64>(0) == 0
    }).await;
    eprintln!(
        "promotion: recovery guard rejected pooled promoted reader without replay; backend lifecycle released primary sessions"
    );
}
