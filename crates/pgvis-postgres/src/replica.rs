//! Replica routing with bounded static and discovered pool lifetimes.
//! Promotion remains the responsibility of an external HA system. No request is
//! replayed after SQL execution begins, including reads that fail READ ONLY.
use crate::discovery::{self, Discovery, Endpoint, PrimaryState, Snapshot};
use crate::execute::ReadMode;
use crate::pool::ManagedPool;
use futures::future::BoxFuture;
use pgvis_core::backend::{
    Backend, ExecContext, IntrospectConfig, QueryResult, ReaderPoolStatus, ReplicaDiscoveryStatus,
    SchemaChangeStream,
};
use pgvis_core::cache::SchemaCache;
use pgvis_core::config::{PoolConfig, ReplicaConfig};
use pgvis_core::dialect::{self, Dialect};
use pgvis_core::error::Error;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_READERS: usize = 64;

struct Reader {
    id: String,
    endpoint: Option<Endpoint>,
    system_id: Option<String>,
    primary: bool,
    standby: bool,
    pool: ManagedPool,
    probe: ManagedPool,
    eligible: AtomicBool,
    retired: AtomicBool,
    reason: Mutex<&'static str>,
}
impl Reader {
    fn set_status(&self, result: Result<(), &'static str>) {
        let mut reason = self.reason.lock().expect("reader status");
        if self.retired.load(Relaxed) {
            return;
        }
        *reason = result.err().unwrap_or("healthy");
        self.eligible.store(result.is_ok(), Relaxed);
    }
    fn retire(&self) {
        let mut reason = self.reason.lock().expect("reader status");
        self.retired.store(true, Relaxed);
        self.eligible.store(false, Relaxed);
        *reason = "retiring";
        self.pool.retire();
        self.probe.retire();
    }
    fn drained(&self) -> bool {
        self.pool.drained() && self.probe.drained()
    }
}

#[derive(Default)]
struct Topology {
    readers: Vec<Arc<Reader>>,
    retiring: Vec<Arc<Reader>>,
    last_success: Option<Instant>,
    successful_refreshes: u64,
    failed_refreshes: u64,
    discovery_reason: &'static str,
    identity: Option<PrimaryState>,
    stale_after: Duration,
}
impl Topology {
    fn eligible(&self, reader: &Reader) -> bool {
        reader.eligible.load(Relaxed)
            && !reader.retired.load(Relaxed)
            && (reader.endpoint.is_none()
                || (self.discovery_reason == "healthy"
                    && self
                        .last_success
                        .is_some_and(|t| t.elapsed() < self.stale_after)))
    }

    fn discovered_count(&self) -> usize {
        self.readers.iter().filter(|r| r.endpoint.is_some()).count()
    }

    fn refresh(
        &mut self,
        snapshot: Snapshot,
        discovery: &Discovery,
        pool_cfg: &PoolConfig,
        probe_cfg: &PoolConfig,
    ) -> Result<(), Error> {
        self.retiring.retain(|r| !r.drained());
        self.readers.retain(|r| {
            let remove = r.endpoint.as_ref().is_some_and(|e| {
                !snapshot.endpoints.contains(e)
                    || r.system_id.as_ref() != Some(&snapshot.primary.system_id)
            });
            if remove {
                r.retire();
                self.retiring.push(r.clone());
            }
            !remove
        });
        for endpoint in snapshot.endpoints {
            if self
                .readers
                .iter()
                .any(|r| r.endpoint.as_ref() == Some(&endpoint))
            {
                continue;
            }
            // Retiring pools consume a slot until their requests AND connection
            // attempts drain. Flapping cannot multiply the aggregate ceiling.
            if self.discovered_count() + self.retiring.len() >= discovery.config.max_nodes {
                break;
            }
            let pg = discovery.connection_config(&endpoint);
            let pool = ManagedPool::from_config(pg.clone(), pool_cfg)?;
            let probe = ManagedPool::from_config(pg, probe_cfg)?;
            self.readers.push(Arc::new(Reader {
                id: format!("{}:{}", snapshot.primary.system_id, endpoint.id()),
                endpoint: Some(endpoint),
                system_id: Some(snapshot.primary.system_id.clone()),
                primary: false,
                standby: true,
                pool,
                probe,
                eligible: AtomicBool::new(false),
                retired: AtomicBool::new(false),
                reason: Mutex::new("unverified"),
            }));
        }
        self.identity = Some(snapshot.primary);
        self.last_success = Some(Instant::now());
        self.successful_refreshes = self.successful_refreshes.saturating_add(1);
        self.discovery_reason = "healthy";
        Ok(())
    }

    fn failed_refresh(&mut self, reason: &'static str, stale: Duration) {
        self.failed_refreshes = self.failed_refreshes.saturating_add(1);
        self.discovery_reason = reason;
        let expired = self.last_success.is_none_or(|t| t.elapsed() >= stale);
        self.readers.retain(|r| {
            if r.endpoint.is_none() {
                return true;
            }
            r.set_status(Err(reason));
            if expired {
                r.retire();
                self.retiring.push(r.clone());
            }
            !expired
        });
        self.retiring.retain(|r| !r.drained());
    }
}

/// PostgreSQL primary plus bounded, independently monitored reader pools.
pub struct PgReplicaBackend {
    primary: ManagedPool,
    primary_dsn: String,
    topology: Arc<Mutex<Topology>>,
    next: AtomicUsize,
    discovery_enabled: bool,
    monitor_handle: tokio::task::JoinHandle<()>,
}

impl PgReplicaBackend {
    pub fn new(
        primary_dsn: &str,
        pool_cfg: &PoolConfig,
        config: &ReplicaConfig,
    ) -> Result<Self, Error> {
        if config.health_check_interval_ms == 0 {
            return Err(Error::Introspection(
                "replica health_check_interval_ms must be positive".into(),
            ));
        }
        let discovery = config
            .discovery
            .as_ref()
            .map(|c| Discovery::new(primary_dsn, c))
            .transpose()?;
        let reserved = discovery.as_ref().map_or(0, |d| d.config.max_nodes);
        if config.replica_dsns.len() + usize::from(config.primary_reads) + reserved > MAX_READERS {
            return Err(Error::Introspection(
                "static readers plus discovery max_nodes exceed the 64-reader limit".into(),
            ));
        }
        let primary = ManagedPool::new(primary_dsn, pool_cfg)?;
        let probe_ms = config.health_check_interval_ms.clamp(500, 5000);
        let probe_cfg = PoolConfig {
            size: 1,
            max_waiters: 1,
            adaptive: None,
            timeout_ms: probe_ms,
            create_timeout_ms: probe_ms,
            recycle_timeout_ms: probe_ms,
            query_timeout_ms: probe_ms,
            ..pool_cfg.clone()
        };
        let monitor_primary = ManagedPool::new(primary_dsn, &probe_cfg)?;
        let mut topology = Topology {
            discovery_reason: "unverified",
            stale_after: discovery.as_ref().map_or(Duration::ZERO, |d| {
                Duration::from_millis(d.config.stale_after_ms)
            }),
            ..Default::default()
        };
        for (index, dsn) in config.replica_dsns.iter().enumerate() {
            topology.readers.push(Arc::new(Reader {
                id: format!("static:{index}"),
                endpoint: None,
                system_id: None,
                primary: false,
                standby: config.max_replication_lag_bytes > 0,
                pool: ManagedPool::new(dsn, pool_cfg)?,
                probe: ManagedPool::new(dsn, &probe_cfg)?,
                eligible: AtomicBool::new(config.max_replication_lag_bytes == 0),
                retired: AtomicBool::new(false),
                reason: Mutex::new("unverified"),
            }));
        }
        if config.primary_reads {
            topology.readers.push(Arc::new(Reader {
                id: "primary".into(),
                endpoint: None,
                system_id: None,
                primary: true,
                standby: false,
                pool: primary.clone(),
                probe: monitor_primary.clone(),
                eligible: AtomicBool::new(true),
                retired: AtomicBool::new(false),
                reason: Mutex::new("healthy"),
            }));
        }
        let topology = Arc::new(Mutex::new(topology));
        let monitor_handle = tokio::spawn(health_monitor_loop(
            monitor_primary,
            topology.clone(),
            config.clone(),
            discovery,
            pool_cfg.clone(),
            probe_cfg,
            Duration::from_millis(probe_ms),
        ));
        Ok(Self {
            primary,
            primary_dsn: primary_dsn.to_owned(),
            topology,
            next: AtomicUsize::new(0),
            discovery_enabled: config.discovery.is_some(),
            monitor_handle,
        })
    }

    pub fn eligible_readers(&self) -> u32 {
        let t = self.topology.lock().expect("replica topology");
        t.readers.iter().filter(|r| t.eligible(r)).count() as u32
    }

    fn pick_read_pool(&self) -> Option<Arc<Reader>> {
        let topology = self.topology.lock().expect("replica topology");
        let eligible = || topology.readers.iter().filter(|r| topology.eligible(r));
        let count = eligible().count();
        if count == 0 {
            return None;
        }
        // No await or network operation while the snapshot lock is held.
        eligible()
            .nth(self.next.fetch_add(1, Relaxed) % count)
            .cloned()
    }
}

impl Drop for PgReplicaBackend {
    fn drop(&mut self) {
        self.monitor_handle.abort();
        let topology = self.topology.lock().expect("replica topology");
        for reader in topology.readers.iter().chain(&topology.retiring) {
            reader.retire();
        }
    }
}

impl Backend for PgReplicaBackend {
    fn introspect(&self, cfg: &IntrospectConfig) -> BoxFuture<'_, Result<SchemaCache, Error>> {
        let cfg = cfg.clone();
        Box::pin(async move { self.primary.introspect(&cfg).await })
    }
    fn execute(
        &self,
        ctx: &ExecContext,
        sql: &str,
        params: &[Value],
    ) -> BoxFuture<'_, Result<QueryResult, Error>> {
        let (ctx, sql, params) = (ctx.clone(), sql.to_owned(), params.to_vec());
        Box::pin(async move {
            let eligible_read =
                !ctx.is_mutation && !ctx.require_primary && ctx.pre_request.is_none();
            if eligible_read {
                // Only checkout failures can try another pool. Once execution
                // starts there is no retry, even for a mislabelled STABLE RPC.
                for _ in 0..2 {
                    let Some(reader) = self.pick_read_pool() else {
                        break;
                    };
                    if let Ok(checkout) = reader.pool.checkout().await {
                        let mode = if reader.primary {
                            ReadMode::ReadOnly
                        } else if reader.standby {
                            ReadMode::Standby
                        } else {
                            ReadMode::ReadOnly
                        };
                        return reader
                            .pool
                            .execute_mode(checkout, &ctx, &sql, &params, mode)
                            .await;
                    }
                }
            }
            let checkout = self.primary.checkout().await?;
            // Falling back must not turn a declared read into a writable
            // transaction (e.g. a STABLE wrapper around a VOLATILE write).
            self.primary
                .execute_mode(
                    checkout,
                    &ctx,
                    &sql,
                    &params,
                    if eligible_read {
                        ReadMode::ReadOnly
                    } else {
                        ReadMode::Writer
                    },
                )
                .await
        })
    }
    fn watch_schema(&self) -> BoxFuture<'_, Option<SchemaChangeStream>> {
        let dsn = self.primary_dsn.clone();
        Box::pin(async move { Some(crate::schema_watch::watch(dsn)) })
    }
    fn dialect(&self) -> &'static Dialect {
        &dialect::POSTGRES
    }
    fn pool_status(&self) -> Option<pgvis_core::backend::PoolStatus> {
        Some(self.primary.status())
    }
    fn reader_pool_status(&self) -> Vec<ReaderPoolStatus> {
        let t = self.topology.lock().expect("replica topology");
        t.readers
            .iter()
            .chain(&t.retiring)
            .enumerate()
            .map(|(index, r)| ReaderPoolStatus {
                index,
                id: r.id.clone(),
                discovered: r.endpoint.is_some(),
                retiring: r.retired.load(Relaxed),
                is_primary: r.primary,
                eligible: t.eligible(r),
                status: *r.reason.lock().expect("reader status"),
                pool: r.pool.status(),
            })
            .collect()
    }
    fn replica_discovery_status(&self) -> Option<ReplicaDiscoveryStatus> {
        if !self.discovery_enabled {
            return None;
        }
        let t = self.topology.lock().expect("replica topology");
        Some(ReplicaDiscoveryStatus {
            status: t.discovery_reason,
            successful_refreshes: t.successful_refreshes,
            failed_refreshes: t.failed_refreshes,
            last_success_age_ms: t
                .last_success
                .map(|s| s.elapsed().as_millis().min(u64::MAX as u128) as u64),
            active_nodes: t.discovered_count(),
            retiring_nodes: t.retiring.len(),
        })
    }
}

async fn health_monitor_loop(
    primary: ManagedPool,
    topology: Arc<Mutex<Topology>>,
    config: ReplicaConfig,
    discovery: Option<Discovery>,
    pool_cfg: PoolConfig,
    probe_cfg: PoolConfig,
    timeout: Duration,
) {
    let interval = Duration::from_millis(
        config.health_check_interval_ms.min(
            discovery
                .as_ref()
                .map_or(u64::MAX, |d| d.config.refresh_interval_ms),
        ),
    );
    let mut last_refresh: Option<Instant> = None;
    loop {
        if let Some(d) = &discovery {
            if last_refresh
                .is_none_or(|t| t.elapsed() >= Duration::from_millis(d.config.refresh_interval_ms))
            {
                let snapshot = tokio::time::timeout(timeout, d.snapshot(&primary))
                    .await
                    .unwrap_or(Err("discovery_timeout"));
                let mut t = topology.lock().expect("replica topology");
                match snapshot {
                    Ok(snapshot) => {
                        if t.refresh(snapshot, d, &pool_cfg, &probe_cfg).is_err() {
                            t.failed_refresh("pool_configuration_error", Duration::ZERO);
                        }
                    }
                    Err(reason) => {
                        t.failed_refresh(reason, Duration::from_millis(d.config.stale_after_ms))
                    }
                }
                last_refresh = Some(Instant::now());
            }
        }
        let primary_state = if discovery.is_some() {
            tokio::time::timeout(timeout, discovery::primary_state(&primary))
                .await
                .ok()
                .and_then(Result::ok)
        } else {
            None
        };
        let primary_lsn = if let Some(state) = &primary_state {
            Some(state.lsn)
        } else if discovery.is_some() {
            None
        } else {
            tokio::time::timeout(timeout, get_primary_lsn(&primary))
                .await
                .ok()
                .flatten()
        };
        let (readers, discovered_valid) = {
            let t = topology.lock().expect("replica topology");
            let valid = t.discovery_reason == "healthy"
                && primary_state.as_ref().is_some_and(|p| {
                    t.identity
                        .as_ref()
                        .is_some_and(|i| i.system_id == p.system_id && i.timeline == p.timeline)
                });
            (t.readers.clone(), valid)
        };
        futures::future::join_all(readers.iter().map(|r| async {
            let check = async {
                if r.primary {
                    return primary_lsn.map(|_| ()).ok_or("primary_unreachable");
                }
                if r.endpoint.is_some() {
                    if !discovered_valid {
                        return Err("discovery_unverified");
                    }
                    discovery::check_discovered(
                        &r.probe,
                        primary_state.as_ref().expect("verified primary"),
                        config.max_replication_lag_bytes,
                    )
                    .await
                } else {
                    check_static(&r.probe, primary_lsn, config.max_replication_lag_bytes).await
                }
            };
            let result = tokio::time::timeout(timeout, check)
                .await
                .unwrap_or(Err("probe_timeout"));
            r.set_status(result);
        }))
        .await;
        // Prune only after all probe futures have relinquished their checkouts.
        topology
            .lock()
            .expect("replica topology")
            .retiring
            .retain(|r| !r.drained());
        tokio::time::sleep(interval).await;
    }
}

async fn get_primary_lsn(pool: &ManagedPool) -> Option<u64> {
    let conn = pool.checkout().await.ok()?;
    let row = conn
        .client()
        .query_one("SELECT pg_catalog.pg_current_wal_lsn() AS lsn", &[])
        .await
        .ok()?;
    let lsn = row.try_get::<_, tokio_postgres::types::PgLsn>("lsn").ok()?;
    conn.recycle();
    Some(lsn.into())
}

async fn check_static(
    pool: &ManagedPool,
    primary_lsn: Option<u64>,
    max_lag: u64,
) -> Result<(), &'static str> {
    let conn = pool.checkout().await.map_err(|_| "unreachable")?;
    // Preserve the documented static-DSN connectivity-only escape hatch.
    if max_lag == 0 {
        conn.client()
            .query_one("SELECT 1", &[])
            .await
            .map_err(|_| "unreachable")?;
    } else {
        let primary_lsn = primary_lsn.ok_or("primary_lsn_unknown")?;
        let row = conn
            .client()
            .query_one(
                "SELECT pg_catalog.pg_last_wal_replay_lsn() AS lsn, pg_catalog.pg_is_in_recovery() AS recovery",
                &[],
            )
            .await
            .map_err(|_| "unreachable")?;
        if !row.try_get::<_, bool>("recovery").unwrap_or(false) {
            return Err("not_in_recovery");
        }
        let lsn = row
            .try_get::<_, tokio_postgres::types::PgLsn>("lsn")
            .map_err(|_| "replay_lsn_unknown")?;
        if primary_lsn.saturating_sub(lsn.into()) > max_lag {
            return Err("lagging");
        }
    }
    conn.recycle();
    Ok(())
}
