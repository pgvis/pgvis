//! # Replica-aware Postgres backend with lag-based routing and load balancing.
//!
//! [`PgReplicaBackend`] wraps a primary connection pool and zero or more replica
//! pools. It implements [`Backend`] by routing:
//! - **Mutations** (`is_mutation = true`) → primary pool exclusively
//! - **Reads** (`is_mutation = false`) → round-robin across healthy replicas
//!   (and optionally the primary)
//!
//! A background [`HealthMonitor`] task periodically checks each replica's
//! replication lag against the primary and marks lagging/unreachable replicas
//! as ineligible for reads.
//!
//! ## Failover
//!
//! pgvis does not handle promotion. External tools (Patroni, pg_auto_failover,
//! DNS/VIP) handle that. When the primary DSN resolves to a new server,
//! `deadpool-postgres` transparently reconnects on the next pool checkout.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use deadpool_postgres::Pool;
use futures::future::BoxFuture;

use pgvis_core::backend::{
    Backend, ExecContext, IntrospectConfig, QueryResult, SchemaChangeStream,
};
use pgvis_core::cache::SchemaCache;
use pgvis_core::config::{PoolConfig, ReplicaConfig};
use pgvis_core::dialect::{self, Dialect};
use pgvis_core::error::Error;
use serde_json::Value;

use crate::pool::ManagedPool;

// ---------------------------------------------------------------------------
// Health state — shared between the monitor task and request routing
// ---------------------------------------------------------------------------

/// Shared health state updated atomically by the background monitor.
///
/// Uses a 64-bit bitfield to track which readers are eligible. Bit N corresponds
/// to reader index N in the `readers` vec (replicas first, then primary if
/// `primary_reads` is enabled). Maximum 64 readers supported.
struct HealthState {
    /// Bitfield: bit N is set if reader N is eligible for read queries.
    eligible: AtomicU64,
    /// Round-robin counter for load balancing.
    next: AtomicU32,
    /// Total number of readers (replicas + optionally primary).
    reader_count: u32,
}

impl HealthState {
    fn new(reader_count: u32) -> Self {
        // Start with all readers eligible.
        Self {
            eligible: AtomicU64::new(all_eligible_bits(reader_count)),
            next: AtomicU32::new(0),
            reader_count,
        }
    }

    /// Pick the next eligible reader index. Returns `None` if no readers are eligible.
    fn pick_reader(&self) -> Option<usize> {
        let eligible = self.eligible.load(Ordering::Relaxed);
        let count = eligible.count_ones();
        if count == 0 {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % count;
        Some(nth_set_bit(eligible, idx) as usize)
    }

    /// Update the eligibility bitfield atomically.
    fn set_eligible(&self, bits: u64) {
        self.eligible.store(bits, Ordering::Relaxed);
    }
}

/// Maximum number of readers supported by the `u64` eligibility bitfield.
const MAX_READERS: usize = 64;

/// Build an "all eligible" bitfield for `count` readers (clamped to 64).
fn all_eligible_bits(count: u32) -> u64 {
    if count >= MAX_READERS as u32 {
        u64::MAX
    } else {
        (1u64 << count) - 1
    }
}

/// Find the position of the Nth set bit (0-indexed) in a u64.
fn nth_set_bit(mut bits: u64, n: u32) -> u32 {
    for _ in 0..n {
        // Clear the lowest set bit
        bits &= bits - 1;
    }
    bits.trailing_zeros()
}

// ---------------------------------------------------------------------------
// PgReplicaBackend
// ---------------------------------------------------------------------------

/// A Postgres backend that distributes reads across replicas with lag-aware routing.
///
/// Created by [`PgReplicaBackend::new()`] when the configuration specifies
/// `replica_dsns`. Implements [`Backend`] identically to [`PgBackend`](crate::PgBackend)
/// from the caller's perspective.
///
/// ## Reader ordering
///
/// The internal `readers` vec is ordered: `[replica_0, replica_1, ..., primary]`
/// (primary is appended only if `config.primary_reads == true`).
pub struct PgReplicaBackend {
    /// The primary connection pool (writes always go here).
    primary: ManagedPool,
    /// For the schema-change listener's own connection.
    primary_dsn: String,
    /// All reader pools in order: replicas first, then optionally primary.
    readers: Vec<ManagedPool>,
    /// Shared health state for routing decisions.
    health: Arc<HealthState>,
    /// Handle to the background health monitor task.
    _monitor_handle: tokio::task::JoinHandle<()>,
}

impl PgReplicaBackend {
    /// Create a new replica-aware backend.
    ///
    /// # Arguments
    ///
    /// * `primary_dsn` — DSN for the primary (read-write) server
    /// * `pool_cfg` — Pool settings (size, timeouts, keepalive, recycling)
    /// * `config` — Replica configuration (DSNs, lag threshold, intervals)
    ///
    /// # Errors
    ///
    /// Returns [`Error::Introspection`] if any pool fails to initialize.
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
        let primary = ManagedPool::new(primary_dsn, pool_cfg)?;

        let mut replica_pools = Vec::with_capacity(config.replica_dsns.len());
        for dsn in &config.replica_dsns {
            replica_pools.push(ManagedPool::new(dsn, pool_cfg)?);
        }

        // Build the readers list: replicas first, then optionally primary.
        // The eligibility bitfield is a u64, so at most 64 readers are supported.
        // Reserve a slot for the primary when primary_reads is enabled, then
        // truncate the replicas so the total reader count never exceeds 64.
        let mut readers: Vec<ManagedPool> = replica_pools;
        let replica_cap = if config.primary_reads {
            MAX_READERS - 1
        } else {
            MAX_READERS
        };
        if readers.len() > replica_cap {
            tracing::warn!(
                requested = readers.len(),
                max = replica_cap,
                "too many replica readers configured; truncating to fit the 64-reader limit"
            );
            readers.truncate(replica_cap);
        }
        // Number of replicas actually retained as readers (replicas come first).
        let replica_count = readers.len();
        if config.primary_reads {
            readers.push(primary.clone());
        }

        let reader_count = readers.len() as u32;
        let health = Arc::new(HealthState::new(reader_count));
        // Until the first probe, no replica has demonstrated the configured
        // lag bound. The primary needs no replay check.
        if config.max_replication_lag_bytes > 0 {
            health.set_eligible(if config.primary_reads {
                1u64 << replica_count
            } else {
                0
            });
        }

        // The health monitor probes over its own one-connection pools with
        // short timeouts. Probing through the request pools made a saturated
        // replica look unreachable and excluded it, piling its load onto the
        // remaining readers.
        let probe_ms = config.health_check_interval_ms.clamp(500, 5000);
        let probe_cfg = PoolConfig {
            size: 1,
            timeout_ms: probe_ms,
            create_timeout_ms: probe_ms,
            recycle_timeout_ms: probe_ms,
            ..pool_cfg.clone()
        };
        let monitor_primary = crate::create_pool(primary_dsn, &probe_cfg)?;
        let mut monitor_readers = config.replica_dsns[..replica_count]
            .iter()
            .map(|dsn| crate::create_pool(dsn, &probe_cfg))
            .collect::<Result<Vec<_>, _>>()?;
        if config.primary_reads {
            monitor_readers.push(monitor_primary.clone());
        }
        let monitor_health = health.clone();
        let monitor_config = config.clone();
        let monitor_replica_count = replica_count;

        let monitor_handle = tokio::spawn(async move {
            health_monitor_loop(
                monitor_primary,
                monitor_readers,
                monitor_replica_count,
                monitor_health,
                monitor_config,
                Duration::from_millis(probe_ms),
            )
            .await;
        });

        Ok(Self {
            primary,
            primary_dsn: primary_dsn.to_string(),
            readers,
            health,
            _monitor_handle: monitor_handle,
        })
    }

    /// How many readers are currently eligible for reads (reachable, and
    /// within the replication-lag limit), as of the last health check.
    pub fn eligible_readers(&self) -> u32 {
        self.health.eligible.load(Ordering::Relaxed).count_ones()
    }

    /// Pick a reader pool for a read query, returning its reader index.
    ///
    /// Returns `None` when no readers are eligible (caller falls back to primary).
    fn pick_read_pool(&self) -> Option<usize> {
        self.health
            .pick_reader()
            .filter(|&idx| idx < self.readers.len())
    }
}

impl Drop for PgReplicaBackend {
    fn drop(&mut self) {
        // Dropping JoinHandle alone detaches the task and retains every probe
        // pool indefinitely (including when an application replaces a backend).
        self._monitor_handle.abort();
    }
}

impl Backend for PgReplicaBackend {
    fn introspect(&self, cfg: &IntrospectConfig) -> BoxFuture<'_, Result<SchemaCache, Error>> {
        // Always introspect from the primary (authoritative schema)
        let cfg = cfg.clone();
        Box::pin(async move { self.primary.introspect(&cfg).await })
    }

    fn execute(
        &self,
        ctx: &ExecContext,
        sql: &str,
        params: &[Value],
    ) -> BoxFuture<'_, Result<QueryResult, Error>> {
        let sql = sql.to_string();
        let params = params.to_vec();
        let ctx = ctx.clone();

        Box::pin(async move {
            // Mutations always go to the primary.
            if ctx.is_mutation {
                let checkout = self.primary.checkout().await?;
                return self.primary.execute(checkout, &ctx, &sql, &params).await;
            }

            // Reads: try an eligible reader, then retry once with the next
            // eligible reader on checkout failure, then fall back to primary.
            for _ in 0..2 {
                let Some(idx) = self.pick_read_pool() else {
                    break;
                };
                match self.readers[idx].checkout().await {
                    Ok(checkout) => {
                        return self.readers[idx]
                            .execute(checkout, &ctx, &sql, &params)
                            .await;
                    }
                    Err(e) => {
                        tracing::warn!(
                            reader_index = idx,
                            error = %e,
                            "reader pool checkout failed, trying next reader"
                        );
                    }
                }
            }

            // Fall back to the primary.
            let checkout = self.primary.checkout().await?;
            self.primary.execute(checkout, &ctx, &sql, &params).await
        })
    }

    /// Schema changes are made on (and announced from) the primary.
    fn watch_schema(&self) -> BoxFuture<'_, Option<SchemaChangeStream>> {
        let dsn = self.primary_dsn.clone();
        Box::pin(async move { Some(crate::schema_watch::watch(dsn)) })
    }

    fn dialect(&self) -> &'static Dialect {
        &dialect::POSTGRES
    }

    /// The primary's pool: it takes every write (and reads when no reader is
    /// eligible). See [`PgReplicaBackend::eligible_readers`] for read health.
    fn pool_status(&self) -> Option<pgvis_core::backend::PoolStatus> {
        Some(self.primary.status())
    }
}

// ---------------------------------------------------------------------------
// Health monitor — background task
// ---------------------------------------------------------------------------

/// The background loop that checks replica lag and updates the health bitfield.
async fn health_monitor_loop(
    primary: Pool,
    readers: Vec<Pool>,
    replica_count: usize,
    health: Arc<HealthState>,
    config: ReplicaConfig,
    probe_timeout: Duration,
) {
    let interval = Duration::from_millis(config.health_check_interval_ms);
    let lag_disabled = config.max_replication_lag_bytes == 0;

    loop {
        let mut eligible: u64 = 0;

        // Without the primary WAL position, an enabled lag limit cannot be
        // verified. Only an explicitly disabled lag limit permits connectivity-
        // only checks.
        let primary_lsn = if lag_disabled {
            None
        } else {
            let lsn = tokio::time::timeout(probe_timeout, get_primary_lsn(&primary))
                .await
                .unwrap_or_else(|_| Err("timed out".to_string()));
            match lsn {
                Ok(lsn) => Some(lsn),
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "health monitor: failed to query primary WAL position; \
                         replicas excluded until lag can be verified"
                    );
                    None
                }
            }
        };

        let primary_healthy = if lag_disabled && config.primary_reads {
            tokio::time::timeout(probe_timeout, async {
                let client = primary.get().await.map_err(|_| ())?;
                client.query_one("SELECT 1", &[]).await.map_err(|_| ())
            })
            .await
            .is_ok_and(|r| r.is_ok())
        } else {
            primary_lsn.is_some()
        };
        // Check every replica at once, each against a deadline. Unknown lag
        // cannot satisfy an enabled lag limit, even if a replica is reachable.
        let checks = readers[..replica_count].iter().map(|pool| async {
            if !lag_disabled && primary_lsn.is_none() {
                return ReplicaStatus::Unreachable("primary WAL position unavailable".into());
            }
            tokio::time::timeout(
                probe_timeout,
                check_replica(pool, primary_lsn, config.max_replication_lag_bytes),
            )
            .await
            .unwrap_or_else(|_| ReplicaStatus::Unreachable("health check timed out".into()))
        });
        let mut statuses = futures::future::join_all(checks).await.into_iter();

        for i in 0..readers.len() {
            let is_replica = i < replica_count;

            if is_replica {
                match statuses.next().expect("one status per replica") {
                    ReplicaStatus::Healthy => {
                        eligible |= 1u64 << i;
                    }
                    ReplicaStatus::Lagging(lag_bytes) => {
                        tracing::info!(
                            replica_index = i,
                            lag_bytes,
                            max_lag = config.max_replication_lag_bytes,
                            "replica excluded: replication lag exceeds threshold"
                        );
                    }
                    ReplicaStatus::Unreachable(e) => {
                        tracing::warn!(
                            replica_index = i,
                            error = %e,
                            "replica excluded: unreachable"
                        );
                    }
                }
            } else {
                if primary_healthy {
                    eligible |= 1u64 << i;
                }
            }
        }

        let prev = health.eligible.load(Ordering::Relaxed);
        if prev != eligible {
            let eligible_count = eligible.count_ones();
            if eligible_count == 0 {
                tracing::warn!("all replicas excluded — reads will fall back to primary");
            } else {
                tracing::info!(
                    eligible_readers = eligible_count,
                    total_readers = health.reader_count,
                    "reader eligibility updated"
                );
            }
        }

        health.set_eligible(eligible);
        tokio::time::sleep(interval).await;
    }
}

/// Result of checking a single replica.
enum ReplicaStatus {
    Healthy,
    Lagging(u64),
    Unreachable(String),
}

/// Get the current WAL LSN from the primary as a byte offset.
async fn get_primary_lsn(pool: &Pool) -> Result<u64, String> {
    let client = pool.get().await.map_err(|e| format!("pool: {e}"))?;
    let row = client
        .query_one("SELECT pg_current_wal_lsn() AS lsn", &[])
        .await
        .map_err(|e| format!("query: {e}"))?;

    // Use try_get: pg_current_wal_lsn() is non-NULL on a primary, but if the
    // node is unexpectedly in recovery it returns NULL — treat that as an error
    // rather than panicking.
    let lsn: tokio_postgres::types::PgLsn = row
        .try_get("lsn")
        .map_err(|e| format!("primary WAL lsn unavailable: {e}"))?;
    Ok(lsn.into())
}

/// Check a replica's health and replication lag.
async fn check_replica(pool: &Pool, primary_lsn: Option<u64>, max_lag_bytes: u64) -> ReplicaStatus {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => return ReplicaStatus::Unreachable(format!("pool: {e}")),
    };

    // Explicitly disabled lag checking permits a connectivity-only probe. A replica that can't run a trivial
    // query is unreachable and must not be marked eligible.
    let Some(primary_lsn) = primary_lsn else {
        return match client.query_one("SELECT 1", &[]).await {
            Ok(_) => ReplicaStatus::Healthy,
            Err(e) => ReplicaStatus::Unreachable(format!("query: {e}")),
        };
    };

    // Query the replica's replayed LSN — the position actually visible to
    // queries on the replica (per plans/replica-support.md), not merely received.
    let row = match client
        .query_one("SELECT pg_last_wal_replay_lsn() AS lsn", &[])
        .await
    {
        Ok(r) => r,
        Err(e) => return ReplicaStatus::Unreachable(format!("query: {e}")),
    };

    // pg_last_wal_replay_lsn() is NULL when the node is not in recovery (e.g. the
    // "replica" DSN actually points at a primary). Treat NULL/error as
    // ineligible rather than panicking (which would kill the monitor task).
    let replica_lsn: tokio_postgres::types::PgLsn = match row.try_get("lsn") {
        Ok(v) => v,
        Err(e) => {
            return ReplicaStatus::Unreachable(format!(
                "replay LSN unavailable (node not in recovery?): {e}"
            ));
        }
    };
    let replica_lsn = u64::from(replica_lsn);

    // Calculate lag (primary is always ahead or equal)
    let lag = primary_lsn.saturating_sub(replica_lsn);

    if max_lag_bytes > 0 && lag > max_lag_bytes {
        ReplicaStatus::Lagging(lag)
    } else {
        ReplicaStatus::Healthy
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nth_set_bit() {
        // 0b1010_1100 = bits 2, 3, 5, 7
        let bits: u64 = 0b1010_1100;
        assert_eq!(nth_set_bit(bits, 0), 2);
        assert_eq!(nth_set_bit(bits, 1), 3);
        assert_eq!(nth_set_bit(bits, 2), 5);
        assert_eq!(nth_set_bit(bits, 3), 7);
    }

    #[test]
    fn test_all_eligible_bits() {
        assert_eq!(all_eligible_bits(0), 0);
        assert_eq!(all_eligible_bits(1), 0b1);
        assert_eq!(all_eligible_bits(3), 0b111);
        // No shift overflow at or beyond the 64-reader limit.
        assert_eq!(all_eligible_bits(63), (1u64 << 63) - 1);
        assert_eq!(all_eligible_bits(64), u64::MAX);
        assert_eq!(all_eligible_bits(200), u64::MAX);
    }

    #[test]
    fn test_health_state_at_64_readers() {
        // Constructing with 64 readers must not panic (shift-overflow guard).
        let state = HealthState::new(64);
        assert_eq!(state.eligible.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(state.pick_reader(), Some(0));
    }

    #[test]
    fn test_health_state_all_eligible() {
        let state = HealthState::new(3);
        // All 3 readers eligible → picks cycle through 0, 1, 2
        assert_eq!(state.pick_reader(), Some(0));
        assert_eq!(state.pick_reader(), Some(1));
        assert_eq!(state.pick_reader(), Some(2));
        assert_eq!(state.pick_reader(), Some(0)); // wraps
    }

    #[test]
    fn test_health_state_some_excluded() {
        let state = HealthState::new(4);
        // Exclude reader 1 and 3: eligible = 0b0101 (bits 0, 2)
        state.set_eligible(0b0101);
        assert_eq!(state.pick_reader(), Some(0));
        assert_eq!(state.pick_reader(), Some(2));
        assert_eq!(state.pick_reader(), Some(0));
    }

    #[test]
    fn test_health_state_none_eligible() {
        let state = HealthState::new(3);
        state.set_eligible(0);
        assert_eq!(state.pick_reader(), None);
    }
}
