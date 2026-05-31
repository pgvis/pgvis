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

use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use futures::future::BoxFuture;
use tokio_postgres::NoTls;

use pgvis_core::backend::{
    Backend, ExecContext, IntrospectConfig, QueryResult, SchemaChangeStream,
};
use pgvis_core::cache::SchemaCache;
use pgvis_core::config::ReplicaConfig;
use pgvis_core::dialect::{self, Dialect};
use pgvis_core::error::Error;
use serde_json::Value;

use crate::execute;
use crate::introspect;

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
        // Start with all readers eligible
        let all_eligible = if reader_count >= 64 {
            u64::MAX
        } else {
            (1u64 << reader_count) - 1
        };
        Self {
            eligible: AtomicU64::new(all_eligible),
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
    primary: Pool,
    /// All reader pools in order: replicas first, then optionally primary.
    readers: Vec<Pool>,
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
    /// * `pool_size` — Max connections per pool (applied to primary AND each replica)
    /// * `pool_timeout_ms` — Pool checkout timeout in milliseconds
    /// * `config` — Replica configuration (DSNs, lag threshold, intervals)
    ///
    /// # Errors
    ///
    /// Returns [`Error::Introspection`] if any pool fails to initialize.
    pub fn new(
        primary_dsn: &str,
        pool_size: u32,
        pool_timeout_ms: u64,
        config: &ReplicaConfig,
    ) -> Result<Self, Error> {
        let primary = create_pool(primary_dsn, pool_size, pool_timeout_ms)?;

        let mut replica_pools = Vec::with_capacity(config.replica_dsns.len());
        for dsn in &config.replica_dsns {
            replica_pools.push(create_pool(dsn, pool_size, pool_timeout_ms)?);
        }

        let replica_count = replica_pools.len();

        // Build the readers list: replicas first, then optionally primary
        let mut readers: Vec<Pool> = replica_pools;
        if config.primary_reads {
            readers.push(primary.clone());
        }

        let reader_count = readers.len() as u32;
        let health = Arc::new(HealthState::new(reader_count));

        // Spawn the health monitor
        let monitor_health = health.clone();
        let monitor_primary = primary.clone();
        // Clone the reader pools for the monitor (it needs separate pool refs)
        let monitor_readers: Vec<Pool> = readers.clone();
        let monitor_config = config.clone();
        let monitor_replica_count = replica_count;

        let monitor_handle = tokio::spawn(async move {
            health_monitor_loop(
                monitor_primary,
                monitor_readers,
                monitor_replica_count,
                monitor_health,
                monitor_config,
            )
            .await;
        });

        Ok(Self {
            primary,
            readers,
            health,
            _monitor_handle: monitor_handle,
        })
    }

    /// Pick a reader pool for a read query.
    ///
    /// Falls back to the primary if no readers are eligible.
    fn pick_read_pool(&self) -> &Pool {
        if let Some(idx) = self.health.pick_reader() {
            if idx < self.readers.len() {
                return &self.readers[idx];
            }
        }
        // Fallback: always use primary
        &self.primary
    }
}

impl Backend for PgReplicaBackend {
    fn introspect(&self, cfg: &IntrospectConfig) -> BoxFuture<'_, Result<SchemaCache, Error>> {
        // Always introspect from the primary (authoritative schema)
        let cfg = cfg.clone();
        Box::pin(async move {
            let client = self
                .primary
                .get()
                .await
                .map_err(|e| Error::Introspection(format!("pool error: {e}")))?;

            introspect::load_schema_cache(&client, &cfg).await
        })
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
            let pool = if ctx.is_mutation {
                &self.primary
            } else {
                self.pick_read_pool()
            };

            let client = pool.get().await.map_err(|e| Error::Execution {
                message: format!("pool error: {e}"),
                db_code: None,
                detail: None,
                hint: None,
            })?;

            execute::execute_query(&client, &ctx, &sql, &params).await
        })
    }

    fn watch_schema(&self) -> BoxFuture<'_, Option<SchemaChangeStream>> {
        // Delegate to primary (same as PgBackend)
        Box::pin(async { None })
    }

    fn dialect(&self) -> &'static Dialect {
        &dialect::POSTGRES
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
) {
    let interval = Duration::from_millis(config.health_check_interval_ms);
    let lag_disabled = config.max_replication_lag_bytes == 0;

    loop {
        tokio::time::sleep(interval).await;

        let mut eligible: u64 = 0;

        // Get primary WAL position (needed for lag calculation)
        let primary_lsn = if !lag_disabled {
            match get_primary_lsn(&primary).await {
                Ok(lsn) => Some(lsn),
                Err(e) => {
                    tracing::error!(error = %e, "health monitor: failed to query primary WAL position");
                    // If we can't reach primary, mark all readers eligible (best-effort)
                    let all = if health.reader_count >= 64 {
                        u64::MAX
                    } else {
                        (1u64 << health.reader_count) - 1
                    };
                    health.set_eligible(all);
                    continue;
                }
            }
        } else {
            None
        };

        // Check each reader
        for (i, pool) in readers.iter().enumerate() {
            let is_replica = i < replica_count;

            if is_replica {
                // Check replica health and lag
                match check_replica(pool, primary_lsn, config.max_replication_lag_bytes).await {
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
                // This is the primary in the readers list — always eligible
                // (if primary can't be reached, execute() will fail naturally)
                eligible |= 1u64 << i;
            }
        }

        let prev = health.eligible.load(Ordering::Relaxed);
        if prev != eligible {
            let eligible_count = eligible.count_ones();
            if eligible_count == 0 {
                tracing::warn!(
                    "all replicas excluded — reads will fall back to primary"
                );
            } else {
                tracing::info!(
                    eligible_readers = eligible_count,
                    total_readers = health.reader_count,
                    "reader eligibility updated"
                );
            }
        }

        health.set_eligible(eligible);
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
        .query_one(
            "SELECT pg_current_wal_lsn() - '0/0'::pg_lsn AS lsn_bytes",
            &[],
        )
        .await
        .map_err(|e| format!("query: {e}"))?;

    let lsn_bytes: i64 = row.get("lsn_bytes");
    Ok(lsn_bytes as u64)
}

/// Check a replica's health and replication lag.
async fn check_replica(
    pool: &Pool,
    primary_lsn: Option<u64>,
    max_lag_bytes: u64,
) -> ReplicaStatus {
    let client = match pool.get().await {
        Ok(c) => c,
        Err(e) => return ReplicaStatus::Unreachable(format!("pool: {e}")),
    };

    // If lag checking is disabled (primary_lsn is None), just check connectivity
    let Some(primary_lsn) = primary_lsn else {
        return ReplicaStatus::Healthy;
    };

    // Query the replica's receive LSN
    let row = match client
        .query_one(
            "SELECT pg_last_wal_receive_lsn() - '0/0'::pg_lsn AS lsn_bytes",
            &[],
        )
        .await
    {
        Ok(r) => r,
        Err(e) => return ReplicaStatus::Unreachable(format!("query: {e}")),
    };

    let replica_lsn: i64 = row.get("lsn_bytes");
    let replica_lsn = replica_lsn as u64;

    // Calculate lag (primary is always ahead or equal)
    let lag = primary_lsn.saturating_sub(replica_lsn);

    if max_lag_bytes > 0 && lag > max_lag_bytes {
        ReplicaStatus::Lagging(lag)
    } else {
        ReplicaStatus::Healthy
    }
}

// ---------------------------------------------------------------------------
// Pool creation helper
// ---------------------------------------------------------------------------

/// Create a `deadpool-postgres` pool from a DSN.
fn create_pool(dsn: &str, pool_size: u32, pool_timeout_ms: u64) -> Result<Pool, Error> {
    let mut cfg = PoolConfig::new();
    cfg.url = Some(dsn.to_string());

    let timeouts = if pool_timeout_ms > 0 {
        deadpool_postgres::Timeouts {
            wait: Some(Duration::from_millis(pool_timeout_ms)),
            ..Default::default()
        }
    } else {
        Default::default()
    };

    cfg.pool = Some(deadpool_postgres::PoolConfig {
        max_size: pool_size as usize,
        timeouts,
        ..Default::default()
    });

    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .map_err(|e| Error::Introspection(format!("failed to create pool: {e}")))
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
