//! # `pgvis-postgres` — Postgres backend for pgvis.
//!
//! Implements [`pgvis_core::Backend`] using `tokio-postgres` + `deadpool-postgres`.
//!
//! ## Responsibilities
//!
//! - **Connection pooling** via `deadpool-postgres`
//! - **Schema introspection** from `pg_catalog` (tables, columns, FKs, functions)
//! - **Query execution** within role-switched transactions
//! - **Schema change notifications** via `LISTEN/NOTIFY` (planned)
//!
//! ## Example
//!
//! ```rust,ignore
//! use pgvis_postgres::PgBackend;
//! use pgvis_core::{Backend, IntrospectConfig};
//!
//! let backend = PgBackend::new("postgres://user:pass@localhost/db")?;
//! let cache = backend.introspect(&IntrospectConfig::default()).await?;
//! println!("Found {} tables", cache.tables.len());
//! ```

pub mod execute;
pub mod introspect;
pub mod replica;

use std::time::Duration;

use deadpool_postgres::{Config as DeadpoolConfig, ManagerConfig, Pool, RecyclingMethod, Runtime};
use futures::future::BoxFuture;
use pgvis_core::backend::{
    Backend, ExecContext, IntrospectConfig, QueryResult, SchemaChangeStream,
};
use pgvis_core::cache::SchemaCache;
use pgvis_core::config::PoolConfig;
use pgvis_core::dialect::{self, Dialect};
use pgvis_core::error::Error;
use serde_json::Value;
use tokio_postgres::NoTls;

pub use replica::PgReplicaBackend;

/// The Postgres backend — implements [`Backend`] for PostgreSQL databases.
///
/// Holds a connection pool (`deadpool-postgres`) and provides:
/// - `introspect()` — loads schema metadata from `pg_catalog`
/// - `execute()` — runs CTE-wrapped SQL within a transaction
/// - `watch_schema()` — LISTEN/NOTIFY for schema changes (planned)
/// - `dialect()` — returns [`POSTGRES`](pgvis_core::dialect::POSTGRES)
pub struct PgBackend {
    pool: Pool,
}

impl PgBackend {
    /// Create a new Postgres backend from a DSN with pool configuration.
    ///
    /// Initialises the connection pool but does NOT connect immediately —
    /// connections are created lazily on first use.
    ///
    /// # Arguments
    ///
    /// * `dsn` — A PostgreSQL connection string (e.g. `postgres://user:pass@host/db`)
    /// * `pool_cfg` — Pool settings (size, timeouts, keepalive, recycling)
    ///
    /// # Errors
    ///
    /// Returns [`Error::Introspection`] if the pool configuration is invalid.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use pgvis_core::config::PoolConfig;
    /// let backend = PgBackend::new("postgres://localhost/mydb", &PoolConfig::default())?;
    /// ```
    pub fn new(dsn: &str, pool_cfg: &pgvis_core::config::PoolConfig) -> Result<Self, Error> {
        let pool = create_pool(dsn, pool_cfg)?;
        Ok(Self { pool })
    }

    /// Get a reference to the underlying connection pool.
    ///
    /// Useful for advanced use cases (custom queries, health checks, metrics).
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

impl Backend for PgBackend {
    fn introspect(&self, cfg: &IntrospectConfig) -> BoxFuture<'_, Result<SchemaCache, Error>> {
        let cfg = cfg.clone();
        Box::pin(async move {
            let client = self
                .pool
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
            let client = self.pool.get().await.map_err(|e| Error::Execution {
                message: format!("pool error: {e}"),
                db_code: None,
                detail: None,
                hint: None,
            })?;

            execute::execute_query(&client, &ctx, &sql, &params).await
        })
    }

    fn watch_schema(&self) -> BoxFuture<'_, Option<SchemaChangeStream>> {
        Box::pin(async {
            // TODO: Implement LISTEN/NOTIFY for schema change detection
            None
        })
    }

    fn dialect(&self) -> &'static Dialect {
        &dialect::POSTGRES
    }
}

// ---------------------------------------------------------------------------
// Pool creation — shared by PgBackend and PgReplicaBackend
// ---------------------------------------------------------------------------

/// Create a `deadpool-postgres` pool from a DSN and pool configuration.
///
/// Applies all settings from [`PoolConfig`]: size, checkout/create/recycle
/// timeouts, TCP keepalive, connect timeout, and recycling method.
pub(crate) fn create_pool(dsn: &str, pool_cfg: &PoolConfig) -> Result<Pool, Error> {
    let mut cfg = DeadpoolConfig::new();
    cfg.url = Some(dsn.to_string());

    // Connection-level settings
    cfg.keepalives = Some(pool_cfg.keepalives);
    if pool_cfg.keepalives {
        cfg.keepalives_idle = Some(Duration::from_secs(pool_cfg.keepalives_idle_secs));
    }
    if pool_cfg.connect_timeout_secs > 0 {
        cfg.connect_timeout = Some(Duration::from_secs(pool_cfg.connect_timeout_secs));
    }

    // Pool-level timeouts
    let timeouts = deadpool_postgres::Timeouts {
        wait: if pool_cfg.timeout_ms > 0 {
            Some(Duration::from_millis(pool_cfg.timeout_ms))
        } else {
            None
        },
        create: if pool_cfg.create_timeout_ms > 0 {
            Some(Duration::from_millis(pool_cfg.create_timeout_ms))
        } else {
            None
        },
        recycle: if pool_cfg.recycle_timeout_ms > 0 {
            Some(Duration::from_millis(pool_cfg.recycle_timeout_ms))
        } else {
            None
        },
    };

    cfg.pool = Some(deadpool_postgres::PoolConfig {
        max_size: pool_cfg.size as usize,
        timeouts,
        ..Default::default()
    });

    // Recycling method
    cfg.manager = Some(ManagerConfig {
        recycling_method: match pool_cfg.recycling_method {
            pgvis_core::config::RecyclingMethod::Fast => RecyclingMethod::Fast,
            pgvis_core::config::RecyclingMethod::Verified => RecyclingMethod::Verified,
            pgvis_core::config::RecyclingMethod::Clean => RecyclingMethod::Clean,
        },
    });

    cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .map_err(|e| Error::Introspection(format!("failed to create pool: {e}")))
}
