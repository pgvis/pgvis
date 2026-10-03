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
pub mod pubsub;
pub mod replica;
mod schema_watch;

use std::sync::{Arc, OnceLock};
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
use tokio_postgres_rustls::MakeRustlsConnect;

pub use pubsub::PgPubSub;
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
    /// For the schema-change listener's own connection.
    dsn: String,
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
        Ok(Self {
            pool,
            dsn: dsn.to_string(),
        })
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
            let mut client = self
                .pool
                .get()
                .await
                .map_err(|e| Error::Introspection(format!("pool error: {e}")))?;

            introspect::load_schema_cache(&mut client, &cfg).await
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
            Checkout::get(&self.pool)
                .await?
                .execute(&ctx, &sql, &params)
                .await
        })
    }

    /// `LISTEN pgrst` on a dedicated connection (PostgREST's convention).
    fn watch_schema(&self) -> BoxFuture<'_, Option<SchemaChangeStream>> {
        let dsn = self.dsn.clone();
        Box::pin(async move { Some(schema_watch::watch(dsn)) })
    }

    fn dialect(&self) -> &'static Dialect {
        &dialect::POSTGRES
    }

    fn pool_status(&self) -> Option<pgvis_core::backend::PoolStatus> {
        Some(pool_status(&self.pool))
    }
}

/// [`PoolStatus`](pgvis_core::backend::PoolStatus) of a deadpool pool.
pub(crate) fn pool_status(pool: &Pool) -> pgvis_core::backend::PoolStatus {
    let s = pool.status();
    pgvis_core::backend::PoolStatus {
        max_size: s.max_size,
        size: s.size,
        available: s.available,
        waiting: s.waiting,
    }
}

// ---------------------------------------------------------------------------
// Checkout — one pooled connection for one request
// ---------------------------------------------------------------------------

/// A pooled connection checked out to run one request.
///
/// The executor pipelines BEGIN … COMMIT without an RAII transaction, so a
/// request dropped mid-flight (client disconnect, timeout) can leave the
/// connection inside the transaction, with the caller's role still set. Such
/// a connection is detached from the pool and closed — the server then rolls
/// the transaction back — instead of being handed to the next request.
pub(crate) struct Checkout(Option<deadpool_postgres::Object>);

impl Checkout {
    pub(crate) async fn get(pool: &Pool) -> Result<Self, Error> {
        pool.get().await.map(|c| Self(Some(c))).map_err(pool_error)
    }

    /// Run one request; the connection goes back to the pool only once its
    /// transaction has ended.
    pub(crate) async fn execute(
        mut self,
        ctx: &ExecContext,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, Error> {
        let client = self.0.as_ref().expect("checked out");
        let result = execute::execute_query(client, ctx, sql, params).await;
        drop(self.0.take());
        result
    }
}

impl Drop for Checkout {
    fn drop(&mut self) {
        if let Some(conn) = self.0.take() {
            drop(deadpool_postgres::Object::take(conn));
        }
    }
}

/// A failed checkout: the pool is exhausted (wait timeout) or the database is
/// unreachable. Reported as 503 `PGRST000`, the same as PostgREST, rather than
/// a 500 — callers can retry.
pub(crate) fn pool_error(e: deadpool_postgres::PoolError) -> Error {
    Error::Introspection(format!("database connection unavailable: {e}"))
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

    cfg.create_pool(Some(Runtime::Tokio1), tls_connector())
        .map_err(|e| Error::Introspection(format!("failed to create pool: {e}")))
}

/// TLS connector shared by every Postgres connection (pool and pub/sub).
///
/// tokio-postgres applies the DSN's `sslmode` (`disable`, `prefer` — the
/// default — or `require`): TLS is negotiated whenever the server offers it,
/// and the server certificate is verified against the platform trust store
/// (honouring `SSL_CERT_FILE`). Built once; loading the store is not free.
pub(crate) fn tls_connector() -> MakeRustlsConnect {
    static CONNECTOR: OnceLock<MakeRustlsConnect> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| {
            let native = rustls_native_certs::load_native_certs();
            for e in &native.errors {
                tracing::warn!(error = %e, "failed to load a platform CA certificate");
            }
            let mut roots = rustls::RootCertStore::empty();
            let (added, ignored) = roots.add_parsable_certificates(native.certs);
            if ignored > 0 {
                tracing::warn!(ignored, "ignored unparsable platform CA certificates");
            }
            if added == 0 {
                tracing::warn!(
                    "no platform CA certificates found; TLS to Postgres will fail verification"
                );
            }
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .expect("ring supports the default TLS protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
            MakeRustlsConnect::new(config)
        })
        .clone()
}
