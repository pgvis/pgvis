//! # The [`Backend`] trait — the async I/O boundary between pgvis and databases.
//!
//! This module defines the core abstraction that separates database-agnostic logic
//! (parsing, planning, SQL building, OpenAPI generation) from database-specific I/O
//! (connection pooling, query execution, schema introspection).
//!
//! ## Object Safety
//!
//! The trait is **object-safe** — adapters hold `Arc<dyn Backend>` and never name
//! a concrete driver type. This is achieved by returning [`BoxFuture`] instead of
//! using `impl Future` or `async fn` in trait methods.
//!
//! ## Comparison with PostgREST
//!
//! PostgREST has no equivalent — it hardcodes Postgres throughout. This trait
//! enables pgvis to support multiple backends (Postgres, SQLite) without any
//! conditional compilation in the REST/MCP layers.
//!
//! ## Implementors
//!
//! - `pgvis-postgres::PgBackend` — Postgres via `tokio-postgres` + `deadpool-postgres`
//! - `pgvis-sqlite::SqliteBackend` (planned) — SQLite via `sqlx` or `rusqlite`

use std::pin::Pin;

use futures::Stream;
use futures::future::BoxFuture;
use serde_json::Value;

use crate::cache::SchemaCache;
use crate::dialect::Dialect;
use crate::error::Error;

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Configuration for introspecting a database schema.
///
/// Passed to [`Backend::introspect`] to control which parts of the database
/// are loaded into the [`SchemaCache`].
///
/// # PostgREST equivalent
///
/// Maps to `configDbSchemas` + `configDbExtraSearchPath` in PostgREST's config.
///
/// # Backend behaviour
///
/// - **Postgres:** `schemas` selects which `pg_namespace` entries to introspect.
///   `extra_search_path` adds schemas for type/function resolution without exposing them as API endpoints.
/// - **SQLite:** `schemas` is ignored (single namespace). `extra_search_path` is a no-op.
#[derive(Debug, Clone)]
pub struct IntrospectConfig {
    /// Which schemas to expose as API endpoints (e.g. `["public"]`).
    ///
    /// Tables/views/functions in these schemas become REST routes.
    pub schemas: Vec<String>,

    /// Additional schemas added to `search_path` for type and function resolution.
    ///
    /// These schemas are NOT exposed as API endpoints but are available for
    /// resolving custom types, domain types, and helper functions referenced
    /// by the exposed schemas.
    pub extra_search_path: Vec<String>,
}

impl Default for IntrospectConfig {
    fn default() -> Self {
        Self {
            schemas: vec!["public".to_string()],
            extra_search_path: Vec::new(),
        }
    }
}

/// Per-request execution context passed to [`Backend::execute`].
///
/// Carries authentication state and transaction configuration that the backend
/// uses to set up the database session before running the main query.
///
/// # PostgREST equivalent
///
/// Maps to the implicit state threaded through `runDbHandler`:
/// - `SET LOCAL role` for row-level security
/// - `SET LOCAL request.jwt.claims` for claim propagation
/// - Pre-request hook function call
/// - Statement timeout enforcement
///
/// # Backend behaviour
///
/// - **Postgres:** All fields are honoured. The backend opens a transaction,
///   runs `SET LOCAL role = $role`, sets GUCs, optionally calls `pre_request`,
///   then executes the main statement.
/// - **SQLite:** `role` and `claims` are informational only (no RLS). `pre_request`
///   is a no-op. `statement_timeout` may use `sqlite3_progress_handler`.
///   `is_mutation` routes the query to the writer connection.
#[derive(Debug, Clone, Default)]
pub struct ExecContext {
    /// Role to `SET LOCAL role` to within the transaction.
    ///
    /// On Postgres, this activates row-level security policies for the given role.
    /// On SQLite, this field is ignored (no role system).
    pub role: Option<String>,

    /// Raw JWT claims as a JSON object, propagated via GUC on Postgres.
    ///
    /// Set as `request.jwt.claims` so SQL functions and RLS policies can
    /// access claim values (e.g. `current_setting('request.jwt.claims')::json->>'sub'`).
    pub claims: Option<Value>,

    /// Qualified name of a pre-request function to call before the main query.
    ///
    /// Called after role switching but before the main statement. Can raise
    /// exceptions to abort the request (e.g. for rate limiting, custom auth).
    ///
    /// PostgREST equivalent: `db-pre-request` config option.
    pub pre_request: Option<String>,

    /// Statement timeout in milliseconds.
    ///
    /// On Postgres: `SET LOCAL statement_timeout = '<ms>ms'`.
    /// On SQLite: enforced via progress handler callback.
    pub statement_timeout: Option<u64>,

    /// Transaction end behaviour override from `Prefer: tx=rollback`.
    ///
    /// When `Some(TxEnd::Rollback)`, the transaction is rolled back after execution
    /// (useful for testing/dry-run). Only honoured if the server config permits it.
    pub tx_end: Option<TxEnd>,

    /// Whether this is a write operation (INSERT/UPDATE/DELETE).
    ///
    /// Used by the SQLite backend to route queries to the writer connection
    /// (serialized via mutex) vs the reader pool (concurrent). Set by the
    /// adapter layer based on the HTTP method / plan type.
    ///
    /// On Postgres this field is informational only (all queries use pooled connections).
    pub is_mutation: bool,

    /// Return the body as the database's JSON text ([`QueryResult::raw_body`])
    /// instead of a parsed [`QueryResult::body`]: no parse and re-serialize
    /// when the caller only forwards it. Backends may ignore it (SQLite does).
    pub raw_body: bool,
}

/// Transaction end behaviour, controlled by `Prefer: tx` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxEnd {
    /// Commit the transaction (default behaviour).
    Commit,
    /// Roll back the transaction after execution (dry-run / testing).
    Rollback,
}

/// The result of executing a CTE-wrapped query via [`Backend::execute`].
///
/// The SQL builder always wraps queries in a CTE that produces a single row
/// with these well-known columns. This unified shape means the driver always
/// decodes exactly one row regardless of the query type (read/insert/update/delete/rpc).
///
/// # PostgREST equivalent
///
/// The `ResultSet` decoded from Hasql's response in `Statements.hs`:
/// ```sql
/// WITH pgrst_source AS (<main_query>)
/// SELECT
///   coalesce(json_agg(_postgrest_t), '[]') AS body,
///   pg_catalog.count(_postgrest_t) AS page_total,
///   current_setting('response.status', true) AS response_status,
///   current_setting('response.headers', true) AS response_headers
/// FROM (SELECT * FROM pgrst_source) _postgrest_t
/// ```
///
/// # Backend behaviour
///
/// - **Postgres:** All fields populated from the CTE result row.
/// - **SQLite:** `response_status` and `response_headers` are always `None`
///   (SQLite has no GUC mechanism).
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The JSON body — an array of objects for reads, or a single object/array
    /// depending on `Prefer: return` for mutations.
    pub body: Value,

    /// Total count of rows matching the WHERE clause (before pagination).
    ///
    /// Populated when `Prefer: count=exact` is requested. For `count=planned`
    /// or `count=estimated`, this comes from `EXPLAIN` parsing (Postgres only).
    pub total_count: Option<i64>,

    /// Count of rows on this page (after LIMIT/OFFSET).
    pub page_total: Option<i64>,

    /// HTTP response status override from the `response.status` GUC.
    ///
    /// SQL functions can call `SET LOCAL response.status = '201'` to override
    /// the default response status code. Postgres only.
    pub response_status: Option<u16>,

    /// HTTP response headers from the `response.headers` GUC.
    ///
    /// SQL functions can call `SET LOCAL response.headers = '[{"X-Custom": "val"}]'`
    /// to inject custom response headers. Postgres only.
    pub response_headers: Option<Vec<(String, String)>>,

    /// Whether this was a fresh insert (vs an update) during UPSERT.
    ///
    /// Used to distinguish 201 Created vs 200 OK for `ON CONFLICT` operations.
    /// Populated from the `pgrst.inserted` GUC. Postgres only.
    pub was_insert: Option<bool>,

    /// The body as the database's JSON text, when requested with
    /// [`ExecContext::raw_body`] and the backend supports it. `body` is then
    /// `Null`; read through [`QueryResult::json`] or [`QueryResult::json_bytes`].
    pub raw_body: Option<bytes::Bytes>,
}

impl QueryResult {
    /// The body as a JSON value, parsing the raw text if that's what we have.
    pub fn json(&self) -> std::borrow::Cow<'_, Value> {
        match &self.raw_body {
            Some(raw) => {
                std::borrow::Cow::Owned(serde_json::from_slice(raw).unwrap_or(Value::Null))
            }
            None => std::borrow::Cow::Borrowed(&self.body),
        }
    }

    /// The body serialized as JSON, without re-serializing when it's raw.
    pub fn json_bytes(&self) -> bytes::Bytes {
        match &self.raw_body {
            Some(raw) => raw.clone(),
            None => serde_json::to_vec(&self.body).unwrap_or_default().into(),
        }
    }
}

/// A stream of schema-change notifications.
///
/// Yields `()` whenever the database schema has changed and the [`SchemaCache`]
/// should be reloaded.
///
/// # Backend behaviour
///
/// - **Postgres:** Driven by `LISTEN pgvis` on a dedicated connection.
///   Schema changes trigger `NOTIFY pgvis` (manually or via event trigger).
/// - **SQLite:** Returns `None` from [`Backend::watch_schema`] — callers fall
///   back to `PRAGMA schema_version` polling, file-mtime watching, or SIGHUP.
pub type SchemaChangeStream = Pin<Box<dyn Stream<Item = ()> + Send>>;

// ---------------------------------------------------------------------------
// The Backend trait
// ---------------------------------------------------------------------------

/// The core database abstraction for pgvis.
///
/// Implemented by `pgvis-postgres` (and later `pgvis-sqlite`). This trait is
/// **object-safe** — adapters store `Arc<dyn Backend>` and never reference
/// concrete backend types.
///
/// # Design Decisions
///
/// 1. **Object-safe via [`BoxFuture`]** — allows `dyn Backend` without `async_trait`
///    proc-macro. The one-allocation-per-call cost is negligible vs network I/O.
///
/// 2. **`Value`-based params** — the SQL builder produces `serde_json::Value` params
///    regardless of backend. The driver converts to native types internally.
///
/// 3. **Single `execute` method** — the SQL builder fully renders the CTE-wrapped
///    SQL string. The backend just runs it. No per-operation methods.
///
/// 4. **`dialect()` is synchronous** — returns a `&'static Dialect` (cheap, no I/O).
///
/// # Example (consuming the trait)
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use pgvis_core::{Backend, IntrospectConfig};
///
/// async fn introspect_and_print(backend: Arc<dyn Backend>) {
///     let cfg = IntrospectConfig::default();
///     let cache = backend.introspect(&cfg).await.unwrap();
///     println!("Found {} tables", cache.tables.len());
/// }
/// ```
pub trait Backend: Send + Sync + 'static {
    /// Load the full schema cache from the database.
    ///
    /// Called at startup and on every schema reload event. The implementation
    /// should run all introspection queries and assemble a complete [`SchemaCache`].
    ///
    /// # PostgREST equivalent
    ///
    /// `SchemaCache.loadSchemaCache` — runs `tablesSqlQuery`, `allM2OandO2ORels`,
    /// `allFunctions`, `dataRepresentations`, `mediaHandlers`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Introspection`] if any introspection query fails.
    fn introspect(&self, cfg: &IntrospectConfig) -> BoxFuture<'_, Result<SchemaCache, Error>>;

    /// Execute a fully-rendered SQL statement against the database.
    ///
    /// The SQL is expected to be CTE-wrapped by the SQL builder, producing a
    /// single result row with `body`, `page_total`, `response_status`, and
    /// `response_headers` columns.
    ///
    /// # Parameters
    ///
    /// - `ctx`: Per-request context (role, claims, timeout, tx preference)
    /// - `sql`: The complete SQL statement to execute
    /// - `params`: Positional parameters as JSON values (the backend converts
    ///   to native types based on context)
    ///
    /// # PostgREST equivalent
    ///
    /// `Hasql.Pool.use` + statement execution within a transaction that applies
    /// role switching and GUC settings.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Execution`] on query failure. The error message should
    /// include the database error code/detail when available.
    fn execute(
        &self,
        ctx: &ExecContext,
        sql: &str,
        params: &[Value],
    ) -> BoxFuture<'_, Result<QueryResult, Error>>;

    /// Subscribe to schema-change notifications.
    ///
    /// Returns `Some(stream)` if the backend supports push-based schema change
    /// detection, or `None` if it doesn't (callers should fall back to polling
    /// or manual reload triggers).
    ///
    /// # Backend behaviour
    ///
    /// - **Postgres:** Returns a stream driven by `LISTEN pgvis` on a dedicated
    ///   connection (outside the pool). Reconnects on transient failure with
    ///   exponential backoff.
    /// - **SQLite:** Returns `None`. The server layer uses `PRAGMA schema_version`
    ///   polling, filesystem `notify` crate watching, or `POST /admin/reload`.
    ///
    /// # Default
    ///
    /// Returns `None` (no push notifications).
    fn watch_schema(&self) -> BoxFuture<'_, Option<SchemaChangeStream>> {
        Box::pin(async { None })
    }

    /// The SQL dialect for this backend.
    ///
    /// Returns a static reference to a [`Dialect`] struct that the SQL builder
    /// uses to emit correct SQL for this database. Cheap to call — no I/O.
    ///
    /// # Implementors
    ///
    /// - Postgres backends return [`&POSTGRES`](crate::dialect::POSTGRES)
    /// - SQLite backends return [`&SQLITE`](crate::dialect::SQLITE)
    fn dialect(&self) -> &'static Dialect;

    /// A snapshot of the connection pool requests run on, for monitoring.
    /// `None` when the backend has no such pool.
    fn pool_status(&self) -> Option<PoolStatus> {
        None
    }
}

/// Connection-pool occupancy, as reported by [`Backend::pool_status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct PoolStatus {
    /// The most connections the pool will open.
    pub max_size: usize,
    /// Connections currently open (idle or checked out).
    pub size: usize,
    /// Idle connections ready to be checked out.
    pub available: usize,
    /// Requests waiting for a connection. Persistently above zero means the
    /// pool is too small for the load (or connections are held too long).
    pub waiting: usize,
}
