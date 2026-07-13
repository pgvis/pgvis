//! # `pgvis-sqlite` — SQLite backend for pgvis.
//!
//! Implements [`pgvis_core::Backend`] using `rusqlite` + `tokio-rusqlite`.
//!
//! ## Responsibilities
//!
//! - **Connection management** — single writer + reader pool in WAL mode
//! - **Schema introspection** from `sqlite_master` and `PRAGMA` queries
//! - **Query execution** with Rust-side JSON assembly (no CTE wrapping)
//! - **Type coercion** — INTEGER→bool, TEXT→JSON parsing based on declared type
//!
//! ## Example
//!
//! ```rust,ignore
//! use pgvis_sqlite::SqliteBackend;
//! use pgvis_core::{Backend, IntrospectConfig};
//!
//! let backend = SqliteBackend::open(":memory:").await?;
//! let cache = backend.introspect(&IntrospectConfig::default()).await?;
//! println!("Found {} tables", cache.tables.len());
//! ```

pub mod execute;
pub mod introspect;
pub(crate) mod util;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::BoxFuture;
use pgvis_core::backend::{
    Backend, ExecContext, IntrospectConfig, QueryResult, SchemaChangeStream,
};
use pgvis_core::cache::SchemaCache;
use pgvis_core::dialect::{self, Dialect};
use pgvis_core::error::Error;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_rusqlite::Connection;

/// PRAGMAs applied to every connection on open.
const INIT_PRAGMAS: &str = "\
    PRAGMA journal_mode = WAL;\
    PRAGMA busy_timeout = 5000;\
    PRAGMA synchronous = NORMAL;\
    PRAGMA foreign_keys = ON;\
    PRAGMA cache_size = -64000;\
    PRAGMA temp_store = MEMORY;\
";

/// The SQLite backend — implements [`Backend`] for SQLite databases.
///
/// Uses a single writer connection (serialized via mutex) and a pool of
/// reader connections for concurrent GET requests. All connections run in
/// WAL mode for maximum concurrency.
pub struct SqliteBackend {
    /// Single writer connection, serialized via async mutex.
    writer: Arc<Mutex<Connection>>,
    /// Pool of reader connections for concurrent reads.
    readers: Vec<Connection>,
    /// Round-robin index for reader selection.
    reader_idx: AtomicUsize,
}

impl SqliteBackend {
    /// Open a SQLite backend from a path or `:memory:`.
    ///
    /// Creates the writer connection and a pool of reader connections (4 by default).
    /// All connections are initialized with performance PRAGMAs.
    ///
    /// # Arguments
    ///
    /// * `path` — A file path, `:memory:`, or `sqlite:` URI.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Introspection`] if the database cannot be opened.
    pub async fn open(path: &str) -> Result<Self, Error> {
        Self::open_with_readers(path, 4).await
    }

    /// Open with a configurable number of reader connections.
    pub async fn open_with_readers(path: &str, reader_count: usize) -> Result<Self, Error> {
        // Always keep at least one reader; `next_reader` indexes `readers` with
        // `% self.readers.len()`, which panics on an empty pool.
        let reader_count = reader_count.max(1);
        let path = normalize_path(path);

        // For :memory: databases, use a shared-cache URI so all connections
        // see the same data. Each open() of ":memory:" creates a SEPARATE db.
        let effective_path = if path == ":memory:" {
            // Use a unique named in-memory database with shared cache
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("file:pgvis_mem_{id}?mode=memory&cache=shared")
        } else {
            path
        };

        // Open writer connection
        let writer = Connection::open(&effective_path)
            .await
            .map_err(|e| Error::Introspection(format!("failed to open SQLite writer: {e}")))?;

        // Initialize writer with PRAGMAs
        writer
            .call(|conn| {
                conn.execute_batch(INIT_PRAGMAS)?;
                Ok(())
            })
            .await
            .map_err(|e| Error::Introspection(format!("failed to init writer: {e}")))?;

        // Open reader connections
        let mut readers = Vec::with_capacity(reader_count);
        for i in 0..reader_count {
            let reader = Connection::open(&effective_path).await.map_err(|e| {
                Error::Introspection(format!("failed to open SQLite reader {i}: {e}"))
            })?;

            reader
                .call(|conn| {
                    conn.execute_batch(INIT_PRAGMAS)?;
                    Ok(())
                })
                .await
                .map_err(|e| Error::Introspection(format!("failed to init reader {i}: {e}")))?;

            readers.push(reader);
        }

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers,
            reader_idx: AtomicUsize::new(0),
        })
    }

    /// Execute raw SQL statements (for schema setup in tests/migrations).
    ///
    /// Uses the writer connection. Intended for DDL and bulk operations.
    pub async fn execute_raw(&self, sql: &str) -> Result<(), Error> {
        let sql = sql.to_string();
        let writer = self.writer.lock().await;
        writer
            .call(move |conn| {
                conn.execute_batch(&sql)?;
                Ok(())
            })
            .await
            .map_err(|e| Error::Execution {
                message: format!("execute_raw failed: {e}"),
                db_code: None,
                detail: None,
                hint: None,
            })
    }

    /// Get the next reader connection (round-robin).
    fn next_reader(&self) -> &Connection {
        let idx = self.reader_idx.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        &self.readers[idx]
    }
}

impl Backend for SqliteBackend {
    fn introspect(&self, cfg: &IntrospectConfig) -> BoxFuture<'_, Result<SchemaCache, Error>> {
        let cfg = cfg.clone();
        Box::pin(async move {
            let reader = self.next_reader();
            introspect::load_schema_cache(reader, &cfg).await
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
            if ctx.is_mutation {
                let writer = self.writer.lock().await;
                execute::execute_query(&writer, &ctx, &sql, &params).await
            } else {
                let reader = self.next_reader();
                execute::execute_query(reader, &ctx, &sql, &params).await
            }
        })
    }

    fn watch_schema(&self) -> BoxFuture<'_, Option<SchemaChangeStream>> {
        Box::pin(async { None })
    }

    fn dialect(&self) -> &'static Dialect {
        &dialect::SQLITE
    }
}

/// Normalize the DSN/path for SQLite.
///
/// Accepts bare paths, `:memory:`, and `sqlite:`/`sqlite://` URIs. For the
/// authority form (`sqlite://foo.db`), the `//` is stripped so relative paths
/// resolve to the intended file rather than `//foo.db`. `sqlite::memory:` maps to
/// `:memory:`.
fn normalize_path(path: &str) -> String {
    if path == ":memory:" {
        return path.to_string();
    }
    let stripped = path.strip_prefix("sqlite:").unwrap_or(path);
    // `sqlite://foo.db` -> `//foo.db` after removing `sqlite:`; drop the leading
    // `//` (empty authority) so the remainder is treated as a plain path. An
    // absolute URI (`sqlite:///abs/path` -> `///abs/path`) keeps its leading `/`.
    let stripped = stripped.strip_prefix("//").unwrap_or(stripped);
    stripped.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_path_memory() {
        assert_eq!(normalize_path(":memory:"), ":memory:");
        assert_eq!(normalize_path("sqlite::memory:"), ":memory:");
    }

    #[test]
    fn normalize_path_relative_authority() {
        assert_eq!(normalize_path("sqlite://foo.db"), "foo.db");
        assert_eq!(normalize_path("sqlite:foo.db"), "foo.db");
        assert_eq!(normalize_path("foo.db"), "foo.db");
    }

    #[test]
    fn normalize_path_absolute() {
        assert_eq!(normalize_path("sqlite:///abs/path.db"), "/abs/path.db");
        assert_eq!(normalize_path("/abs/path.db"), "/abs/path.db");
    }

    #[tokio::test]
    async fn open_with_zero_readers_does_not_panic() {
        // reader_count is clamped to >=1 so next_reader() cannot divide by zero.
        let backend = SqliteBackend::open_with_readers(":memory:", 0)
            .await
            .expect("open should succeed");
        // Exercise the reader pool.
        let _ = backend.next_reader();
    }
}
