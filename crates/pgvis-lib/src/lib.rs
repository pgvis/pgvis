//! `pgvis-lib` — One-liner to get an axum Router from a database DSN.
//!
//! This crate is the **single authoritative way** to construct the pgvis stack.
//! Both end-user applications and `pgvis-server` use this as their library.
//!
//! # Simple: Get a Router
//!
//! ```rust,no_run
//! use pgvis_lib::Builder;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let router = Builder::new("postgres://localhost/mydb")
//!     .schemas(vec!["public"])
//!     .build()
//!     .await?;
//! // Mount `router` into your axum app or serve directly
//! # Ok(())
//! # }
//! ```
//!
//! # SQLite Support
//!
//! ```rust,no_run
//! use pgvis_lib::Builder;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let router = Builder::new("sqlite:./mydb.sqlite3")
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Advanced: Access Internal Components
//!
//! ```rust,no_run
//! use pgvis_lib::Builder;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let components = Builder::new("postgres://localhost/mydb")
//!     .schemas(vec!["public"])
//!     .build_components()
//!     .await?;
//!
//! // Access the schema cache, backend, etc.
//! let cache = components.cache.load();
//! println!("Found {} tables", cache.tables.len());
//!
//! // Serve the router
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
//! axum::serve(listener, components.router).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # REST + MCP (Streamable HTTP)
//!
//! ```rust,no_run
//! use pgvis_lib::Builder;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let router = Builder::new("postgres://localhost/mydb")
//!     .schemas(vec!["public"])
//!     .with_mcp_http()
//!     .build()
//!     .await?;
//! // Serves both REST API and MCP at /mcp
//! # Ok(())
//! # }
//! ```
//!
//! # MCP Server (stdio)
//!
//! ```rust,no_run
//! use pgvis_lib::Builder;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let mcp_server = Builder::new("postgres://localhost/mydb")
//!     .schemas(vec!["public"])
//!     .build_mcp_server()
//!     .await?;
//! pgvis_mcp::serve_stdio(mcp_server).await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use arc_swap::ArcSwap;
use pgvis_core::Config;
use pgvis_core::backend::{Backend, IntrospectConfig};
use pgvis_core::cache::SchemaCache;
use pgvis_core::dialect::Dialect;

// Re-export key types for convenience
pub use pgvis_core;
pub use pgvis_router;

#[cfg(feature = "mcp")]
pub use pgvis_mcp;

#[cfg(feature = "postgres")]
pub use pgvis_postgres;

#[cfg(feature = "sqlite")]
pub use pgvis_sqlite;

// ---------------------------------------------------------------------------
// Components — the assembled pgvis stack
// ---------------------------------------------------------------------------

/// The assembled pgvis components — backend, cache, config, dialect, and router.
///
/// Returned by [`Builder::build_components()`] for consumers that need access to
/// the internal pieces (e.g. for OpenAPI generation, schema inspection, MCP stdio).
///
/// The `router` field is ready to serve with `axum::serve`.
#[non_exhaustive]
pub struct Components {
    /// The database backend (implements query execution).
    pub backend: Arc<dyn Backend>,
    /// The hot-swappable schema cache.
    pub cache: Arc<ArcSwap<SchemaCache>>,
    /// The shared configuration.
    pub config: Arc<Config>,
    /// The SQL dialect (Postgres/SQLite).
    pub dialect: Arc<Dialect>,
    /// The fully-wired axum Router (REST + OpenAPI + optionally MCP HTTP).
    pub router: axum::Router,
}

// ---------------------------------------------------------------------------
// DSN Detection
// ---------------------------------------------------------------------------

/// Detected database backend type from a DSN string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    /// PostgreSQL (DSN starts with `postgres://` or `postgresql://`)
    Postgres,
    /// SQLite (DSN starts with `sqlite:`, or ends with `.db`/`.sqlite`/`.sqlite3`, or is `:memory:`)
    Sqlite,
}

/// Detect the database kind from a DSN string.
///
/// # Detection rules
///
/// - `postgres://...` or `postgresql://...` → Postgres
/// - `sqlite:...`, `file:...`, or `:memory:` → SQLite
/// - Ends with `.db`, `.sqlite`, `.sqlite3` → SQLite
/// - Otherwise → Postgres (backward compatible default)
pub fn detect_db_kind(dsn: &str) -> DbKind {
    let lower = dsn.to_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        DbKind::Postgres
    } else if lower.starts_with("sqlite:") || lower.starts_with("file:") || dsn == ":memory:" {
        DbKind::Sqlite
    } else if lower.ends_with(".db") || lower.ends_with(".sqlite") || lower.ends_with(".sqlite3") {
        DbKind::Sqlite
    } else {
        // Default to Postgres for backward compatibility
        DbKind::Postgres
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for constructing a pgvis-powered axum Router or MCP server.
///
/// This is the single authoritative way to construct the pgvis stack.
/// Both `pgvis-server` and end-user applications use this builder.
///
/// The builder auto-detects the database backend from the DSN:
/// - `postgres://...` → Postgres backend
/// - `sqlite:./path.db` or `:memory:` → SQLite backend
pub struct Builder {
    dsn: String,
    config: Option<Config>,
    schemas: Option<Vec<String>>,
    #[cfg(feature = "mcp")]
    mcp_http: bool,
}

impl Builder {
    /// Create a new builder with the given DSN.
    ///
    /// The DSN determines which backend to use:
    /// - `postgres://...` or `postgresql://...` → Postgres
    /// - `sqlite:path` or `:memory:` → SQLite
    ///
    /// Uses default configuration. Call [`config()`](Self::config) to override.
    pub fn new(dsn: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            config: None,
            schemas: None,
            #[cfg(feature = "mcp")]
            mcp_http: false,
        }
    }

    /// Set a full configuration object.
    ///
    /// When set, this takes precedence over [`schemas()`](Self::schemas).
    /// The `schemas` field in the config will be used directly.
    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    /// Set which schemas to expose (convenience for simple cases).
    ///
    /// If [`config()`](Self::config) is also called, that takes precedence.
    /// For SQLite, schemas are ignored (single `"main"` namespace).
    pub fn schemas(mut self, schemas: Vec<impl Into<String>>) -> Self {
        self.schemas = Some(schemas.into_iter().map(Into::into).collect());
        self
    }

    /// Enable MCP Streamable HTTP transport merged into the router at `/mcp`.
    #[cfg(feature = "mcp")]
    pub fn with_mcp_http(mut self) -> Self {
        self.mcp_http = true;
        self
    }

    /// Build all components: backend, cache, config, dialect, and router.
    ///
    /// Returns a [`Components`] struct giving access to all internal pieces.
    /// Use this when you need the cache or backend for subcommands (inspect, openapi, mcp stdio).
    ///
    /// # Errors
    /// Returns an error if the database connection or introspection fails.
    pub async fn build_components(self) -> Result<Components, pgvis_core::error::Error> {
        let config = Arc::new(self.resolve_config());
        let backend = self.build_backend(&config).await?;

        let introspect_config = IntrospectConfig {
            schemas: config.schemas.clone(),
            extra_search_path: config.extra_search_path.clone(),
        };
        let cache = Arc::new(ArcSwap::new(Arc::new(
            backend.introspect(&introspect_config).await?,
        )));
        let dialect = Arc::new(backend.dialect().clone());

        // Build REST router
        let mut app = pgvis_router::build_app(
            cache.clone(),
            config.clone(),
            dialect.clone(),
            backend.clone(),
        );

        // Optionally merge MCP Streamable HTTP service
        #[cfg(feature = "mcp")]
        if self.mcp_http {
            let mcp_server = pgvis_mcp::McpServer::new(
                cache.clone(),
                config.clone(),
                dialect.clone(),
                backend.clone(),
            );
            let mcp_service = pgvis_mcp::streamable_http_service(mcp_server);
            app = app.route_service("/mcp", mcp_service);
        }

        Ok(Components {
            backend,
            cache,
            config,
            dialect,
            router: app,
        })
    }

    /// Build the axum Router directly.
    ///
    /// Convenience method equivalent to `build_components().await?.router`.
    ///
    /// # Errors
    /// Returns an error if the database connection or introspection fails.
    pub async fn build(self) -> Result<axum::Router, pgvis_core::error::Error> {
        Ok(self.build_components().await?.router)
    }

    /// Build a standalone MCP server for stdio transport.
    ///
    /// Use with [`pgvis_mcp::serve_stdio`] to run as a Claude Desktop MCP server.
    ///
    /// # Errors
    /// Returns an error if the database connection or introspection fails.
    #[cfg(feature = "mcp")]
    pub async fn build_mcp_server(self) -> Result<pgvis_mcp::McpServer, pgvis_core::error::Error> {
        let config = Arc::new(self.resolve_config());
        let backend = self.build_backend(&config).await?;

        let introspect_config = IntrospectConfig {
            schemas: config.schemas.clone(),
            extra_search_path: config.extra_search_path.clone(),
        };
        let cache = Arc::new(ArcSwap::new(Arc::new(
            backend.introspect(&introspect_config).await?,
        )));
        let dialect = Arc::new(backend.dialect().clone());

        Ok(pgvis_mcp::McpServer::new(cache, config, dialect, backend))
    }

    /// Create the appropriate backend based on DSN detection.
    async fn build_backend(
        &self,
        config: &Config,
    ) -> Result<Arc<dyn Backend>, pgvis_core::error::Error> {
        let db_kind = detect_db_kind(&self.dsn);
        match db_kind {
            DbKind::Postgres => {
                #[cfg(feature = "postgres")]
                {
                    if config.replica.replica_dsns.is_empty() {
                        let pg = pgvis_postgres::PgBackend::new(
                            &self.dsn,
                            &config.pool,
                        )?;
                        Ok(Arc::new(pg))
                    } else {
                        let pg = pgvis_postgres::PgReplicaBackend::new(
                            &self.dsn,
                            &config.pool,
                            &config.replica,
                        )?;
                        Ok(Arc::new(pg))
                    }
                }
                #[cfg(not(feature = "postgres"))]
                {
                    Err(pgvis_core::error::Error::Internal(
                        "Postgres support not compiled in (enable `postgres` feature)".into(),
                    ))
                }
            }
            DbKind::Sqlite => {
                #[cfg(feature = "sqlite")]
                {
                    let sqlite = pgvis_sqlite::SqliteBackend::open(&self.dsn).await?;
                    Ok(Arc::new(sqlite))
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    Err(pgvis_core::error::Error::Internal(
                        "SQLite support not compiled in (enable `sqlite` feature)".into(),
                    ))
                }
            }
        }
    }

    /// Resolve the effective Config from builder fields.
    fn resolve_config(&self) -> Config {
        if let Some(config) = &self.config {
            config.clone()
        } else {
            let default_schemas = if detect_db_kind(&self.dsn) == DbKind::Sqlite {
                vec!["main".to_string()]
            } else {
                vec!["public".to_string()]
            };
            let schemas = self.schemas.clone().unwrap_or(default_schemas);
            Config {
                schemas,
                ..Default::default()
            }
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_postgres() {
        assert_eq!(
            detect_db_kind("postgres://localhost/mydb"),
            DbKind::Postgres
        );
        assert_eq!(
            detect_db_kind("postgresql://localhost/mydb"),
            DbKind::Postgres
        );
        assert_eq!(
            detect_db_kind("POSTGRES://localhost/mydb"),
            DbKind::Postgres
        );
    }

    #[test]
    fn test_detect_sqlite() {
        assert_eq!(detect_db_kind("sqlite:./mydb.db"), DbKind::Sqlite);
        assert_eq!(detect_db_kind("sqlite::memory:"), DbKind::Sqlite);
        assert_eq!(detect_db_kind(":memory:"), DbKind::Sqlite);
        assert_eq!(detect_db_kind("./mydb.sqlite3"), DbKind::Sqlite);
        assert_eq!(detect_db_kind("/path/to/file.db"), DbKind::Sqlite);
        assert_eq!(detect_db_kind("data.sqlite"), DbKind::Sqlite);
        // file: URI format (SQLite shared-cache, etc.)
        assert_eq!(
            detect_db_kind("file:mydb?mode=memory&cache=shared"),
            DbKind::Sqlite
        );
        assert_eq!(detect_db_kind("file:/path/to/db.sqlite3"), DbKind::Sqlite);
    }

    #[test]
    fn test_detect_default_postgres() {
        // Unknown formats default to Postgres for backward compatibility
        assert_eq!(
            detect_db_kind("host=localhost dbname=mydb"),
            DbKind::Postgres
        );
    }
}
