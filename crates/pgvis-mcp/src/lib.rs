//! `pgvis-mcp` — Embeddable MCP (Model Context Protocol) server for pgvis.
//!
//! Exposes database tables and functions as MCP tools that mirror the REST
//! route structure. Both adapters share the same `plan_request()` pipeline.
//!
//! # Architecture
//!
//! ```text
//! SchemaCache + Config
//!       │
//!       ├──► pgvis-router::build_app()     → axum Router (REST routes)
//!       │
//!       └──► pgvis-mcp::McpServer::new()   → MCP server (tools + resources)
//!                    │
//!                    ├──► transport::serve_stdio()            → stdio (Claude Desktop)
//!                    └──► transport::streamable_http_service() → HTTP/SSE (hosted agents)
//! ```
//!
//! # Quick Start — Stdio
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use arc_swap::ArcSwap;
//! use pgvis_core::{Config, SchemaCache, Backend, dialect::POSTGRES};
//! use pgvis_mcp::{McpServer, transport::serve_stdio};
//!
//! # async fn example(backend: Arc<dyn Backend>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let cache = Arc::new(ArcSwap::new(Arc::new(SchemaCache::default())));
//! let config = Arc::new(Config::default());
//! let dialect = Arc::new(POSTGRES.clone());
//!
//! let server = McpServer::new(cache, config, dialect, backend);
//! serve_stdio(server).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Quick Start — Streamable HTTP
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use arc_swap::ArcSwap;
//! use pgvis_core::{Config, SchemaCache, Backend, dialect::POSTGRES};
//! use pgvis_mcp::{McpServer, transport::streamable_http_service};
//!
//! # fn example(backend: Arc<dyn Backend>) {
//! let cache = Arc::new(ArcSwap::new(Arc::new(SchemaCache::default())));
//! let config = Arc::new(Config::default());
//! let dialect = Arc::new(POSTGRES.clone());
//!
//! let server = McpServer::new(cache, config, dialect, backend);
//! let mcp_service = streamable_http_service(server);
//! // Mount at /mcp alongside REST routes
//! # }
//! ```

pub mod server;
pub mod tools;
pub mod transport;
pub mod types;

// Primary embeddable APIs
pub use server::McpServer;
pub use transport::{serve_stdio, streamable_http_service};

// Existing APIs (still useful for custom integrations)
pub use tools::{build_mcp_resources, build_mcp_tools, handle_tool_call};
pub use types::*;
