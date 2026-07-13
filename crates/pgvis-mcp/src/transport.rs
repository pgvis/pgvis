//! Transport builders for the pgvis MCP server.
//!
//! Provides ready-to-use functions that wire [`McpServer`](crate::McpServer)
//! to MCP transports provided by the `rmcp` crate.
//!
//! ## Stdio Transport (Claude Desktop / CLI agents)
//!
//! ```rust,no_run
//! # use std::sync::Arc;
//! # use arc_swap::ArcSwap;
//! # use pgvis_core::{Config, SchemaCache, Backend, dialect::POSTGRES};
//! use pgvis_mcp::{McpServer, transport::serve_stdio};
//!
//! # async fn example(backend: Arc<dyn Backend>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! # let cache = Arc::new(ArcSwap::new(Arc::new(SchemaCache::default())));
//! # let config = Arc::new(Config::default());
//! # let dialect = Arc::new(POSTGRES.clone());
//! let server = McpServer::new(cache, config, dialect, backend);
//! serve_stdio(server).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Streamable HTTP Transport (hosted deployments)
//!
//! ```rust,no_run
//! # use std::sync::Arc;
//! # use arc_swap::ArcSwap;
//! # use pgvis_core::{Config, SchemaCache, Backend, dialect::POSTGRES};
//! use pgvis_mcp::{McpServer, transport::streamable_http_service};
//!
//! # fn example(backend: Arc<dyn Backend>) {
//! # let cache = Arc::new(ArcSwap::new(Arc::new(SchemaCache::default())));
//! # let config = Arc::new(Config::default());
//! # let dialect = Arc::new(POSTGRES.clone());
//! let server = McpServer::new(cache, config, dialect, backend);
//! let mcp_service = streamable_http_service(server);
//! // Mount `mcp_service` at e.g. `/mcp` alongside your REST routes
//! # }
//! ```

use std::sync::Arc;

use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

use crate::McpServer;

/// Start the MCP server over stdio (stdin/stdout).
///
/// Blocks until the transport closes (client disconnects, EOF on stdin, broken
/// pipe on stdout), or a shutdown signal arrives (SIGINT / Ctrl-C, and on Unix
/// also SIGTERM). On signal, the rmcp service is gracefully cancelled so any
/// in-flight tool call has a chance to finish writing its response before the
/// process exits.
///
/// Typically called from the `pgvis mcp` CLI subcommand.
///
/// # Errors
///
/// Returns an error if the MCP initialization handshake fails. Transport-level
/// I/O errors (including broken-pipe when the client dies mid-response) are
/// treated as a clean shutdown — they are how stdio MCP normally terminates.
pub async fn serve_stdio(server: McpServer) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let transport = rmcp::transport::io::stdio();

    // Race the initialize handshake against a shutdown signal. Without this,
    // a SIGINT/SIGTERM arriving before the client sends `initialize` (e.g.
    // because the user pressed Ctrl-C right after launch) would hit the
    // process's default signal disposition — which kills it abruptly — since
    // no tokio signal handler is installed until we call shutdown_signal.
    let service = tokio::select! {
        served = rmcp::serve_server(server, transport) => served?,
        reason = shutdown_signal() => {
            tracing::info!(reason = %reason, "MCP stdio server shut down before initialize");
            return Ok(());
        }
    };

    // Wire a signal listener to the service's cancellation token. When a
    // signal arrives, the rmcp event loop observes the token, finishes the
    // current message, then drops out cleanly — at which point `waiting()`
    // returns Ok(QuitReason::Cancelled).
    let cancel = service.cancellation_token();
    let shutdown_task = tokio::spawn(async move {
        let reason = shutdown_signal().await;
        tracing::info!(reason = %reason, "shutting down MCP stdio server");
        cancel.cancel();
    });

    let quit_reason = service.waiting().await?;
    tracing::info!(?quit_reason, "MCP stdio server stopped");

    // The signal task may still be parked on an as-yet-unfired signal; abort
    // it so the runtime can drop cleanly.
    shutdown_task.abort();

    Ok(())
}

/// Resolve when the process should shut down: SIGINT (Ctrl-C) on all platforms,
/// or SIGTERM on Unix. Returns the name of the signal that fired so the caller
/// can log it.
async fn shutdown_signal() -> &'static str {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        // If we can't install SIGTERM, just wait on Ctrl-C — better than
        // failing to start the server.
        let Ok(mut term) = signal(SignalKind::terminate()) else {
            return ctrl_c.await;
        };
        tokio::select! {
            name = ctrl_c => name,
            _ = term.recv() => "SIGTERM",
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await
    }
}

/// Create a tower `Service` that serves MCP over the Streamable HTTP transport.
///
/// The returned service handles the MCP JSON-RPC protocol over HTTP POST/GET
/// with SSE streaming. Mount it at a path (e.g., `/mcp`) using axum or any
/// tower-compatible router.
///
/// Uses a [`LocalSessionManager`] for in-process session management with
/// sensible defaults. Each incoming session spawns a new handler backed by
/// a clone of the provided [`McpServer`].
///
/// # Example
///
/// ```rust,no_run
/// use axum::Router;
/// # use std::sync::Arc;
/// # use arc_swap::ArcSwap;
/// # use pgvis_core::{Config, SchemaCache, Backend, dialect::POSTGRES};
/// # use pgvis_mcp::{McpServer, transport::streamable_http_service};
///
/// # fn example(backend: Arc<dyn Backend>) {
/// # let cache = Arc::new(ArcSwap::new(Arc::new(SchemaCache::default())));
/// # let config = Arc::new(Config::default());
/// # let dialect = Arc::new(POSTGRES.clone());
/// let server = McpServer::new(cache, config, dialect, backend);
/// let mcp_svc = streamable_http_service(server);
/// // Use with hyper/axum at a specific path
/// # }
/// ```
pub fn streamable_http_service(
    server: McpServer,
) -> StreamableHttpService<McpServer, LocalSessionManager> {
    let config = StreamableHttpServerConfig::default();
    let session_manager = Arc::new(LocalSessionManager::default());

    StreamableHttpService::new(move || Ok(server.clone()), session_manager, config)
}
