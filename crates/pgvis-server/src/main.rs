//! pgvis — narrate a Postgres (or SQLite) database over REST, OpenAPI, and MCP.
//!
//! This binary uses [`pgvis_lib`] as its library, ensuring the same code path
//! that end-users get when embedding pgvis in their own applications.

use clap::Parser;
use pgvis_core::Config;

/// Storyvis AI / pgvis — narrate a database over REST, OpenAPI, and MCP.
#[derive(Parser)]
#[command(name = "pgvis", version, about)]
struct Cli {
    /// Database DSN (`postgres://...` or `sqlite:///path.db`).
    #[arg(short, long, env = "PGVIS_DSN")]
    dsn: String,

    /// Path to config file (TOML). Falls back to PGVIS_* env vars.
    #[arg(short, long, env = "PGVIS_CONFIG")]
    config: Option<std::path::PathBuf>,

    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(clap::Subcommand)]
enum Cmd {
    /// Start the HTTP server (REST + optional MCP over Streamable HTTP).
    Serve {
        /// Bind address for the HTTP server.
        #[arg(short, long, default_value = "0.0.0.0:3000", env = "PGVIS_BIND")]
        bind: String,

        /// Which database schemas to expose (comma-separated or repeated).
        /// Defaults to "public".
        #[arg(short, long, env = "PGVIS_SCHEMAS", value_delimiter = ',')]
        schema: Vec<String>,

        /// Also serve MCP over Streamable HTTP at /mcp endpoint.
        #[arg(long, default_value = "false")]
        mcp_http: bool,

        /// Read replica DSNs for load balancing (repeated or comma-separated).
        /// Enables lag-aware read routing across replicas.
        #[arg(long, env = "PGVIS_REPLICA_DSNS", value_delimiter = ',')]
        replica_dsn: Vec<String>,

        /// Enable the in-memory data cache for read queries.
        #[arg(long, env = "PGVIS_CACHE_ENABLED")]
        cache_enabled: bool,

        /// Cache TTL in seconds (how long entries live before expiring).
        #[arg(long, env = "PGVIS_CACHE_TTL")]
        cache_ttl: Option<u64>,

        /// Maximum number of entries the cache can hold.
        #[arg(long, env = "PGVIS_CACHE_MAX_ENTRIES")]
        cache_max_entries: Option<u64>,

        /// Also cache list/collection queries (not just primary key lookups).
        #[arg(long, env = "PGVIS_CACHE_LISTS")]
        cache_lists: bool,
    },
    /// Run MCP server over stdio (for Claude Desktop / agent integrations).
    #[cfg(feature = "mcp")]
    Mcp {
        /// Which database schemas to expose. Defaults to "public" (or "main"
        /// for SQLite). Overrides any `schemas` value from the config file or
        /// `PGVIS_SCHEMAS`.
        #[arg(short, long, env = "PGVIS_SCHEMAS", value_delimiter = ',')]
        schema: Vec<String>,

        /// Expose only read tools (no create/update/delete/RPC). Suitable for
        /// LLMs that should browse but not mutate. Equivalent to setting
        /// `read_only = true` in the config.
        #[arg(long, default_value = "false")]
        read_only: bool,
    },
    /// Print the OpenAPI 3.0 document and exit.
    Openapi,
    /// Dump the introspected schema cache as JSON.
    Inspect,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing. We always write to stderr — the `mcp` subcommand
    // uses stdout for the JSON-RPC protocol stream, and `openapi`/`inspect`
    // print their JSON output to stdout. Logs on stdout would corrupt any of
    // those. stderr is the right channel in every case; for `serve` it's
    // equally fine because HTTP responses go over the network, not stdout.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pgvis=info,tower_http=info".into()),
        )
        .with_writer(std::io::stderr)
        .json()
        .init();

    let cli = Cli::parse();
    let mut config = load_config(cli.config.as_deref())?;

    match cli.cmd.unwrap_or(Cmd::Serve {
        bind: "0.0.0.0:3000".into(),
        schema: vec![],
        mcp_http: false,
        replica_dsn: vec![],
        cache_enabled: false,
        cache_ttl: None,
        cache_max_entries: None,
        cache_lists: false,
    }) {
        Cmd::Serve {
            bind,
            schema,
            mcp_http,
            replica_dsn,
            cache_enabled,
            cache_ttl,
            cache_max_entries,
            cache_lists,
        } => {
            // Override schemas from CLI if provided
            if !schema.is_empty() {
                config.schemas = schema;
            }

            // Override replica DSNs from CLI if provided
            if !replica_dsn.is_empty() {
                config.replica.replica_dsns = replica_dsn;
            }

            // Override cache settings from CLI if provided
            if cache_enabled {
                config.cache.enabled = true;
            }
            if let Some(ttl) = cache_ttl {
                config.cache.ttl_seconds = ttl;
            }
            if let Some(max) = cache_max_entries {
                config.cache.max_entries = max;
            }
            if cache_lists {
                config.cache.cache_lists = true;
            }

            tracing::info!(
                dsn = %cli.dsn,
                bind = %bind,
                schemas = ?config.schemas,
                replicas = config.replica.replica_dsns.len(),
                mcp_http,
                "starting pgvis server",
            );

            let mut builder = pgvis_lib::Builder::new(&cli.dsn).config(config);

            #[cfg(feature = "mcp")]
            if mcp_http {
                builder = builder.with_mcp_http();
            }

            let components = builder.build_components().await?;

            let listener = tokio::net::TcpListener::bind(&bind).await?;
            tracing::info!("listening on {bind}");
            axum::serve(listener, components.router).await?;
        }

        #[cfg(feature = "mcp")]
        Cmd::Mcp { schema, read_only } => {
            // CLI flags override anything coming from the config layer.
            if !schema.is_empty() {
                config.schemas = schema;
            }
            if read_only {
                config.read_only = true;
            }

            tracing::info!(
                dsn = %cli.dsn,
                schemas = ?config.schemas,
                read_only = config.read_only,
                "starting pgvis MCP server (stdio)",
            );

            let mcp_server = pgvis_lib::Builder::new(&cli.dsn)
                .config(config)
                .build_mcp_server()
                .await?;

            pgvis_lib::pgvis_mcp::serve_stdio(mcp_server)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        Cmd::Openapi => {
            let components = pgvis_lib::Builder::new(&cli.dsn)
                .config(config)
                .build_components()
                .await?;

            let cache = components.cache.load();
            let spec = pgvis_lib::pgvis_router::openapi::generate_spec(&cache, &components.config);
            let json = serde_json::to_string_pretty(&spec)?;
            println!("{json}");
        }

        Cmd::Inspect => {
            let components = pgvis_lib::Builder::new(&cli.dsn)
                .config(config)
                .build_components()
                .await?;

            let cache = components.cache.load();
            let json = serde_json::to_string_pretty(&*cache)?;
            println!("{json}");
        }
    }

    Ok(())
}

/// Load configuration from file and/or environment variables.
///
/// Configuration is layered (later sources override earlier):
/// 1. Defaults from [`Config::default()`]
/// 2. TOML config file (if `--config` flag or `PGVIS_CONFIG` env var is set)
/// 3. Environment variables prefixed with `PGVIS_`
fn load_config(path: Option<&std::path::Path>) -> anyhow::Result<Config> {
    use figment::Figment;
    use figment::providers::{Env, Format, Serialized, Toml};

    let mut figment = Figment::from(Serialized::defaults(Config::default()));

    // Layer 2: TOML config file (if provided)
    if let Some(path) = path {
        figment = figment.merge(Toml::file(path));
    }

    // Layer 3: Environment variables (PGVIS_SCHEMAS, PGVIS_JWT_SECRET, etc.)
    // Uses lowercase field names with underscore splitting:
    // PGVIS_JWT_SECRET → jwt_secret, PGVIS_MAX_ROWS → max_rows
    figment = figment.merge(Env::prefixed("PGVIS_").lowercase(true));

    let config: Config = figment.extract()?;
    Ok(config)
}
