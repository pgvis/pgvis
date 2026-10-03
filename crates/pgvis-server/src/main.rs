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
        /// Bind address for the HTTP server. Loopback by default; pass e.g.
        /// `0.0.0.0:3000` to expose it.
        #[arg(short, long, default_value = "127.0.0.1:3000", env = "PGVIS_BIND")]
        bind: String,

        /// Serve without `jwt_secret` or `anon_role`. Every request then runs
        /// as the DSN's own role (often the table owner, bypassing RLS), so
        /// this must be asked for explicitly.
        #[arg(long, env = "PGVIS_INSECURE_NO_AUTH")]
        insecure_no_auth: bool,

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

        /// Enable the pub/sub subsystem (Postgres LISTEN/NOTIFY).
        /// Exposes REST SSE endpoints at /pubsub/{channel} and MCP tools.
        #[arg(long, env = "PGVIS_PUBSUB_ENABLED")]
        pubsub_enabled: bool,

        /// Channel name prefix for pub/sub (default: "pgvis:").
        /// All Postgres LISTEN/NOTIFY channels are prefixed with this value.
        #[arg(long, env = "PGVIS_PUBSUB_CHANNEL_PREFIX")]
        pubsub_channel_prefix: Option<String>,
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
        bind: "127.0.0.1:3000".into(),
        insecure_no_auth: false,
        schema: vec![],
        mcp_http: false,
        replica_dsn: vec![],
        cache_enabled: false,
        cache_ttl: None,
        cache_max_entries: None,
        cache_lists: false,
        pubsub_enabled: false,
        pubsub_channel_prefix: None,
    }) {
        Cmd::Serve {
            bind,
            insecure_no_auth,
            schema,
            mcp_http,
            replica_dsn,
            cache_enabled,
            cache_ttl,
            cache_max_entries,
            cache_lists,
            pubsub_enabled,
            pubsub_channel_prefix,
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

            // Override pub/sub settings from CLI if provided
            if pubsub_enabled {
                config.pubsub.enabled = true;
            }
            if let Some(prefix) = pubsub_channel_prefix {
                config.pubsub.channel_prefix = prefix;
            }

            if config.jwt_secret.is_none() && config.anon_role.is_none() && !insecure_no_auth {
                anyhow::bail!(
                    "refusing to serve without auth: set jwt_secret and/or anon_role, \
                     or pass --insecure-no-auth to run every request as the DSN's role"
                );
            }

            tracing::info!(
                dsn = %redact_dsn(&cli.dsn),
                bind = %bind,
                schemas = ?config.schemas,
                replicas = config.replica.replica_dsns.len(),
                mcp_http,
                pubsub = config.pubsub.enabled,
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
                dsn = %redact_dsn(&cli.dsn),
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
        let text = read_strict_config(path)?;
        figment = figment.merge(Toml::string(&text));
    }

    // Layer 3: Environment variables.
    // - Nested keys use `__` as the separator, e.g.
    //   `PGVIS_CACHE__TTL_SECONDS` → cache.ttl_seconds.
    // - Flat keys map directly: `PGVIS_JWT_SECRET` → jwt_secret.
    // Note: without `.split("__")`, a `PGVIS_SCHEMAS` value crashed extraction
    // because figment tried to coerce the scalar into the `Vec<String>` field.
    figment = figment.merge(Env::prefixed("PGVIS_").split("__").lowercase(true));

    // Sequence-valued keys (`Vec<String>` fields) come in as comma-separated
    // scalars from the environment. `PGVIS_SCHEMAS=public,app` → ["public","app"].
    if let Ok(schemas) = std::env::var("PGVIS_SCHEMAS") {
        let list: Vec<String> = schemas
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !list.is_empty() {
            figment = figment.merge(Serialized::default("schemas", list));
        }
    }

    let config: Config = figment.extract()?;
    Ok(config)
}

/// Read a config file, failing on anything pgvis would silently ignore.
///
/// figment treats a missing file as empty and drops unknown keys, so a typo'd
/// path or a PostgREST-style key (`jwt-secret`) started the server with
/// defaults: no JWT, every request as the DSN's role.
fn read_strict_config(path: &std::path::Path) -> anyhow::Result<String> {
    use anyhow::Context;

    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let mut unknown = Vec::new();
    let _: Config = serde_ignored::deserialize(toml::Deserializer::new(&text), |key| {
        unknown.push(key.to_string())
    })
    .with_context(|| format!("parsing config file {}", path.display()))?;
    if !unknown.is_empty() {
        anyhow::bail!(
            "unknown keys in config file {}: {}",
            path.display(),
            unknown.join(", ")
        );
    }
    Ok(text)
}

/// The DSN with any password replaced, for logging.
///
/// Handles both URL (`postgres://user:pass@host/db`) and key/value
/// (`host=h password=pass`) forms.
fn redact_dsn(dsn: &str) -> String {
    if let Some((scheme, rest)) = dsn.split_once("://") {
        if let Some((userinfo, host)) = rest.split_once('@') {
            if let Some((user, _)) = userinfo.split_once(':') {
                return format!("{scheme}://{user}:***@{host}");
            }
        }
        return dsn.to_string();
    }
    dsn.split_whitespace()
        .map(|kv| match kv.split_once('=') {
            Some((key, _)) if key.eq_ignore_ascii_case("password") => format!("{key}=***"),
            _ => kv.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(name: &str, body: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("pgvis-{}-{name}", std::process::id()));
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn a_missing_config_file_is_an_error() {
        let missing = std::env::temp_dir().join("pgvis-definitely-missing.toml");
        assert!(load_config(Some(&missing)).is_err());
    }

    #[test]
    fn unknown_config_keys_are_an_error() {
        // PostgREST spelling: silently ignored before, which disabled auth.
        let path = write("unknown.toml", "jwt-secret = \"s3cret\"\n");
        let err = load_config(Some(&path)).unwrap_err().to_string();
        assert!(err.contains("jwt-secret"), "got: {err}");

        let nested = write("nested.toml", "[pool]\nsize = 4\nmax_sise = 9\n");
        assert!(load_config(Some(&nested)).is_err());
    }

    #[test]
    fn a_valid_config_file_still_loads() {
        let path = write("valid.toml", "jwt_secret = \"s3cret\"\nanon_role = \"web_anon\"\n");
        let config = load_config(Some(&path)).unwrap();
        assert_eq!(config.anon_role.as_deref(), Some("web_anon"));
        assert!(config.jwt_secret.is_some());
    }

    #[test]
    fn dsn_passwords_are_redacted() {
        assert_eq!(
            redact_dsn("postgres://app:hunter2@db:5432/prod"),
            "postgres://app:***@db:5432/prod"
        );
        assert_eq!(redact_dsn("postgres://app@db/prod"), "postgres://app@db/prod");
        assert_eq!(
            redact_dsn("host=db user=app password=hunter2"),
            "host=db user=app password=***"
        );
        assert_eq!(redact_dsn("sqlite:///tmp/x.db"), "sqlite:///tmp/x.db");
    }
}
