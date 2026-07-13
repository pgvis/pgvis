//! Schema introspection for Postgres.
//!
//! Queries `pg_catalog` system tables to discover tables, columns, relationships,
//! routines, and data representations. Assembles results into a [`SchemaCache`].
//!
//! Ported from PostgREST's `SchemaCache.hs`.

pub mod relationships;
pub mod representations;
pub mod routines;
pub mod tables;

use pgvis_core::backend::IntrospectConfig;
use pgvis_core::cache::SchemaCache;
use pgvis_core::cache_post_process;
use pgvis_core::error::Error;
use tokio_postgres::Client;
use tracing::info;

/// Minimum supported PostgreSQL version (17.0).
/// pgvis requires PostgreSQL 17 or later for:
/// - MERGE statement support (future upsert improvements)
/// - JSON subscript syntax
/// - Improved EXPLAIN output for planned counts
const MIN_PG_VERSION: i32 = 170000;

/// Check that the connected PostgreSQL server meets the minimum version requirement.
///
/// Queries `server_version_num` and rejects anything below PostgreSQL 17.
/// Returns the version number on success.
async fn check_pg_version(client: &Client) -> Result<i32, Error> {
    let row = client
        .query_one("SHOW server_version_num", &[])
        .await
        .map_err(|e| Error::Introspection(format!("failed to check server version: {e}")))?;

    let version_str: String = row.get(0);
    let version_num: i32 = version_str.trim().parse().unwrap_or(0);

    if version_num < MIN_PG_VERSION {
        let major = version_num / 10000;
        let minor = (version_num / 100) % 100;
        return Err(Error::Introspection(format!(
            "PostgreSQL {major}.{minor} detected (version_num={version_num}). \
             pgvis requires PostgreSQL 17 or later."
        )));
    }

    let major = version_num / 10000;
    let minor = (version_num / 100) % 100;
    info!(version = %format!("{major}.{minor}"), version_num, "PostgreSQL version verified");

    Ok(version_num)
}

/// Load the full schema cache by running introspection queries against Postgres.
///
/// Executes all introspection queries within the scope of a voided `search_path`
/// (ensuring fully-qualified names), then applies post-processing to infer
/// M2M relationships and inverse relationships.
///
/// # Parameters
///
/// - `client` — A connected `tokio_postgres::Client`
/// - `cfg` — Specifies which schemas to introspect
///
/// # Errors
///
/// Returns [`Error::Introspection`] if any introspection query fails or if the
/// PostgreSQL version is below 17.
pub async fn load_schema_cache(
    client: &mut Client,
    cfg: &IntrospectConfig,
) -> Result<SchemaCache, Error> {
    // Enforce minimum PG version before proceeding with introspection.
    check_pg_version(client).await?;

    // Wrap in a RAII transaction so SET LOCAL takes effect for all introspection
    // queries. Without a transaction, SET LOCAL is a no-op on pooled connections.
    // If this future is dropped mid-introspection, `Transaction`'s Drop impl
    // rolls back, so the connection never returns to the pool with an open
    // transaction or a lingering `search_path` override.
    let tx = client
        .transaction()
        .await
        .map_err(|e| Error::Introspection(format!("failed to begin introspection tx: {e}")))?;

    tx.batch_execute("SET LOCAL search_path = ''")
        .await
        .map_err(|e| Error::Introspection(format!("failed to set search_path: {e}")))?;

    // Run introspection queries on the transaction's client (SET LOCAL applies).
    let result = run_introspection(tx.client(), cfg).await;

    // Always end the transaction. Either way, SET LOCAL is reverted when it ends.
    let end = if result.is_ok() {
        tx.commit().await
    } else {
        tx.rollback().await
    };
    end.map_err(|e| Error::Introspection(format!("failed to end introspection tx: {e}")))?;

    result
}

/// Inner implementation that runs inside the transaction opened by [`load_schema_cache`].
async fn run_introspection(client: &Client, cfg: &IntrospectConfig) -> Result<SchemaCache, Error> {
    let schemas: &[String] = &cfg.schemas;

    info!(schemas = ?cfg.schemas, "introspecting database schema");

    let tables = tables::query_tables(client, schemas).await?;
    let rels = relationships::query_relationships(client, schemas).await?;
    let routines = routines::query_routines(client, schemas).await?;
    let representations = representations::query_representations(client).await?;

    // Assemble the cache
    let mut cache = SchemaCache {
        built_at: Some(std::time::SystemTime::now()),
        schema_version: None,
        tables,
        relationships: rels,
        computed_relationships: Vec::new(), // TODO: allComputedRels
        routines,
        representations,
        media_handlers: std::collections::HashMap::new(), // TODO: mediaHandlers
        relationship_index: std::collections::HashMap::new(),
    };

    // Post-processing order matches PostgREST: M2M inference first, then inverse rels.
    // In Haskell: `addInverseRels $ addM2MRels` — right-to-left application.
    cache_post_process::infer_m2m_relationships(&mut cache);
    cache_post_process::add_inverse_relationships(&mut cache);
    cache_post_process::build_relationship_index(&mut cache);
    cache_post_process::mark_fk_columns(&mut cache);

    let table_count = cache.tables.len();
    let rel_count = cache.relationships.len();
    let routine_count = cache.routines.len();
    info!(
        tables = table_count,
        relationships = rel_count,
        routines = routine_count,
        "schema cache loaded"
    );

    Ok(cache)
}
