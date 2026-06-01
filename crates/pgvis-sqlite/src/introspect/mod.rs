//! Schema introspection for SQLite.
//!
//! Queries `sqlite_master` and PRAGMAs to discover tables, columns, relationships,
//! and unique constraints. Assembles results into a [`SchemaCache`].

pub mod relationships;
pub mod tables;

use pgvis_core::backend::IntrospectConfig;
use pgvis_core::cache::SchemaCache;
use pgvis_core::error::Error;
use tokio_rusqlite::Connection;
use tracing::info;

/// Load the full schema cache by running introspection queries against SQLite.
///
/// # Parameters
///
/// - `conn` — A `tokio_rusqlite::Connection`
/// - `cfg` — Specifies which schemas to introspect (ignored for SQLite — single namespace)
///
/// # Errors
///
/// Returns [`Error::Introspection`] if any introspection query fails.
pub async fn load_schema_cache(
    conn: &Connection,
    cfg: &IntrospectConfig,
) -> Result<SchemaCache, Error> {
    let _schemas = &cfg.schemas; // Ignored for SQLite (single namespace "main")

    info!("introspecting SQLite database schema");

    let tables = tables::query_tables(conn).await?;
    let relationships = relationships::query_relationships(conn, &tables).await?;

    let mut cache = SchemaCache {
        built_at: Some(std::time::SystemTime::now()),
        schema_version: None,
        tables,
        relationships,
        computed_relationships: Vec::new(), // SQLite has no computed relationships
        routines: indexmap::IndexMap::new(), // SQLite has no stored functions
        representations: std::collections::HashMap::new(),
        media_handlers: std::collections::HashMap::new(),
        relationship_index: std::collections::HashMap::new(),
    };

    // Post-processing: infer M2M relationships and mark FK columns
    pgvis_core::cache_post_process::infer_m2m_relationships(&mut cache);
    pgvis_core::cache_post_process::add_inverse_relationships(&mut cache);
    pgvis_core::cache_post_process::build_relationship_index(&mut cache);
    pgvis_core::cache_post_process::mark_fk_columns(&mut cache);

    let table_count = cache.tables.len();
    let rel_count = cache.relationships.len();
    info!(
        tables = table_count,
        relationships = rel_count,
        "SQLite schema cache loaded"
    );

    Ok(cache)
}
