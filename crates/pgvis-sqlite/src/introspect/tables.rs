//! Tables + columns introspection via SQLite PRAGMAs and `sqlite_master`.
//!
//! Queries:
//! - `sqlite_master` — discover table/view names and types
//! - `PRAGMA table_xinfo(name)` — columns with type, nullability, default, generated flag
//! - `PRAGMA index_list(name)` + `PRAGMA index_info(idx)` — unique constraints and PK

use indexmap::IndexMap;
use pgvis_core::cache::{Column, QualifiedIdentifier, Table, UniqueConstraint};
use pgvis_core::error::Error;
use tokio_rusqlite::Connection;

use crate::util::{SqliteInternalError, escape_ident};

/// Query all tables and views from the SQLite database.
///
/// Returns an ordered map of `QualifiedIdentifier → Table`.
/// All tables use schema `"main"` (SQLite's single namespace).
pub async fn query_tables(
    conn: &Connection,
) -> Result<IndexMap<QualifiedIdentifier, Table>, Error> {
    conn.call(|conn| {
        let mut tables = IndexMap::new();

        // Step 1: Get all table/view names from sqlite_master
        let mut table_stmt = conn
            .prepare(
                "SELECT name, type FROM sqlite_master \
                 WHERE type IN ('table', 'view') \
                 AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
            )
            .map_err(|e| tokio_rusqlite::Error::Rusqlite(e))?;

        let table_entries: Vec<(String, String)> = table_stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| tokio_rusqlite::Error::Rusqlite(e))?
            .filter_map(|r| r.ok())
            .collect();

        // Step 2: For each table/view, introspect columns and constraints
        for (name, obj_type) in &table_entries {
            let is_view = obj_type == "view";
            let ident = QualifiedIdentifier::new("main", name.as_str());

            // Query columns via PRAGMA table_xinfo (includes hidden/generated cols)
            let columns =
                query_columns(conn, name).map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?;

            // Query unique constraints (includes PK) via PRAGMA index_list + index_info
            let unique_constraints = query_unique_constraints(conn, name)
                .map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?;

            // Determine PK columns
            let pk_cols = determine_pk_cols(conn, name, &unique_constraints)
                .map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?;

            tables.insert(
                ident.clone(),
                Table {
                    ident,
                    description: None, // SQLite has no COMMENT mechanism
                    is_view,
                    insertable: !is_view,
                    updatable: !is_view,
                    deletable: !is_view,
                    pk_cols,
                    unique_constraints,
                    columns,
                },
            );
        }

        Ok(tables)
    })
    .await
    .map_err(|e| Error::Introspection(format!("SQLite tables introspection failed: {e}")))
}

/// Introspect columns for a single table using `PRAGMA table_xinfo`.
fn query_columns(
    conn: &rusqlite::Connection,
    table_name: &str,
) -> Result<IndexMap<String, Column>, SqliteInternalError> {
    let sql = format!("PRAGMA table_xinfo(\"{}\")", escape_ident(table_name));
    let mut stmt = conn.prepare(&sql).map_err(|e| {
        SqliteInternalError::msg(format!(
            "failed to prepare table_xinfo for {table_name}: {e}"
        ))
    })?;

    // table_xinfo columns: cid, name, type, notnull, dflt_value, pk, hidden
    let mut columns = IndexMap::new();
    let mut rows = stmt.query([]).map_err(|e| {
        SqliteInternalError::msg(format!("failed to query table_xinfo for {table_name}: {e}"))
    })?;

    while let Some(row) = rows.next().map_err(|e| {
        SqliteInternalError::msg(format!(
            "table_xinfo iteration failed for {table_name}: {e}"
        ))
    })? {
        let cid: i32 = row.get(0).unwrap_or(0);
        let name: String = row.get(1).unwrap_or_default();
        let typ: String = row.get(2).unwrap_or_default();
        let notnull: bool = row.get::<_, i32>(3).unwrap_or(0) != 0;
        let default: Option<String> = row.get(4).ok();
        let pk: i32 = row.get(5).unwrap_or(0);
        let hidden: i32 = row.get(6).unwrap_or(0);

        // hidden values: 0 = normal, 2 = generated stored, 3 = generated virtual
        let is_generated = hidden == 2 || hidden == 3;

        // Normalize the type name for consistent handling
        let normalized_type = normalize_sqlite_type(&typ);

        columns.insert(
            name.clone(),
            Column {
                name,
                description: None,
                nullable: !notnull,
                is_generated,
                updatable: !is_generated,
                typ: normalized_type.clone(),
                nominal_type: if typ.to_uppercase() == normalized_type {
                    None
                } else {
                    Some(typ)
                },
                max_len: None, // SQLite doesn't enforce character length
                default,
                enum_values: Vec::new(), // SQLite has no enums
                is_pk: pk > 0,
                is_fk: false, // Will be set during relationship processing
                ordinal: cid + 1,
            },
        );
    }

    Ok(columns)
}

/// Determine primary key columns for a table.
///
/// Uses `PRAGMA table_info` to find columns with `pk > 0`, ordered by pk index.
fn determine_pk_cols(
    conn: &rusqlite::Connection,
    table_name: &str,
    unique_constraints: &[UniqueConstraint],
) -> Result<Vec<String>, SqliteInternalError> {
    // First check if there's a PK in our unique constraints
    for uc in unique_constraints {
        if uc.is_pk {
            return Ok(uc.columns.clone());
        }
    }

    // Fallback: query PRAGMA table_info for pk columns
    let sql = format!("PRAGMA table_info(\"{}\")", escape_ident(table_name));
    let mut stmt = conn.prepare(&sql).map_err(|e| {
        SqliteInternalError::msg(format!(
            "failed to prepare table_info for {table_name}: {e}"
        ))
    })?;

    let mut pk_entries: Vec<(i32, String)> = Vec::new();
    let mut rows = stmt.query([]).map_err(|e| {
        SqliteInternalError::msg(format!("failed to query table_info for {table_name}: {e}"))
    })?;

    while let Some(row) = rows.next().map_err(|e| {
        SqliteInternalError::msg(format!("table_info iteration failed for {table_name}: {e}"))
    })? {
        let name: String = row.get(1).unwrap_or_default();
        let pk: i32 = row.get(5).unwrap_or(0);
        if pk > 0 {
            pk_entries.push((pk, name));
        }
    }

    // Sort by pk index (composite PKs are ordered)
    pk_entries.sort_by_key(|(idx, _)| *idx);
    Ok(pk_entries.into_iter().map(|(_, name)| name).collect())
}

/// Query unique constraints via `PRAGMA index_list` + `PRAGMA index_info`.
fn query_unique_constraints(
    conn: &rusqlite::Connection,
    table_name: &str,
) -> Result<Vec<UniqueConstraint>, SqliteInternalError> {
    let sql = format!("PRAGMA index_list(\"{}\")", escape_ident(table_name));
    let mut stmt = conn.prepare(&sql).map_err(|e| {
        SqliteInternalError::msg(format!(
            "failed to prepare index_list for {table_name}: {e}"
        ))
    })?;

    // index_list columns: seq, name, unique, origin, partial
    let mut constraints = Vec::new();
    let mut rows = stmt.query([]).map_err(|e| {
        SqliteInternalError::msg(format!("failed to query index_list for {table_name}: {e}"))
    })?;

    while let Some(row) = rows.next().map_err(|e| {
        SqliteInternalError::msg(format!("index_list iteration failed for {table_name}: {e}"))
    })? {
        let index_name: String = row.get(1).unwrap_or_default();
        let is_unique: bool = row.get::<_, i32>(2).unwrap_or(0) != 0;
        let origin: String = row.get(3).unwrap_or_default();

        if !is_unique {
            continue;
        }

        // partial indexes (those with WHERE) don't count as full unique constraints
        let is_partial: bool = row.get::<_, i32>(4).unwrap_or(0) != 0;
        if is_partial {
            continue;
        }

        // Get the columns in this index
        let columns = query_index_columns(conn, &index_name)?;
        if columns.is_empty() {
            continue;
        }

        let is_pk = origin == "pk";
        constraints.push(UniqueConstraint {
            name: index_name,
            columns,
            is_pk,
        });
    }

    Ok(constraints)
}

/// Get columns for a specific index via `PRAGMA index_info`.
fn query_index_columns(
    conn: &rusqlite::Connection,
    index_name: &str,
) -> Result<Vec<String>, SqliteInternalError> {
    let sql = format!("PRAGMA index_info(\"{}\")", escape_ident(index_name));
    let mut stmt = conn.prepare(&sql).map_err(|e| {
        SqliteInternalError::msg(format!(
            "failed to prepare index_info for {index_name}: {e}"
        ))
    })?;

    // index_info columns: seqno, cid, name
    let mut columns: Vec<(i32, String)> = Vec::new();
    let mut rows = stmt.query([]).map_err(|e| {
        SqliteInternalError::msg(format!("failed to query index_info for {index_name}: {e}"))
    })?;

    while let Some(row) = rows.next().map_err(|e| {
        SqliteInternalError::msg(format!("index_info iteration failed for {index_name}: {e}"))
    })? {
        let seqno: i32 = row.get(0).unwrap_or(0);
        let name: String = row.get(2).unwrap_or_default();
        columns.push((seqno, name));
    }

    columns.sort_by_key(|(seq, _)| *seq);
    Ok(columns.into_iter().map(|(_, name)| name).collect())
}

/// Normalize a SQLite declared type to a canonical form.
///
/// SQLite is flexible with type names; this normalizes to standard affinities
/// while preserving useful specifics like BOOLEAN, JSON, etc.
fn normalize_sqlite_type(declared: &str) -> String {
    let upper = declared.to_uppercase();
    let upper = upper.trim();

    // Handle common type patterns
    if upper.is_empty() {
        return "TEXT".to_string(); // SQLite defaults typeless columns to TEXT affinity
    }

    // Preserve known useful types verbatim (uppercased)
    match upper {
        "INTEGER" | "INT" | "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" => {
            "INTEGER".to_string()
        }
        "REAL" | "DOUBLE" | "FLOAT" | "DOUBLE PRECISION" => "REAL".to_string(),
        "TEXT" | "VARCHAR" | "CLOB" => "TEXT".to_string(),
        "BLOB" => "BLOB".to_string(),
        "NUMERIC" | "DECIMAL" => "NUMERIC".to_string(),
        "BOOLEAN" | "BOOL" => "BOOLEAN".to_string(),
        "JSON" | "JSONB" => "JSON".to_string(),
        "DATE" | "DATETIME" | "TIMESTAMP" => upper.to_string(),
        _ => {
            // For types with parameters like VARCHAR(255), extract the base
            if upper.contains("INT") {
                "INTEGER".to_string()
            } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
                "TEXT".to_string()
            } else if upper.contains("BLOB") {
                "BLOB".to_string()
            } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
                "REAL".to_string()
            } else if upper.contains("BOOL") {
                "BOOLEAN".to_string()
            } else if upper.contains("JSON") {
                "JSON".to_string()
            } else {
                // Fall back to NUMERIC affinity (SQLite's default for unknown types)
                "NUMERIC".to_string()
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
    fn test_normalize_sqlite_type_integer() {
        assert_eq!(normalize_sqlite_type("INTEGER"), "INTEGER");
        assert_eq!(normalize_sqlite_type("int"), "INTEGER");
        assert_eq!(normalize_sqlite_type("BIGINT"), "INTEGER");
        assert_eq!(normalize_sqlite_type("smallint"), "INTEGER");
    }

    #[test]
    fn test_normalize_sqlite_type_real() {
        assert_eq!(normalize_sqlite_type("REAL"), "REAL");
        assert_eq!(normalize_sqlite_type("DOUBLE"), "REAL");
        assert_eq!(normalize_sqlite_type("float"), "REAL");
    }

    #[test]
    fn test_normalize_sqlite_type_text() {
        assert_eq!(normalize_sqlite_type("TEXT"), "TEXT");
        assert_eq!(normalize_sqlite_type("VARCHAR"), "TEXT");
        assert_eq!(normalize_sqlite_type("VARCHAR(255)"), "TEXT");
        assert_eq!(normalize_sqlite_type("CHARACTER(100)"), "TEXT");
    }

    #[test]
    fn test_normalize_sqlite_type_boolean() {
        assert_eq!(normalize_sqlite_type("BOOLEAN"), "BOOLEAN");
        assert_eq!(normalize_sqlite_type("BOOL"), "BOOLEAN");
    }

    #[test]
    fn test_normalize_sqlite_type_json() {
        assert_eq!(normalize_sqlite_type("JSON"), "JSON");
        assert_eq!(normalize_sqlite_type("JSONB"), "JSON");
    }

    #[test]
    fn test_normalize_sqlite_type_empty() {
        assert_eq!(normalize_sqlite_type(""), "TEXT");
    }

    #[test]
    fn test_normalize_sqlite_type_unknown() {
        assert_eq!(normalize_sqlite_type("MONEY"), "NUMERIC");
    }

    #[test]
    fn test_escape_ident() {
        assert_eq!(escape_ident("users"), "users");
        assert_eq!(escape_ident("user\"table"), "user\"\"table");
    }
}
