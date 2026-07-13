//! Foreign key relationships introspection via `PRAGMA foreign_key_list`.
//!
//! Discovers M2O and O2O relationships. The O2O determination checks whether
//! the FK source columns have a unique constraint on them.

use indexmap::IndexMap;
use pgvis_core::cache::{Cardinality, QualifiedIdentifier, Relationship, Table, UniqueConstraint};
use pgvis_core::error::Error;
use tokio_rusqlite::Connection;

use crate::util::{SqliteInternalError, escape_ident};

/// Query all foreign key relationships from the introspected tables.
///
/// For each table, runs `PRAGMA foreign_key_list(table)` to discover FK constraints.
/// Determines O2O vs M2O by checking whether the FK source columns have a unique
/// constraint covering exactly those columns.
///
/// Returns M2O and O2O relationships. Inverse (O2M) and M2M relationships
/// are added during post-processing.
pub async fn query_relationships(
    conn: &Connection,
    tables: &IndexMap<QualifiedIdentifier, Table>,
) -> Result<Vec<Relationship>, Error> {
    let table_names: Vec<String> = tables.keys().map(|k| k.name.clone()).collect();
    let tables_clone = tables.clone();

    conn.call(move |conn| {
        let mut rels = Vec::new();

        for table_name in &table_names {
            let table_rels = query_table_fks(conn, table_name, &tables_clone)
                .map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))?;
            rels.extend(table_rels);
        }

        Ok(rels)
    })
    .await
    .map_err(|e| Error::Introspection(format!("SQLite relationships introspection failed: {e}")))
}

/// Query foreign keys for a single table using `PRAGMA foreign_key_list`.
fn query_table_fks(
    conn: &rusqlite::Connection,
    table_name: &str,
    tables: &IndexMap<QualifiedIdentifier, Table>,
) -> Result<Vec<Relationship>, SqliteInternalError> {
    let sql = format!("PRAGMA foreign_key_list(\"{}\")", escape_ident(table_name));
    let mut stmt = conn.prepare(&sql).map_err(|e| {
        SqliteInternalError::msg(format!(
            "failed to prepare foreign_key_list for {table_name}: {e}"
        ))
    })?;

    // foreign_key_list columns: id, seq, table, from, to, on_update, on_delete, match
    // Multiple rows with same `id` form a composite FK (multi-column)
    let mut fk_map: std::collections::BTreeMap<i32, FkEntry> = std::collections::BTreeMap::new();

    let mut rows = stmt.query([]).map_err(|e| {
        SqliteInternalError::msg(format!(
            "failed to query foreign_key_list for {table_name}: {e}"
        ))
    })?;

    while let Some(row) = rows.next().map_err(|e| {
        SqliteInternalError::msg(format!(
            "foreign_key_list iteration failed for {table_name}: {e}"
        ))
    })? {
        let id: i32 = row.get(0).unwrap_or(0);
        let _seq: i32 = row.get(1).unwrap_or(0);
        let target_table: String = row.get(2).unwrap_or_default();
        let from_col: String = row.get(3).unwrap_or_default();
        // The `to` column is NULL for FK shorthand (`REFERENCES table` with no
        // explicit column), meaning the FK targets the referenced table's PK.
        // Read as Option so NULL survives (a plain String read errors on NULL);
        // resolve NULLs to the target PK columns after grouping.
        let to_col: Option<String> = row.get(4).ok().flatten();

        let entry = fk_map.entry(id).or_insert_with(|| FkEntry {
            target_table: target_table.clone(),
            source_columns: Vec::new(),
            target_columns: Vec::new(),
        });

        entry.source_columns.push(from_col);
        entry.target_columns.push(to_col);
    }

    // Convert FK entries to Relationship structs
    let source_ident = QualifiedIdentifier::new("main", table_name);
    let source_table_meta = tables.get(&source_ident);

    let mut rels = Vec::new();
    for (id, entry) in fk_map {
        let target_ident = QualifiedIdentifier::new("main", &entry.target_table);
        let is_self = source_ident == target_ident;
        let target_table_meta = tables.get(&target_ident);

        // Resolve NULL target columns (FK shorthand) to the referenced table's
        // primary-key columns, in declaration order.
        let target_pk_cols = target_table_meta.map(|t| t.pk_cols.as_slice());
        let target_columns =
            resolve_target_columns(entry.target_columns, target_pk_cols);

        // Determine cardinality: O2O if source columns are covered by a unique
        // constraint. `unique_constraints` (from PRAGMA index_list) omits rowid-
        // alias INTEGER PRIMARY KEY and WITHOUT ROWID PKs (no backing index), so
        // also treat the source table's pk_cols as a unique constraint.
        let is_one_to_one = source_table_meta
            .map(|t| {
                has_unique_on_columns(&t.unique_constraints, &entry.source_columns)
                    || columns_match(&t.pk_cols, &entry.source_columns)
            })
            .unwrap_or(false);

        let cardinality = if is_one_to_one {
            Cardinality::O2O
        } else {
            Cardinality::M2O
        };

        // Synthesize a constraint name (SQLite doesn't name FK constraints)
        let constraint_name = format!("{table_name}_{}_fkey_{id}", entry.source_columns.join("_"));

        rels.push(Relationship {
            source_table: source_ident.clone(),
            target_table: target_ident,
            source_columns: entry.source_columns,
            target_columns,
            cardinality,
            constraint_name,
            is_self,
        });
    }

    Ok(rels)
}

/// Check if there's a unique constraint that covers exactly the given columns.
fn has_unique_on_columns(constraints: &[UniqueConstraint], columns: &[String]) -> bool {
    constraints
        .iter()
        .any(|uc| columns_match(&uc.columns, columns))
}

/// Whether two column sets contain exactly the same columns (order-independent).
fn columns_match(a: &[String], b: &[String]) -> bool {
    if a.len() != b.len() || a.is_empty() {
        return false;
    }
    let mut a_sorted = a.to_vec();
    a_sorted.sort();
    let mut b_sorted = b.to_vec();
    b_sorted.sort();
    a_sorted == b_sorted
}

/// Resolve FK target columns, substituting the target table's primary-key columns
/// for any NULL entries (FK shorthand `REFERENCES table` with no explicit column).
///
/// Composite shorthand FKs map their NULL slots onto the PK columns in order. If
/// the target PK is unknown, an empty string is used as a last resort (matching
/// the previous behaviour rather than dropping the relationship).
fn resolve_target_columns(
    target_columns: Vec<Option<String>>,
    target_pk_cols: Option<&[String]>,
) -> Vec<String> {
    let mut pk_iter = target_pk_cols.unwrap_or(&[]).iter();
    target_columns
        .into_iter()
        .map(|col| match col {
            Some(c) => c,
            None => pk_iter.next().cloned().unwrap_or_default(),
        })
        .collect()
}

/// Intermediate FK entry for grouping composite foreign keys.
///
/// `target_columns` holds `None` for shorthand FKs (`REFERENCES table` without an
/// explicit column) where `SQLite` reports the `to` column as NULL; these are
/// resolved to the target table's primary-key columns during conversion.
struct FkEntry {
    target_table: String,
    source_columns: Vec<String>,
    target_columns: Vec<Option<String>>,
}
