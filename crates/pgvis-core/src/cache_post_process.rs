//! Post-processing of introspection results.
//!
//! These algorithms are backend-agnostic — they operate on the already-populated
//! [`SchemaCache`] and infer additional relationships that can't be discovered
//! from a single query.
//!
//! Both `pgvis-postgres` and `pgvis-sqlite` call these after their respective
//! introspection passes.

use std::collections::{HashMap, HashSet};

use crate::cache::{Cardinality, QualifiedIdentifier, Relationship, SchemaCache};

/// Add inverse relationships for every M2O and O2O relationship.
///
/// - For each M2O (A → B), adds O2M (B → A) with swapped columns
/// - For each O2O (A → B), adds O2O (B → A) with swapped columns
///
/// This matches PostgREST's `addInverseRels` behaviour.
///
/// Uses index-based iteration to avoid cloning the entire relationships vector.
pub fn add_inverse_relationships(cache: &mut SchemaCache) {
    let mut inverse_rels = Vec::new();
    let len = cache.relationships.len();

    for i in 0..len {
        let rel = &cache.relationships[i];
        match &rel.cardinality {
            Cardinality::M2O => {
                // M2O(A→B) becomes O2M(B→A) with swapped columns
                inverse_rels.push(Relationship {
                    source_table: rel.target_table.clone(),
                    target_table: rel.source_table.clone(),
                    source_columns: rel.target_columns.clone(),
                    target_columns: rel.source_columns.clone(),
                    cardinality: Cardinality::O2M,
                    constraint_name: rel.constraint_name.clone(),
                    is_self: rel.is_self,
                });
            }
            Cardinality::O2O => {
                // O2O(A→B) becomes O2O(B→A) with swapped columns
                inverse_rels.push(Relationship {
                    source_table: rel.target_table.clone(),
                    target_table: rel.source_table.clone(),
                    source_columns: rel.target_columns.clone(),
                    target_columns: rel.source_columns.clone(),
                    cardinality: Cardinality::O2O,
                    constraint_name: rel.constraint_name.clone(),
                    is_self: rel.is_self,
                });
            }
            // O2M and M2M are already derived — don't double-invert
            _ => {}
        }
    }

    cache.relationships.extend(inverse_rels);
}

/// Infer Many-to-Many relationships from pairs of M2O relationships.
///
/// A junction table is identified when:
/// 1. It has at least two M2O relationships (two different FKs)
/// 2. The union of FK source columns is a subset of the junction table's PK columns
///
/// For each qualifying pair, a M2M relationship is created between the two
/// target tables, via the junction table.
///
/// This matches PostgREST's `addM2MRels` behaviour.
///
/// Uses index-based access to avoid cloning the entire relationships vector.
pub fn infer_m2m_relationships(cache: &mut SchemaCache) {
    // Index M2O rels by their source table using indices (no clone needed)
    let mut m2o_by_source: HashMap<&QualifiedIdentifier, Vec<usize>> = HashMap::new();

    for (i, rel) in cache.relationships.iter().enumerate() {
        if matches!(rel.cardinality, Cardinality::M2O) {
            m2o_by_source.entry(&rel.source_table).or_default().push(i);
        }
    }

    let mut m2m_rels = Vec::new();

    // For each potential junction table with 2+ M2O rels
    for (junction_ident, indices) in &m2o_by_source {
        if indices.len() < 2 {
            continue;
        }

        // Get the junction table's PK columns
        let pk_cols: HashSet<&str> = match cache.tables.get(*junction_ident) {
            Some(table) => table.pk_cols.iter().map(String::as_str).collect(),
            None => continue,
        };

        if pk_cols.is_empty() {
            continue;
        }

        // Try all pairs of M2O rels on this junction
        for (ii, &idx1) in indices.iter().enumerate() {
            for &idx2 in indices.iter().skip(ii + 1) {
                let rel1 = &cache.relationships[idx1];
                let rel2 = &cache.relationships[idx2];

                // Skip if both point to the same constraint
                if rel1.constraint_name == rel2.constraint_name {
                    continue;
                }

                // Check: union of FK source columns ⊆ PK columns
                let fk_cols: HashSet<&str> = rel1
                    .source_columns
                    .iter()
                    .chain(rel2.source_columns.iter())
                    .map(String::as_str)
                    .collect();

                if !fk_cols.is_subset(&pk_cols) {
                    continue;
                }

                // This is a valid M2M relationship!
                let is_self = rel1.target_table == rel2.target_table;

                m2m_rels.push(Relationship {
                    source_table: rel1.target_table.clone(),
                    target_table: rel2.target_table.clone(),
                    // For M2M, source_columns are the FK cols in junction pointing to source
                    source_columns: rel1.target_columns.clone(),
                    target_columns: rel2.target_columns.clone(),
                    cardinality: Cardinality::M2M {
                        junction_table: (*junction_ident).clone(),
                        junction_cols_source: rel1.source_columns.clone(),
                        junction_cols_target: rel2.source_columns.clone(),
                    },
                    constraint_name: format!("{}_{}", rel1.constraint_name, rel2.constraint_name),
                    is_self,
                });
            }
        }
    }

    cache.relationships.extend(m2m_rels);
}

/// Build the relationship index for O(1) lookup by table.
///
/// Maps each table identifier to the indices of all relationships where that
/// table appears as either source or target. Must be called **after** all
/// relationships have been finalised (after `infer_m2m_relationships` and
/// `add_inverse_relationships`).
pub fn build_relationship_index(cache: &mut SchemaCache) {
    let mut index: HashMap<QualifiedIdentifier, Vec<usize>> = HashMap::new();

    for (i, rel) in cache.relationships.iter().enumerate() {
        index.entry(rel.source_table.clone()).or_default().push(i);
        index.entry(rel.target_table.clone()).or_default().push(i);
    }

    cache.relationship_index = index;
}

/// Mark columns that participate in foreign keys.
///
/// Sets `column.is_fk = true` for all columns that appear as source columns
/// in any relationship.
///
/// Uses disjoint field borrows: reads `cache.relationships` (immutable) while
/// mutating `cache.tables` (mutable). This avoids cloning per-column for lookup.
pub fn mark_fk_columns(cache: &mut SchemaCache) {
    // Split the borrow: immutable access to relationships, mutable to tables.
    let SchemaCache {
        ref relationships,
        ref mut tables,
        ..
    } = *cache;

    // Build a HashSet of (table_ident, column_name) that are FK sources.
    // Uses references to avoid allocation in the hot lookup path.
    let fk_cols: HashSet<(&QualifiedIdentifier, &str)> = relationships
        .iter()
        .flat_map(|rel| {
            rel.source_columns
                .iter()
                .map(move |col| (&rel.source_table, col.as_str()))
        })
        .collect();

    // Update the columns using reference-based lookup (no clone per column)
    for (ident, table) in tables.iter_mut() {
        for (col_name, col) in table.columns.iter_mut() {
            if fk_cols.contains(&(ident, col_name.as_str())) {
                col.is_fk = true;
            }
        }
    }
}
