//! # Resolution logic — maps parsed AST to resolved plan types.
//!
//! Each `resolve_*` function takes parsed elements + SchemaCache context
//! and produces resolved plan types, or returns descriptive errors.

use crate::cache::{Cardinality, QualifiedIdentifier, Relationship, SchemaCache, Table};
use crate::dialect::Dialect;
use crate::error::{Error, ErrorCode};
use crate::query_params::types::{
    CursorSpec, Filter, LogicNode, LogicTree, Operator, OrderDirection, OrderTerm, RangeSpec,
};
use crate::select_ast::{FieldSelect, RelationSelect, SelectItem, SpreadSelect};

use super::planner::PlanConfig;
use super::types::*;

// ---------------------------------------------------------------------------
// Table resolution
// ---------------------------------------------------------------------------

/// Resolve a table name within a schema from the cache.
/// Returns descriptive 404 error with "did you mean?" suggestions.
pub fn resolve_table<'a>(
    cache: &'a SchemaCache,
    schema: &str,
    name: &str,
) -> Result<&'a Table, Error> {
    cache.find_table(schema, name).ok_or_else(|| {
        let suggestions = suggest_table(cache, schema, name);
        let hint = if suggestions.is_empty() {
            None
        } else {
            Some(format!("Did you mean: {}?", suggestions.join(", ")))
        };
        Error::Plan {
            message: format!("Table or view '{name}' not found in schema '{schema}'"),
            detail: None,
            hint,
            code: ErrorCode::NotFound,
        }
    })
}

/// Extract `ResolvedTableInfo` from a `Table`.
pub fn resolve_table_info(table: &Table) -> ResolvedTableInfo {
    ResolvedTableInfo {
        is_view: table.is_view,
        insertable: table.insertable,
        updatable: table.updatable,
        deletable: table.deletable,
        primary_key_columns: table.pk_cols.clone(),
    }
}

// ---------------------------------------------------------------------------
// Column resolution
// ---------------------------------------------------------------------------

/// Validate that a column exists on the target table.
pub fn resolve_column<'a>(table: &'a Table, name: &str) -> Result<&'a crate::cache::Column, Error> {
    table.columns.get(name).ok_or_else(|| {
        let suggestions = suggest_column(table, name);
        let hint = if suggestions.is_empty() {
            None
        } else {
            Some(format!("Did you mean: {}?", suggestions.join(", ")))
        };
        Error::Plan {
            message: format!("Column '{}' not found in '{}'", name, table.name()),
            detail: None,
            hint,
            code: ErrorCode::ColumnNotFound,
        }
    })
}

// ---------------------------------------------------------------------------
// Select item resolution
// ---------------------------------------------------------------------------

/// Resolve all select items against the table's columns.
/// Returns resolved columns and embedded resources separately.
pub fn resolve_select_items(
    cache: &SchemaCache,
    table: &Table,
    schema: &str,
    items: &[SelectItem],
    dialect: &Dialect,
    config: &PlanConfig,
) -> Result<(Vec<ResolvedSelect>, Vec<EmbeddedResource>), Error> {
    let mut selects = Vec::new();
    let mut embeds = Vec::new();

    for item in items {
        match item {
            SelectItem::Star => {
                selects.push(ResolvedSelect::Star);
            }
            SelectItem::Field(field) => {
                let resolved = resolve_field_select(table, field)?;
                selects.push(resolved);
            }
            SelectItem::Relation(rel) => {
                let embed = resolve_embed(cache, table, schema, rel, dialect, config)?;
                embeds.push(embed);
            }
            SelectItem::Spread(spread) => {
                let embed = resolve_spread(cache, table, schema, spread, dialect, config)?;
                embeds.push(embed);
            }
        }
    }

    Ok((selects, embeds))
}

/// Resolve a single field select into a `ResolvedSelect`.
fn resolve_field_select(table: &Table, field: &FieldSelect) -> Result<ResolvedSelect, Error> {
    // Handle aggregate without a column (e.g. bare `count()`)
    if let Some(agg_fn) = &field.aggregate {
        if field.name.is_empty() {
            // Bare aggregate like count()
            return Ok(ResolvedSelect::Aggregate(ResolvedAggregate {
                function: *agg_fn,
                column: None,
                alias: field.alias.clone(),
            }));
        }
        // Aggregate on a specific column
        let _col = resolve_column(table, &field.name)?;
        return Ok(ResolvedSelect::Aggregate(ResolvedAggregate {
            function: *agg_fn,
            column: Some(field.name.clone()),
            alias: field.alias.clone(),
        }));
    }

    // Regular column
    let col = resolve_column(table, &field.name)?;
    Ok(ResolvedSelect::Column(ResolvedColumn {
        name: field.name.clone(),
        alias: field.alias.clone(),
        json_path: field.json_path.clone(),
        data_type: col.typ.clone(),
        nullable: col.nullable,
    }))
}

// ---------------------------------------------------------------------------
// Embed resolution
// ---------------------------------------------------------------------------

/// Resolve a relation select (embedded resource) into a fully-resolved embed.
pub fn resolve_embed(
    cache: &SchemaCache,
    parent_table: &Table,
    schema: &str,
    rel: &RelationSelect,
    dialect: &Dialect,
    config: &PlanConfig,
) -> Result<EmbeddedResource, Error> {
    // 1. Find relationship(s) between parent and target
    let parent_id = &parent_table.ident;
    let relationships = cache.find_relationships(parent_id);

    // 2. Filter to those matching the target name or hint
    let matching: Vec<&Relationship> = relationships
        .into_iter()
        .filter(|r| {
            // Match by target name: if parent is source, check target.name
            // If parent is target, check source.name
            let name_matches = if r.source_table == *parent_id {
                r.target_table.name == rel.name
            } else {
                r.source_table.name == rel.name
            };

            // Also match by hint (constraint name)
            let hint_matches = rel
                .hint
                .as_ref()
                .map(|h| r.constraint_name == *h)
                .unwrap_or(false);

            name_matches || hint_matches
        })
        .collect();

    // 3. Deduplicate by constraint name (same FK appears as both M2O and inverse O2M)
    //    Prefer the entry where source_table == parent (forward direction)
    let deduped: Vec<&Relationship> = {
        let mut seen_constraints: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        let mut result: Vec<&Relationship> = Vec::new();
        // First pass: add forward matches (source == parent)
        for r in &matching {
            if r.source_table == *parent_id && seen_constraints.insert(&r.constraint_name) {
                result.push(r);
            }
        }
        // Second pass: add reverse matches not already seen
        for r in &matching {
            if r.source_table != *parent_id && seen_constraints.insert(&r.constraint_name) {
                result.push(r);
            }
        }
        result
    };

    // 4. Disambiguate
    let relationship = disambiguate_relationship(
        &deduped,
        parent_table.name(),
        &rel.name,
        rel.hint.as_deref(),
    )?;

    // 4. Determine which table is the target from the parent's perspective
    let target_id = if relationship.source_table == *parent_id {
        &relationship.target_table
    } else {
        &relationship.source_table
    };

    // 5. Resolve the target table
    let target_table = resolve_table(cache, &target_id.schema, &target_id.name)?;

    // 6. Build the join
    let join = resolve_join(relationship, parent_id);

    // 7. Recursively resolve the child's select items
    let child_items = if rel.children.is_empty() {
        vec![SelectItem::Star]
    } else {
        rel.children.clone()
    };
    let (child_selects, child_embeds) =
        resolve_select_items(cache, target_table, schema, &child_items, dialect, config)?;

    // 7b. Expand Star to explicit columns for embed sub-plans.
    //     SQLite embeds need explicit column names for json_object(); Postgres
    //     row_to_json handles both forms identically, so this is safe for all backends.
    let child_selects = expand_star_for_embed(target_table, child_selects);

    // 8. Build child read plan
    let child_plan = ReadPlan {
        target: target_id.clone(),
        table_info: resolve_table_info(target_table),
        select: child_selects,
        embeds: child_embeds,
        filters: Vec::new(),
        order: Vec::new(),
        range: ResolvedRange {
            limit: None,
            offset: None,
            cursor: None,
            cursor_column: None,
        },
        logic_filters: Vec::new(),
        aggregates: Vec::new(),
        count: None,
        preferences: Default::default(),
    };

    Ok(EmbeddedResource {
        name: rel.name.clone(),
        alias: rel.alias.clone(),
        join,
        plan: child_plan,
        is_spread: false,
    })
}

/// Resolve a spread relation — like `resolve_embed` but validates it's a to-one relationship.
pub fn resolve_spread(
    cache: &SchemaCache,
    parent_table: &Table,
    schema: &str,
    spread: &SpreadSelect,
    dialect: &Dialect,
    config: &PlanConfig,
) -> Result<EmbeddedResource, Error> {
    // Build a temporary RelationSelect to reuse resolve_embed
    let rel_select = RelationSelect {
        name: spread.name.clone(),
        alias: None,
        hint: spread.hint.clone(),
        join_type: spread.join_type,
        children: spread.children.clone(),
    };
    let mut embed = resolve_embed(cache, parent_table, schema, &rel_select, dialect, config)?;

    // Validate: spread only works on to-one relationships
    match &embed.join {
        ResolvedJoin::Direct { cardinality, .. } => {
            if matches!(cardinality, Cardinality::O2M | Cardinality::M2M { .. }) {
                return Err(Error::Plan {
                    message: format!(
                        "Cannot spread '{}' — it is a to-many relationship",
                        spread.name
                    ),
                    detail: Some(
                        "Spread (...) only works on to-one (many-to-one or one-to-one) relationships"
                            .to_string(),
                    ),
                    hint: Some(
                        "Use a regular embed without spread: select=target(*)".to_string(),
                    ),
                    code: ErrorCode::SpreadOnToMany,
                });
            }
        }
        ResolvedJoin::Junction { .. } => {
            return Err(Error::Plan {
                message: format!(
                    "Cannot spread '{}' — junction (M2M) relationships are always to-many",
                    spread.name
                ),
                detail: None,
                hint: Some("Use a regular embed: select=target(*)".to_string()),
                code: ErrorCode::SpreadOnToMany,
            });
        }
        ResolvedJoin::Computed { cardinality, .. } => {
            if matches!(cardinality, Cardinality::O2M | Cardinality::M2M { .. }) {
                return Err(Error::Plan {
                    message: format!(
                        "Cannot spread '{}' — computed relationship is to-many",
                        spread.name
                    ),
                    detail: None,
                    hint: None,
                    code: ErrorCode::SpreadOnToMany,
                });
            }
        }
    }

    embed.is_spread = true;
    Ok(embed)
}

// ---------------------------------------------------------------------------
// Join resolution
// ---------------------------------------------------------------------------

/// Convert a `Relationship` into a `ResolvedJoin`.
/// `parent_id` indicates which side is the "source" for determining direction.
pub fn resolve_join(rel: &Relationship, parent_id: &QualifiedIdentifier) -> ResolvedJoin {
    match &rel.cardinality {
        Cardinality::M2M {
            junction_table,
            junction_cols_source,
            junction_cols_target,
        } => {
            // For M2M, determine direction based on which side is the parent
            if rel.source_table == *parent_id {
                ResolvedJoin::Junction {
                    source_columns: rel.source_columns.clone(),
                    junction_table: junction_table.clone(),
                    junction_source_columns: junction_cols_source.clone(),
                    junction_target_columns: junction_cols_target.clone(),
                    target_columns: rel.target_columns.clone(),
                    target_table: rel.target_table.clone(),
                }
            } else {
                // Parent is the target side, reverse the relationship
                ResolvedJoin::Junction {
                    source_columns: rel.target_columns.clone(),
                    junction_table: junction_table.clone(),
                    junction_source_columns: junction_cols_target.clone(),
                    junction_target_columns: junction_cols_source.clone(),
                    target_columns: rel.source_columns.clone(),
                    target_table: rel.source_table.clone(),
                }
            }
        }
        cardinality => {
            if rel.source_table == *parent_id {
                ResolvedJoin::Direct {
                    source_columns: rel.source_columns.clone(),
                    target_columns: rel.target_columns.clone(),
                    target_table: rel.target_table.clone(),
                    cardinality: cardinality.clone(),
                }
            } else {
                // Parent is the target side — flip direction and adjust cardinality
                let flipped_cardinality = match cardinality {
                    Cardinality::M2O => Cardinality::O2M,
                    Cardinality::O2M => Cardinality::M2O,
                    other => other.clone(),
                };
                ResolvedJoin::Direct {
                    source_columns: rel.target_columns.clone(),
                    target_columns: rel.source_columns.clone(),
                    target_table: rel.source_table.clone(),
                    cardinality: flipped_cardinality,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Relationship disambiguation
// ---------------------------------------------------------------------------

/// Handle multiple FK matches — disambiguate or return error.
fn disambiguate_relationship<'a>(
    matches: &[&'a Relationship],
    source: &str,
    target: &str,
    hint: Option<&str>,
) -> Result<&'a Relationship, Error> {
    match matches.len() {
        0 => Err(Error::Plan {
            message: format!("Could not find a relationship between '{source}' and '{target}'"),
            detail: None,
            hint: None,
            code: ErrorCode::RelationshipNotFound,
        }),
        1 => Ok(matches[0]),
        _ => {
            // If a hint is provided, try to match it
            if let Some(hint) = hint {
                if let Some(rel) = matches.iter().find(|r| r.constraint_name == hint) {
                    return Ok(rel);
                }
            }
            let constraint_names: Vec<&str> =
                matches.iter().map(|r| r.constraint_name.as_str()).collect();
            Err(Error::ambiguous_relationship(
                source,
                target,
                &constraint_names,
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Filter resolution
// ---------------------------------------------------------------------------

/// Resolve and validate filters.
pub fn resolve_filters(
    table: &Table,
    filters: &[Filter],
    dialect: &Dialect,
) -> Result<Vec<ResolvedFilter>, Error> {
    filters
        .iter()
        .map(|f| resolve_filter(table, f, dialect))
        .collect()
}

/// Resolve a single filter against the table schema.
fn resolve_filter(
    table: &Table,
    filter: &Filter,
    dialect: &Dialect,
) -> Result<ResolvedFilter, Error> {
    // Validate column exists
    let _col = resolve_column(table, &filter.field)?;

    // Determine dialect-specific rewrite if needed
    let rewrite = compute_filter_rewrite(&filter.operator, dialect);

    Ok(ResolvedFilter {
        column: filter.field.clone(),
        operator: filter.operator.clone(),
        value: filter.value.clone(),
        negated: filter.negate,
        rewrite,
    })
}

/// Check if an operator needs dialect-specific handling.
fn compute_filter_rewrite(op: &Operator, dialect: &Dialect) -> Option<FilterRewrite> {
    match op {
        Operator::Contains | Operator::ContainedBy if !dialect.supports_array_ops => {
            Some(FilterRewrite::JsonArrayContains)
        }
        Operator::Match | Operator::IMatch if !dialect.supports_regex_match => {
            Some(FilterRewrite::GlobPattern)
        }
        Operator::ILike if !dialect.supports_ilike => Some(FilterRewrite::InstrFallback),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Order resolution
// ---------------------------------------------------------------------------

/// Validate and resolve ORDER BY terms.
pub fn resolve_order(
    table: &Table,
    order_terms: &[OrderTerm],
) -> Result<Vec<ResolvedOrder>, Error> {
    order_terms
        .iter()
        .map(|term| {
            let _col = resolve_column(table, &term.field)?;
            Ok(ResolvedOrder {
                column: term.field.clone(),
                direction: term.direction,
                nulls: term.nulls,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Range resolution
// ---------------------------------------------------------------------------

/// Apply server-side limits to the range specification.
pub fn resolve_range(range: &Option<RangeSpec>, max_rows: Option<u64>) -> ResolvedRange {
    match range {
        Some(r) => {
            let effective_limit = match (r.limit, max_rows) {
                (Some(client_limit), Some(server_max)) => Some(client_limit.min(server_max)),
                (Some(client_limit), None) => Some(client_limit),
                (None, Some(server_max)) => Some(server_max),
                (None, None) => None,
            };
            ResolvedRange {
                limit: effective_limit,
                offset: r.offset,
                cursor: None,
                cursor_column: None,
            }
        }
        None => ResolvedRange {
            limit: max_rows,
            offset: None,
            cursor: None,
            cursor_column: None,
        },
    }
}

// ---------------------------------------------------------------------------
// Cursor resolution
// ---------------------------------------------------------------------------

/// Resolve cursor pagination specification against the table schema and order.
///
/// When `cursor.column` is `None`, defaults to the table's first primary key column.
/// The comparison operator (`>` or `<`) is determined from the ORDER BY direction
/// on the cursor column (defaults to ASC / `>`).
///
/// Returns `None` if the cursor has no value (first page — no seek condition needed),
/// but the caller should still ensure ORDER BY includes the cursor column.
pub fn resolve_cursor(
    table: &Table,
    cursor: &CursorSpec,
    order: &[ResolvedOrder],
) -> Result<(Option<ResolvedCursor>, String), Error> {
    // 1. Determine cursor column: explicit or default to primary key
    let cursor_column = match &cursor.column {
        Some(col) => col.clone(),
        None => table.pk_cols.first().cloned().ok_or_else(|| Error::Plan {
            message: "No cursor_column specified and table has no primary key".into(),
            detail: None,
            hint: Some(
                "Specify cursor_column explicitly for tables without a primary key".into(),
            ),
            code: ErrorCode::InvalidRange,
        })?,
    };

    // 2. Validate cursor column exists in the table
    let col = table.columns.get(&cursor_column).ok_or_else(|| Error::Plan {
        message: format!("Cursor column '{}' not found in table '{}'", cursor_column, table.ident.name),
        detail: None,
        hint: Some("cursor_column must reference an existing column in the table".into()),
        code: ErrorCode::ColumnNotFound,
    })?;

    // 3. If no cursor value, this is the first page — no seek condition needed
    let value = match &cursor.value {
        Some(v) => v.clone(),
        None => return Ok((None, cursor_column)),
    };

    // 4. Determine direction from order: if cursor column is DESC use Lt, else Gt
    let direction = order
        .iter()
        .find(|o| o.column == cursor_column)
        .map(|o| o.direction)
        .unwrap_or(OrderDirection::Asc);

    let operator = match direction {
        OrderDirection::Asc => Operator::Gt,
        OrderDirection::Desc => Operator::Lt,
    };

    Ok((Some(ResolvedCursor {
        column: cursor_column.clone(),
        value,
        operator,
        data_type: col.typ.clone(),
    }), cursor_column))
}

// ---------------------------------------------------------------------------
// Logic tree resolution
// ---------------------------------------------------------------------------

/// Recursively resolve logic trees.
pub fn resolve_logic_tree(
    table: &Table,
    tree: &LogicTree,
    dialect: &Dialect,
) -> Result<ResolvedLogicTree, Error> {
    match tree {
        LogicTree::And(nodes) => {
            let resolved: Result<Vec<_>, _> = nodes
                .iter()
                .map(|n| resolve_logic_node(table, n, dialect))
                .collect();
            Ok(ResolvedLogicTree::And(resolved?))
        }
        LogicTree::Or(nodes) => {
            let resolved: Result<Vec<_>, _> = nodes
                .iter()
                .map(|n| resolve_logic_node(table, n, dialect))
                .collect();
            Ok(ResolvedLogicTree::Or(resolved?))
        }
    }
}

/// Resolve a single node in a logic tree.
fn resolve_logic_node(
    table: &Table,
    node: &LogicNode,
    dialect: &Dialect,
) -> Result<ResolvedLogicNode, Error> {
    match node {
        LogicNode::Filter(f) => {
            let resolved = resolve_filter(table, f, dialect)?;
            Ok(ResolvedLogicNode::Filter(resolved))
        }
        LogicNode::Tree(t) => {
            let resolved = resolve_logic_tree(table, t, dialect)?;
            Ok(ResolvedLogicNode::Tree(resolved))
        }
        LogicNode::Not(inner) => {
            let resolved_inner = resolve_logic_node(table, inner, dialect)?;
            Ok(ResolvedLogicNode::Not(Box::new(resolved_inner)))
        }
    }
}

// ---------------------------------------------------------------------------
// Star expansion for embeds
// ---------------------------------------------------------------------------

/// Expand `ResolvedSelect::Star` entries into explicit `ResolvedSelect::Column` entries.
///
/// This is essential for SQLite embeds which need explicit column names for `json_object()`.
/// For Postgres, `row_to_json(sub_alias)` works with either form, so expansion is harmless.
fn expand_star_for_embed(table: &Table, selects: Vec<ResolvedSelect>) -> Vec<ResolvedSelect> {
    let has_star = selects.iter().any(|s| matches!(s, ResolvedSelect::Star));
    if !has_star {
        return selects;
    }

    let mut expanded = Vec::new();
    for sel in selects {
        match sel {
            ResolvedSelect::Star => {
                // Expand to all table columns (in ordinal order, preserved by IndexMap)
                for (col_name, col) in &table.columns {
                    expanded.push(ResolvedSelect::Column(ResolvedColumn {
                        name: col_name.clone(),
                        alias: None,
                        json_path: vec![],
                        data_type: col.typ.clone(),
                        nullable: col.nullable,
                    }));
                }
            }
            other => expanded.push(other),
        }
    }
    expanded
}

// ---------------------------------------------------------------------------
// Suggestion helpers
// ---------------------------------------------------------------------------

/// Simple similarity-based suggestions for "did you mean?" errors on tables.
fn suggest_table(cache: &SchemaCache, schema: &str, name: &str) -> Vec<String> {
    let tables = cache
        .tables
        .keys()
        .filter(|k| k.schema == schema)
        .map(|k| k.name.as_str());
    find_similar(name, tables, 3)
}

/// Simple similarity-based suggestions for "did you mean?" errors on columns.
fn suggest_column(table: &Table, name: &str) -> Vec<String> {
    find_similar(name, table.columns.keys().map(|k| k.as_str()), 3)
}

/// Find strings similar to `target` using Levenshtein edit distance.
/// Returns up to `max` suggestions sorted by distance (closest first).
fn find_similar<'a>(
    target: &str,
    candidates: impl Iterator<Item = &'a str>,
    max: usize,
) -> Vec<String> {
    let target_lower = target.to_lowercase();
    let max_dist = 3.max(target.len() / 2);
    let mut scored: Vec<(usize, String)> = candidates
        .filter_map(|c| {
            let dist = levenshtein(&target_lower, &c.to_lowercase());
            if dist <= max_dist {
                Some((dist, c.to_string()))
            } else {
                None
            }
        })
        .collect();
    scored.sort_by_key(|(dist, _)| *dist);
    scored.into_iter().take(max).map(|(_, s)| s).collect()
}

/// Compute the Levenshtein edit distance between two strings.
fn levenshtein(a: &str, b: &str) -> usize {
    let a = a.as_bytes();
    let b = b.as_bytes();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut curr = vec![0; b.len() + 1];
    for i in 1..=a.len() {
        curr[0] = i;
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b.len()]
}
