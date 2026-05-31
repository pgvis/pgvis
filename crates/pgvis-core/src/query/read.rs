//! # Read plan → SELECT SQL generation.
//!
//! Renders a [`ReadPlan`] into a SELECT statement with:
//! - Column selection (with JSON paths, casts, aggregates)
//! - Embedded resource subqueries (correlated subqueries with json_agg)
//! - WHERE clause from filters and logic trees
//! - ORDER BY, LIMIT, OFFSET
//! - GROUP BY (synthesised from aggregates)

use crate::cache::Cardinality;
use crate::error::Error;
use crate::plan::types::{EmbeddedResource, ReadPlan, ResolvedJoin};

use super::RenderContext;
use super::fragment;

/// Render a [`ReadPlan`] into the inner SELECT SQL (without CTE wrapper).
///
/// Returns the SQL string for the main query body. The caller wraps it in a CTE.
pub fn render_read(plan: &ReadPlan, ctx: &mut RenderContext<'_>) -> Result<String, Error> {
    let table_ref = ctx.qualified_table(&plan.target.schema, &plan.target.name);
    let table_alias = &plan.target.name;

    // --- SELECT list ---
    let mut select_parts = Vec::new();

    // Regular columns
    let col_list = fragment::render_select_list(&plan.select, Some(table_alias), ctx);
    if !col_list.is_empty() {
        select_parts.push(col_list);
    }

    // Embedded resources as correlated subqueries
    for embed in &plan.embeds {
        let embed_sql = render_embed(embed, table_alias, ctx)?;
        select_parts.push(embed_sql);
    }

    let select_clause = if select_parts.is_empty() {
        format!("{}.*", ctx.quote_ident(table_alias))
    } else {
        select_parts.join(", ")
    };

    // --- FROM ---
    let from_clause = format!("{table_ref} AS {}", ctx.quote_ident(table_alias));

    // --- WHERE ---
    let where_clause =
        fragment::render_where_clause(&plan.filters, &plan.logic_filters, Some(table_alias), ctx);

    // --- Cursor condition (keyset pagination) ---
    let cursor_condition = plan
        .range
        .cursor
        .as_ref()
        .map(|cursor| fragment::render_cursor_condition(cursor, Some(table_alias), ctx));

    // --- GROUP BY ---
    let group_by = fragment::render_group_by(&plan.select, Some(table_alias), ctx);

    // --- ORDER BY ---
    let order_by = fragment::render_order_clause(&plan.order, Some(table_alias), ctx);

    // --- LIMIT / OFFSET ---
    let limit_offset = fragment::render_limit_offset(plan.range.limit, plan.range.offset);

    // --- Assemble ---
    let mut sql = format!("SELECT {select_clause} FROM {from_clause}");

    // Combine WHERE clause and cursor condition
    match (&where_clause, &cursor_condition) {
        (Some(wc), Some(cc)) => {
            sql.push_str(" WHERE ");
            sql.push_str(wc);
            sql.push_str(" AND ");
            sql.push_str(cc);
        }
        (Some(wc), None) => {
            sql.push_str(" WHERE ");
            sql.push_str(wc);
        }
        (None, Some(cc)) => {
            sql.push_str(" WHERE ");
            sql.push_str(cc);
        }
        (None, None) => {}
    }

    if let Some(gb) = group_by {
        sql.push_str(" GROUP BY ");
        sql.push_str(&gb);
    }

    if let Some(ob) = order_by {
        sql.push_str(" ORDER BY ");
        sql.push_str(&ob);
    }

    if let Some(lo) = limit_offset {
        sql.push(' ');
        sql.push_str(&lo);
    }

    Ok(sql)
}

/// Render the count source query for exact count — same as `render_read` but
/// without LIMIT/OFFSET and without embeds (we only need the WHERE clause to count).
///
/// This produces the SQL that goes into the `pgrst_count` CTE to count all
/// matching rows regardless of pagination.
pub fn render_read_count_source(
    plan: &ReadPlan,
    ctx: &mut RenderContext<'_>,
) -> Result<String, Error> {
    let table_ref = ctx.qualified_table(&plan.target.schema, &plan.target.name);
    let table_alias = &plan.target.name;

    // Use simple star select — we only need to count rows
    let select_clause = format!("{}.*", ctx.quote_ident(table_alias));

    // --- FROM ---
    let from_clause = format!("{table_ref} AS {}", ctx.quote_ident(table_alias));

    // --- WHERE --- (same filters as the paginated query)
    let where_clause =
        fragment::render_where_clause(&plan.filters, &plan.logic_filters, Some(table_alias), ctx);

    // --- GROUP BY --- (needed for correct count when aggregates are used)
    let group_by = fragment::render_group_by(&plan.select, Some(table_alias), ctx);

    // --- Assemble (no ORDER BY, no LIMIT/OFFSET) ---
    let mut sql = format!("SELECT {select_clause} FROM {from_clause}");

    if let Some(wc) = where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(&wc);
    }

    if let Some(gb) = group_by {
        sql.push_str(" GROUP BY ");
        sql.push_str(&gb);
    }

    Ok(sql)
}

// ---------------------------------------------------------------------------
// Embedding subqueries
// ---------------------------------------------------------------------------

/// Render an embedded resource as a correlated subquery column expression.
///
/// Produces SQL like:
/// ```sql
/// COALESCE((SELECT json_agg(_sub) FROM (
///   SELECT "orders"."total" FROM "orders"
///   WHERE "orders"."user_id" = "users"."id"
/// ) _sub), '[]'::json) AS "orders"
/// ```
fn render_embed(
    embed: &EmbeddedResource,
    parent_alias: &str,
    ctx: &mut RenderContext<'_>,
) -> Result<String, Error> {
    let child_plan = &embed.plan;
    let child_table_ref = ctx.qualified_table(&child_plan.target.schema, &child_plan.target.name);
    let child_alias = &child_plan.target.name;
    let sub_alias = ctx.next_alias("sub");

    // Build the inner SELECT
    let child_select = fragment::render_select_list(&child_plan.select, Some(child_alias), ctx);

    // Build the JOIN condition based on relationship type
    let join_condition = render_join_condition(&embed.join, parent_alias, child_alias, ctx);

    // Build inner query with child's own filters
    let child_where = fragment::render_where_clause(
        &child_plan.filters,
        &child_plan.logic_filters,
        Some(child_alias),
        ctx,
    );

    let mut inner_sql = format!(
        "SELECT {child_select} FROM {child_table_ref} AS {}",
        ctx.quote_ident(child_alias)
    );

    // Add junction table join for M2M
    if let ResolvedJoin::Junction {
        junction_table,
        junction_source_columns: _,
        junction_target_columns,
        target_columns,
        ..
    } = &embed.join
    {
        let jt_ref = ctx.qualified_table(&junction_table.schema, &junction_table.name);
        let jt_alias = &junction_table.name;
        inner_sql.push_str(&format!(
            " INNER JOIN {jt_ref} AS {} ON ",
            ctx.quote_ident(jt_alias)
        ));
        // junction → target condition
        let jt_conditions: Vec<String> = junction_target_columns
            .iter()
            .zip(target_columns.iter())
            .map(|(jc, tc)| {
                format!(
                    "{}.{} = {}.{}",
                    ctx.quote_ident(jt_alias),
                    ctx.quote_ident(jc),
                    ctx.quote_ident(child_alias),
                    ctx.quote_ident(tc),
                )
            })
            .collect();
        inner_sql.push_str(&jt_conditions.join(" AND "));
    }

    // WHERE clause (join condition + child filters)
    let mut conditions = vec![join_condition];
    if let Some(child_wc) = child_where {
        conditions.push(child_wc);
    }
    inner_sql.push_str(" WHERE ");
    inner_sql.push_str(&conditions.join(" AND "));

    // ORDER BY for child
    if let Some(ob) = fragment::render_order_clause(&child_plan.order, Some(child_alias), ctx) {
        inner_sql.push_str(" ORDER BY ");
        inner_sql.push_str(&ob);
    }

    // LIMIT for child
    if let Some(lo) = fragment::render_limit_offset(child_plan.range.limit, child_plan.range.offset)
    {
        inner_sql.push(' ');
        inner_sql.push_str(&lo);
    }

    // Determine if this is a to-one (object) or to-many (array) embed
    let is_to_one = is_to_one_relationship(&embed.join);

    // Wrap in json aggregation — dialect-aware
    let output_alias = embed.alias.as_deref().unwrap_or(&embed.name);

    let embed_expr = if ctx.dialect.supports_row_to_json {
        // Postgres path: use row_to_json / json_agg on subquery alias
        let json_agg_fn = ctx.dialect.json_array_agg;
        if is_to_one || embed.is_spread {
            format!(
                "(SELECT row_to_json({sub_alias}) FROM ({inner_sql}) AS {sub_alias} LIMIT 1) AS {}",
                ctx.quote_ident(output_alias)
            )
        } else {
            format!(
                "COALESCE((SELECT {json_agg_fn}({sub_alias}) FROM ({inner_sql}) AS {sub_alias}), '[]') AS {}",
                ctx.quote_ident(output_alias)
            )
        }
    } else {
        // SQLite path: enumerate columns into json_object(...)
        // SQLite cannot serialize row references; must use explicit json_object()
        let json_obj_expr = build_sqlite_json_object(&child_plan.select, ctx);
        let json_agg_fn = ctx.dialect.json_array_agg;
        if is_to_one || embed.is_spread {
            format!(
                "(SELECT {json_obj_expr} FROM ({inner_sql}) LIMIT 1) AS {}",
                ctx.quote_ident(output_alias)
            )
        } else {
            format!(
                "COALESCE((SELECT {json_agg_fn}({json_obj_expr}) FROM ({inner_sql})), '[]') AS {}",
                ctx.quote_ident(output_alias)
            )
        }
    };

    Ok(embed_expr)
}

/// Build a `json_object('col1', "col1", 'col2', "col2", ...)` expression
/// from the embed's select list. Used for SQLite which cannot serialize row references.
fn build_sqlite_json_object(
    selects: &[crate::plan::types::ResolvedSelect],
    ctx: &RenderContext<'_>,
) -> String {
    use crate::plan::types::ResolvedSelect;

    let mut pairs = Vec::new();
    for sel in selects {
        match sel {
            ResolvedSelect::Star => {
                // Can't enumerate star without schema info at this point;
                // fall back to json_object() with no args which just returns '{}'
                // In practice, the plan layer should resolve star to columns.
                // This is a safety fallback.
                return "json_object()".to_string();
            }
            ResolvedSelect::Column(col) => {
                let name = col.alias.as_deref().unwrap_or(&col.name);
                pairs.push(format!("'{}', {}", name, ctx.quote_ident(&col.name)));
            }
            ResolvedSelect::Aggregate(agg) => {
                let func = agg.function.sql_name();
                let inner = match &agg.column {
                    Some(col_name) => ctx.quote_ident(col_name),
                    None => "*".to_string(),
                };
                let alias_name = agg
                    .alias
                    .as_deref()
                    .unwrap_or_else(|| agg.column.as_deref().unwrap_or(func));
                pairs.push(format!("'{}', {func}({inner})", alias_name));
            }
            ResolvedSelect::Embed(_) => {
                // Embeds are appended separately by render_embed; not part of the row JSON
            }
        }
    }

    if pairs.is_empty() {
        "json_object()".to_string()
    } else {
        format!("json_object({})", pairs.join(", "))
    }
}

/// Render the JOIN condition between parent and child.
fn render_join_condition(
    join: &ResolvedJoin,
    parent_alias: &str,
    child_alias: &str,
    ctx: &RenderContext<'_>,
) -> String {
    match join {
        ResolvedJoin::Direct {
            source_columns,
            target_columns,
            ..
        } => {
            let conditions: Vec<String> = source_columns
                .iter()
                .zip(target_columns.iter())
                .map(|(src, tgt)| {
                    format!(
                        "{}.{} = {}.{}",
                        ctx.quote_ident(child_alias),
                        ctx.quote_ident(tgt),
                        ctx.quote_ident(parent_alias),
                        ctx.quote_ident(src),
                    )
                })
                .collect();
            conditions.join(" AND ")
        }
        ResolvedJoin::Junction {
            source_columns,
            junction_table,
            junction_source_columns,
            ..
        } => {
            // For M2M: the WHERE condition links parent → junction
            let jt_alias = &junction_table.name;
            let conditions: Vec<String> = junction_source_columns
                .iter()
                .zip(source_columns.iter())
                .map(|(jc, sc)| {
                    format!(
                        "{}.{} = {}.{}",
                        ctx.quote_ident(jt_alias),
                        ctx.quote_ident(jc),
                        ctx.quote_ident(parent_alias),
                        ctx.quote_ident(sc),
                    )
                })
                .collect();
            conditions.join(" AND ")
        }
        ResolvedJoin::Computed {
            function_name,
            function_schema,
            ..
        } => {
            // Computed relationships use function call
            let fn_ref = ctx.qualified_table(function_schema, function_name);
            format!("TRUE /* computed via {fn_ref} */")
        }
    }
}

/// Determine if a join represents a to-one relationship.
fn is_to_one_relationship(join: &ResolvedJoin) -> bool {
    match join {
        ResolvedJoin::Direct { cardinality, .. } => {
            matches!(cardinality, Cardinality::M2O | Cardinality::O2O)
        }
        ResolvedJoin::Junction { .. } => false, // M2M is always to-many
        ResolvedJoin::Computed { cardinality, .. } => {
            matches!(cardinality, Cardinality::M2O | Cardinality::O2O)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::QualifiedIdentifier;
    use crate::dialect::POSTGRES;
    use crate::plan::types::{
        ReadPlan, ResolvedColumn, ResolvedRange, ResolvedSelect, ResolvedTableInfo,
    };
    use crate::preferences::Preferences;

    fn make_simple_read_plan() -> ReadPlan {
        ReadPlan {
            target: QualifiedIdentifier::new("public", "users"),
            table_info: ResolvedTableInfo {
                is_view: false,
                insertable: true,
                updatable: true,
                deletable: true,
                primary_key_columns: vec!["id".to_string()],
            },
            select: vec![
                ResolvedSelect::Column(ResolvedColumn {
                    name: "id".to_string(),
                    alias: None,
                    json_path: vec![],
                    data_type: "integer".to_string(),
                    nullable: false,
                }),
                ResolvedSelect::Column(ResolvedColumn {
                    name: "name".to_string(),
                    alias: None,
                    json_path: vec![],
                    data_type: "text".to_string(),
                    nullable: true,
                }),
            ],
            embeds: vec![],
            filters: vec![],
            order: vec![],
            range: ResolvedRange {
                limit: Some(10),
                offset: None,
                cursor: None,
                cursor_column: None,
            },
            logic_filters: vec![],
            aggregates: vec![],
            count: None,
            preferences: Preferences::default(),
        }
    }

    #[test]
    fn test_simple_read() {
        let plan = make_simple_read_plan();
        let mut ctx = RenderContext::new(&POSTGRES);
        let sql = render_read(&plan, &mut ctx).unwrap();

        assert!(sql.contains("SELECT"));
        assert!(sql.contains("\"users\".\"id\""));
        assert!(sql.contains("\"users\".\"name\""));
        assert!(sql.contains("FROM \"public\".\"users\" AS \"users\""));
        assert!(sql.contains("LIMIT 10"));
    }
}
