//! # Mutate plan → INSERT / UPDATE / DELETE SQL generation.
//!
//! Renders [`MutatePlan`] into parameterised DML SQL. The CTE wrapper in `cte.rs`
//! wraps this to produce the unified response shape.

use serde_json::Value;

use super::RenderContext;
use super::fragment;
use crate::error::Error;
use crate::plan::types::{ConflictResolution, MutatePlan, MutationType, RequestBody};

/// Render a [`MutatePlan`] into the inner DML SQL (without CTE wrapper).
///
/// Returns the SQL string for the mutation body.
pub fn render_mutate(plan: &MutatePlan, ctx: &mut RenderContext<'_>) -> Result<String, Error> {
    let table_ref = ctx.qualified_table(&plan.target.schema, &plan.target.name);

    match &plan.mutation {
        MutationType::Insert {
            payload_columns,
            is_bulk: _,
            on_conflict,
        } => render_insert(&table_ref, payload_columns, on_conflict.as_ref(), plan, ctx),
        MutationType::Update { payload_columns } => {
            render_update(&table_ref, payload_columns, plan, ctx)
        }
        MutationType::Delete => render_delete(&table_ref, plan, ctx),
    }
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

fn render_insert(
    table_ref: &str,
    columns: &[String],
    on_conflict: Option<&crate::plan::types::ResolvedConflict>,
    plan: &MutatePlan,
    ctx: &mut RenderContext<'_>,
) -> Result<String, Error> {
    if columns.is_empty() {
        // INSERT with default values
        let mut sql = format!("INSERT INTO {table_ref} DEFAULT VALUES");
        append_returning(&mut sql, plan, ctx);
        return Ok(sql);
    }

    // Extract body object(s) for values
    let body_objects = extract_body_objects(&plan.body);

    // Column list
    let col_list: Vec<String> = columns.iter().map(|c| ctx.quote_ident(c)).collect();
    let col_sql = col_list.join(", ");

    let mut sql = if body_objects.len() <= 1 {
        // Single-row INSERT
        let obj = body_objects.first();
        let placeholders: Vec<String> = columns
            .iter()
            .map(|col| {
                let val = obj.and_then(|o| o.get(col).cloned()).unwrap_or(Value::Null);
                ctx.push_param(val)
            })
            .collect();
        let values_sql = placeholders.join(", ");
        format!("INSERT INTO {table_ref} ({col_sql}) VALUES ({values_sql})")
    } else {
        // Multi-row INSERT: VALUES (...), (...), ...
        let rows_sql: Vec<String> = body_objects
            .iter()
            .map(|obj| {
                let placeholders: Vec<String> = columns
                    .iter()
                    .map(|col| {
                        let val = obj.get(col).cloned().unwrap_or(Value::Null);
                        ctx.push_param(val)
                    })
                    .collect();
                format!("({})", placeholders.join(", "))
            })
            .collect();
        format!(
            "INSERT INTO {table_ref} ({col_sql}) VALUES {}",
            rows_sql.join(", ")
        )
    };

    // ON CONFLICT clause (upsert)
    if let Some(conflict) = on_conflict {
        let conflict_cols: Vec<String> = conflict
            .columns
            .iter()
            .map(|c| ctx.quote_ident(c))
            .collect();
        let conflict_sql = conflict_cols.join(", ");

        match conflict.resolution {
            ConflictResolution::MergeDuplicates => {
                // ON CONFLICT (cols) DO UPDATE SET col = EXCLUDED.col, ...
                let set_clauses: Vec<String> = columns
                    .iter()
                    .filter(|c| !conflict.columns.contains(c))
                    .map(|c| format!("{} = EXCLUDED.{}", ctx.quote_ident(c), ctx.quote_ident(c)))
                    .collect();

                if set_clauses.is_empty() {
                    sql.push_str(&format!(" ON CONFLICT ({conflict_sql}) DO NOTHING"));
                } else {
                    sql.push_str(&format!(
                        " ON CONFLICT ({conflict_sql}) DO UPDATE SET {}",
                        set_clauses.join(", ")
                    ));
                }
            }
            ConflictResolution::IgnoreDuplicates => {
                sql.push_str(&format!(" ON CONFLICT ({conflict_sql}) DO NOTHING"));
            }
        }
    }

    append_returning(&mut sql, plan, ctx);
    Ok(sql)
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

fn render_update(
    table_ref: &str,
    columns: &[String],
    plan: &MutatePlan,
    ctx: &mut RenderContext<'_>,
) -> Result<String, Error> {
    let table_alias = &plan.target.name;

    // Extract body for values
    let body_objects = extract_body_objects(&plan.body);
    let obj = body_objects.first();

    // SET clause
    let set_clauses: Vec<String> = columns
        .iter()
        .map(|c| {
            let val = obj.and_then(|o| o.get(c).cloned()).unwrap_or(Value::Null);
            let placeholder = ctx.push_param(val);
            format!("{} = {placeholder}", ctx.quote_ident(c))
        })
        .collect();

    let mut sql = format!("UPDATE {table_ref} SET {}", set_clauses.join(", "));

    // WHERE clause
    if let Some(wc) =
        fragment::render_where_clause(&plan.filters, &plan.logic_filters, Some(table_alias), ctx)
    {
        sql.push_str(" WHERE ");
        sql.push_str(&wc);
    }

    append_returning(&mut sql, plan, ctx);
    Ok(sql)
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

fn render_delete(
    table_ref: &str,
    plan: &MutatePlan,
    ctx: &mut RenderContext<'_>,
) -> Result<String, Error> {
    let table_alias = &plan.target.name;

    let mut sql = format!("DELETE FROM {table_ref}");

    // WHERE clause
    if let Some(wc) =
        fragment::render_where_clause(&plan.filters, &plan.logic_filters, Some(table_alias), ctx)
    {
        sql.push_str(" WHERE ");
        sql.push_str(&wc);
    }

    append_returning(&mut sql, plan, ctx);
    Ok(sql)
}

// ---------------------------------------------------------------------------
// RETURNING clause
// ---------------------------------------------------------------------------

/// Append `RETURNING *` or `RETURNING col1, col2` if the dialect supports it.
fn append_returning(sql: &mut String, _plan: &MutatePlan, ctx: &RenderContext<'_>) {
    if !ctx.dialect.supports_returning {
        return;
    }

    // Check if we need a RETURNING clause (based on preferences)
    // For now, always add RETURNING * — the CTE wrapper handles column selection
    sql.push_str(" RETURNING *");
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract body objects as a Vec of serde_json Maps.
fn extract_body_objects(body: &Option<RequestBody>) -> Vec<&serde_json::Map<String, Value>> {
    match body {
        Some(RequestBody::Single(obj)) => {
            if let Some(map) = obj.as_object() {
                vec![map]
            } else {
                vec![]
            }
        }
        Some(RequestBody::Bulk(arr)) => arr.iter().filter_map(|v| v.as_object()).collect(),
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::QualifiedIdentifier;
    use crate::dialect::POSTGRES;
    use crate::plan::types::{
        ConflictResolution, MutatePlan, MutationType, ResolvedConflict, ResolvedRange,
        ResolvedSelect, ResolvedTableInfo,
    };
    use crate::preferences::Preferences;

    fn make_insert_plan() -> MutatePlan {
        MutatePlan {
            target: QualifiedIdentifier::new("public", "users"),
            table_info: ResolvedTableInfo {
                is_view: false,
                insertable: true,
                updatable: true,
                deletable: true,
                primary_key_columns: vec!["id".to_string()],
            },
            mutation: MutationType::Insert {
                payload_columns: vec!["name".to_string(), "email".to_string()],
                is_bulk: false,
                on_conflict: None,
            },
            returning: vec![ResolvedSelect::Star],
            filters: vec![],
            logic_filters: vec![],
            order: vec![],
            range: ResolvedRange {
                limit: None,
                offset: None,
                cursor: None,
                cursor_column: None,
            },
            embeds: vec![],
            count: None,
            preferences: Preferences::default(),
            body: Some(RequestBody::Single(serde_json::json!({
                "name": "Alice",
                "email": "alice@example.com"
            }))),
        }
    }

    #[test]
    fn test_simple_insert() {
        let plan = make_insert_plan();
        let mut ctx = RenderContext::new(&POSTGRES);
        let sql = render_mutate(&plan, &mut ctx).unwrap();

        assert!(sql.contains("INSERT INTO \"public\".\"users\""));
        assert!(sql.contains("(\"name\", \"email\")"));
        assert!(sql.contains("VALUES ($1, $2)"));
        assert!(sql.contains("RETURNING *"));
    }

    #[test]
    fn test_insert_with_upsert() {
        let mut plan = make_insert_plan();
        plan.mutation = MutationType::Insert {
            payload_columns: vec!["name".to_string(), "email".to_string()],
            is_bulk: false,
            on_conflict: Some(ResolvedConflict {
                columns: vec!["email".to_string()],
                resolution: ConflictResolution::MergeDuplicates,
            }),
        };

        let mut ctx = RenderContext::new(&POSTGRES);
        let sql = render_mutate(&plan, &mut ctx).unwrap();

        assert!(sql.contains("ON CONFLICT (\"email\") DO UPDATE SET"));
        assert!(sql.contains("\"name\" = EXCLUDED.\"name\""));
    }

    #[test]
    fn test_delete_with_filter() {
        let plan = MutatePlan {
            target: QualifiedIdentifier::new("public", "users"),
            table_info: ResolvedTableInfo {
                is_view: false,
                insertable: true,
                updatable: true,
                deletable: true,
                primary_key_columns: vec!["id".to_string()],
            },
            mutation: MutationType::Delete,
            returning: vec![ResolvedSelect::Star],
            filters: vec![crate::plan::types::ResolvedFilter {
                column: "id".to_string(),
                operator: crate::query_params::types::Operator::Eq,
                value: crate::query_params::types::FilterValue::Single("5".to_string()),
                negated: false,
                rewrite: None,
            }],
            logic_filters: vec![],
            order: vec![],
            range: ResolvedRange {
                limit: None,
                offset: None,
                cursor: None,
                cursor_column: None,
            },
            embeds: vec![],
            count: None,
            preferences: Preferences::default(),
            body: None,
        };

        let mut ctx = RenderContext::new(&POSTGRES);
        let sql = render_mutate(&plan, &mut ctx).unwrap();

        assert!(sql.contains("DELETE FROM \"public\".\"users\""));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("\"users\".\"id\" = $1"));
        assert!(sql.contains("RETURNING *"));
    }
}
