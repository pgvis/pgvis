//! # SQL fragment helpers — operator rendering, filter SQL, order SQL.
//!
//! Small pure functions that render individual SQL fragments. Used by
//! `read.rs`, `mutate.rs`, and `call.rs` to avoid duplication.

use crate::plan::types::{
    FilterRewrite, ResolvedCursor, ResolvedFilter, ResolvedLogicNode, ResolvedLogicTree,
    ResolvedOrder, ResolvedSelect,
};
use crate::query_params::types::{
    FilterValue, IsKind, NullsOrder, Operator, OrderDirection, Quantifier,
};
use crate::select_ast::{JsonOperand, JsonOperation};
use serde_json::Value;

use super::RenderContext;

// ---------------------------------------------------------------------------
// Filter → WHERE clause
// ---------------------------------------------------------------------------

/// Render a list of filters as a WHERE clause (without the `WHERE` keyword).
/// Returns `None` if there are no filters.
pub fn render_where_clause(
    filters: &[ResolvedFilter],
    logic_filters: &[ResolvedLogicTree],
    table_alias: Option<&str>,
    ctx: &mut RenderContext<'_>,
) -> Option<String> {
    let mut conditions = Vec::new();

    for filter in filters {
        conditions.push(render_filter(filter, table_alias, ctx));
    }

    for logic_tree in logic_filters {
        conditions.push(render_logic_tree(logic_tree, table_alias, ctx));
    }

    if conditions.is_empty() {
        None
    } else {
        Some(conditions.join(" AND "))
    }
}

/// Render a single filter condition to SQL.
fn render_filter(
    filter: &ResolvedFilter,
    table_alias: Option<&str>,
    ctx: &mut RenderContext<'_>,
) -> String {
    // Apply any JSON path (`data->>'key'`) to the column reference.
    let mut col = qualified_column(table_alias, &filter.column, ctx);
    for json_op in &filter.json_path {
        col = render_json_path(&col, json_op);
    }

    // Check for dialect-specific rewrite
    if let Some(rewrite) = &filter.rewrite {
        return render_rewritten_filter(&col, filter, rewrite, ctx);
    }

    let negation = if filter.negated { "NOT " } else { "" };

    // Full-text search operators wrap the query text in a *_tsquery function.
    if let Some(fts) = render_fts_filter(&col, filter, negation, ctx) {
        return fts;
    }

    // Quantified comparison: `col op ANY(ARRAY[...])` / `ALL(...)`.
    if let Some(quantifier) = filter.quantifier {
        return render_quantified_filter(&col, filter, quantifier, negation, ctx);
    }

    match &filter.operator {
        // IS operator — no parameter needed
        Operator::Is => {
            let is_val = match &filter.value {
                FilterValue::Is(kind) => match kind {
                    IsKind::Null => "NULL",
                    IsKind::NotNull => "NOT NULL",
                    IsKind::True => "TRUE",
                    IsKind::False => "FALSE",
                    IsKind::Unknown => "UNKNOWN",
                },
                _ => "NULL",
            };
            format!("{col} IS {negation}{is_val}")
        }

        // IS DISTINCT FROM
        Operator::IsDistinct => {
            let placeholder = push_filter_value(&filter.value, ctx);
            if ctx.dialect.supports_is_distinct {
                format!("{col} IS {negation}DISTINCT FROM {placeholder}")
            } else {
                // Fallback for older SQLite: use IS NOT
                if filter.negated {
                    format!("{col} IS {placeholder}")
                } else {
                    format!("{col} IS NOT {placeholder}")
                }
            }
        }

        // IN operator — multiple placeholders
        Operator::In => {
            let placeholders = match &filter.value {
                FilterValue::List(values) => values
                    .iter()
                    .map(|v| ctx.push_param(Value::from(v.as_str())))
                    .collect::<Vec<_>>()
                    .join(", "),
                _ => push_filter_value(&filter.value, ctx),
            };
            format!("{col} {negation}IN ({placeholders})")
        }

        // Comparison and pattern operators
        op => {
            let placeholder = push_filter_value_with_op(&filter.value, Some(op), ctx);
            let sql_op = operator_to_sql(op);
            format!("{negation}{col} {sql_op} {placeholder}")
        }
    }
}

/// Render a full-text-search filter, or `None` if the operator isn't FTS.
///
/// Postgres wraps the query text in the appropriate `*_tsquery` function; SQLite
/// (FTS5) uses the `MATCH` operator.
fn render_fts_filter(
    col: &str,
    filter: &ResolvedFilter,
    negation: &str,
    ctx: &mut RenderContext<'_>,
) -> Option<String> {
    let (fn_name, lang) = match &filter.operator {
        Operator::Fts(l) => ("to_tsquery", l),
        Operator::PlainFts(l) => ("plainto_tsquery", l),
        Operator::PhraseFts(l) => ("phraseto_tsquery", l),
        Operator::WebFts(l) => ("websearch_to_tsquery", l),
        _ => return None,
    };
    let query_ph = push_filter_value(&filter.value, ctx);
    if ctx.dialect.supports_row_to_json {
        // Postgres: `col @@ to_tsquery(['lang'::regconfig,] query)`
        let tsquery = match lang {
            Some(l) => {
                let lang_ph = ctx.push_param(Value::from(l.as_str()));
                format!("{fn_name}({lang_ph}::regconfig, {query_ph})")
            }
            None => format!("{fn_name}({query_ph})"),
        };
        Some(format!("{negation}{col} @@ {tsquery}"))
    } else {
        // SQLite FTS5 exposes matching via the MATCH operator.
        Some(format!("{negation}{col} MATCH {query_ph}"))
    }
}

/// Render a quantified comparison: `col op ANY(ARRAY[...])` / `ALL(...)`.
fn render_quantified_filter(
    col: &str,
    filter: &ResolvedFilter,
    quantifier: Quantifier,
    negation: &str,
    ctx: &mut RenderContext<'_>,
) -> String {
    let quant = match quantifier {
        Quantifier::Any => "ANY",
        Quantifier::All => "ALL",
    };
    let placeholders = match &filter.value {
        FilterValue::List(values) => values
            .iter()
            .map(|v| ctx.push_param(Value::from(v.as_str())))
            .collect::<Vec<_>>()
            .join(", "),
        other => push_filter_value(other, ctx),
    };
    let sql_op = operator_to_sql(&filter.operator);
    format!("{negation}{col} {sql_op} {quant}(ARRAY[{placeholders}])")
}

/// Render a filter that has a dialect-specific rewrite annotation.
fn render_rewritten_filter(
    col: &str,
    filter: &ResolvedFilter,
    rewrite: &FilterRewrite,
    ctx: &mut RenderContext<'_>,
) -> String {
    let negation = if filter.negated { "NOT " } else { "" };
    let placeholder = push_filter_value(&filter.value, ctx);

    match rewrite {
        FilterRewrite::InstrFallback => {
            // ILIKE → LOWER(col) LIKE LOWER(?)
            format!("{negation}LOWER({col}) LIKE LOWER({placeholder})")
        }
        FilterRewrite::GlobPattern => {
            // regex match → GLOB
            format!("{negation}{col} GLOB {placeholder}")
        }
        FilterRewrite::JsonArrayContains => {
            // @> → EXISTS (SELECT 1 FROM json_each(col) WHERE value = ?)
            format!("{negation}EXISTS (SELECT 1 FROM json_each({col}) WHERE value = {placeholder})")
        }
        FilterRewrite::JsonExtractFunction => {
            // -> / ->> → json_extract()
            format!("{negation}json_extract({col}, {placeholder})")
        }
        FilterRewrite::LikePattern(pattern) => {
            // Custom LIKE pattern
            let _ = placeholder; // Don't use the original placeholder
            let p = ctx.push_param(Value::from(pattern.as_str()));
            format!("{negation}{col} LIKE {p}")
        }
    }
}

/// Push a filter value as a parameter and return the placeholder.
fn push_filter_value(value: &FilterValue, ctx: &mut RenderContext<'_>) -> String {
    push_filter_value_with_op(value, None, ctx)
}

/// Push a filter value with optional operator context for LIKE/ILIKE wildcard conversion.
fn push_filter_value_with_op(
    value: &FilterValue,
    op: Option<&Operator>,
    ctx: &mut RenderContext<'_>,
) -> String {
    match value {
        FilterValue::Single(s) => {
            // PostgREST convention: LIKE/ILIKE patterns use `*` instead of `%`
            let val = if matches!(op, Some(Operator::Like | Operator::ILike)) {
                s.replace('*', "%")
            } else {
                s.clone()
            };
            ctx.push_param(Value::from(val))
        }
        FilterValue::List(values) => {
            // For single-placeholder contexts (shouldn't typically happen for lists)
            let json_array = Value::Array(values.iter().map(|v| Value::from(v.as_str())).collect());
            ctx.push_param(json_array)
        }
        FilterValue::Is(_) => String::new(), // IS doesn't use placeholders
    }
}

/// Convert an [`Operator`] to its SQL representation.
fn operator_to_sql(op: &Operator) -> &'static str {
    match op {
        Operator::Eq => "=",
        Operator::Neq => "<>",
        Operator::Gt => ">",
        Operator::Gte => ">=",
        Operator::Lt => "<",
        Operator::Lte => "<=",
        Operator::Like => "LIKE",
        Operator::ILike => "ILIKE",
        Operator::Match => "~",
        Operator::IMatch => "~*",
        Operator::In => "IN",
        Operator::Is => "IS",
        Operator::IsDistinct => "IS DISTINCT FROM",
        Operator::Contains => "@>",
        Operator::ContainedBy => "<@",
        Operator::Overlap => "&&",
        Operator::StrictlyLeft => "<<",
        Operator::StrictlyRight => ">>",
        Operator::NotExtendsRight => "&<",
        Operator::NotExtendsLeft => "&>",
        Operator::Adjacent => "-|-",
        Operator::Fts(_) => "@@",
        Operator::PlainFts(_) => "@@",
        Operator::PhraseFts(_) => "@@",
        Operator::WebFts(_) => "@@",
    }
}

// ---------------------------------------------------------------------------
// Logic tree rendering
// ---------------------------------------------------------------------------

/// Render a logic tree (AND/OR grouping) to SQL.
fn render_logic_tree(
    tree: &ResolvedLogicTree,
    table_alias: Option<&str>,
    ctx: &mut RenderContext<'_>,
) -> String {
    match tree {
        ResolvedLogicTree::And(nodes) => {
            let parts: Vec<String> = nodes
                .iter()
                .map(|n| render_logic_node(n, table_alias, ctx))
                .collect();
            format!("({})", parts.join(" AND "))
        }
        ResolvedLogicTree::Or(nodes) => {
            let parts: Vec<String> = nodes
                .iter()
                .map(|n| render_logic_node(n, table_alias, ctx))
                .collect();
            format!("({})", parts.join(" OR "))
        }
    }
}

/// Render a single node in a logic tree.
fn render_logic_node(
    node: &ResolvedLogicNode,
    table_alias: Option<&str>,
    ctx: &mut RenderContext<'_>,
) -> String {
    match node {
        ResolvedLogicNode::Filter(f) => render_filter(f, table_alias, ctx),
        ResolvedLogicNode::Tree(t) => render_logic_tree(t, table_alias, ctx),
        ResolvedLogicNode::Not(inner) => {
            let inner_sql = render_logic_node(inner, table_alias, ctx);
            format!("NOT ({inner_sql})")
        }
    }
}

// ---------------------------------------------------------------------------
// ORDER BY rendering
// ---------------------------------------------------------------------------

/// Render ORDER BY clause (without the `ORDER BY` keywords).
/// Returns `None` if empty.
pub fn render_order_clause(
    order: &[ResolvedOrder],
    table_alias: Option<&str>,
    ctx: &RenderContext<'_>,
) -> Option<String> {
    if order.is_empty() {
        return None;
    }

    let terms: Vec<String> = order
        .iter()
        .map(|term| {
            let mut col = qualified_column(table_alias, &term.column, ctx);
            for json_op in &term.json_path {
                col = render_json_path(&col, json_op);
            }
            let dir = match term.direction {
                OrderDirection::Asc => "ASC",
                OrderDirection::Desc => "DESC",
            };
            let nulls = match term.nulls {
                Some(NullsOrder::First) => " NULLS FIRST",
                Some(NullsOrder::Last) => " NULLS LAST",
                None => "",
            };
            format!("{col} {dir}{nulls}")
        })
        .collect();

    Some(terms.join(", "))
}

// ---------------------------------------------------------------------------
// SELECT list rendering
// ---------------------------------------------------------------------------

/// Render the SELECT column list from resolved select items.
pub fn render_select_list(
    selects: &[ResolvedSelect],
    table_alias: Option<&str>,
    ctx: &RenderContext<'_>,
) -> String {
    if selects.is_empty() || matches!(selects.first(), Some(ResolvedSelect::Star)) {
        return format!(
            "{}.*",
            table_alias
                .map(|a| ctx.quote_ident(a))
                .unwrap_or_else(|| "*".to_string())
        );
    }

    let items: Vec<String> = selects
        .iter()
        .filter_map(|sel| match sel {
            ResolvedSelect::Star => Some(if let Some(alias) = table_alias {
                format!("{}.*", ctx.quote_ident(alias))
            } else {
                "*".to_string()
            }),
            ResolvedSelect::Column(col) => {
                let mut expr = qualified_column(table_alias, &col.name, ctx);

                // Apply JSON path operations
                for json_op in &col.json_path {
                    expr = render_json_path(&expr, json_op);
                }

                // Apply cast (`::type`)
                if let Some(cast) = &col.cast {
                    expr = render_cast(&expr, cast, ctx);
                }

                // Apply alias
                if let Some(alias) = &col.alias {
                    Some(format!("{expr} AS {}", ctx.quote_ident(alias)))
                } else {
                    Some(expr)
                }
            }
            ResolvedSelect::Aggregate(agg) => {
                let func = agg.function.sql_name();
                let mut inner = match &agg.column {
                    Some(col_name) => qualified_column(table_alias, col_name, ctx),
                    None => "*".to_string(),
                };
                // JSON path + pre-aggregation cast apply to the column argument.
                for json_op in &agg.json_path {
                    inner = render_json_path(&inner, json_op);
                }
                if let Some(cast) = &agg.cast {
                    inner = render_cast(&inner, cast, ctx);
                }
                let mut expr = format!("{func}({inner})");
                // Post-aggregation cast applies to the aggregate result.
                if let Some(cast) = &agg.aggregate_cast {
                    expr = render_cast(&expr, cast, ctx);
                }
                let alias_name = agg
                    .alias
                    .as_deref()
                    .unwrap_or_else(|| agg.column.as_deref().unwrap_or(func));
                Some(format!("{expr} AS {}", ctx.quote_ident(alias_name)))
            }
            ResolvedSelect::Embed(_) => None, // Embeds are handled separately
        })
        .collect();

    if items.is_empty() {
        "*".to_string()
    } else {
        items.join(", ")
    }
}

/// Render a JSON path operation on an expression.
fn render_json_path(expr: &str, op: &JsonOperation) -> String {
    match op {
        JsonOperation::Arrow(operand) => {
            let key = json_operand_to_sql(operand);
            format!("{expr}->{key}")
        }
        JsonOperation::DoubleArrow(operand) => {
            let key = json_operand_to_sql(operand);
            format!("{expr}->>{key}")
        }
    }
}

/// Render a `CAST(expr AS type)` expression.
///
/// The target type name is sanitised to a conservative identifier subset
/// (letters, digits, underscore, spaces, and `[]` for arrays) so a user-supplied
/// `::type` token cannot break out of the cast.
fn render_cast(expr: &str, cast_type: &str, _ctx: &RenderContext<'_>) -> String {
    let safe: String = cast_type
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '[' | ']'))
        .collect();
    if safe.is_empty() {
        expr.to_string()
    } else {
        format!("CAST({expr} AS {safe})")
    }
}

/// Convert a JSON operand to its SQL representation.
fn json_operand_to_sql(operand: &JsonOperand) -> String {
    match operand {
        // Escape single quotes to keep the key inside the string literal.
        JsonOperand::Key(k) => format!("'{}'", k.replace('\'', "''")),
        JsonOperand::Index(i) => i.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Aggregate / GROUP BY
// ---------------------------------------------------------------------------

/// Render the GROUP BY clause from the select list (non-aggregate columns).
/// Returns `None` if no GROUP BY is needed.
pub fn render_group_by(
    selects: &[ResolvedSelect],
    table_alias: Option<&str>,
    ctx: &RenderContext<'_>,
) -> Option<String> {
    // Check if there are any aggregates
    let has_aggregates = selects
        .iter()
        .any(|s| matches!(s, ResolvedSelect::Aggregate(_)));

    if !has_aggregates {
        return None;
    }

    // Collect non-aggregate columns for GROUP BY
    let group_cols: Vec<String> = selects
        .iter()
        .filter_map(|s| match s {
            ResolvedSelect::Column(col) => Some(qualified_column(table_alias, &col.name, ctx)),
            _ => None,
        })
        .collect();

    if group_cols.is_empty() {
        None
    } else {
        Some(group_cols.join(", "))
    }
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET
// ---------------------------------------------------------------------------

/// Render LIMIT/OFFSET clause. Returns `None` if neither is set.
///
/// Uses literal numbers instead of parameters because:
/// 1. LIMIT/OFFSET don't support implicit TEXT→BIGINT coercion in Postgres
/// 2. These values are system-generated from Range header parsing, not user input
pub fn render_limit_offset(limit: Option<u64>, offset: Option<u64>) -> Option<String> {
    match (limit, offset) {
        (Some(l), Some(o)) => Some(format!("LIMIT {l} OFFSET {o}")),
        (Some(l), None) => Some(format!("LIMIT {l}")),
        (None, Some(o)) => Some(format!("OFFSET {o}")),
        (None, None) => None,
    }
}

// ---------------------------------------------------------------------------
// Cursor condition
// ---------------------------------------------------------------------------

/// Render a cursor-based WHERE condition for keyset pagination.
///
/// Generates a condition like `"table"."id" > $N` where the value is pushed
/// as a bind parameter. The operator is pre-determined by the planner based
/// on the ORDER BY direction (ASC → `>`, DESC → `<`).
pub fn render_cursor_condition(
    cursor: &ResolvedCursor,
    table_alias: Option<&str>,
    ctx: &mut RenderContext<'_>,
) -> String {
    let col_ref = qualified_column(table_alias, &cursor.column, ctx);
    let op = operator_to_sql(&cursor.operator);
    let placeholder = ctx.push_param(Value::from(cursor.value.as_str()));
    format!("{col_ref} {op} {placeholder}")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a qualified column reference: `"alias"."column"` or just `"column"`.
pub fn qualified_column(
    table_alias: Option<&str>,
    column: &str,
    ctx: &RenderContext<'_>,
) -> String {
    if let Some(alias) = table_alias {
        format!("{}.{}", ctx.quote_ident(alias), ctx.quote_ident(column))
    } else {
        ctx.quote_ident(column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::POSTGRES;
    use crate::plan::types::ResolvedFilter;
    use crate::query_params::types::{FilterValue, Operator};

    #[test]
    fn test_operator_to_sql() {
        assert_eq!(operator_to_sql(&Operator::Eq), "=");
        assert_eq!(operator_to_sql(&Operator::Gte), ">=");
        assert_eq!(operator_to_sql(&Operator::Like), "LIKE");
        assert_eq!(operator_to_sql(&Operator::Contains), "@>");
    }

    #[test]
    fn test_qualified_column() {
        let ctx = RenderContext::new(&POSTGRES);
        assert_eq!(qualified_column(Some("t"), "id", &ctx), "\"t\".\"id\"");
        assert_eq!(qualified_column(None, "name", &ctx), "\"name\"");
    }

    #[test]
    fn test_render_filter_eq() {
        let mut ctx = RenderContext::new(&POSTGRES);
        let filter = ResolvedFilter {
            column: "age".to_string(),
            json_path: vec![],
            operator: Operator::Eq,
            quantifier: None,
            value: FilterValue::Single("25".to_string()),
            negated: false,
            rewrite: None,
        };
        let sql = render_filter(&filter, Some("t"), &mut ctx);
        assert_eq!(sql, "\"t\".\"age\" = $1");
    }

    #[test]
    fn test_render_filter_is_null() {
        let mut ctx = RenderContext::new(&POSTGRES);
        let filter = ResolvedFilter {
            column: "bio".to_string(),
            json_path: vec![],
            operator: Operator::Is,
            quantifier: None,
            value: FilterValue::Is(IsKind::Null),
            negated: false,
            rewrite: None,
        };
        let sql = render_filter(&filter, None, &mut ctx);
        assert_eq!(sql, "\"bio\" IS NULL");
    }

    #[test]
    fn test_render_filter_in() {
        let mut ctx = RenderContext::new(&POSTGRES);
        let filter = ResolvedFilter {
            column: "status".to_string(),
            json_path: vec![],
            operator: Operator::In,
            quantifier: None,
            value: FilterValue::List(vec!["active".to_string(), "pending".to_string()]),
            negated: false,
            rewrite: None,
        };
        let sql = render_filter(&filter, None, &mut ctx);
        assert_eq!(sql, "\"status\" IN ($1, $2)");
    }

    #[test]
    fn test_render_filter_negated() {
        let mut ctx = RenderContext::new(&POSTGRES);
        let filter = ResolvedFilter {
            column: "name".to_string(),
            json_path: vec![],
            operator: Operator::Like,
            quantifier: None,
            value: FilterValue::Single("%smith%".to_string()),
            negated: true,
            rewrite: None,
        };
        let sql = render_filter(&filter, None, &mut ctx);
        assert_eq!(sql, "NOT \"name\" LIKE $1");
    }

    #[test]
    fn test_render_rewrite_ilike_sqlite() {
        let mut ctx = RenderContext::new(&crate::dialect::SQLITE);
        let filter = ResolvedFilter {
            column: "name".to_string(),
            json_path: vec![],
            operator: Operator::ILike,
            quantifier: None,
            value: FilterValue::Single("%smith%".to_string()),
            negated: false,
            rewrite: Some(FilterRewrite::InstrFallback),
        };
        let sql = render_filter(&filter, None, &mut ctx);
        assert_eq!(sql, "LOWER(\"name\") LIKE LOWER(?)");
    }
}
