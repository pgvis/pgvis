//! # Call plan → RPC function call SQL generation.
//!
//! Renders a [`CallPlan`] into a SELECT statement that invokes a stored function.
//! Postgres-only — gated by `dialect.has_routines` at plan time.

use crate::error::Error;
use crate::plan::types::CallPlan;
use serde_json::Value;

use super::RenderContext;
use super::fragment;

/// Alias bound to a set/table-returning function's result for projection.
const RPC_RESULT_ALIAS: &str = "pgvis_rpc";

/// Render a [`CallPlan`] into the inner SQL (without CTE wrapper).
///
/// Produces SQL like:
/// ```sql
/// SELECT * FROM "schema"."function_name"("param1" := $1, "param2" := $2)
/// ```
///
/// For set-returning functions, the result is a table expression.
/// For scalar functions, it wraps in a single-row result.
pub fn render_call(plan: &CallPlan, ctx: &mut RenderContext<'_>) -> Result<String, Error> {
    let fn_ref = ctx.qualified_table(&plan.function.schema, &plan.function.name);

    // Extract body object for parameter values
    let body_obj = match &plan.body {
        Some(crate::plan::types::RequestBody::Single(obj)) => obj.as_object().cloned(),
        _ => None,
    };

    // Build parameter list with named arguments
    let args: Vec<String> = plan
        .params
        .iter()
        .filter(|p| p.has_value)
        .map(|p| {
            let val = body_obj
                .as_ref()
                .and_then(|o| o.get(&p.name).cloned())
                .unwrap_or(Value::Null);
            let placeholder = ctx.push_param(val);
            format!("{} := {placeholder}", ctx.quote_ident(&p.name))
        })
        .collect();

    let args_sql = args.join(", ");

    let sql = if plan.function_info.returns_set || plan.function_info.returns_table {
        // Set/composite-returning function. Wrap the call in a subquery so the
        // client's select projection, filters, ordering, and pagination apply to
        // the result set (PostgREST supports `?select=`/`?col=eq.x`/`?order=`/`?limit=`).
        let alias = RPC_RESULT_ALIAS;
        let projection = fragment::render_select_list(&plan.returning, Some(alias), ctx);
        let mut out = format!(
            "SELECT {projection} FROM {fn_ref}({args_sql}) AS {}",
            ctx.quote_ident(alias)
        );

        if let Some(wc) =
            fragment::render_where_clause(&plan.filters, &plan.logic_filters, Some(alias), ctx)
        {
            out.push_str(" WHERE ");
            out.push_str(&wc);
        }
        if let Some(ob) = fragment::render_order_clause(&plan.order, Some(alias), ctx) {
            out.push_str(" ORDER BY ");
            out.push_str(&ob);
        }
        if let Some(lo) = fragment::render_limit_offset(plan.range.limit, plan.range.offset) {
            out.push(' ');
            out.push_str(&lo);
        }
        out
    } else {
        // Scalar/single-row function: SELECT fn(args) AS result
        format!("SELECT {fn_ref}({args_sql}) AS result")
    };

    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{QualifiedIdentifier, Volatility};
    use crate::dialect::POSTGRES;
    use crate::plan::types::{
        CallPlan, ResolvedFunctionInfo, ResolvedParam, ResolvedRange, ResolvedSelect,
    };

    fn empty_range() -> ResolvedRange {
        ResolvedRange {
            limit: None,
            offset: None,
            cursor: None,
            cursor_column: None,
        }
    }
    use crate::preferences::Preferences;

    #[test]
    fn test_set_returning_function() {
        let plan = CallPlan {
            function: QualifiedIdentifier::new("public", "get_users"),
            function_info: ResolvedFunctionInfo {
                volatility: Volatility::Stable,
                return_type: "users".to_string(),
                returns_set: true,
                returns_table: true,
                isolation_level: None,
            },
            params: vec![ResolvedParam {
                name: "min_age".to_string(),
                param_type: "integer".to_string(),
                has_value: true,
                is_variadic: false,
            }],
            returning: vec![ResolvedSelect::Star],
            filters: vec![],
            logic_filters: vec![],
            order: vec![],
            range: empty_range(),
            is_singular: false,
            preferences: Preferences::default(),
            body: None,
        };

        let mut ctx = RenderContext::new(&POSTGRES);
        let sql = render_call(&plan, &mut ctx).unwrap();

        assert_eq!(
            sql,
            "SELECT \"pgvis_rpc\".* FROM \"public\".\"get_users\"(\"min_age\" := $1) AS \"pgvis_rpc\""
        );
    }

    #[test]
    fn test_scalar_function() {
        let plan = CallPlan {
            function: QualifiedIdentifier::new("public", "add"),
            function_info: ResolvedFunctionInfo {
                volatility: Volatility::Immutable,
                return_type: "integer".to_string(),
                returns_set: false,
                returns_table: false,
                isolation_level: None,
            },
            params: vec![
                ResolvedParam {
                    name: "a".to_string(),
                    param_type: "integer".to_string(),
                    has_value: true,
                    is_variadic: false,
                },
                ResolvedParam {
                    name: "b".to_string(),
                    param_type: "integer".to_string(),
                    has_value: true,
                    is_variadic: false,
                },
            ],
            returning: vec![ResolvedSelect::Star],
            filters: vec![],
            logic_filters: vec![],
            order: vec![],
            range: empty_range(),
            is_singular: true,
            preferences: Preferences::default(),
            body: None,
        };

        let mut ctx = RenderContext::new(&POSTGRES);
        let sql = render_call(&plan, &mut ctx).unwrap();

        assert_eq!(
            sql,
            "SELECT \"public\".\"add\"(\"a\" := $1, \"b\" := $2) AS result"
        );
    }

    #[test]
    fn test_function_no_params() {
        let plan = CallPlan {
            function: QualifiedIdentifier::new("public", "now_utc"),
            function_info: ResolvedFunctionInfo {
                volatility: Volatility::Stable,
                return_type: "timestamptz".to_string(),
                returns_set: false,
                returns_table: false,
                isolation_level: None,
            },
            params: vec![],
            returning: vec![ResolvedSelect::Star],
            filters: vec![],
            logic_filters: vec![],
            order: vec![],
            range: empty_range(),
            is_singular: true,
            preferences: Preferences::default(),
            body: None,
        };

        let mut ctx = RenderContext::new(&POSTGRES);
        let sql = render_call(&plan, &mut ctx).unwrap();

        assert_eq!(sql, "SELECT \"public\".\"now_utc\"() AS result");
    }
}
