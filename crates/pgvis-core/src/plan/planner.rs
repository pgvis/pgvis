//! # The planner — entry point for `plan_request()`.
//!
//! Transforms an `ApiRequest` + `SchemaCache` + `Dialect` + `Config` into
//! a fully-resolved `ActionPlan`.

use std::collections::HashSet;

use crate::cache::{QualifiedIdentifier, SchemaCache};
use crate::config::Config;
use crate::dialect::Dialect;
use crate::error::{Error, ErrorCode};
use crate::preferences::{PreferCount, Preferences};
use crate::query_params::types::OrderDirection;
use crate::select_ast::SelectItem;

use super::resolve;
use super::types::*;
use super::validate;

// ---------------------------------------------------------------------------
// PlanConfig
// ---------------------------------------------------------------------------

/// Focused configuration for the plan layer.
/// Extracted from `Config` to keep the planner interface clean.
#[derive(Debug, Clone)]
pub struct PlanConfig {
    /// Whether aggregate functions are enabled.
    pub aggregates_enabled: bool,
    /// Server-side max rows cap.
    pub max_rows: Option<u64>,
}

impl From<&Config> for PlanConfig {
    fn from(config: &Config) -> Self {
        Self {
            aggregates_enabled: config.aggregates_enabled,
            max_rows: config.max_rows,
        }
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Transform an `ApiRequest` into an `ActionPlan`.
///
/// This is the **main entry point** of the plan layer. It:
/// 1. Resolves the target table/function from the schema cache
/// 2. Validates the request against the dialect's capabilities
/// 3. Resolves all select items, filters, ordering against the schema
/// 4. Produces a fully-resolved plan the SQL builder can consume directly
///
/// # Errors
///
/// Returns descriptive `Error::Plan` variants for:
/// - Table/function not found (with "did you mean?" suggestions)
/// - Column not found
/// - Ambiguous relationships
/// - Spread on to-many relationships
/// - Aggregates disabled
/// - Unsupported dialect features
pub fn plan_request(
    request: &ApiRequest,
    cache: &SchemaCache,
    dialect: &Dialect,
    config: &Config,
) -> Result<ActionPlan, Error> {
    let plan_config = PlanConfig::from(config);

    // Validate dialect support for the request
    validate::validate_dialect_support(request, dialect)?;

    match request.method {
        RequestMethod::Get | RequestMethod::Head => {
            if request.is_rpc {
                // GET /rpc/fn — immutable function call with args from query params
                plan_call(request, cache, dialect, &plan_config)
            } else {
                plan_read(request, cache, dialect, &plan_config)
            }
        }
        RequestMethod::Post => {
            if request.is_rpc {
                plan_call(request, cache, dialect, &plan_config)
            } else {
                plan_mutate(request, cache, dialect, &plan_config)
            }
        }
        RequestMethod::Patch | RequestMethod::Put => {
            plan_mutate(request, cache, dialect, &plan_config)
        }
        RequestMethod::Delete => plan_mutate(request, cache, dialect, &plan_config),
    }
}

// ---------------------------------------------------------------------------
// plan_read
// ---------------------------------------------------------------------------

/// Build a `ReadPlan` from a GET/HEAD request.
fn plan_read(
    request: &ApiRequest,
    cache: &SchemaCache,
    dialect: &Dialect,
    config: &PlanConfig,
) -> Result<ActionPlan, Error> {
    let table = resolve::resolve_table(cache, &request.schema, &request.target)?;
    let table_info = resolve::resolve_table_info(table);

    // Resolve select items (columns + embeds)
    let default_star = vec![SelectItem::Star];
    let items: &[SelectItem] = if request.select.is_empty() {
        &default_star
    } else {
        &request.select
    };
    let (selects, embeds) =
        resolve::resolve_select_items(cache, table, &request.schema, items, dialect, config)?;

    // Validate aggregates
    let aggregates = validate::validate_aggregates(&selects, config)?;

    // Resolve filters
    let filters = resolve::resolve_filters(table, &request.filters, dialect)?;

    // Resolve logic tree filters
    let logic_filters: Result<Vec<_>, _> = request
        .logic_filters
        .iter()
        .map(|lt| resolve::resolve_logic_tree(table, lt, dialect))
        .collect();

    // Resolve ordering
    let mut order = resolve::resolve_order(table, &request.order)?;

    // Resolve range with server-side cap
    let mut range = resolve::resolve_range(&request.range, config.max_rows);

    // Resolve cursor pagination (if present)
    if let Some(ref cursor_spec) = request.cursor {
        let (resolved_cursor, cursor_col_name) =
            resolve::resolve_cursor(table, cursor_spec, &order)?;
        range.cursor = resolved_cursor;
        range.cursor_column = Some(cursor_col_name.clone());
        // When cursor is present, offset is meaningless — clear it
        range.offset = None;

        // Ensure the cursor column appears in ORDER BY for deterministic paging.
        // If not already present, prepend it with ASC direction.
        if !order.iter().any(|o| o.column == cursor_col_name) {
            order.insert(
                0,
                ResolvedOrder {
                    column: cursor_col_name,
                    json_path: Vec::new(),
                    direction: OrderDirection::Asc,
                    nulls: None,
                },
            );
        }
    }

    // Determine count strategy from preferences
    let count = resolve_count_strategy(&request.preferences);

    Ok(ActionPlan::Read(ReadPlan {
        target: QualifiedIdentifier::new(&request.schema, &request.target),
        table_info,
        select: selects,
        embeds,
        filters,
        order,
        range,
        logic_filters: logic_filters?,
        aggregates,
        count,
        preferences: request.preferences.clone(),
    }))
}

// ---------------------------------------------------------------------------
// plan_mutate
// ---------------------------------------------------------------------------

/// Build a `MutatePlan` from a POST/PATCH/PUT/DELETE request.
fn plan_mutate(
    request: &ApiRequest,
    cache: &SchemaCache,
    dialect: &Dialect,
    config: &PlanConfig,
) -> Result<ActionPlan, Error> {
    let table = resolve::resolve_table(cache, &request.schema, &request.target)?;
    let table_info = resolve::resolve_table_info(table);

    // Validate the table supports this mutation
    validate::validate_mutation_target(&table_info, &request.target, request.method)?;

    // Resolve returning columns (from select parameter)
    let default_star = vec![SelectItem::Star];
    let items: &[SelectItem] = if request.select.is_empty() {
        &default_star
    } else {
        &request.select
    };
    let (returning, embeds) =
        resolve::resolve_select_items(cache, table, &request.schema, items, dialect, config)?;

    // Resolve filters (for UPDATE/DELETE)
    let filters = resolve::resolve_filters(table, &request.filters, dialect)?;
    let logic_filters: Result<Vec<_>, _> = request
        .logic_filters
        .iter()
        .map(|lt| resolve::resolve_logic_tree(table, lt, dialect))
        .collect();

    // Resolve ordering
    let order = resolve::resolve_order(table, &request.order)?;
    // Only the client's own limit/offset: `max_rows` caps what a read returns,
    // and must never silently turn a PATCH/DELETE into a limited one.
    let range = resolve::resolve_range(&request.range, None);
    let count = resolve_count_strategy(&request.preferences);
    let limited = range.limit.is_some() || range.offset.is_some();
    if limited && request.method == RequestMethod::Post {
        return Err(Error::invalid_filter("limit/offset do not apply to an insert"));
    }
    if limited && table_info.is_view {
        return Err(Error::invalid_filter(
            "a limited update/delete needs a table: a view has no row identifier",
        ));
    }

    // Determine mutation type
    let mutation = match request.method {
        RequestMethod::Post => {
            let payload_columns = extract_payload_columns(&request.body);
            validate_payload(request.method, &request.body, table, &payload_columns, dialect)?;
            let is_bulk = matches!(&request.body, Some(RequestBody::Bulk(_)));
            // Determine the conflict target. An explicit `on_conflict=` param wins;
            // otherwise, when the client asked for upsert semantics via
            // `Prefer: resolution=merge-duplicates`/`ignore-duplicates`, PostgREST
            // defaults the target to the table's primary key.
            let conflict_resolution = match request.preferences.resolution {
                Some(crate::preferences::PreferResolution::IgnoreDuplicates) => {
                    Some(ConflictResolution::IgnoreDuplicates)
                }
                Some(crate::preferences::PreferResolution::MergeDuplicates) => {
                    Some(ConflictResolution::MergeDuplicates)
                }
                None => None,
            };
            let on_conflict = match (&request.on_conflict, conflict_resolution) {
                (Some(col), resolution) => Some(ResolvedConflict {
                    columns: col.split(',').map(|s| s.trim().to_string()).collect(),
                    resolution: resolution.unwrap_or(ConflictResolution::MergeDuplicates),
                }),
                (None, Some(resolution)) if !table_info.primary_key_columns.is_empty() => {
                    Some(ResolvedConflict {
                        columns: table_info.primary_key_columns.clone(),
                        resolution,
                    })
                }
                (None, _) => None,
            };
            MutationType::Insert {
                payload_columns,
                is_bulk,
                on_conflict,
            }
        }
        RequestMethod::Patch | RequestMethod::Put => {
            let payload_columns = extract_payload_columns(&request.body);
            validate_payload(request.method, &request.body, table, &payload_columns, dialect)?;
            MutationType::Update { payload_columns }
        }
        RequestMethod::Delete => MutationType::Delete,
        _ => unreachable!("plan_mutate called with non-mutation method"),
    };

    Ok(ActionPlan::Mutate(MutatePlan {
        target: QualifiedIdentifier::new(&request.schema, &request.target),
        table_info,
        mutation,
        returning,
        filters,
        logic_filters: logic_filters?,
        order,
        range,
        embeds,
        count,
        preferences: request.preferences.clone(),
        body: request.body.clone(),
    }))
}

// ---------------------------------------------------------------------------
// plan_call
// ---------------------------------------------------------------------------

/// Build a `CallPlan` for an RPC function call.
fn plan_call(
    request: &ApiRequest,
    cache: &SchemaCache,
    dialect: &Dialect,
    config: &PlanConfig,
) -> Result<ActionPlan, Error> {
    let routines = cache
        .find_routines(&request.schema, &request.target)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let routine = resolve_overload(routines, request)?;

    let function_info = ResolvedFunctionInfo {
        volatility: routine.volatility,
        return_type: routine.return_type.clone(),
        returns_set: routine.return_type_is_set,
        returns_table: routine.return_type_is_composite,
        isolation_level: routine.isolation_level,
    };

    // Resolve parameters from the request body
    let params = resolve_call_params(routine, &request.body)?;

    // For a table/composite-returning function whose result type is a known
    // table, we can resolve select/filters/order against its columns. Otherwise
    // projection and filtering can't be validated, so they're left empty.
    let return_table = if routine.return_type_is_composite {
        // `return_type` may be schema-qualified (`schema.type`) when the composite
        // type lives outside pg_catalog; fall back to the request schema otherwise.
        let (rt_schema, rt_name) = match routine.return_type.split_once('.') {
            Some((schema, name)) => (schema, name),
            None => (request.schema.as_str(), routine.return_type.as_str()),
        };
        cache.find_table(rt_schema, rt_name)
    } else {
        None
    };

    let returning = match (return_table, request.select.is_empty()) {
        (Some(return_table), false) => {
            let (selects, _embeds) = resolve::resolve_select_items(
                cache,
                return_table,
                &request.schema,
                &request.select,
                dialect,
                config,
            )?;
            selects
        }
        _ => vec![ResolvedSelect::Star],
    };

    // Resolve result-level filters/order/range for set/table-returning functions.
    let (filters, logic_filters, order, range) = if let Some(return_table) = return_table {
        let filters = resolve::resolve_filters(return_table, &request.filters, dialect)?;
        let logic_filters = request
            .logic_filters
            .iter()
            .map(|lt| resolve::resolve_logic_tree(return_table, lt, dialect))
            .collect::<Result<Vec<_>, _>>()?;
        let order = resolve::resolve_order(return_table, &request.order)?;
        let range = resolve::resolve_range(&request.range, config.max_rows);
        (filters, logic_filters, order, range)
    } else {
        (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            resolve::resolve_range(&None, config.max_rows),
        )
    };

    let is_singular = !routine.return_type_is_set;

    Ok(ActionPlan::Call(CallPlan {
        function: QualifiedIdentifier::new(&request.schema, &request.target),
        function_info,
        params,
        returning,
        filters,
        logic_filters,
        order,
        range,
        is_singular,
        preferences: request.preferences.clone(),
        body: request.body.clone(),
    }))
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Determine the count strategy from preferences.
fn resolve_count_strategy(prefs: &Preferences) -> Option<CountStrategy> {
    prefs.count.map(|c| match c {
        PreferCount::Exact => CountStrategy::Exact,
        PreferCount::Planned => CountStrategy::Planned,
        PreferCount::Estimated => CountStrategy::Estimated,
    })
}

/// Extract column names from the request body.
fn extract_payload_columns(body: &Option<RequestBody>) -> Vec<String> {
    match body {
        Some(RequestBody::Single(obj)) => obj
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default(),
        Some(RequestBody::Bulk(arr)) => {
            // Union of all keys across all objects. A heterogeneous bulk insert
            // may introduce new columns in any object, so every object must be
            // scanned (an early break would silently drop later columns).
            let mut cols = indexmap::IndexSet::new();
            for obj in arr {
                if let Some(map) = obj.as_object() {
                    for key in map.keys() {
                        cols.insert(key.clone());
                    }
                }
            }
            cols.into_iter().collect()
        }
        Some(RequestBody::Raw(_)) => Vec::new(),
        None => Vec::new(),
    }
}

/// Postgres accepts at most this many bind parameters in one statement.
const MAX_BIND_PARAMS: usize = 65_535;

/// Reject a body a mutation can't faithfully apply, before anything is rendered.
///
/// Without this, unknown keys reached the renderer (multiplied across a bulk
/// insert, enough to exhaust memory before the database saw a thing, even
/// for a role without INSERT), a non-object or empty body inserted a row of
/// defaults, an oversized bulk insert failed with a 500, and `PATCH {}`
/// rendered `SET` with nothing after it.
fn validate_payload(
    method: RequestMethod,
    body: &Option<RequestBody>,
    table: &crate::cache::Table,
    columns: &[String],
    dialect: &Dialect,
) -> Result<(), Error> {
    let rows: Vec<&serde_json::Value> = match body {
        Some(RequestBody::Single(v)) => vec![v],
        Some(RequestBody::Bulk(rows)) => rows.iter().collect(),
        Some(RequestBody::Raw(_)) | None => return Ok(()),
    };
    if rows.iter().any(|v| !v.is_object()) {
        return Err(Error::invalid_body(
            "the body must be a JSON object or an array of objects",
        ));
    }
    for column in columns {
        resolve::resolve_column(table, column)?;
    }
    let is_bulk = matches!(body, Some(RequestBody::Bulk(_)));
    if method == RequestMethod::Post {
        if is_bulk && columns.is_empty() {
            return Err(Error::invalid_body("nothing to insert: no rows with columns"));
        }
        // One parameter per value (no JSON-recordset insert): Postgres's limit.
        if !dialect.supports_json_recordset
            && rows.len().saturating_mul(columns.len()) > MAX_BIND_PARAMS
        {
            return Err(Error::invalid_body(format!(
                "too many values for one insert (rows x columns over {MAX_BIND_PARAMS}); \
                 split the request"
            )));
        }
    } else {
        if is_bulk {
            return Err(Error::invalid_body("an update takes one object, not an array"));
        }
        if columns.is_empty() {
            return Err(Error::invalid_body("nothing to update: the body has no columns"));
        }
    }
    Ok(())
}

/// The argument names a call supplies: the keys of a single-object body.
fn call_arg_names(body: &Option<RequestBody>) -> HashSet<String> {
    match body {
        Some(RequestBody::Single(obj)) => obj
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default(),
        _ => HashSet::new(),
    }
}

/// Pick the overload a call's argument names select (PostgREST semantics).
///
/// A candidate must declare every supplied argument name and receive every
/// parameter that has no default. None → `PGRST202` (404), so an unknown or
/// misspelled argument is an error rather than silently dropped. More than one
/// → `PGRST203` (300).
fn resolve_overload<'a>(
    routines: &'a [crate::cache::Routine],
    request: &ApiRequest,
) -> Result<&'a crate::cache::Routine, Error> {
    let args = call_arg_names(&request.body);
    let candidates: Vec<&crate::cache::Routine> = routines
        .iter()
        .filter(|r| {
            args.iter()
                .all(|a| r.params.iter().any(|p| !p.name.is_empty() && p.name == *a))
                && r.params
                    .iter()
                    .all(|p| !p.required || args.contains(&p.name))
        })
        .collect();

    let function = format!("{}.{}", request.schema, request.target);
    match candidates.as_slice() {
        [routine] => Ok(routine),
        [] => {
            let mut names: Vec<&str> = args.iter().map(String::as_str).collect();
            names.sort_unstable();
            Err(Error::Plan {
                message: format!(
                    "Could not find the function {function}({}) in the schema cache",
                    names.join(", ")
                ),
                detail: Some(format!(
                    "Searched for the function {function} with parameters [{}], but no \
                     matches were found in the schema cache",
                    names.join(", ")
                )),
                hint: None,
                code: ErrorCode::FunctionNotFound,
            })
        }
        _ => {
            let signatures: Vec<String> = candidates
                .iter()
                .map(|r| {
                    let params: Vec<String> = r
                        .params
                        .iter()
                        .map(|p| format!("{} => {}", p.name, p.typ))
                        .collect();
                    format!("{function}({})", params.join(", "))
                })
                .collect();
            Err(Error::Plan {
                message: format!(
                    "Could not choose the best candidate function between: {}",
                    signatures.join(", ")
                ),
                detail: None,
                hint: Some(
                    "Try renaming the parameters or the function itself in the database so \
                     function overloading can be resolved"
                        .to_string(),
                ),
                code: ErrorCode::AmbiguousFunction,
            })
        }
    }
}

/// Resolve RPC call parameters from the routine signature and request body.
fn resolve_call_params(
    routine: &crate::cache::Routine,
    body: &Option<RequestBody>,
) -> Result<Vec<ResolvedParam>, Error> {
    let body_keys = call_arg_names(body);

    Ok(routine
        .params
        .iter()
        .map(|p| ResolvedParam {
            name: p.name.clone(),
            param_type: p.typ.clone(),
            has_value: body_keys.contains(&p.name),
            is_variadic: p.is_variadic,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{Routine, RoutineParam, Volatility};
    use crate::dialect::POSTGRES;
    use serde_json::json;

    fn param(name: &str, typ: &str, required: bool, is_variadic: bool) -> RoutineParam {
        RoutineParam {
            name: name.to_string(),
            typ: typ.to_string(),
            required,
            is_variadic,
        }
    }

    fn routine(name: &str, params: Vec<RoutineParam>, volatility: Volatility) -> Routine {
        Routine {
            ident: QualifiedIdentifier::new("public", name),
            description: None,
            is_variadic: params.iter().any(|p| p.is_variadic),
            params,
            return_type: "integer".to_string(),
            return_type_is_set: false,
            return_type_is_composite: false,
            volatility,
            isolation_level: None,
            settings: Vec::new(),
        }
    }

    fn cache_with(routines: Vec<Routine>) -> SchemaCache {
        let mut cache = SchemaCache::default();
        for r in routines {
            cache.routines.entry(r.ident.clone()).or_default().push(r);
        }
        cache
    }

    fn call(name: &str, args: serde_json::Value) -> ApiRequest {
        ApiRequest {
            schema: "public".to_string(),
            target: name.to_string(),
            method: RequestMethod::Post,
            is_rpc: true,
            select: vec![SelectItem::Star],
            filters: Vec::new(),
            order: Vec::new(),
            range: None,
            preferences: Preferences::default(),
            body: Some(RequestBody::Single(args)),
            on_conflict: None,
            columns: None,
            logic_filters: Vec::new(),
            cursor: None,
        }
    }

    /// Plan and render the call, returning its SQL and the chosen volatility.
    fn plan_sql(cache: &SchemaCache, req: &ApiRequest) -> Result<(String, Volatility), Error> {
        let ActionPlan::Call(plan) = plan_request(req, cache, &POSTGRES, &Config::default())?
        else {
            panic!("expected a call plan");
        };
        let mut ctx = crate::query::RenderContext::new(&POSTGRES);
        let sql = crate::query::call::render_call(&plan, &mut ctx)?;
        Ok((sql, plan.function_info.volatility))
    }

    /// `f(a)` (stable) and `f(a, b)` (volatile).
    fn overloaded() -> SchemaCache {
        cache_with(vec![
            routine(
                "f",
                vec![param("a", "integer", true, false)],
                Volatility::Stable,
            ),
            routine(
                "f",
                vec![
                    param("a", "integer", true, false),
                    param("b", "integer", true, false),
                ],
                Volatility::Volatile,
            ),
        ])
    }

    #[test]
    fn overload_matching_all_keys_is_chosen() {
        let cache = overloaded();
        let (sql, volatility) = plan_sql(&cache, &call("f", json!({"a": 1, "b": 2}))).unwrap();
        assert_eq!(
            sql,
            "SELECT \"public\".\"f\"(\"a\" := $1, \"b\" := $2) AS result"
        );
        // Metadata comes from the chosen overload, not the first one.
        assert_eq!(volatility, Volatility::Volatile);

        let (sql, volatility) = plan_sql(&cache, &call("f", json!({"a": 1}))).unwrap();
        assert_eq!(sql, "SELECT \"public\".\"f\"(\"a\" := $1) AS result");
        assert_eq!(volatility, Volatility::Stable);
    }

    #[test]
    fn missing_required_param_is_function_not_found() {
        let cache = overloaded();
        let err = plan_sql(&cache, &call("f", json!({"b": 2}))).unwrap_err();
        assert_eq!(err.code(), ErrorCode::FunctionNotFound);
        assert_eq!(err.code().as_str(), "PGRST202");
        assert_eq!(err.http_status(), 404);
    }

    #[test]
    fn unknown_key_is_function_not_found() {
        let cache = cache_with(vec![routine(
            "g",
            vec![param("user_id", "integer", false, false)],
            Volatility::Stable,
        )]);
        let err = plan_sql(&cache, &call("g", json!({"user_idd": 5}))).unwrap_err();
        assert_eq!(err.code(), ErrorCode::FunctionNotFound);

        // An unknown function is the same error.
        let err = plan_sql(&cache, &call("nope", json!({}))).unwrap_err();
        assert_eq!(err.code(), ErrorCode::FunctionNotFound);
    }

    #[test]
    fn several_matching_overloads_are_ambiguous() {
        // h(a) and h(a, b DEFAULT ...) both accept {"a": 1}.
        let cache = cache_with(vec![
            routine(
                "h",
                vec![param("a", "integer", true, false)],
                Volatility::Stable,
            ),
            routine(
                "h",
                vec![
                    param("a", "integer", true, false),
                    param("b", "integer", false, false),
                ],
                Volatility::Stable,
            ),
        ]);
        let err = plan_sql(&cache, &call("h", json!({"a": 1}))).unwrap_err();
        assert_eq!(err.code(), ErrorCode::AmbiguousFunction);
        assert_eq!(err.http_status(), 300);
    }

    #[test]
    fn single_function_with_defaults_unchanged() {
        let cache = cache_with(vec![routine(
            "echo",
            vec![
                param("name", "text", false, false),
                param("greeting", "text", false, false),
            ],
            Volatility::Stable,
        )]);
        let (sql, _) = plan_sql(&cache, &call("echo", json!({}))).unwrap();
        assert_eq!(sql, "SELECT \"public\".\"echo\"() AS result");
        let (sql, _) = plan_sql(&cache, &call("echo", json!({"greeting": "hi"}))).unwrap();
        assert_eq!(
            sql,
            "SELECT \"public\".\"echo\"(\"greeting\" := $1) AS result"
        );
    }

    #[test]
    fn named_variadic_argument_is_marked() {
        let cache = cache_with(vec![routine(
            "sum_all",
            vec![
                param("label", "text", true, false),
                param("nums", "integer[]", true, true),
            ],
            Volatility::Immutable,
        )]);
        let req = call("sum_all", json!({"label": "s", "nums": "{1,2,3}"}));
        let (sql, _) = plan_sql(&cache, &req).unwrap();
        assert_eq!(
            sql,
            "SELECT \"public\".\"sum_all\"(\"label\" := $1, VARIADIC \"nums\" := $2) AS result"
        );
    }
}
