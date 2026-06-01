//! # The planner — entry point for `plan_request()`.
//!
//! Transforms an `ApiRequest` + `SchemaCache` + `Dialect` + `Config` into
//! a fully-resolved `ActionPlan`.

use std::collections::HashSet;

use crate::cache::{QualifiedIdentifier, SchemaCache};
use crate::config::Config;
use crate::dialect::Dialect;
use crate::error::Error;
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
    let range = resolve::resolve_range(&request.range, config.max_rows);
    let count = resolve_count_strategy(&request.preferences);

    // Determine mutation type
    let mutation = match request.method {
        RequestMethod::Post => {
            let payload_columns = extract_payload_columns(&request.body);
            let is_bulk = matches!(&request.body, Some(RequestBody::Bulk(_)));
            let on_conflict = request.on_conflict.as_ref().map(|col| {
                let columns: Vec<String> = col.split(',').map(|s| s.trim().to_string()).collect();
                let resolution = match request.preferences.resolution {
                    Some(crate::preferences::PreferResolution::IgnoreDuplicates) => {
                        ConflictResolution::IgnoreDuplicates
                    }
                    _ => ConflictResolution::MergeDuplicates,
                };
                ResolvedConflict {
                    columns,
                    resolution,
                }
            });
            MutationType::Insert {
                payload_columns,
                is_bulk,
                on_conflict,
            }
        }
        RequestMethod::Patch | RequestMethod::Put => {
            let payload_columns = extract_payload_columns(&request.body);
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
        .ok_or_else(|| {
            Error::not_found(format!("function {}.{}", request.schema, request.target))
        })?;

    // For now, take the first matching routine
    // TODO: Implement overload resolution (scoring algorithm)
    let routine = routines.first().ok_or_else(|| {
        Error::not_found(format!("function {}.{}", request.schema, request.target))
    })?;

    let function_info = ResolvedFunctionInfo {
        volatility: routine.volatility,
        return_type: routine.return_type.clone(),
        returns_set: routine.return_type_is_set,
        returns_table: routine.return_type_is_composite,
        isolation_level: routine.isolation_level,
    };

    // Resolve parameters from the request body
    let params = resolve_call_params(routine, &request.body)?;

    // Resolve returning columns from select parameter
    let returning = if routine.return_type_is_composite && !request.select.is_empty() {
        // For table-valued functions, resolve select columns against the return type
        if let Some(return_table) = cache.find_table(&request.schema, &routine.return_type) {
            let (selects, _embeds) = resolve::resolve_select_items(
                cache,
                return_table,
                &request.schema,
                &request.select,
                dialect,
                config,
            )?;
            selects
        } else {
            // Can't resolve columns — just pass star
            vec![ResolvedSelect::Star]
        }
    } else if request.select.is_empty() {
        vec![ResolvedSelect::Star]
    } else {
        // Non-composite return: can't validate columns, pass through as star
        vec![ResolvedSelect::Star]
    };

    let is_singular = !routine.return_type_is_set;

    Ok(ActionPlan::Call(CallPlan {
        function: QualifiedIdentifier::new(&request.schema, &request.target),
        function_info,
        params,
        returning,
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
            // Union of all keys across all objects.
            // Optimisation: most bulk inserts have uniform shape, so once we've
            // seen all keys from the first object we can skip subsequent objects
            // that don't introduce new columns.
            let mut cols = indexmap::IndexSet::new();
            for obj in arr {
                if let Some(map) = obj.as_object() {
                    let prev_len = cols.len();
                    for key in map.keys() {
                        cols.insert(key.clone());
                    }
                    // If this object didn't add any new columns and we already
                    // have at least one object's worth, subsequent uniform
                    // objects won't either — skip the rest.
                    if cols.len() == prev_len && prev_len > 0 {
                        break;
                    }
                }
            }
            cols.into_iter().collect()
        }
        Some(RequestBody::Raw(_)) => Vec::new(),
        None => Vec::new(),
    }
}

/// Resolve RPC call parameters from the routine signature and request body.
fn resolve_call_params(
    routine: &crate::cache::Routine,
    body: &Option<RequestBody>,
) -> Result<Vec<ResolvedParam>, Error> {
    let body_keys: HashSet<String> = match body {
        Some(RequestBody::Single(obj)) => obj
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default(),
        _ => HashSet::new(),
    };

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
