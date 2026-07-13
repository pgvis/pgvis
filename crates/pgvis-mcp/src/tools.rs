//! MCP tool generation and execution.
//!
//! [`build_mcp_tools`] generates tool definitions from the SchemaCache (parallel to REST's `build_app`).
//! [`handle_tool_call`] executes a tool call through the same `plan_request()` → SQL → execute pipeline as REST.

use pgvis_core::backend::{Backend, ExecContext};
use pgvis_core::cache::{Routine, Table, Volatility};
use pgvis_core::config::RoutingConfig;
use pgvis_core::plan::{ActionPlan, ApiRequest, RequestBody, RequestMethod, plan_request};
use pgvis_core::preferences::{PreferCount, PreferResolution, PreferReturn, Preferences};
use pgvis_core::query;
use pgvis_core::query_params;
use pgvis_core::query_params::OrderItem;
use pgvis_core::query_params::types::{Filter, LogicNode, LogicTree, OrderTerm, RangeSpec};
use pgvis_core::select_ast::SelectItem;
use pgvis_core::{Config, Dialect, SchemaCache};

use crate::types::*;

/// Keys the tool handler interprets for every verb. When building the argument
/// map for an RPC `call`, these must NOT be forwarded as function arguments —
/// they control the request itself (selection, filtering, paging, preferences).
const RPC_RESERVED_KEYS: &[&str] = &[
    "select",
    "filters",
    "order",
    "limit",
    "offset",
    "count",
    "and",
    "or",
    "not.and",
    "not.or",
    "on_conflict",
    "return",
    "resolution",
];

// ---------------------------------------------------------------------------
// build_mcp_tools — parallel to build_app()
// ---------------------------------------------------------------------------

/// Generate MCP tool definitions from the SchemaCache.
///
/// This is the MCP equivalent of `pgvis_rest::build_app()`. Both consume
/// the same SchemaCache + Config and produce their respective surfaces.
pub fn build_mcp_tools(cache: &SchemaCache, config: &Config) -> Vec<McpToolDefinition> {
    let routing = &config.routing;
    let mut tools = Vec::new();

    for schema in &config.schemas {
        // Table CRUD tools
        for (_ident, table) in &cache.tables {
            if table.schema() != schema {
                continue;
            }

            // Always add list (read) tool
            tools.push(make_list_tool(routing, schema, table));

            // Skip mutation tools entirely in read-only mode. They would only
            // be rejected at call time anyway; omitting them keeps the tool
            // catalogue honest and prevents the model from even attempting a
            // write that will fail.
            if config.read_only {
                continue;
            }

            // Add create tool if insertable
            if table.insertable {
                tools.push(make_create_tool(routing, schema, table));
            }

            // Add update tool if updatable
            if table.updatable {
                tools.push(make_update_tool(routing, schema, table));
            }

            // Add delete tool if deletable
            if table.deletable {
                tools.push(make_delete_tool(routing, schema, table));
            }
        }

        // RPC tools from routines. RPCs may have side effects we can't see
        // from the catalogue, so under read_only we drop them entirely too;
        // exposing them while disallowing mutations would be misleading.
        if !config.read_only {
            for (_ident, routine_group) in &cache.routines {
                for routine in routine_group {
                    if routine.ident.schema == *schema {
                        tools.push(make_call_tool(routing, schema, routine));
                    }
                }
            }
        }
    }

    tools
}

// ---------------------------------------------------------------------------
// build_mcp_resources — schema discovery
// ---------------------------------------------------------------------------

/// Generate MCP resources for schema discovery.
///
/// Resources give LLMs awareness of the database structure before invoking tools.
pub fn build_mcp_resources(cache: &SchemaCache, config: &Config) -> Vec<McpResource> {
    let mut resources = vec![McpResource {
        uri: "pgvis://schemas".to_string(),
        name: "Available schemas".to_string(),
        description: format!(
            "List of database schemas exposed by this server: {}",
            config.schemas.join(", ")
        ),
        mime_type: Some("application/json".to_string()),
    }];

    for schema in &config.schemas {
        // Per-schema resource
        let table_count = cache
            .tables
            .values()
            .filter(|t| t.schema() == schema)
            .count();
        let routine_count: usize = cache
            .routines
            .values()
            .flat_map(|g| g.iter())
            .filter(|r| r.ident.schema == *schema)
            .count();

        resources.push(McpResource {
            uri: format!("pgvis://{schema}/schema"),
            name: format!("{schema} schema"),
            description: format!(
                "{} tables/views, {} functions in the {schema} schema",
                table_count, routine_count
            ),
            mime_type: Some("application/json".to_string()),
        });

        // Per-table resources
        for table in cache.tables.values().filter(|t| t.schema() == schema) {
            let col_names: Vec<&str> = table
                .columns
                .values()
                .take(5)
                .map(|c| c.name.as_str())
                .collect();

            resources.push(McpResource {
                uri: format!("pgvis://{schema}/{}/columns", table.name()),
                name: format!("{schema}.{}", table.name()),
                description: format!(
                    "{} with {} columns ({})",
                    if table.is_view { "View" } else { "Table" },
                    table.columns.len(),
                    col_names.join(", ")
                ),
                mime_type: Some("application/json".to_string()),
            });
        }
    }

    resources
}

// ---------------------------------------------------------------------------
// handle_tool_call — execute a tool through the plan pipeline
// ---------------------------------------------------------------------------

/// Handle an MCP tool call by converting it to an ApiRequest and running the
/// full plan → render SQL → execute pipeline.
///
/// This is the MCP equivalent of the REST handler's dispatch logic. Both
/// convert their input format to `ApiRequest` and run through the same pipeline.
///
/// # Auth model
///
/// MCP tool calls always execute as [`Config::anon_role`]. Unlike the REST path
/// (which extracts and verifies a JWT from the `Authorization` header), the MCP
/// surface has no token-passing mechanism in the current protocol. For stdio
/// transport this is acceptable because the process itself is trusted (e.g.
/// Claude Desktop launches it). For Streamable HTTP deployments, consider
/// placing an auth proxy in front of the MCP endpoint or implementing
/// session-level token injection in a future protocol revision.
pub async fn handle_tool_call(
    call: &McpToolCall,
    cache: &SchemaCache,
    dialect: &Dialect,
    config: &Config,
    backend: &dyn Backend,
) -> McpToolResult {
    // 1. Parse tool name → schema + verb + target
    let (schema, verb, target) = match parse_tool_name(&call.name, &config.routing) {
        Ok(parsed) => parsed,
        Err(e) => {
            // No such tool → reuse the existing NotFound code (PGRST205)
            // rather than inventing a non-table code.
            return McpToolResult::error_structured(
                pgvis_core::error::ErrorCode::NotFound.as_str(),
                e,
                None,
                Some("Tool names follow `{schema}{sep}{verb}_{target}`.".to_string()),
            );
        }
    };

    // 2. Convert verb to RequestMethod. `parse_tool_name` only ever yields a
    // verb from `MCP_VERBS`, so this match is exhaustive in practice; the
    // fallthrough maps to NotFound for defence-in-depth.
    let method = match verb {
        "list" => RequestMethod::Get,
        "create" => RequestMethod::Post,
        "update" => RequestMethod::Patch,
        "delete" => RequestMethod::Delete,
        "call" => RequestMethod::Post,
        _ => {
            return McpToolResult::error_structured(
                pgvis_core::error::ErrorCode::NotFound.as_str(),
                format!("Unknown verb: {verb}"),
                None,
                None,
            );
        }
    };

    // Refuse mutations up front when the server is read-only. We also strip
    // these tools from the catalogue in `build_mcp_tools`, but a model may
    // have a stale tool list cached, so guard at call time too.
    if config.read_only && matches!(verb, "create" | "update" | "delete" | "call") {
        return McpToolResult::error_structured(
            "PGRST303",
            format!("MCP server is read-only; '{verb}' is not permitted"),
            None,
            Some("Restart without --read-only to enable mutations.".to_string()),
        );
    }

    // 3. Build ApiRequest from tool arguments
    let args = call.arguments.as_object();

    let select = args
        .and_then(|a| a.get("select"))
        .and_then(|v| v.as_str())
        .map(|s| parse_mcp_select(s))
        .unwrap_or_else(|| vec![SelectItem::Star]);

    let filters = match parse_mcp_filters(args) {
        Ok(filters) => filters,
        Err(e) => {
            // A malformed / unsupported filter operator is a hard parse error
            // (PGRST100), never a silent drop — see `parse_mcp_filters`.
            return McpToolResult::error_structured(
                pgvis_core::error::ErrorCode::InvalidFilter.as_str(),
                e,
                None,
                Some(
                    "Use PostgREST filter syntax, e.g. \"eq.5\", \"in.(1,2,3)\", \
                     \"is.notnull\", \"not.like.*foo*\"."
                        .to_string(),
                ),
            );
        }
    };

    let body = match verb {
        "create" => args.and_then(|a| a.get("rows")).map(|v| {
            if v.is_array() {
                RequestBody::Bulk(v.as_array().cloned().unwrap_or_default())
            } else {
                RequestBody::Single(v.clone())
            }
        }),
        "update" => args
            .and_then(|a| a.get("values"))
            .map(|v| RequestBody::Single(v.clone())),
        "call" => {
            // For RPC, all arguments except the keys the handler itself
            // interprets become the function's named arguments. The exclusion
            // set must cover EVERY key read below (count/return/resolution),
            // the logic-filter keys (and/or/not.and/not.or) and on_conflict —
            // otherwise a caller sending e.g. `{"count": true}` would have
            // `count` forwarded as a bogus function argument.
            let mut body_map = serde_json::Map::new();
            if let Some(a) = args {
                for (k, v) in a {
                    if !RPC_RESERVED_KEYS.contains(&k.as_str()) {
                        body_map.insert(k.clone(), v.clone());
                    }
                }
            }
            if body_map.is_empty() {
                None
            } else {
                Some(RequestBody::Single(serde_json::Value::Object(body_map)))
            }
        }
        _ => None,
    };

    let on_conflict = args
        .and_then(|a| a.get("on_conflict"))
        .and_then(|v| v.as_str())
        .map(String::from);

    let limit = args.and_then(|a| a.get("limit")).and_then(json_as_u64);
    let offset = args.and_then(|a| a.get("offset")).and_then(json_as_u64);

    let range = if limit.is_some() || offset.is_some() {
        Some(RangeSpec { limit, offset })
    } else {
        None
    };

    // Table verbs that write. RPC (`call`) is handled separately once the plan
    // resolves the function's volatility (see below) — a volatile function must
    // be routed as a write even though the verb alone doesn't say so.
    let is_table_mutation = matches!(verb, "create" | "update" | "delete");

    // Parse logic filters (MCP-17): support "or" and "and" arguments
    let logic_filters = parse_mcp_logic_filters(args);

    // Parse preferences (MCP-18 + MCP-20). return/resolution prefs only apply to
    // table mutations, so gate on that (not on RPC volatility).
    let preferences = parse_mcp_preferences(args, is_table_mutation);

    // A malformed order is a hard parse error (PGRST100), matching REST, rather
    // than silently defaulting an unknown direction to asc.
    let order = match parse_mcp_order(args) {
        Ok(order) => order,
        Err(e) => {
            return McpToolResult::error_structured(
                pgvis_core::error::ErrorCode::InvalidOrder.as_str(),
                e,
                None,
                Some("Use PostgREST order syntax, e.g. \"name.asc\", \"age.desc.nullslast\".".to_string()),
            );
        }
    };

    let api_request = ApiRequest {
        schema: schema.to_string(),
        target: target.to_string(),
        method,
        is_rpc: verb == "call",
        select,
        filters,
        order,
        range,
        preferences,
        body,
        on_conflict,
        columns: None,
        logic_filters,
        cursor: None,
    };

    // 4. Plan the request (same pipeline as REST)
    let plan = match plan_request(&api_request, cache, dialect, config) {
        Ok(plan) => plan,
        Err(err) => return McpToolResult::from_core_error(&err),
    };

    // Routing flag for read-replica selection. For table verbs this is the verb;
    // for RPC we must consult the resolved function's volatility — a Volatile
    // function may write and must not be routed to a read replica. If the plan
    // isn't a Call (shouldn't happen for `call`), fall back to conservative
    // `true` so an RPC is never mis-routed as a read.
    let is_mutation = match &plan {
        ActionPlan::Call(call_plan) => {
            matches!(call_plan.function_info.volatility, Volatility::Volatile)
        }
        // A `call` that somehow didn't resolve to a Call plan: be conservative
        // and treat as a write so it stays on the primary.
        _ if verb == "call" => true,
        _ => is_table_mutation,
    };

    // For Inspect plans, return metadata directly
    if let ActionPlan::Inspect(_) = &plan {
        return McpToolResult::success(serde_json::json!({
            "status": "inspect",
            "message": "Schema inspection is available via MCP resources (pgvis://schemas)"
        }));
    }

    // 5. Render the plan to SQL + parameters
    let render_result = if dialect.supports_set_local {
        // Postgres path: CTE-wrapped SQL
        query::render(&plan, dialect)
    } else {
        // SQLite path: render without CTE wrapping
        query::render_inner(&plan, dialect)
    };
    let (sql, params) = match render_result {
        Ok(rendered) => rendered,
        Err(err) => return McpToolResult::from_core_error(&err),
    };

    // 6. Resolve the execution role and refuse anonymous access when the
    //    deployment requires auth.
    //
    // The MCP surface currently has no token-passing / CallerIdentity
    // mechanism (see the fn-level "Auth model" doc), so every call is
    // anonymous. If a `jwt_secret` is configured the operator has opted into
    // auth; when there is ALSO no `anon_role`, REST rejects anonymous requests
    // (PGRST300/302) rather than running as the pool's connection role. MCP
    // must do the same — otherwise it would execute as the DSN role (often the
    // table owner, bypassing RLS), a privilege escalation. Mirror REST and
    // refuse with the anonymous-access-disabled code (PGRST302).
    if config.jwt_secret.is_some() && config.anon_role.is_none() {
        return McpToolResult::error_structured(
            pgvis_core::error::ErrorCode::JwtMissing.as_str(),
            "Anonymous access is disabled: this server requires authentication \
             (jwt_secret is set) and no anon_role is configured.",
            None,
            Some(
                "Configure an anon_role to allow unauthenticated MCP tool calls, \
                 or place an auth proxy in front of the MCP endpoint."
                    .to_string(),
            ),
        );
    }

    // 7. Build ExecContext. Role is the configured anon_role (may be None when
    // no jwt_secret is set — the guard above ensures we never fall through to
    // the connection role while auth is required).
    let exec_ctx = ExecContext {
        role: config.anon_role.clone(),
        claims: None,
        pre_request: config.pre_request.clone(),
        statement_timeout: config.statement_timeout_ms,
        tx_end: None,
        is_mutation,
    };

    // 8. Execute via backend, bounded by a per-call deadline.
    //
    // `statement_timeout_ms` is what Postgres uses to abort the SQL itself
    // (`SET LOCAL statement_timeout`). We additionally wrap the future in
    // `tokio::time::timeout` so that backends that can't honour a SQL-level
    // timeout (e.g. SQLite, or a Postgres connection stuck in TLS handshake)
    // still bound the MCP tool call. We allow a small grace window past the
    // SQL timeout so the database's own timeout error can win when both
    // would fire near-simultaneously.
    let exec_fut = backend.execute(&exec_ctx, &sql, &params);
    let exec_result = match config.statement_timeout_ms {
        Some(ms) if ms > 0 => {
            let deadline = std::time::Duration::from_millis(ms.saturating_add(1_000));
            match tokio::time::timeout(deadline, exec_fut).await {
                Ok(res) => res,
                Err(_) => {
                    return McpToolResult::error_structured(
                        "PGRST401",
                        format!("MCP tool call exceeded {ms}ms statement timeout"),
                        None,
                        Some(
                            "Tighten the filter, lower `limit`, or raise \
                             statement_timeout_ms in the config."
                                .to_string(),
                        ),
                    );
                }
            }
        }
        _ => exec_fut.await,
    };

    match exec_result {
        Ok(result) => {
            // If count was requested, return structured response with total
            let count_requested = args
                .and_then(|a| a.get("count"))
                .and_then(json_as_bool)
                .unwrap_or(false);

            if count_requested {
                let response = serde_json::json!({
                    "rows": result.body,
                    "total": result.total_count,
                    "page_total": result.page_total,
                });
                let body_str = serde_json::to_string_pretty(&response)
                    .unwrap_or_else(|_| response.to_string());
                McpToolResult::success_text(body_str)
            } else {
                let body_str = serde_json::to_string_pretty(&result.body)
                    .unwrap_or_else(|_| result.body.to_string());
                McpToolResult::success_text(body_str)
            }
        }
        Err(err) => McpToolResult::from_core_error(&err),
    }
}

// ---------------------------------------------------------------------------
// Individual tool builders
// ---------------------------------------------------------------------------

fn make_list_tool(routing: &RoutingConfig, schema: &str, table: &Table) -> McpToolDefinition {
    let name = mcp_tool_name(routing, schema, "list", table.name());
    let description = format!(
        "List rows from {}.{} with filtering, ordering, and embedding",
        schema,
        table.name()
    );

    let mut properties = serde_json::Map::new();
    properties.insert(
        "select".to_string(),
        serde_json::json!({
            "type": "string",
            "description": "Comma-separated columns to return. Supports embedding: 'id,name,posts(title)'"
        }),
    );
    properties.insert(
        "filters".to_string(),
        serde_json::json!({
            "type": "object",
            "description": "Column filters as key-value pairs. Values use operator syntax: 'eq.5', 'gt.10', 'like.*foo*'",
            "additionalProperties": { "type": "string" }
        }),
    );
    properties.insert(
        "order".to_string(),
        serde_json::json!({
            "type": "string",
            "description": "Ordering: 'column.asc', 'column.desc.nullsfirst'"
        }),
    );
    properties.insert(
        "limit".to_string(),
        serde_json::json!({
            "type": "integer",
            "description": "Max rows to return"
        }),
    );
    properties.insert(
        "offset".to_string(),
        serde_json::json!({
            "type": "integer",
            "description": "Rows to skip"
        }),
    );
    properties.insert(
        "or".to_string(),
        serde_json::json!({
            "type": "string",
            "description": "OR logic filter using PostgREST syntax: '(col.eq.val1,col.eq.val2)'"
        }),
    );
    properties.insert(
        "and".to_string(),
        serde_json::json!({
            "type": "string",
            "description": "AND logic filter using PostgREST syntax: '(col.gte.1,col.lte.10)'"
        }),
    );
    properties.insert(
        "count".to_string(),
        serde_json::json!({
            "type": "boolean",
            "description": "If true, returns total count of matching rows alongside the data"
        }),
    );

    McpToolDefinition {
        name,
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": properties,
        }),
    }
}

fn make_create_tool(routing: &RoutingConfig, schema: &str, table: &Table) -> McpToolDefinition {
    let name = mcp_tool_name(routing, schema, "create", table.name());
    let description = format!("Insert rows into {}.{}", schema, table.name());

    // Build column descriptions from the table's columns
    let column_desc: Vec<String> = table
        .columns
        .values()
        .filter(|c| !c.is_generated)
        .map(|c| {
            format!(
                "{}: {} {}",
                c.name,
                c.typ,
                if c.nullable { "(nullable)" } else { "" }
            )
        })
        .collect();

    McpToolDefinition {
        name,
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "rows": {
                    "oneOf": [
                        { "type": "object", "description": "Single row to insert" },
                        { "type": "array", "items": { "type": "object" }, "description": "Multiple rows" }
                    ]
                },
                "select": {
                    "type": "string",
                    "description": "Columns to return from inserted rows"
                },
                "on_conflict": {
                    "type": "string",
                    "description": "Upsert resolution column"
                },
                "return": {
                    "type": "string",
                    "enum": ["representation", "minimal"],
                    "description": "Whether to return the affected rows (representation) or nothing (minimal)"
                },
                "resolution": {
                    "type": "string",
                    "enum": ["merge-duplicates", "ignore-duplicates"],
                    "description": "Conflict resolution strategy for upsert"
                },
            },
            "required": ["rows"],
            "description": format!("Columns: {}", column_desc.join(", ")),
        }),
    }
}

fn make_update_tool(routing: &RoutingConfig, schema: &str, table: &Table) -> McpToolDefinition {
    let name = mcp_tool_name(routing, schema, "update", table.name());
    let description = format!(
        "Update rows in {}.{} matching filter conditions",
        schema,
        table.name()
    );

    McpToolDefinition {
        name,
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "values": {
                    "type": "object",
                    "description": "Column values to update"
                },
                "filters": {
                    "type": "object",
                    "description": "Column filters to match rows. Values use operator syntax: 'eq.5'",
                    "additionalProperties": { "type": "string" }
                },
                "select": {
                    "type": "string",
                    "description": "Columns to return from updated rows"
                },
                "return": {
                    "type": "string",
                    "enum": ["representation", "minimal"],
                    "description": "Whether to return the affected rows (representation) or nothing (minimal)"
                },
            },
            "required": ["values", "filters"],
        }),
    }
}

fn make_delete_tool(routing: &RoutingConfig, schema: &str, table: &Table) -> McpToolDefinition {
    let name = mcp_tool_name(routing, schema, "delete", table.name());
    let description = format!(
        "Delete rows from {}.{} matching filter conditions",
        schema,
        table.name()
    );

    McpToolDefinition {
        name,
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "filters": {
                    "type": "object",
                    "description": "Column filters to match rows for deletion. Values use operator syntax: 'eq.5'",
                    "additionalProperties": { "type": "string" }
                },
                "select": {
                    "type": "string",
                    "description": "Columns to return from deleted rows"
                },
                "return": {
                    "type": "string",
                    "enum": ["representation", "minimal"],
                    "description": "Whether to return the affected rows (representation) or nothing (minimal)"
                },
            },
            "required": ["filters"],
        }),
    }
}

fn make_call_tool(routing: &RoutingConfig, schema: &str, routine: &Routine) -> McpToolDefinition {
    let name = mcp_tool_name(routing, schema, "call", &routine.ident.name);

    // Build parameter description from routine params
    let param_desc: Vec<String> = routine
        .params
        .iter()
        .map(|p| {
            format!(
                "{}: {}{}",
                p.name,
                p.typ,
                if p.is_variadic { " (variadic)" } else { "" }
            )
        })
        .collect();
    let description = format!(
        "Call function {}.{}({}) → {}",
        schema,
        routine.ident.name,
        param_desc.join(", "),
        routine.return_type,
    );

    // Build input schema from routine parameters
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for param in &routine.params {
        properties.insert(
            param.name.clone(),
            serde_json::json!({
                "type": pg_type_to_json_type(&param.typ),
                "description": format!("Parameter: {} ({})", param.name, param.typ),
            }),
        );
        if param.required {
            required.push(serde_json::Value::String(param.name.clone()));
        }
    }

    McpToolDefinition {
        name,
        description,
        input_schema: serde_json::json!({
            "type": "object",
            "properties": properties,
            "required": required,
        }),
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// The verbs an MCP tool name can encode. Single source of truth so name
/// generation and parsing agree, and so [`parse_tool_name`] can anchor on the
/// verb to disambiguate (see below).
const MCP_VERBS: &[&str] = &["list", "create", "update", "delete", "call"];

/// The separator used *inside* generated tool names.
///
/// The MCP spec restricts tool names to `^[a-zA-Z0-9_-]{1,128}$`. pgvis-core
/// defaults `routing.mcp_separator` to `'/'`, which is **not** allowed — a name
/// like `public/list_users` would be rejected by strict MCP clients. We cannot
/// change the core default, so within pgvis-mcp we honour the configured
/// separator only when it is itself MCP-safe (ASCII alphanumeric, `_`, or `-`);
/// otherwise we fall back to `-`. Generation ([`mcp_tool_name`]) and parsing
/// ([`parse_tool_name`]) both go through this helper, so names round-trip.
fn safe_separator(routing: &RoutingConfig) -> char {
    let sep = routing.mcp_separator;
    if sep.is_ascii_alphanumeric() || sep == '_' || sep == '-' {
        sep
    } else {
        '-'
    }
}

/// Build an MCP-safe tool name of the form `{schema}{sep}{verb}_{target}`
/// (or `{verb}_{target}` when the schema is elided — mirroring core's
/// `RoutingConfig::mcp_tool_name`). Uses [`safe_separator`] so the emitted name
/// always satisfies the MCP `^[a-zA-Z0-9_-]{1,128}$` pattern even though core's
/// default separator (`'/'`) does not.
fn mcp_tool_name(routing: &RoutingConfig, schema: &str, verb: &str, target: &str) -> String {
    let sep = safe_separator(routing);
    if routing.schema_in_path || schema != routing.default_schema {
        format!("{schema}{sep}{verb}_{target}")
    } else {
        format!("{verb}_{target}")
    }
}

/// Parse a tool name into (schema, verb, target).
///
/// Inverse of [`mcp_tool_name`]. Names are `{schema}{sep}{verb}_{target}` or
/// `{verb}_{target}`. Because a schema name may itself contain the separator
/// char, we cannot naively split on the first separator (that mis-parses
/// `my_sep_schema-list_users`). Instead we anchor on the verb: the verb is
/// always one of [`MCP_VERBS`] and always immediately precedes the first `_` of
/// the `{verb}_{target}` suffix. We scan for `"{sep}{verb}_"` (schema-qualified)
/// or a leading `"{verb}_"` (default schema), which disambiguates even when the
/// schema contains the separator. Unknown verbs / malformed names return a
/// clear error rather than mis-routing.
fn parse_tool_name<'a>(
    name: &'a str,
    routing: &'a RoutingConfig,
) -> Result<(&'a str, &'a str, &'a str), String> {
    let sep = safe_separator(routing);
    let mut sep_buf = [0u8; 4];
    let sep_str: &str = sep.encode_utf8(&mut sep_buf);

    // Schema-qualified form: locate "{sep}{verb}_" so the split lands on the
    // real verb boundary, not a separator embedded inside the schema name.
    for verb in MCP_VERBS {
        let needle = format!("{sep_str}{verb}_");
        if let Some(pos) = name.find(&needle) {
            let schema = &name[..pos];
            let target = &name[pos + needle.len()..];
            if !schema.is_empty() && !target.is_empty() {
                return Ok((schema, verb, target));
            }
        }
    }

    // Default-schema form: "{verb}_{target}" with no separator prefix.
    for verb in MCP_VERBS {
        let prefix = format!("{verb}_");
        if let Some(target) = name.strip_prefix(&prefix)
            && !target.is_empty()
        {
            return Ok((routing.default_schema.as_str(), verb, target));
        }
    }

    Err(format!(
        "Unrecognized tool name: '{name}'. Expected '{{schema}}{sep}{{verb}}_{{target}}' or \
         '{{verb}}_{{target}}' where verb is one of {MCP_VERBS:?}."
    ))
}

/// Parse MCP filter arguments into pgvis `Filter` types.
///
/// Each `{column: "op.value"}` MCP filter argument is fed to the SAME parser
/// the REST surface uses (`pgvis_core::query_params::parse_filter`), passing
/// `key = column` and `value = "op.value"`. This gives MCP the full `PostgREST`
/// operator set (quantifiers, json-path, `fts`, `in.(...)`, `is.notnull`, …)
/// for free and — critically — a parse *error* on any unsupported/typo'd
/// operator instead of silently dropping the filter. Dropping a filter on a
/// DELETE/UPDATE would turn a targeted mutation into an unguarded full-table
/// mutation, so a bad operator is a hard error (mapped to PGRST100 by the
/// caller).
fn parse_mcp_filters(
    args: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Vec<Filter>, String> {
    let mut filters = Vec::new();
    if let Some(filter_obj) = args
        .and_then(|a| a.get("filters"))
        .and_then(|v| v.as_object())
    {
        for (column, value) in filter_obj {
            let value_str = value.as_str().ok_or_else(|| {
                format!("filter for column '{column}' must be a string like \"op.value\"")
            })?;
            let filter = query_params::parse_filter(column, value_str)?;
            filters.push(filter);
        }
    }
    Ok(filters)
}

/// Coerce a JSON value into a `u64`, accepting both a JSON number and a numeric
/// string. LLMs frequently emit `"limit": "10"` (a string) rather than a number;
/// without this coercion the value is silently dropped.
fn json_as_u64(v: &serde_json::Value) -> Option<u64> {
    v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Coerce a JSON value into a `bool`, accepting both a JSON bool and the strings
/// `"true"`/`"false"` (case-insensitive). Mirrors [`json_as_u64`]'s leniency for
/// LLM-generated `"count": "true"` arguments.
fn json_as_bool(v: &serde_json::Value) -> Option<bool> {
    v.as_bool().or_else(|| match v.as_str() {
        Some(s) if s.eq_ignore_ascii_case("true") => Some(true),
        Some(s) if s.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    })
}

/// Map PostgreSQL type names to JSON Schema types (best effort).
fn pg_type_to_json_type(pg_type: &str) -> &'static str {
    match pg_type {
        "integer" | "int4" | "int8" | "bigint" | "smallint" | "int2" => "integer",
        "real" | "float4" | "float8" | "double precision" | "numeric" | "decimal" => "number",
        "boolean" | "bool" => "boolean",
        "json" | "jsonb" => "object",
        _ => "string",
    }
}

/// Parse a select string using the full PostgREST select DSL parser.
///
/// Supports the complete grammar: columns, aliases, JSON paths, casts,
/// aggregates, embeddings with hints/joins, and spreads.
///
/// Falls back to `[SelectItem::Star]` if parsing fails.
fn parse_mcp_select(s: &str) -> Vec<SelectItem> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed == "*" {
        return vec![SelectItem::Star];
    }

    query_params::parse_select(trimmed).unwrap_or_else(|_| vec![SelectItem::Star])
}

/// Parse logic filter arguments from MCP tool call into `LogicTree` nodes.
///
/// Accepts the PostgREST string syntax in `"or"` and `"and"` arguments:
/// ```json
/// { "or": "(status.eq.active,status.eq.pending)" }
/// { "and": "(age.gte.18,age.lte.65)" }
/// ```
fn parse_mcp_logic_filters(
    args: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Vec<LogicTree> {
    let mut trees = Vec::new();
    let args = match args {
        Some(a) => a,
        None => return trees,
    };

    for key in &["and", "or", "not.and", "not.or"] {
        if let Some(value) = args.get(*key).and_then(|v| v.as_str()) {
            match query_params::parse_logic_tree(key, value) {
                Ok(node) => match node {
                    LogicNode::Tree(tree) => trees.push(tree),
                    LogicNode::Not(inner) => {
                        trees.push(LogicTree::And(vec![LogicNode::Not(inner)]));
                    }
                    LogicNode::Filter(f) => {
                        trees.push(LogicTree::And(vec![LogicNode::Filter(f)]));
                    }
                },
                Err(_) => {} // Silently skip malformed logic filters in MCP
            }
        }
    }

    trees
}

/// Parse MCP preferences from tool arguments.
///
/// Supports:
/// - `"count": true` → `Prefer: count=exact` (MCP-18)
/// - `"return": "representation"|"minimal"` → `Prefer: return=...` (MCP-20)
/// - `"resolution": "merge-duplicates"|"ignore-duplicates"` → upsert (MCP-20)
fn parse_mcp_preferences(
    args: Option<&serde_json::Map<String, serde_json::Value>>,
    is_mutation: bool,
) -> Preferences {
    let mut prefs = Preferences::default();
    let args = match args {
        Some(a) => a,
        None => return prefs,
    };

    // count=true → exact count
    if args.get("count").and_then(json_as_bool).unwrap_or(false) {
        prefs.count = Some(PreferCount::Exact);
    }

    // return preference (for mutations)
    if is_mutation {
        if let Some(ret) = args.get("return").and_then(|v| v.as_str()) {
            prefs.return_repr = match ret {
                "representation" => Some(PreferReturn::Representation),
                "minimal" => Some(PreferReturn::Minimal),
                _ => None,
            };
        }

        // resolution preference (for create/upsert)
        if let Some(res) = args.get("resolution").and_then(|v| v.as_str()) {
            prefs.resolution = match res {
                "merge-duplicates" => Some(PreferResolution::MergeDuplicates),
                "ignore-duplicates" => Some(PreferResolution::IgnoreDuplicates),
                _ => None,
            };
        }
    }

    prefs
}

/// Parse the `order` argument string into `OrderTerm` entries.
///
/// Delegates to the SAME parser REST uses (`pgvis_core::query_params::parse_order`)
/// so MCP gets identical semantics: `nullsfirst`/`nullslast` are honoured and an
/// unknown direction token (e.g. `col.ascending`) is a parse *error* rather than
/// being silently coerced to `asc`. Relation-order terms (`rel(col).desc`) are
/// dropped, matching REST. Returns `Err(msg)` on a malformed order string.
fn parse_mcp_order(
    args: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Vec<OrderTerm>, String> {
    let order_str = match args.and_then(|a| a.get("order")).and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(Vec::new()),
    };

    let items = query_params::parse_order(order_str)?;
    Ok(items
        .into_iter()
        .filter_map(|item| match item {
            OrderItem::Term(t) => Some(t),
            OrderItem::Relation(_) => None,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgvis_core::query_params::types::{FilterValue, IsKind, Operator};
    use serde_json::json;

    fn args(v: serde_json::Value) -> Option<serde_json::Map<String, serde_json::Value>> {
        v.as_object().cloned()
    }

    // ---- item 8/9: MCP-safe tool name generation + round-trip parsing -------

    #[test]
    fn tool_name_sanitizes_default_slash_separator() {
        // Core default separator is '/', which is NOT MCP-safe. We must emit '-'.
        let routing = RoutingConfig::default();
        let name = mcp_tool_name(&routing, "public", "list", "users");
        assert!(
            !name.contains('/'),
            "generated name must not contain '/': {name}"
        );
        // MCP pattern: ^[a-zA-Z0-9_-]{1,128}$
        assert!(
            name.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
            "name violates MCP pattern: {name}"
        );

        // Round-trip.
        let (schema, verb, target) = parse_tool_name(&name, &routing).unwrap();
        assert_eq!((schema, verb, target), ("public", "list", "users"));
    }

    #[test]
    fn tool_name_round_trip_with_separator_in_schema() {
        // Schema name itself contains the (sanitized) separator '-'. Parsing must
        // anchor on the verb, not the first '-'.
        let routing = RoutingConfig::default();
        let name = mcp_tool_name(&routing, "my-schema", "delete", "orders");
        let (schema, verb, target) = parse_tool_name(&name, &routing).unwrap();
        assert_eq!((schema, verb, target), ("my-schema", "delete", "orders"));
    }

    #[test]
    fn tool_name_honours_safe_configured_separator() {
        let routing = RoutingConfig {
            mcp_separator: '_',
            ..RoutingConfig::default()
        };
        let name = mcp_tool_name(&routing, "public", "create", "users");
        let (schema, verb, target) = parse_tool_name(&name, &routing).unwrap();
        assert_eq!((schema, verb, target), ("public", "create", "users"));
    }

    #[test]
    fn parse_tool_name_rejects_unknown_verb() {
        let routing = RoutingConfig::default();
        assert!(parse_tool_name("public-frobnicate_users", &routing).is_err());
        assert!(parse_tool_name("garbage", &routing).is_err());
    }

    // ---- item 1/2/3: filter parsing reuses core parser, hard errors ---------

    #[test]
    fn filter_in_list_maps_to_list_value() {
        let a = args(json!({ "filters": { "id": "in.(1,2,3)" } }));
        let filters = parse_mcp_filters(a.as_ref()).unwrap();
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].operator, Operator::In);
        assert_eq!(
            filters[0].value,
            FilterValue::List(vec!["1".into(), "2".into(), "3".into()])
        );
    }

    #[test]
    fn filter_is_notnull_maps_to_is_kind() {
        let a = args(json!({ "filters": { "email": "is.notnull" } }));
        let filters = parse_mcp_filters(a.as_ref()).unwrap();
        assert_eq!(filters[0].value, FilterValue::Is(IsKind::NotNull));
    }

    #[test]
    fn unknown_operator_is_hard_error_not_silent_drop() {
        // The whole point of item 1: a bad operator must NOT vanish (which would
        // turn a filtered DELETE into a full-table DELETE).
        let a = args(json!({ "filters": { "id": "totallybogus.5" } }));
        assert!(parse_mcp_filters(a.as_ref()).is_err());
    }

    // ---- item 5: numeric-string / bool-string coercion ----------------------

    #[test]
    fn limit_accepts_number_and_numeric_string() {
        assert_eq!(json_as_u64(&json!(10)), Some(10));
        assert_eq!(json_as_u64(&json!("10")), Some(10));
        assert_eq!(json_as_u64(&json!("nope")), None);
    }

    #[test]
    fn count_accepts_bool_and_bool_string() {
        assert_eq!(json_as_bool(&json!(true)), Some(true));
        assert_eq!(json_as_bool(&json!("true")), Some(true));
        assert_eq!(json_as_bool(&json!("FALSE")), Some(false));
        assert_eq!(json_as_bool(&json!("maybe")), None);
    }

    // ---- item 6: RPC reserved keys ------------------------------------------

    #[test]
    fn rpc_reserved_keys_cover_all_interpreted_keys() {
        for k in [
            "select",
            "filters",
            "order",
            "limit",
            "offset",
            "count",
            "and",
            "or",
            "not.and",
            "not.or",
            "on_conflict",
            "return",
            "resolution",
        ] {
            assert!(RPC_RESERVED_KEYS.contains(&k), "missing reserved key: {k}");
        }
    }

    // ---- order parsing reuse ------------------------------------------------

    #[test]
    fn order_unknown_direction_is_error() {
        let a = args(json!({ "order": "name.ascending" }));
        assert!(parse_mcp_order(a.as_ref()).is_err());
    }

    #[test]
    fn order_nullsfirst_preserved() {
        let a = args(json!({ "order": "age.desc.nullsfirst" }));
        let terms = parse_mcp_order(a.as_ref()).unwrap();
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].field, "age");
    }
}
