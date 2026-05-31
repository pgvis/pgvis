//! MCP tool generation and execution.
//!
//! [`build_mcp_tools`] generates tool definitions from the SchemaCache (parallel to REST's `build_app`).
//! [`handle_tool_call`] executes a tool call through the same `plan_request()` → SQL → execute pipeline as REST.

use pgvis_core::backend::{Backend, ExecContext};
use pgvis_core::cache::{Routine, Table};
use pgvis_core::config::RoutingConfig;
use pgvis_core::plan::{ActionPlan, ApiRequest, RequestBody, RequestMethod, plan_request};
use pgvis_core::preferences::{PreferCount, PreferResolution, PreferReturn, Preferences};
use pgvis_core::query;
use pgvis_core::query_params;
use pgvis_core::query_params::types::{
    Filter, FilterValue, LogicNode, LogicTree, NullsOrder, Operator, OrderDirection, OrderTerm,
    RangeSpec,
};
use pgvis_core::select_ast::SelectItem;
use pgvis_core::{Config, Dialect, SchemaCache};

use crate::types::*;

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
            return McpToolResult::error_structured(
                "PGRST404",
                e,
                None,
                Some("Tool names follow `{schema}{sep}{verb}_{target}`.".to_string()),
            );
        }
    };

    // 2. Convert verb to RequestMethod
    let method = match verb {
        "list" => RequestMethod::Get,
        "create" => RequestMethod::Post,
        "update" => RequestMethod::Patch,
        "delete" => RequestMethod::Delete,
        "call" => RequestMethod::Post,
        _ => {
            return McpToolResult::error_structured(
                "PGRST404",
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

    let filters = parse_mcp_filters(args);

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
            // For RPC, all arguments (except reserved keys) become the body
            let mut body_map = serde_json::Map::new();
            if let Some(a) = args {
                for (k, v) in a {
                    if !["select", "filters", "order", "limit", "offset"].contains(&k.as_str()) {
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

    let limit = args.and_then(|a| a.get("limit")).and_then(|v| v.as_u64());
    let offset = args.and_then(|a| a.get("offset")).and_then(|v| v.as_u64());

    let range = if limit.is_some() || offset.is_some() {
        Some(RangeSpec { limit, offset })
    } else {
        None
    };

    let is_mutation = matches!(verb, "create" | "update" | "delete");

    // Parse logic filters (MCP-17): support "or" and "and" arguments
    let logic_filters = parse_mcp_logic_filters(args);

    // Parse preferences (MCP-18 + MCP-20)
    let preferences = parse_mcp_preferences(args, is_mutation);

    let api_request = ApiRequest {
        schema: schema.to_string(),
        target: target.to_string(),
        method,
        is_rpc: verb == "call",
        select,
        filters,
        order: parse_mcp_order(args),
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

    // 6. Build ExecContext
    let exec_ctx = ExecContext {
        role: config.anon_role.clone(),
        claims: None,
        pre_request: config.pre_request.clone(),
        statement_timeout: config.statement_timeout_ms,
        tx_end: None,
        is_mutation,
    };

    // 7. Execute via backend, bounded by a per-call deadline.
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
                .and_then(|v| v.as_bool())
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
    let name = routing.mcp_tool_name(schema, "list", table.name());
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
    let name = routing.mcp_tool_name(schema, "create", table.name());
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
    let name = routing.mcp_tool_name(schema, "update", table.name());
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
    let name = routing.mcp_tool_name(schema, "delete", table.name());
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
    let name = routing.mcp_tool_name(schema, "call", &routine.ident.name);

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

/// Parse a tool name into (schema, verb, target).
///
/// Tool names follow the pattern: `{schema}{sep}{verb}_{target}`
/// e.g., `public/list_users` → ("public", "list", "users")
fn parse_tool_name<'a>(
    name: &'a str,
    routing: &'a RoutingConfig,
) -> Result<(&'a str, &'a str, &'a str), String> {
    let sep = routing.mcp_separator;

    if let Some(sep_pos) = name.find(sep) {
        let schema = &name[..sep_pos];
        let rest = &name[sep_pos + sep.len_utf8()..];

        // Split verb_target on first underscore
        if let Some(underscore_pos) = rest.find('_') {
            let verb = &rest[..underscore_pos];
            let target = &rest[underscore_pos + 1..];
            Ok((schema, verb, target))
        } else {
            Err(format!(
                "Invalid tool name format: '{name}'. Expected '{{schema}}{sep}{{verb}}_{{target}}'"
            ))
        }
    } else {
        // No schema prefix — use default schema
        let default_schema = &routing.default_schema;
        if let Some(underscore_pos) = name.find('_') {
            let verb = &name[..underscore_pos];
            let target = &name[underscore_pos + 1..];
            Ok((default_schema.as_str(), verb, target))
        } else {
            Err(format!(
                "Invalid tool name format: '{name}'. Expected '{{verb}}_{{target}}'"
            ))
        }
    }
}

/// Parse MCP filter arguments into pgvis Filter types.
///
/// Supports the PostgREST filter syntax: `"op.value"` or `"not.op.value"`.
/// Examples: `"eq.5"`, `"not.eq.5"`, `"gt.10"`, `"like.*foo*"`, `"is.null"`.
fn parse_mcp_filters(args: Option<&serde_json::Map<String, serde_json::Value>>) -> Vec<Filter> {
    let mut filters = Vec::new();
    if let Some(filter_obj) = args
        .and_then(|a| a.get("filters"))
        .and_then(|v| v.as_object())
    {
        for (column, value) in filter_obj {
            if let Some(value_str) = value.as_str() {
                // Handle negation prefix: "not.op.value" → negate=true, rest="op.value"
                let (negate, rest) = if let Some(stripped) = value_str.strip_prefix("not.") {
                    (true, stripped)
                } else {
                    (false, value_str)
                };

                // Parse "operator.value" from the remainder
                if let Some(dot_pos) = rest.find('.') {
                    let op_str = &rest[..dot_pos];
                    let val_str = &rest[dot_pos + 1..];

                    if let Some(operator) = parse_operator(op_str) {
                        filters.push(Filter {
                            field: column.clone(),
                            json_path: Vec::new(),
                            operator,
                            negate,
                            quantifier: None,
                            value: FilterValue::Single(val_str.to_string()),
                        });
                    }
                }
            }
        }
    }
    filters
}

fn parse_operator(s: &str) -> Option<Operator> {
    match s {
        "eq" => Some(Operator::Eq),
        "neq" => Some(Operator::Neq),
        "gt" => Some(Operator::Gt),
        "gte" => Some(Operator::Gte),
        "lt" => Some(Operator::Lt),
        "lte" => Some(Operator::Lte),
        "like" => Some(Operator::Like),
        "ilike" => Some(Operator::ILike),
        "is" => Some(Operator::Is),
        "in" => Some(Operator::In),
        "cs" => Some(Operator::Contains),
        "cd" => Some(Operator::ContainedBy),
        "ov" => Some(Operator::Overlap),
        _ => None,
    }
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
    if args.get("count").and_then(|v| v.as_bool()).unwrap_or(false) {
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
/// Supports the PostgREST order format: `"column.direction.nulls"`.
/// Examples: `"name.asc"`, `"age.desc.nullsfirst"`, `"id"` (defaults to asc).
fn parse_mcp_order(args: Option<&serde_json::Map<String, serde_json::Value>>) -> Vec<OrderTerm> {
    let order_str = match args.and_then(|a| a.get("order")).and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s,
        _ => return Vec::new(),
    };

    order_str
        .split(',')
        .filter_map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return None;
            }

            let segments: Vec<&str> = part.splitn(3, '.').collect();
            let field = segments[0].to_string();

            let direction = segments
                .get(1)
                .map(|s| match *s {
                    "desc" => OrderDirection::Desc,
                    _ => OrderDirection::Asc,
                })
                .unwrap_or(OrderDirection::Asc);

            let nulls = segments.get(2).and_then(|s| match *s {
                "nullsfirst" => Some(NullsOrder::First),
                "nullslast" => Some(NullsOrder::Last),
                _ => None,
            });

            Some(OrderTerm {
                field,
                json_path: Vec::new(),
                direction,
                nulls,
            })
        })
        .collect()
}
