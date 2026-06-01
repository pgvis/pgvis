//! Schema-driven routing — builds axum routes from the schema cache.
//!
//! The [`build_app`] function is the primary entry point. It takes a [`SchemaCache`],
//! [`Config`], [`Dialect`], and [`Backend`] and produces an `axum::Router` with all routes
//! registered for the exposed schemas.
//!
//! ## Routing Modes
//!
//! Three routing modes controlled by [`RoutingConfig`](pgvis_core::config::RoutingConfig):
//! 1. **Full path** (`schema_in_path=true`): `/{prefix}/{schema}/{table}` and `/{prefix}/{schema}/rpc/{fn}`
//! 2. **Prefix only** (`schema_in_path=false`, `prefix="api"`): `/{prefix}/{table}` (schema from `Accept-Profile` header or default)
//! 3. **PostgREST compat** (`schema_in_path=false`, `prefix=""`): `/{table}` (PostgREST drop-in)

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use pgvis_core::backend::{Backend, ExecContext, QueryResult, TxEnd};
use pgvis_core::config::OpenApiMode;
use pgvis_core::plan::{ActionPlan, ApiRequest, RequestBody, RequestMethod, plan_request};
use pgvis_core::preferences::{PreferTx, Preferences};
use pgvis_core::query;
use pgvis_core::query_params::{self, CursorSpec, LogicTree, OrderItem};
use pgvis_core::select_ast::SelectItem;
use pgvis_core::{Config, Dialect, SchemaCache};

use crate::data_cache::DataCache;
use crate::openapi;
use crate::response;

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

/// Shared application state, hot-swappable via `ArcSwap`.
///
/// The [`SchemaCache`] is stored behind `ArcSwap` so it can be atomically
/// updated without rebuilding routes. Handlers load the latest snapshot on
/// each request.
#[derive(Clone)]
pub struct AppState {
    /// The schema cache — hot-swappable for live schema reloads.
    pub cache: Arc<ArcSwap<SchemaCache>>,
    /// The shared configuration (routing, auth, feature gates).
    pub config: Arc<Config>,
    /// The SQL dialect (Postgres or SQLite capability flags).
    pub dialect: Arc<Dialect>,
    /// The database backend for query execution.
    pub backend: Arc<dyn Backend>,
    /// In-memory data cache for read responses (None when caching is disabled).
    pub data_cache: Option<Arc<DataCache>>,
}

// ---------------------------------------------------------------------------
// build_app — the main entry point
// ---------------------------------------------------------------------------

/// Build an axum Router from the [`SchemaCache`], configuration, and backend.
///
/// Routes are generated based on `config.routing`:
/// - `schema_in_path = true`: `/{prefix}/{schema}/{table}` and `/{prefix}/{schema}/rpc/{fn}`
/// - `schema_in_path = false`: `/{prefix}/{table}` and `/{prefix}/rpc/{fn}`
///
/// # Hot Reload
///
/// The returned router uses `ArcSwap<SchemaCache>` so handlers always reference the
/// latest schema cache snapshot. Call `ArcSwap::store` to atomically update the cache
/// without rebuilding routes.
///
/// # Approach
///
/// Rather than generating one route per table (which would require rebuilding the router
/// on schema changes), we use wildcard path parameters and resolve the target at
/// request time against the current schema cache snapshot.
pub fn build_app(
    cache: Arc<ArcSwap<SchemaCache>>,
    config: Arc<Config>,
    dialect: Arc<Dialect>,
    backend: Arc<dyn Backend>,
) -> Router {
    // Create data cache if enabled in config
    let data_cache = if config.cache.enabled {
        Some(Arc::new(DataCache::new(&config.cache)))
    } else {
        None
    };

    let state = AppState {
        cache,
        config: config.clone(),
        dialect,
        backend,
        data_cache,
    };

    let routing = &config.routing;
    let prefix = routing.normalized_prefix();

    let mut router = Router::new();

    if routing.schema_in_path {
        // Mode 1: /{prefix}/{schema}/{table} and /{prefix}/{schema}/rpc/{fn}
        if prefix.is_empty() {
            router = router
                .route(
                    "/{schema}/rpc/{function}",
                    get(handle_rpc_with_schema).post(handle_rpc_with_schema),
                )
                .route(
                    "/{schema}/{target}",
                    get(handle_table_with_schema)
                        .head(handle_table_with_schema)
                        .post(handle_table_with_schema)
                        .put(handle_table_with_schema)
                        .patch(handle_table_with_schema)
                        .delete(handle_table_with_schema),
                )
                .route("/", get(handle_root));
        } else {
            let rpc_path = format!("/{prefix}/{{schema}}/rpc/{{function}}");
            let table_path = format!("/{prefix}/{{schema}}/{{target}}");
            let root_path = format!("/{prefix}/");

            router = router
                .route(
                    &rpc_path,
                    get(handle_rpc_with_schema).post(handle_rpc_with_schema),
                )
                .route(
                    &table_path,
                    get(handle_table_with_schema)
                        .head(handle_table_with_schema)
                        .post(handle_table_with_schema)
                        .put(handle_table_with_schema)
                        .patch(handle_table_with_schema)
                        .delete(handle_table_with_schema),
                )
                .route(&root_path, get(handle_root));
        }
    } else {
        // Mode 2/3: /{prefix}/{table} or /{table} (schema from header/default)
        if prefix.is_empty() {
            router = router
                .route(
                    "/rpc/{function}",
                    get(handle_rpc_no_schema).post(handle_rpc_no_schema),
                )
                .route(
                    "/{target}",
                    get(handle_table_no_schema)
                        .head(handle_table_no_schema)
                        .post(handle_table_no_schema)
                        .put(handle_table_no_schema)
                        .patch(handle_table_no_schema)
                        .delete(handle_table_no_schema),
                )
                .route("/", get(handle_root));
        } else {
            let rpc_path = format!("/{prefix}/rpc/{{function}}");
            let table_path = format!("/{prefix}/{{target}}");
            let root_path = format!("/{prefix}/");

            router = router
                .route(
                    &rpc_path,
                    get(handle_rpc_no_schema).post(handle_rpc_no_schema),
                )
                .route(
                    &table_path,
                    get(handle_table_no_schema)
                        .head(handle_table_no_schema)
                        .post(handle_table_no_schema)
                        .put(handle_table_no_schema)
                        .patch(handle_table_no_schema)
                        .delete(handle_table_no_schema),
                )
                .route(&root_path, get(handle_root));
        }
    }

    // Admin/diagnostic endpoint: cache stats and settings
    router = router.route("/pgvis/cache", get(handle_cache_info));

    router.with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers — schema_in_path = true
// ---------------------------------------------------------------------------

/// Handle table requests when the schema is in the URL path.
async fn handle_table_with_schema(
    State(state): State<AppState>,
    method: axum::http::Method,
    Path(params): Path<HashMap<String, String>>,
    headers: HeaderMap,
    Query(query_params): Query<HashMap<String, String>>,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    let schema = params.get("schema").cloned().unwrap_or_default();
    let target = params.get("target").cloned().unwrap_or_default();
    let request_method = http_method_to_request_method(&method);

    dispatch_request(
        &state,
        schema,
        target,
        request_method,
        false,
        &headers,
        &query_params,
        body.map(|b| b.0),
    )
    .await
}

/// Handle RPC requests when the schema is in the URL path.
async fn handle_rpc_with_schema(
    State(state): State<AppState>,
    method: axum::http::Method,
    Path(params): Path<HashMap<String, String>>,
    headers: HeaderMap,
    Query(query_params): Query<HashMap<String, String>>,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    let schema = params.get("schema").cloned().unwrap_or_default();
    let function = params.get("function").cloned().unwrap_or_default();

    // RPC accepts GET (immutable functions, args from query params) and POST (args from body)
    let request_method = http_method_to_request_method(&method);
    dispatch_request(
        &state,
        schema,
        function,
        request_method,
        true,
        &headers,
        &query_params,
        body.map(|b| b.0),
    )
    .await
}

// ---------------------------------------------------------------------------
// Handlers — schema_in_path = false
// ---------------------------------------------------------------------------

/// Handle table requests when the schema comes from headers/config.
async fn handle_table_no_schema(
    State(state): State<AppState>,
    method: axum::http::Method,
    Path(params): Path<HashMap<String, String>>,
    headers: HeaderMap,
    Query(query_params): Query<HashMap<String, String>>,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    let target = params.get("target").cloned().unwrap_or_default();
    let schema = resolve_schema_from_headers(&headers, &state.config);
    let request_method = http_method_to_request_method(&method);

    dispatch_request(
        &state,
        schema,
        target,
        request_method,
        false,
        &headers,
        &query_params,
        body.map(|b| b.0),
    )
    .await
}

/// Handle RPC requests when the schema comes from headers/config.
async fn handle_rpc_no_schema(
    State(state): State<AppState>,
    method: axum::http::Method,
    Path(params): Path<HashMap<String, String>>,
    headers: HeaderMap,
    Query(query_params): Query<HashMap<String, String>>,
    body: Option<Json<serde_json::Value>>,
) -> Response {
    let function = params.get("function").cloned().unwrap_or_default();
    let schema = resolve_schema_from_headers(&headers, &state.config);

    // RPC accepts GET (immutable functions, args from query params) and POST (args from body)
    let request_method = http_method_to_request_method(&method);
    dispatch_request(
        &state,
        schema,
        function,
        request_method,
        true,
        &headers,
        &query_params,
        body.map(|b| b.0),
    )
    .await
}

// ---------------------------------------------------------------------------
// Root handler
// ---------------------------------------------------------------------------

/// Root endpoint handler — returns available schemas or the OpenAPI spec.
async fn handle_root(State(state): State<AppState>, headers: HeaderMap) -> Response {
    // Check if the client accepts OpenAPI JSON
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if accept.contains("application/openapi+json")
        || accept.contains("application/vnd.pgrst.object")
    {
        // Check if OpenAPI is disabled
        if state.config.openapi_mode == OpenApiMode::Disabled {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "code": "PGRST404",
                    "message": "OpenAPI spec is disabled",
                    "details": null,
                    "hint": "Set openapi_mode to IgnorePrivileges or FollowPrivileges to enable.",
                })),
            )
                .into_response();
        }

        // Generate OpenAPI spec
        let cache = state.cache.load();
        let spec = openapi::generate_spec(&cache, &state.config);
        match serde_json::to_value(&spec) {
            Ok(val) => (StatusCode::OK, Json(val)).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "code": "PGV500",
                    "message": format!("Failed to serialize OpenAPI spec: {e}"),
                })),
            )
                .into_response(),
        }
    } else {
        let resp = serde_json::json!({
            "schemas": state.config.schemas,
            "hint": "Append a table/view name to query it. Use Accept: application/openapi+json for the OpenAPI spec.",
        });
        (StatusCode::OK, Json(resp)).into_response()
    }
}

/// Cache info endpoint — returns cache stats and current settings.
///
/// `GET /pgvis/cache` → JSON with stats (hits, misses, hit_rate, entries, invalidations)
/// and current settings (enabled, ttl_seconds, max_entries, cache_lists).
async fn handle_cache_info(State(state): State<AppState>) -> Response {
    let settings = serde_json::json!({
        "enabled": state.config.cache.enabled,
        "ttl_seconds": state.config.cache.ttl_seconds,
        "max_entries": state.config.cache.max_entries,
        "cache_lists": state.config.cache.cache_lists,
    });

    let stats = state.data_cache.as_ref().map(|dc| dc.stats());

    let body = serde_json::json!({
        "settings": settings,
        "stats": stats,
    });

    (StatusCode::OK, Json(body)).into_response()
}

// ---------------------------------------------------------------------------
// Core dispatch logic — the full pipeline
// ---------------------------------------------------------------------------

/// Core request dispatch — plan → render SQL → execute → format response.
///
/// This is the heart of the pgvis pipeline:
/// 1. Parse HTTP concerns into an [`ApiRequest`]
/// 2. Plan the request against the schema cache → [`ActionPlan`]
/// 3. Render the plan to parameterised SQL via [`query::render`]
/// 4. Execute via [`Backend::execute`] with transaction/role/claims
/// 5. Format the [`QueryResult`] into an HTTP response
async fn dispatch_request(
    state: &AppState,
    schema: String,
    target: String,
    method: RequestMethod,
    is_rpc: bool,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: Option<serde_json::Value>,
) -> Response {
    let cache = state.cache.load();

    // Parse preferences early — needed for ExecContext and response formatting
    let preferences = headers
        .get("prefer")
        .and_then(|v| v.to_str().ok())
        .map(|s| Preferences::parse(s).0)
        .unwrap_or_default();

    // 1. Build the adapter-agnostic ApiRequest
    let api_request = build_api_request(
        schema,
        target,
        method.clone(),
        is_rpc,
        headers,
        params,
        body,
        &preferences,
    );

    // 2. Plan the request against the schema cache
    let plan = match plan_request(&api_request, &cache, &state.dialect, &state.config) {
        Ok(plan) => plan,
        Err(err) => return response::format_error(&err),
    };

    // For Inspect plans, return the inspection result directly
    if let ActionPlan::Inspect(_) = &plan {
        let resp = serde_json::json!({"status": "inspect", "message": "not yet implemented"});
        return (StatusCode::OK, Json(resp)).into_response();
    }

    // 3. Render the plan to SQL + parameters
    //    Postgres: uses CTE wrapper for single-row JSON response + GUC headers
    //    SQLite: uses raw SQL — Rust-side JSON assembly in execute module
    let (sql, params_vec) = if state.dialect.supports_set_local {
        // Postgres path: CTE-wrapped SQL that returns body + page_total in one row
        match query::render(&plan, &state.dialect) {
            Ok(rendered) => rendered,
            Err(err) => return response::format_error(&err),
        }
    } else {
        // SQLite path: render without CTE wrapping — executor assembles JSON
        match query::render_inner(&plan, &state.dialect) {
            Ok(rendered) => rendered,
            Err(err) => return response::format_error(&err),
        }
    };

    tracing::debug!(sql = %sql, params = ?params_vec, "executing query");

    // 4. Verify JWT and build ExecContext
    let auth = match verify_jwt(headers, &state.config) {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let is_mutation = matches!(&plan, ActionPlan::Mutate(_));
    let exec_ctx = build_exec_context(&state.config, &auth, &preferences, is_mutation);

    // 4b. Data cache: compute key and check for cache hit (reads only)
    let cache_key = if let ActionPlan::Read(ref read_plan) = plan {
        state.data_cache.as_ref().and_then(|dc| {
            dc.compute_key(
                read_plan,
                &sql,
                &params_vec,
                auth.role.as_deref(),
                auth.claims.as_ref(),
            )
        })
    } else {
        None
    };

    // Try cache hit — if found, skip backend execution entirely
    if let Some(ref key) = cache_key {
        if let Some(dc) = &state.data_cache {
            if let Some(cached) = dc.get(key) {
                tracing::debug!(cache_key = %key, "data cache hit");

                let cached_result = QueryResult {
                    body: cached.body,
                    total_count: cached.total_count,
                    page_total: cached.page_total,
                    response_status: None,
                    response_headers: None,
                    was_insert: None,
                };

                let cursor_column = if let ActionPlan::Read(ref read_plan) = plan {
                    read_plan.range.cursor_column.clone()
                } else {
                    None
                };

                let is_singular = headers
                    .get("accept")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.contains("application/vnd.pgrst.object"))
                    .unwrap_or(false);

                let request_offset =
                    params.get("offset").and_then(|s| s.parse::<u64>().ok());

                return response::format_response(
                    &cached_result,
                    &method,
                    &preferences,
                    is_singular,
                    request_offset,
                    cursor_column.as_deref(),
                );
            }
        }
    }

    // 5. Execute via backend
    let result = match state.backend.execute(&exec_ctx, &sql, &params_vec).await {
        Ok(result) => result,
        Err(err) => return response::format_error(&err),
    };

    // 5b. Data cache: store result on cache miss. `cache_key` is `Some` only for
    // cacheable reads, so no need to re-match the plan here.
    if let (Some(key), Some(dc)) = (&cache_key, &state.data_cache) {
        dc.store(key, result.body.clone(), result.total_count, result.page_total);
        tracing::debug!(cache_key = %key, "data cache store");
    }

    // 5c. Data cache: invalidate on writes. Mutations target a known table;
    // volatile RPCs can modify anything, so they clear the whole cache.
    if let Some(dc) = &state.data_cache {
        match &plan {
            ActionPlan::Mutate(mutate_plan) => {
                dc.invalidate_table(&mutate_plan.target);
                tracing::debug!(table = %mutate_plan.target, "data cache invalidated for table");
            }
            ActionPlan::Call(call_plan)
                if call_plan.function_info.volatility
                    == pgvis_core::cache::Volatility::Volatile =>
            {
                dc.invalidate_all();
                tracing::debug!(function = %call_plan.function, "data cache invalidated (volatile RPC)");
            }
            _ => {}
        }
    }

    // 6. Extract cursor column from the plan (for X-Next-Cursor header)
    let cursor_column = match &plan {
        ActionPlan::Read(read_plan) => read_plan.range.cursor_column.clone(),
        _ => None,
    };

    // 7. Format the QueryResult into an HTTP response
    let is_singular = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("application/vnd.pgrst.object"))
        .unwrap_or(false);

    let request_offset = params.get("offset").and_then(|s| s.parse::<u64>().ok());

    response::format_response(
        &result,
        &method,
        &preferences,
        is_singular,
        request_offset,
        cursor_column.as_deref(),
    )
}

// ---------------------------------------------------------------------------
// build_api_request — parse HTTP concerns into the adapter-agnostic request
// ---------------------------------------------------------------------------

/// Build an [`ApiRequest`] from raw HTTP query parameters, headers, and body.
///
/// This is where the REST adapter converts HTTP-level concerns into the
/// adapter-agnostic `ApiRequest` that the plan layer consumes.
fn build_api_request(
    schema: String,
    target: String,
    method: RequestMethod,
    is_rpc: bool,
    _headers: &HeaderMap,
    params: &HashMap<String, String>,
    body: Option<serde_json::Value>,
    preferences: &Preferences,
) -> ApiRequest {
    let _ = preferences; // Will be used for count strategy, etc.

    // Parse select parameter
    let select = params
        .get("select")
        .and_then(|s| query_params::parse_select(s).ok())
        .unwrap_or_default();

    // If select is empty, default to star
    let select = if select.is_empty() {
        vec![SelectItem::Star]
    } else {
        select
    };

    // Parse filters from query params (columns not named select/order/limit/offset)
    let filters = parse_filters_from_params(params);

    // Parse order — extract only direct OrderTerms (skip relation terms for now)
    let order = params
        .get("order")
        .and_then(|s| query_params::parse_order(s).ok())
        .map(|items| {
            items
                .into_iter()
                .filter_map(|item| match item {
                    OrderItem::Term(t) => Some(t),
                    OrderItem::Relation(_) => None,
                })
                .collect()
        })
        .unwrap_or_default();

    // Parse range (limit/offset)
    let range = parse_range_from_params(params);

    // Parse body into RequestBody
    let request_body = body.map(|v| {
        if v.is_array() {
            RequestBody::Bulk(v.as_array().cloned().unwrap_or_default())
        } else {
            RequestBody::Single(v)
        }
    });

    // On-conflict
    let on_conflict = params.get("on_conflict").cloned();

    // Columns
    let columns = params
        .get("columns")
        .map(|s| s.split(',').map(|c| c.trim().to_string()).collect());

    // Parse logic filters (and=, or=, not.and=, not.or=)
    let logic_filters = parse_logic_filters_from_params(params);

    // Parse cursor pagination (cursor_column, cursor_value)
    let cursor = parse_cursor_from_params(params);

    ApiRequest {
        schema,
        target,
        method,
        is_rpc,
        select,
        filters,
        order,
        range,
        preferences: preferences.clone(),
        body: request_body,
        on_conflict,
        columns,
        logic_filters,
        cursor,
    }
}

/// The result of JWT verification — either authenticated claims or anonymous.
struct AuthResult {
    /// The role to SET LOCAL to (from JWT claim or anon_role).
    role: Option<String>,
    /// The full JWT claims as a JSON value (for GUC propagation).
    claims: Option<serde_json::Value>,
}

/// Verify the JWT from the Authorization header and extract role + claims.
///
/// Returns `Ok(AuthResult)` on success (including anonymous access when no JWT
/// is required). Returns `Err(Response)` when auth fails and the request should
/// be rejected immediately.
fn verify_jwt(headers: &HeaderMap, config: &Config) -> Result<AuthResult, Response> {
    // If no JWT secret is configured, all requests are anonymous
    let secret = match &config.jwt_secret {
        Some(s) => s,
        None => {
            return Ok(AuthResult {
                role: config.anon_role.clone(),
                claims: None,
            });
        }
    };

    // Extract the Bearer token from the Authorization header
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.strip_prefix("Bearer ")
                .or_else(|| s.strip_prefix("bearer "))
        });

    let token = match token {
        Some(t) => t,
        None => {
            // No token provided — use anonymous role if configured
            if config.anon_role.is_some() {
                return Ok(AuthResult {
                    role: config.anon_role.clone(),
                    claims: None,
                });
            }
            // No anon role and no token — reject
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "code": "PGRST300",
                    "message": "JWT token required but not provided",
                    "details": null,
                    "hint": "Provide an Authorization: Bearer <token> header",
                })),
            )
                .into_response());
        }
    };

    // Build the decoding key based on the algorithm
    use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
    use pgvis_core::config::JwtAlgorithm;

    let algorithm = match config.jwt_algo {
        JwtAlgorithm::HS256 => Algorithm::HS256,
        JwtAlgorithm::HS384 => Algorithm::HS384,
        JwtAlgorithm::HS512 => Algorithm::HS512,
        JwtAlgorithm::RS256 => Algorithm::RS256,
        JwtAlgorithm::EdDSA => Algorithm::EdDSA,
    };

    let decoding_key = match algorithm {
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            DecodingKey::from_secret(secret.as_bytes())
        }
        Algorithm::RS256 => DecodingKey::from_rsa_pem(secret.as_bytes())
            .unwrap_or_else(|_| DecodingKey::from_secret(secret.as_bytes())),
        Algorithm::EdDSA => DecodingKey::from_ed_pem(secret.as_bytes())
            .unwrap_or_else(|_| DecodingKey::from_secret(secret.as_bytes())),
        _ => DecodingKey::from_secret(secret.as_bytes()),
    };

    let mut validation = Validation::new(algorithm);
    validation.validate_exp = true;
    // Don't require specific claims beyond exp
    validation.required_spec_claims = std::collections::HashSet::new();

    match decode::<serde_json::Value>(token, &decoding_key, &validation) {
        Ok(token_data) => {
            let claims = token_data.claims;
            // Extract role from claims using the configured key
            let role = claims
                .get(&config.role_claim_key)
                .and_then(|v| v.as_str())
                .map(String::from)
                .or_else(|| config.anon_role.clone());

            Ok(AuthResult {
                role,
                claims: Some(claims),
            })
        }
        Err(err) => {
            use jsonwebtoken::errors::ErrorKind;
            let (code, message) = match err.kind() {
                ErrorKind::ExpiredSignature => ("PGRST302", "JWT token has expired"),
                _ => ("PGRST301", "JWT token verification failed"),
            };
            Err((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "code": code,
                    "message": message,
                    "details": err.to_string(),
                    "hint": null,
                })),
            )
                .into_response())
        }
    }
}

/// Build an [`ExecContext`] from configuration, auth result, and request preferences.
fn build_exec_context(
    config: &Config,
    auth: &AuthResult,
    preferences: &Preferences,
    is_mutation: bool,
) -> ExecContext {
    let tx_end = preferences.tx.and_then(|tx| match tx {
        PreferTx::Commit => Some(TxEnd::Commit),
        PreferTx::Rollback => Some(TxEnd::Rollback),
    });

    ExecContext {
        role: auth.role.clone(),
        claims: auth.claims.clone(),
        pre_request: config.pre_request.clone(),
        statement_timeout: config.statement_timeout_ms,
        tx_end,
        is_mutation,
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Parse filter expressions from query parameters.
///
/// Any parameter whose key is not a reserved keyword (`select`, `order`, `limit`,
/// `offset`, `on_conflict`, `columns`) is treated as a column filter.
///
/// Filters are sorted by column name for deterministic SQL output,
/// which improves Postgres prepared-statement cache hit rates and
/// makes debugging/logging reproducible.
fn parse_filters_from_params(
    params: &HashMap<String, String>,
) -> Vec<pgvis_core::query_params::Filter> {
    const RESERVED: &[&str] = &[
        "select",
        "order",
        "limit",
        "offset",
        "on_conflict",
        "columns",
        "cursor_column",
        "cursor_value",
    ];
    let mut filters = Vec::new();

    for (key, value) in params {
        if RESERVED.contains(&key.as_str()) {
            continue;
        }
        // Skip logic filter keys — they're handled by parse_logic_filters_from_params
        if is_logic_filter_key(key) {
            continue;
        }
        // Try to parse as a filter: column=operator.value
        if let Ok(filter) = query_params::parse_filter(key, value) {
            filters.push(filter);
        }
    }

    // Sort by column name for deterministic SQL output
    filters.sort_by(|a, b| a.field.cmp(&b.field));

    filters
}

/// Check if a query parameter key is a logic filter operator.
///
/// Logic filter keys are: `and`, `or`, `not.and`, `not.or`.
fn is_logic_filter_key(key: &str) -> bool {
    matches!(key, "and" | "or" | "not.and" | "not.or")
}

/// Parse logic filter expressions (`and=`, `or=`, `not.and=`, `not.or=`) from query parameters.
///
/// Returns parsed `LogicTree` nodes that express boolean combinations of leaf filters.
fn parse_logic_filters_from_params(params: &HashMap<String, String>) -> Vec<LogicTree> {
    let mut trees = Vec::new();

    for (key, value) in params {
        if !is_logic_filter_key(key) {
            continue;
        }
        match query_params::parse_logic_tree(key, value) {
            Ok(node) => {
                // Wrap the top-level LogicNode in a LogicTree
                match node {
                    pgvis_core::query_params::LogicNode::Tree(tree) => trees.push(tree),
                    pgvis_core::query_params::LogicNode::Not(inner) => {
                        // not.and/not.or: wrap in a single-item And with negation
                        // The plan layer handles Not nodes within the tree
                        trees.push(LogicTree::And(vec![
                            pgvis_core::query_params::LogicNode::Not(inner),
                        ]));
                    }
                    pgvis_core::query_params::LogicNode::Filter(f) => {
                        trees.push(LogicTree::And(vec![
                            pgvis_core::query_params::LogicNode::Filter(f),
                        ]));
                    }
                }
            }
            Err(e) => {
                tracing::debug!(key = %key, error = %e, "failed to parse logic filter, skipping");
            }
        }
    }

    trees
}

/// Parse limit/offset from query parameters into a `RangeSpec`.
fn parse_range_from_params(
    params: &HashMap<String, String>,
) -> Option<pgvis_core::query_params::RangeSpec> {
    let limit = params.get("limit").and_then(|s| s.parse().ok());
    let offset = params.get("offset").and_then(|s| s.parse().ok());

    if limit.is_some() || offset.is_some() {
        Some(pgvis_core::query_params::RangeSpec { limit, offset })
    } else {
        None
    }
}

/// Parse cursor pagination parameters (`cursor_column`, `cursor_value`).
///
/// Returns `Some(CursorSpec)` if either parameter is present, activating cursor mode.
/// When `cursor_column` is omitted, the planner defaults to the table's primary key.
fn parse_cursor_from_params(params: &HashMap<String, String>) -> Option<CursorSpec> {
    let column = params.get("cursor_column").cloned();
    let value = params.get("cursor_value").cloned();

    // Activate cursor pagination if either param is present
    if column.is_some() || value.is_some() {
        Some(CursorSpec { column, value })
    } else {
        None
    }
}

/// Resolve the schema name from headers (when `schema_in_path = false`).
///
/// Checks `Accept-Profile` header first, falls back to `config.routing.default_schema`.
fn resolve_schema_from_headers(headers: &HeaderMap, config: &Config) -> String {
    headers
        .get("accept-profile")
        .or_else(|| headers.get("content-profile"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| config.routing.default_schema.clone())
}

/// Convert an axum HTTP method to our [`RequestMethod`].
fn http_method_to_request_method(method: &axum::http::Method) -> RequestMethod {
    match *method {
        axum::http::Method::GET => RequestMethod::Get,
        axum::http::Method::HEAD => RequestMethod::Head,
        axum::http::Method::POST => RequestMethod::Post,
        axum::http::Method::PATCH => RequestMethod::Patch,
        axum::http::Method::PUT => RequestMethod::Put,
        axum::http::Method::DELETE => RequestMethod::Delete,
        _ => RequestMethod::Get,
    }
}
