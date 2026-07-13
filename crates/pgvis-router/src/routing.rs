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
use pgvis_core::{Config, Dialect, Error, SchemaCache};
use serde::de::DeserializeOwned;

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
    /// Cached OpenAPI JSON, lazily populated and invalidated on schema reload.
    /// Tuple: (schema_cache_ptr as usize, serialized Value).
    openapi_cache: Arc<std::sync::Mutex<(usize, Option<serde_json::Value>)>>,
}

// ---------------------------------------------------------------------------
// In-process RPC adapter
// ---------------------------------------------------------------------------

/// Identity for an in-process RPC call.
///
/// Substitutes for JWT verification on the HTTP path: the supplied `role` and
/// `claims` are applied via `SET LOCAL role` / `request.jwt.claims` GUC inside
/// the execution transaction, so row-level security and `current_setting(...)`
/// logic behave exactly as they would for an authenticated HTTP request.
///
/// Use [`CallerIdentity::anonymous`] for the default (anon) role.
#[derive(Debug, Clone, Default)]
pub struct CallerIdentity {
    /// Role to `SET LOCAL role` to (e.g. `Some("authenticated")`). `None` keeps
    /// the connection's default role.
    pub role: Option<String>,
    /// Raw JWT claims propagated as the `request.jwt.claims` GUC.
    pub claims: Option<serde_json::Value>,
}

impl CallerIdentity {
    /// An anonymous caller — no role switch, no claims.
    pub fn anonymous() -> Self {
        Self::default()
    }

    /// A caller bound to a specific database role.
    pub fn with_role(role: impl Into<String>) -> Self {
        Self {
            role: Some(role.into()),
            claims: None,
        }
    }
}

impl AppState {
    /// Assemble shared application state from its parts.
    ///
    /// The in-memory data cache is created when `config.cache.enabled`. Build the
    /// state once and share it between [`build_router`] (for the HTTP API) and
    /// [`call_rpc`](Self::call_rpc) (for in-process calls) so both observe the
    /// same cache and schema snapshot.
    pub fn new(
        cache: Arc<ArcSwap<SchemaCache>>,
        config: Arc<Config>,
        dialect: Arc<Dialect>,
        backend: Arc<dyn Backend>,
    ) -> Self {
        let data_cache = if config.cache.enabled {
            Some(Arc::new(DataCache::new(&config.cache)))
        } else {
            None
        };

        Self {
            cache,
            config,
            dialect,
            backend,
            data_cache,
            openapi_cache: Arc::new(std::sync::Mutex::new((0, None))),
        }
    }

    /// Invoke a database function in-process through the full pgvis pipeline
    /// (plan → render → role/GUC-applied execute), returning the raw
    /// [`QueryResult`]. This is the programmatic equivalent of a
    /// `POST /rpc/{function}` request — no HTTP round-trip, and unlike a direct
    /// pool query it honours the auth/RLS context carried by `caller`.
    ///
    /// `args` is the function argument object (named arguments); pass
    /// `serde_json::json!({})` for a no-argument function.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] if planning fails (e.g. unknown function/schema) or the
    /// backend execution fails.
    pub async fn call_rpc(
        &self,
        schema: &str,
        function: &str,
        args: serde_json::Value,
        caller: &CallerIdentity,
    ) -> Result<QueryResult, Error> {
        let cache = self.cache.load();

        // Build the adapter-agnostic request for an RPC POST call. Mirrors the
        // RPC shape produced by `build_api_request` on the HTTP path, minus the
        // HTTP-only concerns (filters, ordering, range, preferences).
        let api_request = ApiRequest {
            schema: schema.to_string(),
            target: function.to_string(),
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
        };

        let plan = plan_request(&api_request, &cache, &self.dialect, &self.config)?;

        // Postgres: CTE-wrapped single-row JSON result. SQLite: raw SQL (the
        // backend assembles JSON). Matches the dispatch_request branching.
        let (sql, params) = if self.dialect.supports_set_local {
            query::render(&plan, &self.dialect)?
        } else {
            query::render_inner(&plan, &self.dialect)?
        };

        // Build the execution context from the explicit caller identity rather
        // than from parsed HTTP headers.
        let exec_ctx = ExecContext {
            role: caller.role.clone(),
            claims: caller.claims.clone(),
            pre_request: self.config.pre_request.clone(),
            statement_timeout: self.config.statement_timeout_ms,
            tx_end: None,
            is_mutation: matches!(plan, ActionPlan::Mutate(_)),
        };

        let result = self.backend.execute(&exec_ctx, &sql, &params).await?;

        // Invalidate the data cache on a successful mutating call, mirroring the
        // HTTP dispatch path. A volatile RPC may touch any table, so clear all.
        if let Some(dc) = &self.data_cache {
            match &plan {
                ActionPlan::Mutate(mutate_plan) => dc.invalidate_table(&mutate_plan.target),
                ActionPlan::Call(call_plan)
                    if call_plan.function_info.volatility
                        == pgvis_core::cache::Volatility::Volatile =>
                {
                    dc.invalidate_all()
                }
                _ => {}
            }
        }

        Ok(result)
    }

    /// Call a scalar-returning function and deserialize its single value into `T`.
    ///
    /// Scalar and `RETURNS jsonb`/composite functions render as
    /// `SELECT fn(...) AS result`, so the response body is `[{"result": <value>}]`.
    /// This unwraps that to `<value>` before deserializing. Use it for functions
    /// returning a scalar (e.g. `BIGINT`) or a single JSON object.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] on call failure, or if the function produced no row or a
    /// SQL `NULL` (use [`call_rpc`](Self::call_rpc) directly if `NULL` is valid).
    pub async fn call_rpc_scalar<T: DeserializeOwned>(
        &self,
        schema: &str,
        function: &str,
        args: serde_json::Value,
        caller: &CallerIdentity,
    ) -> Result<T, Error> {
        let result = self.call_rpc(schema, function, args, caller).await?;
        let value = unwrap_scalar_body(result.body);
        if value.is_null() {
            return Err(Error::Internal(format!(
                "rpc {schema}.{function} returned no scalar value"
            )));
        }
        serde_json::from_value(value)
            .map_err(|e| Error::Internal(format!("rpc {schema}.{function} deserialize: {e}")))
    }

    /// Like [`call_rpc_scalar`](Self::call_rpc_scalar) but returns `None` when
    /// the function yields SQL `NULL` or no row (for nullable-scalar functions).
    pub async fn call_rpc_scalar_opt<T: DeserializeOwned>(
        &self,
        schema: &str,
        function: &str,
        args: serde_json::Value,
        caller: &CallerIdentity,
    ) -> Result<Option<T>, Error> {
        let result = self.call_rpc(schema, function, args, caller).await?;
        let value = unwrap_scalar_body(result.body);
        if value.is_null() {
            return Ok(None);
        }
        serde_json::from_value(value)
            .map(Some)
            .map_err(|e| Error::Internal(format!("rpc {schema}.{function} deserialize: {e}")))
    }

    /// Call a set-returning function (`RETURNS TABLE(...)` / `SETOF`) and
    /// deserialize each row into `T`. The body is a JSON array of row objects.
    pub async fn call_rpc_rows<T: DeserializeOwned>(
        &self,
        schema: &str,
        function: &str,
        args: serde_json::Value,
        caller: &CallerIdentity,
    ) -> Result<Vec<T>, Error> {
        let result = self.call_rpc(schema, function, args, caller).await?;
        serde_json::from_value(result.body)
            .map_err(|e| Error::Internal(format!("rpc {schema}.{function} rows deserialize: {e}")))
    }

    /// Read rows from a table/view in-process — the programmatic equivalent of a
    /// `GET /{table}?<params>` request against the exposed table API. `params`
    /// uses the same PostgREST-style query syntax as the HTTP endpoint, e.g.
    /// `{"id": "eq.123", "select": "id,email", "deleted_at": "is.null", "limit": "1"}`.
    ///
    /// Runs the full plan → render → role/GUC-applied execute pipeline (no HTTP
    /// round-trip) and returns the raw [`QueryResult`] whose `body` is a JSON
    /// array of row objects. The data cache is bypassed (always a fresh read).
    pub async fn call_read(
        &self,
        schema: &str,
        table: &str,
        params: &HashMap<String, String>,
        caller: &CallerIdentity,
    ) -> Result<QueryResult, Error> {
        let cache = self.cache.load();

        let select = params
            .get("select")
            .and_then(|s| query_params::parse_select(s).ok())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec![SelectItem::Star]);

        let order = params
            .get("order")
            .and_then(|s| query_params::parse_order(s).ok())
            .map(|items| {
                items
                    .into_iter()
                    .filter_map(|it| match it {
                        OrderItem::Term(t) => Some(t),
                        OrderItem::Relation(_) => None,
                    })
                    .collect()
            })
            .unwrap_or_default();

        let api_request = ApiRequest {
            schema: schema.to_string(),
            target: table.to_string(),
            method: RequestMethod::Get,
            is_rpc: false,
            select,
            filters: parse_filters_from_params(params)?,
            order,
            range: parse_range_from_params(params)?,
            preferences: Preferences::default(),
            body: None,
            on_conflict: None,
            columns: None,
            logic_filters: parse_logic_filters_from_params(params)?,
            cursor: parse_cursor_from_params(params),
        };

        let plan = plan_request(&api_request, &cache, &self.dialect, &self.config)?;
        let (sql, sql_params) = if self.dialect.supports_set_local {
            query::render(&plan, &self.dialect)?
        } else {
            query::render_inner(&plan, &self.dialect)?
        };

        let exec_ctx = ExecContext {
            role: caller.role.clone(),
            claims: caller.claims.clone(),
            pre_request: self.config.pre_request.clone(),
            statement_timeout: self.config.statement_timeout_ms,
            tx_end: None,
            is_mutation: false,
        };

        self.backend.execute(&exec_ctx, &sql, &sql_params).await
    }

    /// Typed convenience over [`call_read`](Self::call_read): deserialize each
    /// returned row into `T`.
    pub async fn call_read_rows<T: DeserializeOwned>(
        &self,
        schema: &str,
        table: &str,
        params: &HashMap<String, String>,
        caller: &CallerIdentity,
    ) -> Result<Vec<T>, Error> {
        let result = self.call_read(schema, table, params, caller).await?;
        serde_json::from_value(result.body)
            .map_err(|e| Error::Internal(format!("read {schema}.{table} rows deserialize: {e}")))
    }
}

/// Unwrap a scalar/single-object RPC response body to its inner value.
///
/// Scalar functions render as `SELECT fn(...) AS result`, producing a body of
/// `[{"result": <value>}]`. This returns `<value>`, tolerating the bare-object
/// and bare-value shapes too. Returns `Value::Null` for an empty result.
fn unwrap_scalar_body(body: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    let row = match body {
        Value::Array(arr) => match arr.into_iter().next() {
            Some(row) => row,
            None => return Value::Null,
        },
        other => other,
    };
    match row {
        Value::Object(mut obj) => obj.remove("result").unwrap_or(Value::Null),
        other => other,
    }
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
    build_router(AppState::new(cache, config, dialect, backend))
}

/// Build the axum Router from a pre-assembled [`AppState`].
///
/// Use this (instead of [`build_app`]) when you also need to keep the
/// [`AppState`] for in-process [`call_rpc`](AppState::call_rpc) calls — build
/// the state once via [`AppState::new`], clone it for the router, and retain a
/// clone for RPC, so both share one data cache and schema snapshot.
pub fn build_router(state: AppState) -> Router {
    let routing = &state.config.routing;
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

        // Use cached OpenAPI spec, regenerating only when schema cache changes.
        let cache = state.cache.load();
        let cache_ptr = Arc::as_ptr(&cache) as usize;

        // Fast path: check if we already have a cached spec for this schema version.
        let cached_val = {
            let guard = state.openapi_cache.lock().unwrap();
            if guard.0 == cache_ptr {
                guard.1.clone()
            } else {
                None
            }
        };

        if let Some(val) = cached_val {
            return (StatusCode::OK, Json(val)).into_response();
        }

        // Slow path: generate and cache.
        let spec = openapi::generate_spec(&cache, &state.config);
        match serde_json::to_value(&spec) {
            Ok(val) => {
                let mut guard = state.openapi_cache.lock().unwrap();
                *guard = (cache_ptr, Some(val.clone()));
                (StatusCode::OK, Json(val)).into_response()
            }
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

    // Parse preferences early — needed for ExecContext and response formatting.
    // `handling=strict` rejects unknown Prefer tokens with 400 (PGRST122).
    let (mut preferences, unknown_prefs) = headers
        .get("prefer")
        .and_then(|v| v.to_str().ok())
        .map(Preferences::parse)
        .unwrap_or_default();

    // `Prefer: tx=` is ignored unless the server opts in; drop it so it isn't
    // applied (build_exec_context) nor echoed in Preference-Applied.
    if !state.config.tx_allow_override {
        preferences.tx = None;
    }

    if preferences.handling == Some(pgvis_core::preferences::PreferHandling::Strict)
        && !unknown_prefs.is_empty()
    {
        return response::format_error(&Error::Parse {
            message: format!("Invalid preference: {}", unknown_prefs.join(", ")),
            detail: None,
            code: pgvis_core::error::ErrorCode::InvalidPreference,
        });
    }

    // Authorization first: verify the JWT BEFORE planning so unauthenticated
    // requests can't enumerate schema/tables or consume planning work.
    let auth = match verify_jwt(headers, &state.config) {
        Ok(auth) => auth,
        Err(response) => return response,
    };

    // 1. Build the adapter-agnostic ApiRequest. Parse failures (malformed
    //    select/filter/order/logic/range) surface as 400 rather than being
    //    silently dropped (PostgREST fail-closed behaviour).
    let api_request = match build_api_request(
        schema,
        target,
        method.clone(),
        is_rpc,
        headers,
        params,
        body,
        &preferences,
    ) {
        Ok(req) => req,
        Err(err) => return response::format_error(&err),
    };

    // 2. Plan the request against the schema cache
    let plan = match plan_request(&api_request, &cache, &state.dialect, &state.config) {
        Ok(plan) => plan,
        Err(err) => return response::format_error(&err),
    };

    // For Inspect plans: not yet implemented — return 501 (was misreporting 200).
    if let ActionPlan::Inspect(_) = &plan {
        let resp = serde_json::json!({
            "code": "PGV001",
            "message": "inspect endpoint is not yet implemented",
        });
        return (StatusCode::NOT_IMPLEMENTED, Json(resp)).into_response();
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

    // 4. Build ExecContext (JWT already verified above, before planning).
    let is_mutation = matches!(&plan, ActionPlan::Mutate(_));
    let exec_ctx = build_exec_context(&state.config, &auth, &preferences, is_mutation);

    // 4b. Data cache: compute key and check for cache hit (reads only).
    //     When a `pre_request` hook is configured we bypass the read cache
    //     entirely, otherwise a cache hit would skip `backend.execute` and the
    //     hook would never run (it must observe every request).
    let cache_key = if state.config.pre_request.is_some() {
        None
    } else if let ActionPlan::Read(ref read_plan) = plan {
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
                    None,
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

    // 5d. Scalar RPC: PostgREST returns the bare scalar for a singular,
    // non-set, non-composite function. Our renderer produces `[{"result": v}]`,
    // so unwrap it to `v` in the response layer (core is untouched).
    let mut result = result;
    if let ActionPlan::Call(call_plan) = &plan {
        if call_plan.is_singular
            && !call_plan.function_info.returns_set
            && !call_plan.function_info.returns_table
        {
            result.body = unwrap_result_wrapper(std::mem::take(&mut result.body));
        }
    }

    // 6. Extract cursor column from the plan (for X-Next-Cursor header)
    let cursor_column = match &plan {
        ActionPlan::Read(read_plan) => read_plan.range.cursor_column.clone(),
        _ => None,
    };

    // 6b. Mutation info for status/Location selection (derived from the plan).
    let mutation = match &plan {
        ActionPlan::Mutate(mutate_plan) => {
            let was_insert =
                matches!(mutate_plan.mutation, pgvis_core::plan::types::MutationType::Insert { .. });
            Some(response::MutationInfo {
                was_insert,
                primary_key_columns: mutate_plan.table_info.primary_key_columns.clone(),
                table_path: Some(build_table_path(&state.config, &mutate_plan.target.name)),
            })
        }
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
        mutation.as_ref(),
    )
}

/// Unwrap a scalar RPC body of shape `[{"result": v}]` (or `{"result": v}`) to
/// the bare inner value. Leaves other shapes untouched. Used to match
/// PostgREST's bare-scalar response for singular scalar functions.
fn unwrap_result_wrapper(body: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    let is_result_wrapper = |v: &Value| {
        v.as_object()
            .map(|o| o.len() == 1 && o.contains_key("result"))
            .unwrap_or(false)
    };
    match body {
        Value::Array(arr) if arr.len() == 1 && is_result_wrapper(&arr[0]) => {
            let mut arr = arr;
            match arr.remove(0) {
                Value::Object(mut o) => o.remove("result").unwrap_or(Value::Null),
                other => other,
            }
        }
        Value::Object(mut o) if o.len() == 1 && o.contains_key("result") => {
            o.remove("result").unwrap_or(Value::Null)
        }
        other => other,
    }
}

/// Build the request path for a table (best-effort, for the `Location` header).
///
/// Mirrors the route shapes in [`build_router`] for the non-`schema_in_path`
/// modes. When `schema_in_path` is set the schema segment is included.
fn build_table_path(config: &Config, table: &str) -> String {
    let routing = &config.routing;
    let prefix = routing.normalized_prefix();
    let base = if prefix.is_empty() {
        String::new()
    } else {
        format!("/{prefix}")
    };
    if routing.schema_in_path {
        format!("{base}/{}/{table}", routing.default_schema)
    } else {
        format!("{base}/{table}")
    }
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
) -> Result<ApiRequest, Error> {
    let _ = preferences; // Will be used for count strategy, etc.

    // Parse select parameter — a malformed select is a 400 (PGRST100), NOT a
    // silent fall back to `SELECT *` (which would over-expose columns).
    let select = match params.get("select") {
        Some(s) => {
            let parsed = query_params::parse_select(s)
                .map_err(|e| Error::invalid_select(e.to_string()))?;
            if parsed.is_empty() {
                vec![SelectItem::Star]
            } else {
                parsed
            }
        }
        None => vec![SelectItem::Star],
    };

    // GET-based RPC: query parameters are the function's named arguments, not
    // row filters. Turn them into a `RequestBody::Single` object so the planner
    // resolves them as call params. Filters/order/range don't apply to a call.
    let is_get_rpc = is_rpc && matches!(method, RequestMethod::Get | RequestMethod::Head);
    if is_get_rpc {
        let args = rpc_args_from_params(params);
        return Ok(ApiRequest {
            schema,
            target,
            method,
            is_rpc,
            select,
            filters: Vec::new(),
            order: Vec::new(),
            range: None,
            preferences: preferences.clone(),
            body: Some(RequestBody::Single(args)),
            on_conflict: None,
            columns: None,
            logic_filters: Vec::new(),
            cursor: None,
        });
    }

    // Parse filters from query params (columns not named select/order/limit/offset)
    let filters = parse_filters_from_params(params)?;

    // Parse order — extract only direct OrderTerms (skip relation terms for now).
    // A malformed order is a 400.
    let order = match params.get("order") {
        Some(s) => {
            let items =
                query_params::parse_order(s).map_err(|e| Error::Parse {
                    message: e.to_string(),
                    detail: None,
                    code: pgvis_core::error::ErrorCode::InvalidOrder,
                })?;
            items
                .into_iter()
                .filter_map(|item| match item {
                    OrderItem::Term(t) => Some(t),
                    OrderItem::Relation(_) => None,
                })
                .collect()
        }
        None => Vec::new(),
    };

    // Parse range (limit/offset) — non-numeric values are a 416 InvalidRange.
    let range = parse_range_from_params(params)?;

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

    // Parse logic filters (and=, or=, not.and=, not.or=) — malformed logic → 400.
    let logic_filters = parse_logic_filters_from_params(params)?;

    // Parse cursor pagination (cursor_column, cursor_value)
    let cursor = parse_cursor_from_params(params);

    Ok(ApiRequest {
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
    })
}

/// The result of JWT verification — either authenticated claims or anonymous.
pub(crate) struct AuthResult {
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
pub(crate) fn verify_jwt(headers: &HeaderMap, config: &Config) -> Result<AuthResult, Response> {
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

    // For asymmetric algorithms a PEM parse failure is a configuration error —
    // do NOT silently fall back to HMAC (that would let an attacker forge tokens
    // by knowing the *public* key). HMAC algorithms use the shared secret.
    let jwt_config_error = || {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "PGRST301",
                "message": "JWT verification is misconfigured",
                "details": "The configured jwt_secret is not a valid PEM key for the selected algorithm",
                "hint": "Provide a valid PEM public key for RS256/EdDSA, or use an HS* algorithm with a shared secret",
            })),
        )
            .into_response()
    };

    let decoding_key = match algorithm {
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            DecodingKey::from_secret(secret.as_bytes())
        }
        Algorithm::RS256 => match DecodingKey::from_rsa_pem(secret.as_bytes()) {
            Ok(key) => key,
            Err(e) => {
                tracing::error!(error = %e, "RS256 JWT public key (PEM) failed to parse");
                return Err(jwt_config_error());
            }
        },
        Algorithm::EdDSA => match DecodingKey::from_ed_pem(secret.as_bytes()) {
            Ok(key) => key,
            Err(e) => {
                tracing::error!(error = %e, "EdDSA JWT public key (PEM) failed to parse");
                return Err(jwt_config_error());
            }
        },
        _ => DecodingKey::from_secret(secret.as_bytes()),
    };

    let mut validation = Validation::new(algorithm);
    validation.validate_exp = true;
    // PostgREST ignores the `aud` claim unless `jwt-aud` is configured. Disable
    // aud validation (jsonwebtoken 9 defaults it on), otherwise any token that
    // carries an `aud` is rejected.
    validation.validate_aud = false;
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
    // `Prefer: tx=commit/rollback` is only honoured when the server opts in via
    // `tx_allow_override`; otherwise the client cannot override transaction end.
    let tx_end = if config.tx_allow_override {
        preferences.tx.and_then(|tx| match tx {
            PreferTx::Commit => Some(TxEnd::Commit),
            PreferTx::Rollback => Some(TxEnd::Rollback),
        })
    } else {
        None
    };

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
) -> Result<Vec<pgvis_core::query_params::Filter>, Error> {
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
        // Parse as a filter: column=operator.value. A malformed filter is a 400
        // (PGRST100), not silently dropped — dropping would broaden the result set.
        let filter = query_params::parse_filter(key, value)
            .map_err(|e| Error::invalid_filter(e.to_string()))?;
        filters.push(filter);
    }

    // Sort by column name for deterministic SQL output
    filters.sort_by(|a, b| a.field.cmp(&b.field));

    Ok(filters)
}

/// Build the argument object for a GET-based RPC call from query parameters.
///
/// Every non-reserved query parameter becomes a named argument. Values are
/// passed as JSON strings (the function's parameter types drive coercion in the
/// database), except `select` which is reserved for the response projection.
fn rpc_args_from_params(params: &HashMap<String, String>) -> serde_json::Value {
    use serde_json::Value;
    let mut obj = serde_json::Map::new();
    for (key, value) in params {
        if key == "select" {
            continue;
        }
        // Coerce obvious scalar literals so bound parameters carry the right JSON
        // type (e.g. an integer argument binds as a number, not text). Anything
        // else stays a string; the function's parameter type drives final casting.
        let coerced = if let Ok(i) = value.parse::<i64>() {
            Value::from(i)
        } else if let Ok(f) = value.parse::<f64>() {
            Value::from(f)
        } else if value == "true" || value == "false" {
            Value::from(value == "true")
        } else {
            Value::String(value.clone())
        };
        obj.insert(key.clone(), coerced);
    }
    Value::Object(obj)
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
fn parse_logic_filters_from_params(
    params: &HashMap<String, String>,
) -> Result<Vec<LogicTree>, Error> {
    let mut trees = Vec::new();

    for (key, value) in params {
        if !is_logic_filter_key(key) {
            continue;
        }
        // A malformed logic filter is a 400 (PGRST100), not silently dropped.
        let node = query_params::parse_logic_tree(key, value)
            .map_err(|e| Error::invalid_filter(e.to_string()))?;
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

    Ok(trees)
}

/// Parse limit/offset from query parameters into a `RangeSpec`.
///
/// Non-numeric `limit`/`offset` values are a 416 InvalidRange (PGRST103), not
/// silently ignored — silently ignoring an invalid `limit` would return the
/// full unpaginated set.
fn parse_range_from_params(
    params: &HashMap<String, String>,
) -> Result<Option<pgvis_core::query_params::RangeSpec>, Error> {
    fn parse_u64(
        params: &HashMap<String, String>,
        key: &str,
    ) -> Result<Option<u64>, Error> {
        match params.get(key) {
            Some(s) => s.parse::<u64>().map(Some).map_err(|_| Error::Parse {
                message: format!("Invalid {key}: {s}"),
                detail: None,
                code: pgvis_core::error::ErrorCode::InvalidRange,
            }),
            None => Ok(None),
        }
    }

    let limit = parse_u64(params, "limit")?;
    let offset = parse_u64(params, "offset")?;

    if limit.is_some() || offset.is_some() {
        Ok(Some(pgvis_core::query_params::RangeSpec { limit, offset }))
    } else {
        Ok(None)
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
