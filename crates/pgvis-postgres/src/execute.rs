//! # Query execution — parameter binding, transaction management, and result extraction.
//!
//! Bridges the gap between the SQL builder's output (`String` + `Vec<serde_json::Value>`)
//! and tokio-postgres's execution interface.
//!
//! ## Design: Text-Protocol Approach
//!
//! Like PostgREST, all parameter values are sent as text strings. Postgres will coerce
//! them to the correct type based on the query context. This avoids needing to match
//! Rust types to Postgres OIDs at the driver level.

use pgvis_core::backend::{ExecContext, QueryResult, TxEnd};
use pgvis_core::error::Error;
use serde_json::Value;
use tokio_postgres::Client;
use tokio_postgres::types::{Format, IsNull, ToSql, Type};

// ---------------------------------------------------------------------------
// TextParam — sends all values as text for Postgres to coerce
// ---------------------------------------------------------------------------

/// A wrapper that sends `serde_json::Value` as text to Postgres.
///
/// Postgres will coerce the text representation to the correct column type
/// based on query context (same approach PostgREST uses).
///
/// Mapping:
/// - `Value::Null` → SQL NULL
/// - `Value::String(s)` → text `s`
/// - `Value::Number(n)` → text representation of the number
/// - `Value::Bool(b)` → `"true"` / `"false"`
/// - `Value::Array(...)` → JSON text (Postgres parses as array literal or json)
/// - `Value::Object(...)` → JSON text
#[derive(Debug)]
pub struct TextParam<'a>(pub &'a Value);

impl ToSql for TextParam<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Text-protocol approach: all non-null values are written as their raw
        // UTF-8 string representation and sent with `Format::Text` (see
        // `encode_format` below). Postgres coerces the text to the inferred
        // parameter type for every case — including dates, timestamptz, uuid,
        // interval, arrays, numeric, etc.
        match &self.0 {
            Value::Null => Ok(IsNull::Yes),
            Value::String(s) => {
                out.extend_from_slice(s.as_bytes());
                Ok(IsNull::No)
            }
            Value::Number(n) => {
                out.extend_from_slice(n.to_string().as_bytes());
                Ok(IsNull::No)
            }
            Value::Bool(b) => {
                out.extend_from_slice(if *b { b"true" } else { b"false" });
                Ok(IsNull::No)
            }
            // Arrays and objects → serialize as JSON text.
            other => {
                let s = serde_json::to_string(other)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Sync + Send>)?;
                out.extend_from_slice(s.as_bytes());
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    /// Always send parameters using the Postgres text format so that the raw
    /// UTF-8 bytes written by `to_sql` are interpreted as text (not binary).
    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    tokio_postgres::types::to_sql_checked!();
}

// ---------------------------------------------------------------------------
// Execute a CTE-wrapped query
// ---------------------------------------------------------------------------

/// Execute a CTE-wrapped SQL statement within a transaction.
///
/// This is the full execution pipeline:
/// 1. BEGIN transaction
/// 2. SET LOCAL role (if provided)
/// 3. SET LOCAL claims GUCs (if provided)
/// 4. SET LOCAL statement_timeout (if provided)
/// 5. Call pre-request function (if configured)
/// 6. Execute the main SQL with parameters
/// 7. Extract result from the CTE row
/// 8. COMMIT or ROLLBACK based on preference
pub async fn execute_query(
    client: &mut Client,
    ctx: &ExecContext,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, Error> {
    // Open a RAII transaction. If this future is dropped (e.g. client
    // cancellation) before we explicitly commit/rollback, `Transaction`'s Drop
    // impl issues a ROLLBACK, so the pooled connection never returns to the pool
    // with an open transaction or a lingering SET LOCAL role.
    //
    // READ COMMITTED is the default isolation level in Postgres; the previous
    // explicit `BEGIN ISOLATION LEVEL READ COMMITTED` was a no-op relative to
    // the default, so `transaction()` preserves the same semantics.
    let tx = client
        .transaction()
        .await
        .map_err(|e| execution_error("BEGIN failed", &e))?;

    // Run the inner execution; on error the transaction is rolled back on drop.
    let result = execute_inner(&tx, ctx, sql, params).await;

    // Determine transaction end. On error, or when the caller requested
    // `Prefer: tx=rollback`, roll back explicitly; otherwise commit.
    let should_rollback = match &result {
        Err(_) => true,
        Ok(_) => matches!(ctx.tx_end, Some(TxEnd::Rollback)),
    };

    if should_rollback {
        if let Err(tx_err) = tx.rollback().await {
            tracing::error!(error = %tx_err, command = "ROLLBACK", "transaction end failed");
            // Original result already carries the real error (or the caller
            // asked for rollback) — preserve it.
        }
    } else if let Err(tx_err) = tx.commit().await {
        tracing::error!(error = %tx_err, command = "COMMIT", "transaction end failed");
        return Err(execution_error("COMMIT failed", &tx_err));
    }

    result
}

/// Inner execution logic (within the transaction).
///
/// Applies all session setup via parameterized `set_config` statements, then
/// executes the main query with a prepared statement.
async fn execute_inner(
    tx: &tokio_postgres::Transaction<'_>,
    ctx: &ExecContext,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, Error> {
    apply_session_setup(tx, ctx).await?;

    // Execute the main query with parameters.
    // Using prepare() enables per-connection statement caching in tokio-postgres,
    // avoiding repeated parse cycles for identical SQL on the same connection.
    let text_params: Vec<TextParam> = params.iter().map(TextParam).collect();
    let param_refs: Vec<&(dyn ToSql + Sync)> = text_params
        .iter()
        .map(|p| p as &(dyn ToSql + Sync))
        .collect();

    let stmt = tx
        .prepare(sql)
        .await
        .map_err(|e| execution_error("prepare failed", &e))?;
    let rows = tx
        .query(&stmt, &param_refs)
        .await
        .map_err(|e| execution_error("query execution failed", &e))?;

    // Extract result from the CTE row
    extract_cte_result(&rows)
}

/// Apply all session setup (role, JWT claims, statement_timeout, pre-request)
/// to the open transaction.
///
/// GUC values are applied via `set_config($1, $2, true)` (the `true` third
/// argument makes the setting local to the transaction, equivalent to
/// `SET LOCAL`). Using bound parameters avoids SQL-injection surface and, for
/// the bulk `request.jwt.claims` JSON, sidesteps GUC-name restrictions such as
/// hyphenated or leading-digit claim keys.
/// Collect the ordered list of `(guc_name, value)` pairs to apply via
/// `set_config($1, $2, true)`.
///
/// - `role` is set first so subsequent settings run under the target role.
/// - The bulk `request.jwt.claims` JSON is always included; individual
///   `request.jwt.claim.<key>` GUCs are included only for keys that form valid
///   GUC names and have no null bytes.
/// - `statement_timeout` is included last.
fn collect_guc_settings(ctx: &ExecContext) -> Vec<(String, String)> {
    let mut settings: Vec<(String, String)> = Vec::new();

    // role — a GUC like any other; set_config validates it as a role name.
    if let Some(role) = &ctx.role {
        settings.push(("role".to_string(), role.clone()));
    }

    // claims — always set the bulk JSON GUC, plus individual claim GUCs for keys
    // that form valid GUC names.
    if let Some(claims) = &ctx.claims {
        settings.push(("request.jwt.claims".to_string(), claims.to_string()));

        if let Value::Object(map) = claims {
            for (key, val) in map {
                // Only set an individual GUC when the key is a valid GUC name.
                // The bulk request.jwt.claims JSON always carries every claim.
                if !is_safe_guc_key(key) {
                    tracing::debug!(key = %key, "skipping JWT claim with unsafe GUC key");
                    continue;
                }
                let val_str = match val {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                if val_str.contains('\0') {
                    tracing::debug!(key = %key, "skipping JWT claim with null byte in value");
                    continue;
                }
                settings.push((format!("request.jwt.claim.{key}"), val_str));
            }
        }
    }

    // statement_timeout
    if let Some(timeout_ms) = ctx.statement_timeout {
        settings.push(("statement_timeout".to_string(), format!("{timeout_ms}ms")));
    }

    settings
}

async fn apply_session_setup(
    tx: &tokio_postgres::Transaction<'_>,
    ctx: &ExecContext,
) -> Result<(), Error> {
    for (name, value) in collect_guc_settings(ctx) {
        tx.execute("SELECT set_config($1, $2, true)", &[&name, &value])
            .await
            .map_err(|e| execution_error("session setup failed", &e))?;
    }

    // Pre-request function call.
    // pre_request is a qualified function name like "auth.check_request".
    // Quote each identifier part to prevent SQL injection via config values.
    if let Some(pre_req) = &ctx.pre_request {
        let quoted = pre_req
            .split('.')
            .map(|part| quote_ident(part))
            .collect::<Vec<_>>()
            .join(".");
        let stmt = format!("SELECT {quoted}()");
        tx.batch_execute(&stmt)
            .await
            .map_err(|e| execution_error("pre-request function failed", &e))?;
    }

    Ok(())
}

/// Check if a JWT claim key is safe to use as a GUC name component.
///
/// Postgres GUC names are dot-separated identifiers. Each dot-separated part
/// must be a valid identifier (`[A-Za-z_][A-Za-z0-9_]*`) — hyphens and
/// leading digits are rejected by the server. Keys that fail this check are
/// still carried in the bulk `request.jwt.claims` JSON; only the per-claim GUC
/// is skipped.
fn is_safe_guc_key(key: &str) -> bool {
    if key.is_empty() || key.len() > 128 {
        return false;
    }
    key.split('.').all(|part| {
        let mut chars = part.chars();
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
            _ => return false,
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

// ---------------------------------------------------------------------------
// CTE result extraction
// ---------------------------------------------------------------------------

/// Extract a [`QueryResult`] from the CTE-wrapped result rows.
///
/// The CTE produces a single row with columns:
/// - `body` — JSON array (coalesced to '[]')
/// - `page_total` — count of rows on this page
/// - `total_count` — total count (only when Prefer: count=exact)
/// - `response_status` — GUC override (Postgres only)
/// - `response_headers` — GUC override (Postgres only)
fn extract_cte_result(rows: &[tokio_postgres::Row]) -> Result<QueryResult, Error> {
    if rows.is_empty() {
        // No rows from CTE means something went wrong, but we handle gracefully
        return Ok(QueryResult {
            body: Value::Array(vec![]),
            total_count: None,
            page_total: Some(0),
            response_status: None,
            response_headers: None,
            was_insert: None,
        });
    }

    let row = &rows[0];

    // body — json_agg result. With `with-serde_json-1` feature, tokio-postgres
    // can directly deserialize json/jsonb columns to serde_json::Value.
    let body: Value = try_get_column(row, "body").unwrap_or(Value::Array(vec![]));

    // page_total
    let page_total: Option<i64> = try_get_column(row, "page_total");

    // total_count (only present when count preference was requested)
    let total_count: Option<i64> = try_get_column(row, "total_count");

    // response_status — from GUC current_setting('response.status', true)
    let response_status_str: Option<String> = try_get_column(row, "response_status");
    let response_status = response_status_str
        .as_deref()
        .and_then(|s| s.parse::<u16>().ok());

    // response_headers — from GUC current_setting('response.headers', true)
    let response_headers_str: Option<String> = try_get_column(row, "response_headers");
    let response_headers = response_headers_str.as_deref().and_then(parse_guc_headers);

    Ok(QueryResult {
        body,
        total_count,
        page_total,
        response_status,
        response_headers,
        was_insert: None,
    })
}

/// Try to get a column value, returning None if the column doesn't exist or is NULL.
fn try_get_column<'a, T: tokio_postgres::types::FromSql<'a>>(
    row: &'a tokio_postgres::Row,
    name: &str,
) -> Option<T> {
    row.try_get(name).ok()
}

// ---------------------------------------------------------------------------
// GUC header parsing
// ---------------------------------------------------------------------------

/// Parse response headers from the GUC value.
///
/// PostgREST format: `[{"Header-Name": "value"}, ...]`
fn parse_guc_headers(raw: &str) -> Option<Vec<(String, String)>> {
    let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(raw).ok()?;
    let mut headers = Vec::new();
    for obj in parsed {
        for (key, val) in obj {
            let value = match val {
                Value::String(s) => s,
                other => other.to_string(),
            };
            headers.push((key, value));
        }
    }
    Some(headers)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Quote a Postgres identifier (role name, schema name) using double-quote escaping.
/// Prevents SQL injection through crafted identifiers.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Create an execution error from a tokio-postgres error.
fn execution_error(context: &str, e: &tokio_postgres::Error) -> Error {
    Error::Execution {
        message: format!("{context}: {e}"),
        db_code: e.code().map(|c| c.code().to_string()),
        detail: e.as_db_error().and_then(|db| db.detail().map(String::from)),
        hint: e.as_db_error().and_then(|db| db.hint().map(String::from)),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // TextParam text-format encoding tests
    // -----------------------------------------------------------------------

    /// All non-null params are sent using the Postgres text format so that the
    /// raw bytes written by `to_sql` are the value's UTF-8 string form.
    fn encoded_text(val: &Value) -> String {
        let param = TextParam(val);
        assert!(matches!(param.encode_format(&Type::TEXT), Format::Text));
        let mut buf = BytesMut::new();
        let is_null = param.to_sql(&Type::TEXT, &mut buf).unwrap();
        assert!(matches!(is_null, IsNull::No));
        String::from_utf8(buf.to_vec()).unwrap()
    }

    #[test]
    fn text_param_reports_text_format() {
        let val = json!(42);
        let param = TextParam(&val);
        // Regardless of the inferred type, the format must be Text.
        assert!(matches!(param.encode_format(&Type::INT4), Format::Text));
        assert!(matches!(param.encode_format(&Type::NUMERIC), Format::Text));
        assert!(matches!(param.encode_format(&Type::TIMESTAMPTZ), Format::Text));
    }

    #[test]
    fn text_param_number_integer() {
        assert_eq!(encoded_text(&json!(12345)), "12345");
    }

    #[test]
    fn text_param_number_large() {
        // Values outside i32/i16 range are simply written as text — Postgres
        // coerces (or rejects) based on the target column type.
        assert_eq!(encoded_text(&json!(3000000000i64)), "3000000000");
    }

    #[test]
    fn text_param_number_decimal() {
        assert_eq!(encoded_text(&json!(123.456)), "123.456");
    }

    #[test]
    fn text_param_string() {
        assert_eq!(encoded_text(&json!("2024-01-15")), "2024-01-15");
    }

    #[test]
    fn text_param_bool() {
        assert_eq!(encoded_text(&json!(true)), "true");
        assert_eq!(encoded_text(&json!(false)), "false");
    }

    #[test]
    fn text_param_array_as_json() {
        assert_eq!(encoded_text(&json!([1, 2, 3])), "[1,2,3]");
    }

    #[test]
    fn text_param_object_as_json() {
        assert_eq!(encoded_text(&json!({"a": 1})), "{\"a\":1}");
    }

    #[test]
    fn text_param_null() {
        let val = json!(null);
        let param = TextParam(&val);
        let mut buf = BytesMut::new();
        let result = param.to_sql(&Type::TEXT, &mut buf);
        assert!(matches!(result, Ok(IsNull::Yes)));
    }

    #[test]
    fn text_param_bool_native() {
        let val = json!(true);
        let param = TextParam(&val);
        let mut buf = BytesMut::new();
        let result = param.to_sql(&Type::BOOL, &mut buf);
        assert!(result.is_ok());
    }

    // -----------------------------------------------------------------------
    // Session setup (set_config) tests
    // -----------------------------------------------------------------------

    /// Look up the value for a GUC name in the collected settings.
    fn guc<'a>(settings: &'a [(String, String)], name: &str) -> Option<&'a str> {
        settings
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn collect_guc_settings_empty() {
        let ctx = ExecContext::default();
        assert!(collect_guc_settings(&ctx).is_empty());
    }

    #[test]
    fn collect_guc_settings_with_role() {
        let ctx = ExecContext {
            role: Some("web_user".to_string()),
            ..Default::default()
        };
        let settings = collect_guc_settings(&ctx);
        assert_eq!(guc(&settings, "role"), Some("web_user"));
    }

    #[test]
    fn collect_guc_settings_role_with_quotes() {
        // The role is passed as a bound parameter value — no quoting/escaping
        // is applied by us; set_config handles it safely.
        let ctx = ExecContext {
            role: Some("user\"name".to_string()),
            ..Default::default()
        };
        let settings = collect_guc_settings(&ctx);
        assert_eq!(guc(&settings, "role"), Some("user\"name"));
    }

    #[test]
    fn collect_guc_settings_with_claims() {
        let ctx = ExecContext {
            claims: Some(json!({"sub": "user123", "role": "admin"})),
            ..Default::default()
        };
        let settings = collect_guc_settings(&ctx);
        assert!(guc(&settings, "request.jwt.claims").is_some());
        assert_eq!(guc(&settings, "request.jwt.claim.sub"), Some("user123"));
        assert_eq!(guc(&settings, "request.jwt.claim.role"), Some("admin"));
    }

    #[test]
    fn collect_guc_settings_skips_null_byte_in_individual_claims() {
        // Null bytes in claim values cause individual GUC settings to be skipped
        // (the bulk JSON is still safe because it is a bound parameter).
        let mut map = serde_json::Map::new();
        map.insert("safe".to_string(), Value::String("good".to_string()));
        map.insert("bad".to_string(), Value::String("user\x00evil".to_string()));
        let ctx = ExecContext {
            claims: Some(Value::Object(map)),
            ..Default::default()
        };
        let settings = collect_guc_settings(&ctx);
        assert!(guc(&settings, "request.jwt.claims").is_some());
        assert!(guc(&settings, "request.jwt.claim.safe").is_some());
        assert!(guc(&settings, "request.jwt.claim.bad").is_none());
    }

    #[test]
    fn collect_guc_settings_with_timeout() {
        let ctx = ExecContext {
            statement_timeout: Some(5000),
            ..Default::default()
        };
        let settings = collect_guc_settings(&ctx);
        assert_eq!(guc(&settings, "statement_timeout"), Some("5000ms"));
    }

    #[test]
    fn collect_guc_settings_full() {
        let ctx = ExecContext {
            role: Some("api_user".to_string()),
            claims: Some(json!({"sub": "abc"})),
            pre_request: Some("auth.pre".to_string()),
            statement_timeout: Some(30000),
            tx_end: None,
            is_mutation: false,
        };
        let settings = collect_guc_settings(&ctx);
        // role must come first so subsequent settings run under the target role.
        assert_eq!(settings[0].0, "role");
        assert!(guc(&settings, "request.jwt.claims").is_some());
        assert_eq!(guc(&settings, "request.jwt.claim.sub"), Some("abc"));
        assert_eq!(guc(&settings, "statement_timeout"), Some("30000ms"));
    }

    // -----------------------------------------------------------------------
    // GUC key safety tests
    // -----------------------------------------------------------------------

    #[test]
    fn is_safe_guc_key_normal() {
        assert!(is_safe_guc_key("sub"));
        assert!(is_safe_guc_key("user_id"));
        assert!(is_safe_guc_key("org.name"));
        assert!(is_safe_guc_key("_leading_underscore"));
    }

    #[test]
    fn is_safe_guc_key_unsafe() {
        assert!(!is_safe_guc_key("")); // empty
        assert!(!is_safe_guc_key("foo bar")); // space
        assert!(!is_safe_guc_key("foo'bar")); // quote
        assert!(!is_safe_guc_key("foo;bar")); // semicolon
        assert!(!is_safe_guc_key("my-claim")); // hyphen (invalid GUC name)
        assert!(!is_safe_guc_key("1abc")); // leading digit
        assert!(!is_safe_guc_key("org.1abc")); // leading digit in a part
        assert!(!is_safe_guc_key(".foo")); // empty first part
        assert!(!is_safe_guc_key(&"a".repeat(200))); // too long
    }

    #[test]
    fn collect_guc_settings_skips_unsafe_claim_keys() {
        let ctx = ExecContext {
            claims: Some(json!({
                "safe_key": "value1",
                "unsafe key": "value2",
                "also;bad": "value3"
            })),
            ..Default::default()
        };
        let settings = collect_guc_settings(&ctx);
        // The safe key should have its individual GUC set.
        assert!(guc(&settings, "request.jwt.claim.safe_key").is_some());
        // Unsafe keys should NOT get individual GUC settings (they still appear
        // in the bulk request.jwt.claims JSON — that's fine).
        assert!(guc(&settings, "request.jwt.claim.unsafe key").is_none());
        assert!(guc(&settings, "request.jwt.claim.also;bad").is_none());
    }

    // -----------------------------------------------------------------------
    // Helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("my_role"), "\"my_role\"");
    }

    #[test]
    fn quote_ident_with_double_quotes() {
        assert_eq!(quote_ident("my\"role"), "\"my\"\"role\"");
    }

    // -----------------------------------------------------------------------
    // GUC header parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_guc_headers_valid() {
        let raw = r#"[{"X-Custom": "value"}, {"Cache-Control": "no-cache"}]"#;
        let headers = parse_guc_headers(raw).unwrap();
        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0], ("X-Custom".to_string(), "value".to_string()));
        assert_eq!(
            headers[1],
            ("Cache-Control".to_string(), "no-cache".to_string())
        );
    }

    #[test]
    fn parse_guc_headers_empty_array() {
        let raw = "[]";
        let headers = parse_guc_headers(raw).unwrap();
        assert!(headers.is_empty());
    }

    #[test]
    fn parse_guc_headers_invalid_json() {
        let raw = "not json";
        assert!(parse_guc_headers(raw).is_none());
    }
}
