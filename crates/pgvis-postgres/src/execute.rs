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
/// BEGIN, the session setup (role, claims, statement_timeout), the optional
/// pre-request call and the main query go out as one pipelined flight — a
/// single round trip — followed by COMMIT or ROLLBACK. Statements are unnamed
/// (`query_typed`): no per-request PREPARE round trip, and safe behind
/// transaction-mode poolers. The result is decoded after the transaction ends.
///
/// There is no RAII transaction here: if this future is dropped mid-way, the
/// connection may still be inside the transaction. Callers must not return
/// such a connection to the pool — see [`crate::Checkout`].
pub async fn execute_query(
    client: &Client,
    ctx: &ExecContext,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, Error> {
    let settings = collect_guc_settings(ctx);
    let setup_sql = set_config_sql(settings.len());
    let setup_params: Vec<(&(dyn ToSql + Sync), Type)> = settings
        .iter()
        .flat_map(|(name, value)| [name, value])
        .map(|s| (s as &(dyn ToSql + Sync), Type::TEXT))
        .collect();
    let pre_request_sql = ctx.pre_request.as_deref().map(pre_request_sql);

    let text_params: Vec<TextParam> = params.iter().map(TextParam).collect();
    let main_params: Vec<(&(dyn ToSql + Sync), Type)> = text_params
        .iter()
        .map(|p| (p as &(dyn ToSql + Sync), Type::UNKNOWN))
        .collect();

    // `join!` polls the futures in order; each sends its request on its first
    // poll, so all four go out in one flight, in this order. After a failure
    // the server rejects the rest of the transaction, so the first error is
    // the real one.
    let (begin, setup, pre_request, main) = futures::join!(
        client.batch_execute("BEGIN"),
        async {
            match &setup_sql {
                Some(sql) => client.query_typed(sql, &setup_params).await.map(drop),
                None => Ok(()),
            }
        },
        async {
            match &pre_request_sql {
                Some(sql) => client.batch_execute(sql).await,
                None => Ok(()),
            }
        },
        client.query_typed(sql, &main_params),
    );
    let result = begin
        .map_err(|e| execution_error("BEGIN failed", &e))
        .and(setup.map_err(|e| execution_error("session setup failed", &e)))
        .and(pre_request.map_err(|e| execution_error("pre-request function failed", &e)))
        .and(main.map_err(|e| execution_error("query execution failed", &e)));

    // On error, or when the caller asked for `Prefer: tx=rollback`, roll back.
    let rollback = result.is_err() || matches!(ctx.tx_end, Some(TxEnd::Rollback));
    if rollback {
        if let Err(e) = client.batch_execute("ROLLBACK").await {
            // The result already carries the real error (or the caller asked
            // for rollback); the connection is discarded if it's broken.
            tracing::error!(error = %e, command = "ROLLBACK", "transaction end failed");
        }
    } else if let Err(e) = client.batch_execute("COMMIT").await {
        tracing::error!(error = %e, command = "COMMIT", "transaction end failed");
        return Err(execution_error("COMMIT failed", &e));
    }

    // Decode only now, with the transaction (and its locks) already released.
    extract_cte_result(&result?, ctx.raw_body)
}

/// `SELECT set_config($1, $2, true), set_config($3, $4, true), …` for `n`
/// settings: one statement instead of one round trip per GUC.
fn set_config_sql(n: usize) -> Option<String> {
    if n == 0 {
        return None;
    }
    let calls: Vec<String> = (0..n)
        .map(|i| format!("set_config(${}, ${}, true)", 2 * i + 1, 2 * i + 2))
        .collect();
    Some(format!("SELECT {}", calls.join(", ")))
}

/// `SELECT "schema"."fn"()` for the configured pre-request function. Each
/// part is quoted so a config value can't inject SQL.
fn pre_request_sql(name: &str) -> String {
    let quoted = name
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".");
    format!("SELECT {quoted}()")
}

/// The ordered `(guc_name, value)` pairs applied via `set_config(…, true)`
/// (local to the transaction, like `SET LOCAL`).
///
/// - `role` first, so the later settings run under the target role.
/// - `request.jwt.claims`: every claim as one JSON value, as current PostgREST
///   does. (Per-claim `request.jwt.claim.<key>` GUCs are no longer set.)
/// - `statement_timeout` last.
fn collect_guc_settings(ctx: &ExecContext) -> Vec<(String, String)> {
    let mut settings: Vec<(String, String)> = Vec::new();

    // role — a GUC like any other; set_config validates it as a role name.
    if let Some(role) = &ctx.role {
        settings.push(("role".to_string(), role.clone()));
    }

    if let Some(claims) = &ctx.claims {
        settings.push(("request.jwt.claims".to_string(), claims.to_string()));
    }

    if let Some(timeout_ms) = ctx.statement_timeout {
        settings.push(("statement_timeout".to_string(), format!("{timeout_ms}ms")));
    }

    settings
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
///
/// With `raw`, the body is kept as the database's JSON text (`raw_body`).
fn extract_cte_result(rows: &[tokio_postgres::Row], raw: bool) -> Result<QueryResult, Error> {
    let empty = || bytes::Bytes::from_static(b"[]");
    let Some(row) = rows.first() else {
        // No rows from CTE means something went wrong, but we handle gracefully
        return Ok(QueryResult {
            body: if raw { Value::Null } else { Value::Array(vec![]) },
            total_count: None,
            page_total: Some(0),
            response_status: None,
            response_headers: None,
            was_insert: None,
            raw_body: raw.then(empty),
        });
    };

    // body — json_agg result: as raw JSON text, or parsed (tokio-postgres's
    // `with-serde_json-1` deserializes json/jsonb to serde_json::Value).
    let (body, raw_body) = if raw {
        let text = try_get_column::<RawJson>(row, "body").map_or_else(empty, |r| r.0);
        (Value::Null, Some(text))
    } else {
        (try_get_column(row, "body").unwrap_or(Value::Array(vec![])), None)
    };

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
        raw_body,
    })
}

/// A json/jsonb/text column as the database sent it: JSON text, never parsed.
struct RawJson(bytes::Bytes);

impl<'a> tokio_postgres::types::FromSql<'a> for RawJson {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // jsonb's binary format is a version byte (1) followed by the text.
        let text = if *ty == Type::JSONB {
            raw.strip_prefix(&[1]).ok_or("unsupported jsonb binary version")?
        } else {
            raw
        };
        Ok(Self(bytes::Bytes::copy_from_slice(text)))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB | Type::TEXT)
    }
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
    fn collect_guc_settings_sets_claims_as_one_json_value() {
        let ctx = ExecContext {
            claims: Some(json!({"sub": "user123", "my-claim": "x"})),
            ..Default::default()
        };
        let settings = collect_guc_settings(&ctx);
        let claims: Value = serde_json::from_str(guc(&settings, "request.jwt.claims").unwrap()).unwrap();
        assert_eq!(claims["sub"], "user123");
        // No per-claim `request.jwt.claim.<key>` GUCs any more.
        assert!(settings.iter().all(|(k, _)| !k.starts_with("request.jwt.claim.")));
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
            raw_body: false,
        };
        let settings = collect_guc_settings(&ctx);
        // role must come first so subsequent settings run under the target role.
        assert_eq!(settings[0].0, "role");
        assert!(guc(&settings, "request.jwt.claims").is_some());
        assert_eq!(guc(&settings, "statement_timeout"), Some("30000ms"));
    }

    #[test]
    fn raw_json_keeps_the_text_and_strips_the_jsonb_version_byte() {
        use tokio_postgres::types::FromSql;
        let text = br#"[{"id":1}]"#;
        let json = RawJson::from_sql(&Type::JSON, text).unwrap();
        assert_eq!(&json.0[..], text);
        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(text);
        assert_eq!(&RawJson::from_sql(&Type::JSONB, &jsonb).unwrap().0[..], text);
        assert!(RawJson::from_sql(&Type::JSONB, &[2, b'1']).is_err());
    }

    #[test]
    fn query_result_reads_raw_and_parsed_bodies_alike() {
        let parsed = QueryResult {
            body: json!([{"id": 1}]),
            total_count: None,
            page_total: Some(1),
            response_status: None,
            response_headers: None,
            was_insert: None,
            raw_body: None,
        };
        let raw = QueryResult {
            body: Value::Null,
            raw_body: Some(bytes::Bytes::from_static(br#"[{"id":1}]"#)),
            ..parsed.clone()
        };
        assert_eq!(*raw.json(), *parsed.json());
        assert_eq!(raw.json_bytes(), parsed.json_bytes());
    }

    #[test]
    fn set_config_sql_batches_every_setting_into_one_statement() {
        assert_eq!(set_config_sql(0), None);
        assert_eq!(
            set_config_sql(2).unwrap(),
            "SELECT set_config($1, $2, true), set_config($3, $4, true)"
        );
    }

    #[test]
    fn pre_request_sql_quotes_each_part() {
        assert_eq!(pre_request_sql("auth.check"), "SELECT \"auth\".\"check\"()");
        assert_eq!(pre_request_sql("a\"b"), "SELECT \"a\"\"b\"()");
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
