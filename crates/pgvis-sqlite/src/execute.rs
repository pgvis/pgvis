//! # Query execution — parameter binding, JSON assembly, and type coercion.
//!
//! Unlike the Postgres backend which uses a CTE wrapper to get a single JSON row,
//! the SQLite backend executes raw SQL and assembles JSON in Rust. This is necessary
//! because SQLite's `json_group_array()` cannot serialize bare row references.

use pgvis_core::backend::{ExecContext, QueryResult, TxEnd};
use pgvis_core::error::Error;
use rusqlite::types::ValueRef;
use serde_json::Value;
use tokio_rusqlite::Connection;

use crate::util::SqliteInternalError;

// ---------------------------------------------------------------------------
// Parameter binding: serde_json::Value → rusqlite params
// ---------------------------------------------------------------------------

/// Convert a slice of `serde_json::Value` into boxed rusqlite params.
fn json_to_rusqlite_params(params: &[Value]) -> Vec<Box<dyn rusqlite::types::ToSql>> {
    params.iter().map(json_value_to_sql).collect()
}

/// Convert a single `serde_json::Value` to a boxed `ToSql` value.
fn json_value_to_sql(val: &Value) -> Box<dyn rusqlite::types::ToSql> {
    match val {
        Value::Null => Box::new(rusqlite::types::Value::Null),
        Value::Bool(b) => Box::new(if *b { 1i64 } else { 0i64 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i)
            } else if let Some(f) = n.as_f64() {
                Box::new(f)
            } else {
                // Fallback: send as text
                Box::new(n.to_string())
            }
        }
        Value::String(s) => Box::new(s.clone()),
        // Arrays and objects → serialize as JSON text
        other => Box::new(serde_json::to_string(other).unwrap_or_default()),
    }
}

// ---------------------------------------------------------------------------
// Row → JSON conversion with type coercion
// ---------------------------------------------------------------------------

/// Convert a rusqlite row column value to a `serde_json::Value`.
///
/// Uses the declared column type to guide coercion:
/// - INTEGER columns with "BOOL" in the type → JSON true/false
/// - TEXT columns with "JSON" in the type → parsed JSON value
/// - BLOB → base64 encoded string
fn column_to_json(value_ref: ValueRef<'_>, declared_type: Option<&str>) -> Value {
    let decl_upper = declared_type.unwrap_or("").to_uppercase();

    match value_ref {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => {
            // Check if this is a boolean column
            if decl_upper.contains("BOOL") {
                Value::Bool(i != 0)
            } else {
                Value::from(i)
            }
        }
        ValueRef::Real(f) => {
            // serde_json::Number from f64
            serde_json::Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        ValueRef::Text(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            // Check if this is a JSON column — parse the text as JSON
            if decl_upper.contains("JSON") {
                serde_json::from_str(&s).unwrap_or_else(|_| Value::String(s.into_owned()))
            } else {
                // Embedded resources are produced by expression columns wrapping
                // `COALESCE((SELECT json_group_array(json_object(...))), '[]')`.
                // SQLite's `sqlite3_column_decltype` returns NULL for expression
                // columns, so `declared_type` (hence `decl_upper`) is empty and we
                // never see the "JSON" hint above. To keep embed nesting working,
                // heuristically parse TEXT that looks like a JSON array/object: if
                // the trimmed value starts with `[` or `{` AND parses cleanly, use
                // the parsed value; otherwise keep it as a plain string so genuine
                // string columns that merely resemble JSON are preserved.
                let trimmed = s.trim_start();
                if trimmed.starts_with('[') || trimmed.starts_with('{') {
                    match serde_json::from_str(&s) {
                        Ok(parsed) => parsed,
                        Err(_) => Value::String(s.into_owned()),
                    }
                } else {
                    Value::String(s.into_owned())
                }
            }
        }
        ValueRef::Blob(bytes) => {
            use serde_json::json;
            // Encode as base64
            let encoded = base64_encode(bytes);
            json!(encoded)
        }
    }
}

/// Simple base64 encoding (no external dependency needed for this).
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Execute a query and assemble JSON result
// ---------------------------------------------------------------------------

/// Execute a SQL statement and assemble the result as a [`QueryResult`].
///
/// This is the main execution pipeline for SQLite:
/// 1. Begin transaction (IMMEDIATE for writes, DEFERRED for reads)
/// 2. Execute the SQL with parameters
/// 3. Iterate rows, converting each to a JSON object
/// 4. Commit or rollback based on preference
/// 5. Return assembled `QueryResult`
pub async fn execute_query(
    conn: &Connection,
    ctx: &ExecContext,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, Error> {
    let sql = sql.to_string();
    let params = params.to_vec();
    let is_mutation = ctx.is_mutation;
    let should_rollback = matches!(ctx.tx_end, Some(TxEnd::Rollback));

    conn.call(move |conn| {
        // Begin transaction
        let tx_behavior = if is_mutation {
            rusqlite::TransactionBehavior::Immediate
        } else {
            rusqlite::TransactionBehavior::Deferred
        };
        let tx = conn
            .transaction_with_behavior(tx_behavior)
            .map_err(|e| tokio_rusqlite::Error::Rusqlite(e))?;

        // Execute and collect results
        let result = execute_and_collect(&tx, &sql, &params);

        // Commit or rollback
        match &result {
            Ok(_) if should_rollback => {
                tx.rollback()
                    .map_err(|e| tokio_rusqlite::Error::Rusqlite(e))?;
            }
            Ok(_) => {
                tx.commit()
                    .map_err(|e| tokio_rusqlite::Error::Rusqlite(e))?;
            }
            Err(_) => {
                // Transaction will auto-rollback on drop
            }
        }

        result.map_err(|e| tokio_rusqlite::Error::Other(Box::new(e)))
    })
    .await
    .map_err(|e| {
        // Recover the SQLSTATE-equivalent db_code from our internal error, which
        // is wrapped in `tokio_rusqlite::Error::Other`. The extended SQLite code
        // was captured at the failure site (before stringification) so constraint
        // violations map to the right HTTP status instead of a blanket 500.
        let db_code = if let tokio_rusqlite::Error::Other(boxed) = &e {
            boxed
                .downcast_ref::<SqliteInternalError>()
                .and_then(|err| err.db_code.clone())
        } else {
            None
        };
        Error::Execution {
            message: format!("SQLite execution failed: {e}"),
            db_code,
            detail: None,
            hint: None,
        }
    })
}

/// Inner execution: prepare, bind, iterate rows, build JSON.
fn execute_and_collect(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, SqliteInternalError> {
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| SqliteInternalError::from_rusqlite("prepare failed", &e))?;

    // Get column metadata
    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
        .collect();
    let col_types: Vec<Option<String>> = (0..col_count)
        .map(|i| stmt.columns()[i].decl_type().map(|s| s.to_string()))
        .collect();

    // Bind parameters
    let rusqlite_params = json_to_rusqlite_params(params);
    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        rusqlite_params.iter().map(|p| p.as_ref()).collect();

    // Execute and iterate rows
    let mut rows = stmt
        .query(param_refs.as_slice())
        .map_err(|e| SqliteInternalError::from_rusqlite("query failed", &e))?;

    let mut body = Vec::with_capacity(64);
    while let Some(row) = rows
        .next()
        .map_err(|e| SqliteInternalError::from_rusqlite("row iteration failed", &e))?
    {
        let mut obj = serde_json::Map::with_capacity(col_count);
        for i in 0..col_count {
            let value_ref = row.get_ref(i).unwrap_or(ValueRef::Null);
            let json_val = column_to_json(value_ref, col_types[i].as_deref());
            obj.insert(col_names[i].clone(), json_val);
        }
        body.push(Value::Object(obj));
    }

    let page_total = body.len() as i64;

    Ok(QueryResult {
        body: Value::Array(body),
        // KNOWN GAP: `Prefer: count=exact` is not implemented on SQLite. The
        // backend receives pre-rendered inner SQL (via `query::render_inner`) with
        // no count column and no count signal in `ExecContext`, so it cannot run
        // the separate `SELECT count(*)` here without a router/core change to
        // thread the count strategy (and an unpaginated count source) through to
        // `Backend::execute`. Planned/estimated counts are Postgres-only.
        total_count: None,
        page_total: Some(page_total),
        response_status: None,  // No GUC mechanism
        response_headers: None, // No GUC mechanism
        was_insert: None,       // No GUC mechanism
    })
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_rusqlite_null() {
        let params = vec![Value::Null];
        let converted = json_to_rusqlite_params(&params);
        assert_eq!(converted.len(), 1);
    }

    #[test]
    fn test_json_to_rusqlite_bool() {
        let params = vec![Value::Bool(true), Value::Bool(false)];
        let converted = json_to_rusqlite_params(&params);
        assert_eq!(converted.len(), 2);
    }

    #[test]
    fn test_json_to_rusqlite_number() {
        let params = vec![Value::from(42), Value::from(3.14), Value::from(i64::MAX)];
        let converted = json_to_rusqlite_params(&params);
        assert_eq!(converted.len(), 3);
    }

    #[test]
    fn test_json_to_rusqlite_string() {
        let params = vec![Value::String("hello".to_string())];
        let converted = json_to_rusqlite_params(&params);
        assert_eq!(converted.len(), 1);
    }

    #[test]
    fn test_json_to_rusqlite_array_as_text() {
        let params = vec![Value::Array(vec![Value::from(1), Value::from(2)])];
        let converted = json_to_rusqlite_params(&params);
        assert_eq!(converted.len(), 1);
    }

    #[test]
    fn test_column_to_json_integer() {
        let val = column_to_json(ValueRef::Integer(42), Some("INTEGER"));
        assert_eq!(val, Value::from(42));
    }

    #[test]
    fn test_column_to_json_integer_as_bool() {
        let val = column_to_json(ValueRef::Integer(1), Some("BOOLEAN"));
        assert_eq!(val, Value::Bool(true));

        let val = column_to_json(ValueRef::Integer(0), Some("BOOL"));
        assert_eq!(val, Value::Bool(false));
    }

    #[test]
    fn test_column_to_json_real() {
        let val = column_to_json(ValueRef::Real(3.14), Some("REAL"));
        assert_eq!(val.as_f64().unwrap(), 3.14);
    }

    #[test]
    fn test_column_to_json_text() {
        let val = column_to_json(ValueRef::Text(b"hello"), Some("TEXT"));
        assert_eq!(val, Value::String("hello".to_string()));
    }

    #[test]
    fn test_column_to_json_text_as_json() {
        let json_str = r#"{"key":"value","count":42}"#;
        let val = column_to_json(ValueRef::Text(json_str.as_bytes()), Some("JSON"));
        assert_eq!(val["key"], "value");
        assert_eq!(val["count"], 42);
    }

    #[test]
    fn test_column_to_json_text_as_jsonb() {
        let json_str = r#"[1,2,3]"#;
        let val = column_to_json(ValueRef::Text(json_str.as_bytes()), Some("JSONB"));
        assert_eq!(
            val,
            Value::Array(vec![Value::from(1), Value::from(2), Value::from(3)])
        );
    }

    #[test]
    fn test_column_to_json_embed_array_no_decltype() {
        // Embedded resources arrive as expression columns with no declared type;
        // TEXT that looks like a JSON array must be parsed, not returned as a string.
        let json_str = r#"[{"id":1},{"id":2}]"#;
        let val = column_to_json(ValueRef::Text(json_str.as_bytes()), None);
        assert_eq!(val, serde_json::json!([{"id": 1}, {"id": 2}]));
    }

    #[test]
    fn test_column_to_json_embed_object_no_decltype() {
        let json_str = r#"{"id":1,"name":"a"}"#;
        let val = column_to_json(ValueRef::Text(json_str.as_bytes()), None);
        assert_eq!(val, serde_json::json!({"id": 1, "name": "a"}));
    }

    #[test]
    fn test_column_to_json_plain_string_not_parsed() {
        // A genuine string column that doesn't look like JSON stays a string.
        let val = column_to_json(ValueRef::Text(b"hello world"), None);
        assert_eq!(val, Value::String("hello world".to_string()));
    }

    #[test]
    fn test_column_to_json_bracket_but_invalid_json_kept_as_string() {
        // Starts with `[` but isn't valid JSON: preserve as string, don't error.
        let val = column_to_json(ValueRef::Text(b"[not json"), None);
        assert_eq!(val, Value::String("[not json".to_string()));
    }

    #[test]
    fn test_column_to_json_null() {
        let val = column_to_json(ValueRef::Null, Some("TEXT"));
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn test_column_to_json_blob() {
        let data = b"\x00\x01\x02\x03";
        let val = column_to_json(ValueRef::Blob(data), Some("BLOB"));
        // Should be a base64-encoded string
        assert!(val.is_string());
        assert_eq!(val.as_str().unwrap(), "AAECAw==");
    }

    #[test]
    fn test_base64_encode_empty() {
        assert_eq!(base64_encode(b""), "");
    }

    #[test]
    fn test_base64_encode_simple() {
        assert_eq!(base64_encode(b"Hello"), "SGVsbG8=");
    }

    #[test]
    fn test_base64_encode_padding() {
        assert_eq!(base64_encode(b"Hi"), "SGk=");
        assert_eq!(base64_encode(b"H"), "SA==");
    }
}
