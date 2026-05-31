//! # Response formatting — convert [`QueryResult`] into HTTP responses.
//!
//! Maps the unified `QueryResult` (body, page_total, total_count, response_status,
//! response_headers) into axum HTTP responses with correct status codes, headers,
//! and content negotiation.
//!
//! ## PostgREST Compatibility
//!
//! - `Content-Range` header for paginated responses
//! - `Preference-Applied` header echoing honoured preferences
//! - `Location` header for 201 Created (inserts)
//! - Custom headers from `response.headers` GUC

use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use pgvis_core::backend::QueryResult;
use pgvis_core::plan::types::RequestMethod;
use pgvis_core::preferences::{PreferReturn, Preferences};
use serde_json::Value;

/// Format a [`QueryResult`] into an HTTP response.
///
/// This handles:
/// - Status code selection (200/201/204/206 depending on context)
/// - Content-Range header for pagination
/// - GUC-override status and headers
/// - Prefer: return=minimal → 204 with empty body
/// - HEAD requests → headers only
pub fn format_response(
    result: &QueryResult,
    method: &RequestMethod,
    preferences: &Preferences,
    is_singular: bool,
    request_offset: Option<u64>,
    cursor_column: Option<&str>,
) -> Response {
    let mut headers = HeaderMap::new();

    // Determine base status code
    let mut status = determine_status(result, method);

    // Content-Range header
    let content_range = build_content_range(result, request_offset);
    if let Ok(val) = HeaderValue::from_str(&content_range) {
        headers.insert("content-range", val);
    }

    // X-Next-Cursor header (for cursor-based pagination)
    if let Some(col) = cursor_column {
        if let Some(cursor_val) = extract_next_cursor(&result.body, col) {
            if let Ok(val) = HeaderValue::from_str(&cursor_val) {
                headers.insert("x-next-cursor", val);
            }
        }
    }

    // If partial content (has pagination), set 206
    if result.page_total.is_some() && result.total_count.is_some() {
        let page = result.page_total.unwrap_or(0);
        let total = result.total_count.unwrap_or(0);
        if page < total && page > 0 {
            status = StatusCode::PARTIAL_CONTENT;
        }
    }

    // GUC-override status
    if let Some(override_status) = result.response_status {
        if let Ok(s) = StatusCode::from_u16(override_status) {
            status = s;
        }
    }

    // GUC-override headers
    if let Some(ref guc_headers) = result.response_headers {
        for (name, value) in guc_headers {
            if let (Ok(n), Ok(v)) = (
                HeaderName::try_from(name.as_str()),
                HeaderValue::from_str(value),
            ) {
                headers.insert(n, v);
            }
        }
    }

    // Content-Type
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/json; charset=utf-8"),
    );

    // Preference-Applied
    let applied = preferences.applied_header();
    if !applied.is_empty() {
        if let Ok(val) = HeaderValue::from_str(&applied) {
            headers.insert("preference-applied", val);
        }
    }

    // Handle Prefer: return=minimal → 204 No Content
    if preferences.return_repr == Some(PreferReturn::Minimal) {
        return (status, headers).into_response();
    }

    // Handle HEAD → headers only
    if matches!(method, RequestMethod::Head) {
        return (status, headers).into_response();
    }

    // Build body
    let body = if is_singular {
        // Singular: unwrap first element from array
        match &result.body {
            Value::Array(arr) if arr.len() == 1 => serde_json::to_vec(&arr[0]).unwrap_or_default(),
            Value::Array(arr) if arr.is_empty() => {
                // 406 Not Acceptable for singular with no rows
                status = StatusCode::NOT_ACCEPTABLE;
                serde_json::to_vec(&serde_json::json!({
                    "code": "PGRST116",
                    "message": "JSON object requested, multiple (or no) rows returned",
                }))
                .unwrap_or_default()
            }
            Value::Array(arr) if arr.len() > 1 => {
                // 406 for singular with multiple rows
                status = StatusCode::NOT_ACCEPTABLE;
                serde_json::to_vec(&serde_json::json!({
                    "code": "PGRST116",
                    "message": "JSON object requested, multiple (or no) rows returned",
                }))
                .unwrap_or_default()
            }
            other => serde_json::to_vec(other).unwrap_or_default(),
        }
    } else {
        serde_json::to_vec(&result.body).unwrap_or_default()
    };

    (status, headers, body).into_response()
}

/// Determine the appropriate status code based on the request method and result.
fn determine_status(result: &QueryResult, method: &RequestMethod) -> StatusCode {
    match method {
        RequestMethod::Post => {
            // POST on table = INSERT → 201 Created
            // POST on RPC = function call → 200 OK (unless overridden by GUC)
            if result.was_insert == Some(true) {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            }
        }
        RequestMethod::Put => StatusCode::OK,
        RequestMethod::Patch => StatusCode::OK,
        RequestMethod::Delete => StatusCode::OK,
        _ => StatusCode::OK,
    }
}

/// Build the Content-Range header value.
///
/// Format: `{offset}-{offset+page-1}/{total}` or `*/{total}` or `*/*`
fn build_content_range(result: &QueryResult, request_offset: Option<u64>) -> String {
    let page = result.page_total.unwrap_or(0);
    let total = match result.total_count {
        Some(t) => t.to_string(),
        None => "*".to_string(),
    };

    if page == 0 {
        format!("*/{total}")
    } else {
        let offset = request_offset.unwrap_or(0);
        let range_end = offset + (page as u64) - 1;
        format!("{offset}-{range_end}/{total}")
    }
}

/// Format an error into an HTTP response matching PostgREST's error shape.
///
/// ```json
/// {
///   "code": "PGRST200",
///   "message": "...",
///   "details": "...",
///   "hint": "..."
/// }
/// ```
pub fn format_error(err: &pgvis_core::error::Error) -> Response {
    let status =
        StatusCode::from_u16(err.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    let body = match err {
        pgvis_core::error::Error::Execution {
            message,
            db_code,
            detail,
            hint,
        } => serde_json::json!({
            "code": db_code.as_deref().unwrap_or(err.code().as_str()),
            "message": message,
            "details": detail,
            "hint": hint,
        }),
        pgvis_core::error::Error::Plan {
            message,
            detail,
            hint,
            ..
        } => serde_json::json!({
            "code": err.code().as_str(),
            "message": message,
            "details": detail,
            "hint": hint,
        }),
        other => serde_json::json!({
            "code": other.code().as_str(),
            "message": other.to_string(),
            "details": null,
            "hint": null,
        }),
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/json; charset=utf-8"),
    );

    (
        status,
        headers,
        serde_json::to_vec(&body).unwrap_or_default(),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Cursor pagination helpers
// ---------------------------------------------------------------------------

/// Extract the next cursor value from the last row of the response body.
///
/// Looks at the last element of the JSON array result and extracts the value
/// of the specified cursor column. Returns `None` if the body is empty or
/// the column is not present.
fn extract_next_cursor(body: &Value, cursor_column: &str) -> Option<String> {
    let rows = body.as_array()?;
    let last_row = rows.last()?;
    let cursor_val = last_row.get(cursor_column)?;

    Some(match cursor_val {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => return None,
        other => other.to_string(),
    })
}
