//! Shared utilities for the pgvis-sqlite crate.

/// Escape a SQLite identifier (double-quote any embedded double-quotes).
pub(crate) fn escape_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Internal error type used across the crate for wrapping low-level SQLite errors
/// before converting them into [`pgvis_core::error::Error`].
#[derive(Debug)]
pub(crate) struct SqliteInternalError {
    pub message: String,
    /// SQLSTATE-equivalent code mapped from a SQLite extended result code, if the
    /// underlying error was a constraint violation. Populated so that
    /// [`pgvis_core::error::Error::http_status`] can produce the correct status
    /// (e.g. 409 for a unique violation instead of a blanket 500).
    pub db_code: Option<String>,
}

impl SqliteInternalError {
    /// Construct an error with only a message (no SQLSTATE mapping).
    pub fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            db_code: None,
        }
    }

    /// Construct an error from a `rusqlite::Error`, capturing the extended result
    /// code and mapping constraint violations to SQLSTATE-equivalent strings
    /// before the error is stringified through the `tokio_rusqlite` layers.
    pub fn from_rusqlite(context: &str, err: &rusqlite::Error) -> Self {
        Self {
            message: format!("{context}: {err}"),
            db_code: sqlite_error_to_sqlstate(err),
        }
    }
}

/// Map a `rusqlite::Error` to a Postgres SQLSTATE-equivalent code recognised by
/// pgvis-core's `Error::http_status`.
///
/// Only constraint-violation extended codes are mapped; anything else yields
/// `None` (surfacing as a generic database error / HTTP 500).
fn sqlite_error_to_sqlstate(err: &rusqlite::Error) -> Option<String> {
    if let rusqlite::Error::SqliteFailure(ffi_err, _) = err {
        // SQLite extended result codes (see sqlite3.h):
        //   SQLITE_CONSTRAINT_UNIQUE     = 2067 -> 23505 unique_violation
        //   SQLITE_CONSTRAINT_PRIMARYKEY = 1555 -> 23505 unique_violation
        //   SQLITE_CONSTRAINT_FOREIGNKEY =  787 -> 23503 foreign_key_violation
        //   SQLITE_CONSTRAINT_NOTNULL    = 1299 -> 23502 not_null_violation
        //   SQLITE_CONSTRAINT_CHECK      =  275 -> 23514 check_violation
        let code = match ffi_err.extended_code {
            2067 | 1555 => "23505",
            787 => "23503",
            1299 => "23502",
            275 => "23514",
            _ => return None,
        };
        return Some(code.to_string());
    }
    None
}

impl std::fmt::Display for SqliteInternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SqliteInternalError {}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::ffi;

    fn failure(extended_code: i32) -> rusqlite::Error {
        rusqlite::Error::SqliteFailure(ffi::Error::new(extended_code), Some("boom".to_string()))
    }

    #[test]
    fn maps_constraint_codes_to_sqlstate() {
        assert_eq!(sqlite_error_to_sqlstate(&failure(2067)).as_deref(), Some("23505")); // UNIQUE
        assert_eq!(sqlite_error_to_sqlstate(&failure(1555)).as_deref(), Some("23505")); // PRIMARYKEY
        assert_eq!(sqlite_error_to_sqlstate(&failure(787)).as_deref(), Some("23503")); // FOREIGNKEY
        assert_eq!(sqlite_error_to_sqlstate(&failure(1299)).as_deref(), Some("23502")); // NOTNULL
        assert_eq!(sqlite_error_to_sqlstate(&failure(275)).as_deref(), Some("23514")); // CHECK
    }

    #[test]
    fn unmapped_codes_yield_none() {
        // A non-constraint failure (e.g. SQLITE_BUSY = 5) should not map.
        assert_eq!(sqlite_error_to_sqlstate(&failure(5)), None);
        assert_eq!(
            sqlite_error_to_sqlstate(&rusqlite::Error::QueryReturnedNoRows),
            None
        );
    }

    #[test]
    fn from_rusqlite_captures_code_and_message() {
        let err = SqliteInternalError::from_rusqlite("insert failed", &failure(2067));
        assert_eq!(err.db_code.as_deref(), Some("23505"));
        assert!(err.message.starts_with("insert failed:"));
    }
}
