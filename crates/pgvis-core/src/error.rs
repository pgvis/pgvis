//! # Error types for pgvis-core.
//!
//! Defines [`Error`] — the unified error type used throughout the pgvis stack,
//! and [`ErrorCode`] — machine-readable error codes compatible with PostgREST's
//! error code scheme.
//!
//! ## Error Philosophy
//!
//! Every error variant maps to a specific HTTP status code and carries a
//! machine-readable [`ErrorCode`]. This enables:
//!
//! 1. **Consistent JSON error responses** across all adapters (REST, MCP)
//! 2. **PostgREST-compatible error codes** for client libraries that expect them
//! 3. **Actionable error messages** that help developers fix the issue
//!
//! ## PostgREST Comparison
//!
//! PostgREST uses `PGRST` prefixed codes (e.g. `PGRST100` for parse errors).
//! pgvis uses the same scheme for compatibility, enabling drop-in replacement.
//!
//! ## JSON Error Shape
//!
//! All errors serialize to:
//! ```json
//! {
//!   "code": "PGRST200",
//!   "message": "Could not find a relationship...",
//!   "details": "Searched for relationship between...",
//!   "hint": "Try using !hint to specify..."
//! }
//! ```

use thiserror::Error;

/// Machine-readable error codes, compatible with PostgREST's `PGRST*` scheme.
///
/// # Code Ranges
///
/// | Range | Category | Example |
/// |---|---|---|
/// | `PGRST0xx` | Connection/pool errors | `PGRST000` — pool timeout |
/// | `PGRST1xx` | Parse errors (query string, body) | `PGRST100` — invalid select |
/// | `PGRST2xx` | Plan/schema resolution errors | `PGRST200` — ambiguous relationship |
/// | `PGRST3xx` | Auth errors | `PGRST301` — JWT expired |
/// | `PGV0xx` | pgvis-specific errors (no PostgREST equivalent) | `PGV001` — backend unsupported op |
///
/// Using string codes (not numeric) for forward compatibility — new codes can
/// be added without breaking semver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    // --- Connection / Infrastructure (0xx) ---
    /// Pool connection timeout or unavailable.
    ConnectionError,

    // --- Parse errors (1xx) ---
    /// Invalid `select` parameter syntax.
    InvalidSelect,
    /// Invalid filter expression syntax.
    InvalidFilter,
    /// Invalid `order` parameter syntax.
    InvalidOrder,
    /// Invalid `limit`/`offset` value.
    InvalidRange,
    /// Invalid request body (JSON parse failure or schema mismatch).
    InvalidBody,
    /// Invalid `Prefer` header value.
    InvalidPreference,

    // --- Plan / Schema resolution errors (2xx) ---
    /// Ambiguous relationship — multiple FKs between same table pair, no hint.
    AmbiguousRelationship,
    /// Ambiguous function — multiple overloads match the given arguments.
    AmbiguousFunction,
    /// Requested resource not found (table, view, function not in schema cache).
    NotFound,
    /// Relationship not found between the specified tables.
    RelationshipNotFound,
    /// Column not found in the target table.
    ColumnNotFound,
    /// Spread on a to-many relationship (not allowed).
    SpreadOnToMany,
    /// Aggregate function used when `db-aggregates-enabled = false`.
    AggregatesDisabled,

    // --- Auth errors (3xx) ---
    /// No JWT token provided when one is required.
    JwtMissing,
    /// JWT signature verification failed.
    JwtInvalid,
    /// JWT token has expired.
    JwtExpired,
    /// Insufficient privileges for the requested operation.
    InsufficientPrivilege,

    // --- Execution errors ---
    /// Database returned an error during query execution.
    DatabaseError,
    /// Statement timeout exceeded.
    StatementTimeout,
    /// `Prefer: max-affected` exceeded.
    MaxAffectedExceeded,

    // --- pgvis-specific ---
    /// Operation not supported by the current backend/dialect.
    UnsupportedOperation,
    /// Internal error (should not happen — indicates a bug).
    Internal,
    /// Configuration error at startup.
    ConfigError,
}

impl ErrorCode {
    /// The PostgREST-compatible string code.
    ///
    /// Returns codes like `"PGRST100"`, `"PGRST200"`, etc. for PostgREST-compatible
    /// errors, and `"PGV001"` for pgvis-specific errors.
    pub fn as_str(&self) -> &'static str {
        match self {
            // All query-string parse failures share PGRST100 in PostgREST.
            Self::ConnectionError => "PGRST000",
            Self::InvalidSelect | Self::InvalidFilter | Self::InvalidOrder => "PGRST100",
            Self::InvalidRange => "PGRST103",
            Self::InvalidBody => "PGRST102",
            Self::InvalidPreference => "PGRST122",
            // PGRST200 = relationship not found; PGRST201 = ambiguous embedding.
            Self::AmbiguousRelationship => "PGRST201",
            Self::RelationshipNotFound => "PGRST200",
            Self::AmbiguousFunction => "PGRST203",
            // PGRST202 = function not found; PGRST204 = column not found; PGRST205 = table not found.
            Self::NotFound => "PGRST205",
            Self::ColumnNotFound => "PGRST204",
            Self::SpreadOnToMany => "PGRST119",
            Self::AggregatesDisabled => "PGRST123",
            // PGRST301 = JWT invalid/expired; PGRST302 = anonymous access disabled.
            Self::JwtMissing => "PGRST302",
            Self::JwtInvalid => "PGRST301",
            Self::JwtExpired => "PGRST301",
            Self::InsufficientPrivilege => "PGRST303",
            Self::DatabaseError => "PGRST400",
            Self::StatementTimeout => "PGRST109",
            Self::MaxAffectedExceeded => "PGRST124",
            Self::UnsupportedOperation => "PGV001",
            Self::Internal => "PGV500",
            Self::ConfigError => "PGV002",
        }
    }

    /// The HTTP status code this error should produce.
    ///
    /// Used by the REST adapter to set the response status. Other adapters
    /// (MCP) may use this for analogous error signalling.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::ConnectionError => 503,
            Self::InvalidSelect
            | Self::InvalidFilter
            | Self::InvalidOrder
            | Self::InvalidBody
            | Self::InvalidPreference => 400,
            // PostgREST returns 416 for an unsatisfiable range (PGRST103).
            Self::InvalidRange => 416,
            // Ambiguous embedding/function → 300 Multiple Choices.
            Self::AmbiguousRelationship | Self::AmbiguousFunction => 300,
            // Table/function not found → 404; relationship/column not found → 400.
            Self::NotFound => 404,
            Self::RelationshipNotFound | Self::ColumnNotFound => 400,
            Self::SpreadOnToMany | Self::AggregatesDisabled => 400,
            Self::JwtMissing | Self::JwtInvalid | Self::JwtExpired => 401,
            Self::InsufficientPrivilege => 403,
            Self::DatabaseError => 500,
            Self::StatementTimeout => 504,
            Self::MaxAffectedExceeded => 400,
            Self::UnsupportedOperation => 400,
            Self::Internal => 500,
            Self::ConfigError => 500,
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The core error type used throughout pgvis.
///
/// Each variant carries:
/// - A human-readable message (via `thiserror` `#[error(...)]`)
/// - An implicit [`ErrorCode`] (via the [`Error::code`] method)
/// - Optional `details` and `hint` for richer error responses
///
/// # JSON Serialisation
///
/// The REST adapter serialises errors as:
/// ```json
/// {
///   "code": "PGRST200",
///   "message": "Could not find a relationship between 'users' and 'orders'",
///   "details": null,
///   "hint": "Try specifying the relationship with !hint syntax"
/// }
/// ```
///
/// # PostgREST Compatibility
///
/// Error codes follow PostgREST's `PGRST*` scheme so existing client libraries
/// (e.g. `supabase-js`, `postgrest-py`) can handle errors without changes.
#[derive(Debug, Error)]
pub enum Error {
    /// Introspection query failed (startup or reload).
    ///
    /// HTTP 503. Typically a connection failure or permission issue.
    #[error("introspection failed: {0}")]
    Introspection(String),

    /// SQL execution failed.
    ///
    /// HTTP status depends on the database error (constraint violation → 409,
    /// permission denied → 403, general → 500).
    #[error("execution failed: {message}")]
    Execution {
        /// Human-readable error message.
        message: String,
        /// Database error code (e.g. PostgreSQL's `23505` for unique violation).
        db_code: Option<String>,
        /// Additional detail from the database.
        detail: Option<String>,
        /// Hint from the database.
        hint: Option<String>,
    },

    /// Query string or body parsing failed.
    ///
    /// HTTP 400.
    #[error("parse error: {message}")]
    Parse {
        /// What went wrong.
        message: String,
        /// Which part of the request failed to parse (for user guidance).
        detail: Option<String>,
        /// Specific error code within the parse family.
        code: ErrorCode,
    },

    /// Plan/schema resolution failed.
    ///
    /// HTTP 404, 300, or 400 depending on the specific code.
    #[error("plan error: {message}")]
    Plan {
        /// What went wrong.
        message: String,
        /// Additional context.
        detail: Option<String>,
        /// Suggested fix.
        hint: Option<String>,
        /// Specific error code within the plan family.
        code: ErrorCode,
    },

    /// Configuration error (startup).
    ///
    /// HTTP 500 (should never reach clients in production).
    #[error("config error: {0}")]
    Config(String),

    /// Authentication/authorization error.
    ///
    /// HTTP 401 or 403.
    #[error("auth error: {message}")]
    Auth {
        /// What went wrong.
        message: String,
        /// Specific error code within the auth family.
        code: ErrorCode,
    },

    /// Operation not supported by the current backend.
    ///
    /// HTTP 400. Returned when a request uses a feature the dialect doesn't support
    /// (e.g. array operators on SQLite, `/rpc/*` on SQLite).
    #[error("unsupported operation: {0}")]
    Unsupported(String),

    /// Internal bug — should not happen.
    ///
    /// HTTP 500. If this is reached, it indicates a pgvis logic error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Pub/sub operation failed.
    ///
    /// HTTP status varies by error code (400, 403, 501, 503).
    #[error("pubsub error: {message}")]
    PubSub {
        /// What went wrong.
        message: String,
        /// Specific pub/sub error code.
        code: crate::pubsub::PubSubErrorCode,
    },
}

impl Error {
    /// The machine-readable error code for this error.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Introspection(_) => ErrorCode::ConnectionError,
            Self::Execution { .. } => ErrorCode::DatabaseError,
            Self::Parse { code, .. } => code.clone(),
            Self::Plan { code, .. } => code.clone(),
            Self::Config(_) => ErrorCode::ConfigError,
            Self::Auth { code, .. } => code.clone(),
            Self::Unsupported(_) => ErrorCode::UnsupportedOperation,
            Self::Internal(_) => ErrorCode::Internal,
            Self::PubSub { .. } => ErrorCode::Internal, // code is in the PubSubErrorCode
        }
    }

    /// The HTTP status code this error should produce.
    pub fn http_status(&self) -> u16 {
        // Execution errors need special handling for database-specific codes
        if let Self::Execution { db_code, .. } = self {
            if let Some(code) = db_code {
                return match code.as_str() {
                    "23505" => 409, // unique_violation
                    "23503" => 409, // foreign_key_violation
                    "23502" => 400, // not_null_violation
                    "23514" => 400, // check_violation
                    "22P02" | "22003" | "22007" => 400, // invalid text/numeric/datetime input
                    "42501" => 403, // insufficient_privilege
                    "42P01" => 404, // undefined_table
                    "42883" => 404, // undefined_function
                    "57014" => 504, // query_canceled (statement timeout)
                    "25006" => 405, // read_only_sql_transaction
                    // Connection-exception class (08xxx) → service unavailable.
                    c if c.starts_with("08") => 503,
                    _ => 500,
                };
            }
        }
        // PubSub errors carry their own HTTP status in PubSubErrorCode
        if let Self::PubSub { code, .. } = self {
            return code.http_status();
        }
        self.code().http_status()
    }

    // --- Convenience constructors ---

    /// Create a parse error for an invalid `select` parameter.
    pub fn invalid_select(message: impl Into<String>) -> Self {
        Self::Parse {
            message: message.into(),
            detail: None,
            code: ErrorCode::InvalidSelect,
        }
    }

    /// Create a parse error for an invalid filter expression.
    pub fn invalid_filter(message: impl Into<String>) -> Self {
        Self::Parse {
            message: message.into(),
            detail: None,
            code: ErrorCode::InvalidFilter,
        }
    }

    /// Create a plan error for an ambiguous relationship.
    pub fn ambiguous_relationship(source: &str, target: &str, constraint_names: &[&str]) -> Self {
        Self::Plan {
            message: format!(
                "Could not find a unique relationship between '{source}' and '{target}'"
            ),
            detail: Some(format!(
                "Multiple relationships found: {}",
                constraint_names.join(", ")
            )),
            hint: Some(
                "Try using !hint to specify the relationship, e.g. select=target!constraint_name(*)"
                    .to_string(),
            ),
            code: ErrorCode::AmbiguousRelationship,
        }
    }

    /// Create a plan error for a not-found resource.
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self::Plan {
            message: format!("Not found: {}", resource.into()),
            detail: None,
            hint: None,
            code: ErrorCode::NotFound,
        }
    }

    /// Create an unsupported operation error.
    pub fn unsupported(operation: impl Into<String>, backend: &str) -> Self {
        Self::Unsupported(format!(
            "{} is not supported on the {} backend",
            operation.into(),
            backend
        ))
    }
}
