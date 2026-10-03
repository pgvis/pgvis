//! # SQL dialect description — pure data, no I/O.
//!
//! The [`Dialect`] struct describes how the SQL builder should emit
//! dialect-specific constructs (identifiers, placeholders, JSON aggregation,
//! feature availability). It is **pure data** — no trait, no dynamic dispatch
//! in the SQL-rendering hot path.
//!
//! ## Design Choice: Struct, Not Trait
//!
//! A trait-based dialect would require dynamic dispatch (`dyn Dialect`) on every
//! SQL fragment emission — thousands of virtual calls per complex query. Instead,
//! `Dialect` is a flat struct with boolean flags that the SQL builder pattern-matches
//! on. The flags are cheap to test (branch prediction friendly) and the struct is
//! `'static` (zero-cost to pass around).
//!
//! ## PostgREST Comparison
//!
//! PostgREST has no dialect concept — it only supports Postgres. All SQL rendering
//! is hardcoded. pgvis needs dialect awareness because it supports both Postgres
//! and SQLite, which differ in:
//! - Placeholder syntax (`$1` vs `?`)
//! - JSON aggregation functions (`json_agg` vs `json_group_array`)
//! - Feature availability (roles, LISTEN/NOTIFY, routines, array/range ops)
//!
//! ## Static Constants
//!
//! [`POSTGRES`] and [`SQLITE`] are `&'static Dialect` constants defined here
//! (not in driver crates) so the SQL builder can be tested in isolation without
//! pulling in `tokio-postgres` or `sqlx`.

use serde::{Deserialize, Serialize};

/// How prepared statement placeholders are rendered.
///
/// # Variants
///
/// - `Numbered` — PostgreSQL style: `$1`, `$2`, `$3`, …
/// - `Question` — SQLite/MySQL style: `?`, `?`, `?`, …
///
/// The SQL builder maintains a counter and calls
/// `placeholder.render(n)` to emit the correct syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Placeholder {
    /// Numbered: `$1`, `$2`, … (PostgreSQL)
    Numbered,
    /// Positional: `?` (SQLite, MySQL)
    Question,
}

impl Placeholder {
    /// Render a placeholder for the given 1-based parameter position.
    ///
    /// - `Numbered` → `"$1"`, `"$2"`, etc.
    /// - `Question` → always `"?"`
    pub fn render(self, position: u32) -> String {
        match self {
            Self::Numbered => format!("${position}"),
            Self::Question => "?".to_string(),
        }
    }
}

/// Describes how the SQL builder should emit dialect-specific constructs.
///
/// This is the central configuration point for multi-backend SQL generation.
/// Every difference between Postgres and SQLite SQL output is captured here.
///
/// # Usage
///
/// ```rust
/// use pgvis_core::dialect::POSTGRES;
///
/// // The SQL builder uses dialect fields to choose syntax:
/// assert_eq!(POSTGRES.identifier_quote, '"');
/// assert_eq!(POSTGRES.json_array_agg, "json_agg");
/// assert!(POSTGRES.supports_returning);
/// assert!(POSTGRES.has_routines);
/// ```
///
/// # Categories of Fields
///
/// 1. **Syntax** — `identifier_quote`, `placeholder`
/// 2. **JSON functions** — `json_array_agg`, `json_object`
/// 3. **Feature gates** — boolean flags like `supports_returning`, `has_routines`
///
/// The REST/MCP layers check feature gates before exposing functionality.
/// For example, `/rpc/*` routes are only registered when `has_routines = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dialect {
    // --- Syntax -----------------------------------------------------------
    /// Character used to quote identifiers (table/column names).
    ///
    /// Both Postgres and SQLite use `"` (double-quote), but this is explicit
    /// for future backends that might differ (e.g. MySQL uses backtick).
    pub identifier_quote: char,

    /// How prepared statement placeholders are rendered.
    pub placeholder: Placeholder,

    // --- JSON aggregation functions ----------------------------------------
    /// Function name for aggregating rows into a JSON array.
    ///
    /// - Postgres: `"json_agg"` — `json_agg(row_to_json(_t))`
    /// - SQLite: `"json_group_array"` — `json_group_array(json_object(...))`
    pub json_array_agg: &'static str,

    /// Function name for building a JSON object from key-value pairs.
    ///
    /// - Postgres: `"json_build_object"` — `json_build_object('k1', v1, 'k2', v2)`
    /// - SQLite: `"json_object"` — `json_object('k1', v1, 'k2', v2)`
    pub json_object: &'static str,

    // --- Feature gates: DDL/Transaction capabilities -----------------------
    /// Whether `INSERT/UPDATE/DELETE ... RETURNING *` is supported.
    ///
    /// - Postgres: always true
    /// - SQLite: true since 3.35 (April 2021)
    pub supports_returning: bool,

    /// Whether the database has a role/user system for row-level security.
    ///
    /// - Postgres: true — `SET LOCAL role = 'web_user'`
    /// - SQLite: false — no role system
    pub supports_roles: bool,

    /// Whether the database supports push-based schema change notifications.
    ///
    /// - Postgres: true — `LISTEN pgvis` / `NOTIFY pgvis`
    /// - SQLite: false — must poll `PRAGMA schema_version` or use file watching
    pub supports_listen_notify: bool,

    /// Whether `SET LOCAL` (transaction-scoped variable setting) is supported.
    ///
    /// Required for role switching, claim propagation, and GUC-driven response
    /// headers/status. Without this, the backend skips those operations.
    ///
    /// - Postgres: true
    /// - SQLite: false
    pub supports_set_local: bool,

    /// Whether the database has schema namespacing (multiple schemas).
    ///
    /// When true, `Accept-Profile`/`Content-Profile` headers select the schema.
    /// When false, those headers are silently ignored.
    ///
    /// - Postgres: true — `public`, `api`, custom schemas
    /// - SQLite: false — single namespace per file
    pub schema_namespacing: bool,

    // --- Feature gates: Query capabilities ---------------------------------
    /// Whether stored functions/procedures are available (`/rpc/*` routes).
    ///
    /// When false, no `/rpc/*` routes are registered, no `call_<fn>` MCP tools
    /// are created, and the routines map in SchemaCache is empty.
    ///
    /// - Postgres: true — `pg_proc` + `SELECT fn(args)`
    /// - SQLite: false — no row-set-returning functions
    pub has_routines: bool,

    /// Whether aggregate functions (`sum`, `avg`, `max`, `min`, `count`) are
    /// supported in the `select` parameter.
    ///
    /// Both Postgres and SQLite support standard SQL aggregates, so this is
    /// `true` for both. Gated separately from `Config::aggregates_enabled`
    /// (which is the admin on/off switch).
    ///
    /// - Postgres: true
    /// - SQLite: true
    pub supports_aggregates: bool,

    /// Whether native `ILIKE` (case-insensitive LIKE) is supported.
    ///
    /// - Postgres: true — native `ILIKE` operator
    /// - SQLite: false — must rewrite to `LOWER(col) LIKE LOWER(pattern)`
    pub supports_ilike: bool,

    /// Whether native regex matching (`~`, `~*`) is supported.
    ///
    /// - Postgres: true — `col ~ pattern` / `col ~* pattern`
    /// - SQLite: false — requires `REGEXP` loadable extension; may be rejected
    pub supports_regex_match: bool,

    /// Whether full-text search is supported.
    ///
    /// - Postgres: true — `tsvector` + `to_tsquery` with configurable language
    /// - SQLite: true — FTS5 virtual tables (different syntax, opt-in per table)
    pub supports_fts: bool,

    /// Whether array containment/overlap operators are supported (`cs`, `cd`, `ov`).
    ///
    /// - Postgres: true — `@>`, `<@`, `&&` on arrays and JSONB
    /// - SQLite: false — no native array type; these operators are rejected
    pub supports_array_ops: bool,

    /// Whether range operators are supported (`sl`, `sr`, `nxr`, `nxl`, `adj`).
    ///
    /// - Postgres: true — range types with `<<`, `>>`, `&<`, `&>`, `-|-`
    /// - SQLite: false — no range types; these operators are rejected
    pub supports_range_ops: bool,

    /// Whether `Prefer: count=estimated` (via `EXPLAIN` parsing) is supported.
    ///
    /// - Postgres: true — can parse `EXPLAIN` output for row estimates
    /// - SQLite: false — no equivalent; falls back to `count=exact`
    pub supports_estimated_count: bool,

    /// Whether filter quantifiers `op(any)` / `op(all)` are supported.
    ///
    /// - Postgres: true — `= ANY(ARRAY[...])` / `= ALL(ARRAY[...])`
    /// - SQLite: false — must be rejected or rewritten to OR/AND fan-out
    pub supports_quantifiers: bool,

    /// Whether `SET LOCAL timezone = '...'` (`Prefer: timezone`) is supported.
    ///
    /// - Postgres: true
    /// - SQLite: false — no session timezone concept
    pub supports_set_timezone: bool,

    /// Whether `IS DISTINCT FROM` is supported.
    ///
    /// - Postgres: true (always)
    /// - SQLite: true (since 3.39, June 2022)
    pub supports_is_distinct: bool,

    /// Whether `row_to_json(alias)` on a subquery alias is supported.
    ///
    /// Postgres can serialize an entire row by referencing the subquery alias:
    /// `SELECT row_to_json(_sub) FROM (...) AS _sub`. SQLite (and some other
    /// databases) cannot — they require explicit `json_object('k', v, ...)`.
    ///
    /// Used by the embed renderer to choose serialization strategy.
    ///
    /// - Postgres: true
    /// - SQLite: false
    pub supports_row_to_json: bool,

    /// Whether an INSERT can read its rows from one JSON parameter into the
    /// table's row type (`json_populate_recordset(NULL::t, $1)`).
    ///
    /// One parameter for any row count, so the SQL text is constant and there
    /// is no bind-parameter limit; JSON arrays and objects convert to array,
    /// composite and json columns. Otherwise every value is its own parameter.
    ///
    /// - Postgres: true
    /// - SQLite: false
    pub supports_json_recordset: bool,

    /// System column identifying a table row, for limited UPDATE/DELETE
    /// (`WHERE <id> IN (SELECT <id> ... ORDER BY ... LIMIT n)`). Views have
    /// none, so a limited mutation on a view is rejected.
    ///
    /// - Postgres: `ctid`
    /// - SQLite: `rowid`
    pub row_identifier: &'static str,
}

// ---------------------------------------------------------------------------
// Static dialect constants
// ---------------------------------------------------------------------------

/// The PostgreSQL dialect — all features enabled.
///
/// Used by `PgBackend::dialect()` and by SQL builder tests.
pub static POSTGRES: Dialect = Dialect {
    identifier_quote: '"',
    placeholder: Placeholder::Numbered,
    json_array_agg: "json_agg",
    json_object: "json_build_object",
    supports_returning: true,
    supports_roles: true,
    supports_listen_notify: true,
    supports_set_local: true,
    schema_namespacing: true,
    has_routines: true,
    supports_aggregates: true,
    supports_ilike: true,
    supports_regex_match: true,
    supports_fts: true,
    supports_array_ops: true,
    supports_range_ops: true,
    supports_estimated_count: true,
    supports_quantifiers: true,
    supports_set_timezone: true,
    supports_is_distinct: true,
    supports_row_to_json: true,
    supports_json_recordset: true,
    row_identifier: "ctid",
};

/// The SQLite dialect — limited feature set.
///
/// Used by `SqliteBackend::dialect()` (planned) and by SQL builder tests.
///
/// # Minimum SQLite Version
///
/// Assumes SQLite ≥ 3.38 (March 2022):
/// - JSON functions built-in by default (3.38+)
/// - `RETURNING` clause (3.35+, April 2021)
/// - `IS DISTINCT FROM` (3.39+, June 2022 — set to false for safety)
pub static SQLITE: Dialect = Dialect {
    identifier_quote: '"',
    placeholder: Placeholder::Question,
    json_array_agg: "json_group_array",
    json_object: "json_object",
    supports_returning: true, // SQLite >= 3.35
    supports_roles: false,
    supports_listen_notify: false,
    supports_set_local: false,
    schema_namespacing: false,
    has_routines: false,
    supports_aggregates: true,
    supports_ilike: false,       // must rewrite via LOWER()
    supports_regex_match: false, // requires REGEXP extension
    supports_fts: true,          // FTS5 virtual tables
    supports_array_ops: false,
    supports_range_ops: false,
    supports_estimated_count: false,
    supports_quantifiers: false, // must fan-out or reject
    supports_set_timezone: false,
    supports_is_distinct: false, // SQLite 3.39+ — conservative default
    supports_row_to_json: false,
    supports_json_recordset: false,
    row_identifier: "rowid",
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder_rendering() {
        assert_eq!(Placeholder::Numbered.render(1), "$1");
        assert_eq!(Placeholder::Numbered.render(42), "$42");
        assert_eq!(Placeholder::Question.render(1), "?");
        assert_eq!(Placeholder::Question.render(99), "?");
    }

    #[test]
    fn postgres_has_all_features() {
        assert!(POSTGRES.supports_returning);
        assert!(POSTGRES.supports_roles);
        assert!(POSTGRES.has_routines);
        assert!(POSTGRES.supports_array_ops);
        assert!(POSTGRES.supports_range_ops);
        assert!(POSTGRES.supports_quantifiers);
    }

    #[test]
    fn sqlite_lacks_pg_specific_features() {
        assert!(!SQLITE.supports_roles);
        assert!(!SQLITE.has_routines);
        assert!(!SQLITE.supports_array_ops);
        assert!(!SQLITE.supports_range_ops);
        assert!(!SQLITE.supports_listen_notify);
        assert!(!SQLITE.supports_set_local);
    }
}
