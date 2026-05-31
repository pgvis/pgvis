//! # Query parameter types — parsed filter, order, and logic tree representations.
//!
//! These are the typed outputs of the query-string parser. They represent
//! PostgREST's horizontal filtering, ordering, and boolean logic DSL.

use serde::{Deserialize, Serialize};

use crate::select_ast::JsonOperation;

/// A filter expression parsed from a query parameter like `age=gte.18`.
///
/// # PostgREST equivalent
///
/// The `Filter` / `OpExpr` types in `ApiRequest/Types.hs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    /// Column name being filtered (e.g. `"age"`, `"name"`).
    pub field: String,

    /// JSON path on the field (e.g. `data->key` → `[Arrow(Key("key"))]`).
    pub json_path: Vec<JsonOperation>,

    /// The filter operator (e.g. `eq`, `gte`, `like`).
    pub operator: Operator,

    /// Whether this filter is negated (`not.eq.5`).
    pub negate: bool,

    /// Optional quantifier (`eq(any).val`, `gte(all).val`).
    pub quantifier: Option<Quantifier>,

    /// The filter value (fully parsed into its typed form).
    pub value: FilterValue,
}

/// The right-hand side of a filter, parsed into its semantic shape.
///
/// The parser produces this directly — downstream code never re-parses strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterValue {
    /// A single scalar value: `eq.John`, `gte.18`, `fts.hello world`.
    Single(String),
    /// A parenthesised list: `in.(a,b,"c,d")` → `["a","b","c,d"]`.
    List(Vec<String>),
    /// `is.null` / `is.notnull` / `is.true` / `is.false` / `is.unknown`.
    Is(IsKind),
}

/// Valid right-hand sides for the `is` operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsKind {
    Null,
    NotNull,
    True,
    False,
    Unknown,
}

/// All supported filter operators.
///
/// Matches PostgREST's operator set. Backend availability is checked
/// against `Dialect` capability flags at plan time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    // --- Comparison (quantifiable) ---
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Neq,

    // --- Pattern matching (quantifiable) ---
    Like,
    /// `ILIKE` — gated on `dialect.supports_ilike`.
    ILike,
    /// `~` — gated on `dialect.supports_regex_match`.
    Match,
    /// `~*` — gated on `dialect.supports_regex_match`.
    IMatch,

    // --- Set ---
    /// `IN (v1,v2,v3)`.
    In,

    // --- Null/Boolean ---
    Is,
    IsDistinct,

    // --- Array/JSONB (Postgres only) ---
    /// `@>` — gated on `dialect.supports_array_ops`.
    Contains,
    /// `<@` — gated on `dialect.supports_array_ops`.
    ContainedBy,
    /// `&&` — gated on `dialect.supports_array_ops`.
    Overlap,

    // --- Range (Postgres only, all gated on `dialect.supports_range_ops`) ---
    StrictlyLeft,
    StrictlyRight,
    NotExtendsRight,
    NotExtendsLeft,
    Adjacent,

    // --- Full-text search (gated on `dialect.supports_fts`) ---
    /// `to_tsquery` with optional language config.
    Fts(Option<String>),
    /// `plainto_tsquery`.
    PlainFts(Option<String>),
    /// `phraseto_tsquery`.
    PhraseFts(Option<String>),
    /// `websearch_to_tsquery`.
    WebFts(Option<String>),
}

/// Filter quantifier — `op(any)` or `op(all)`.
///
/// Applied to quantifiable operators (eq, gt, gte, lt, lte, like, ilike, match, imatch).
/// Gated on `dialect.supports_quantifiers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Quantifier {
    /// `= ANY(ARRAY[...])` — true if any element matches.
    Any,
    /// `= ALL(ARRAY[...])` — true if all elements match.
    All,
}

/// An ordering term parsed from the `order` parameter.
///
/// Example: `order=name.asc.nullsfirst,age.desc`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderTerm {
    pub field: String,
    pub json_path: Vec<JsonOperation>,
    pub direction: OrderDirection,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NullsOrder {
    First,
    Last,
}

/// Boolean logic tree for `and=(...)` / `or=(...)` expressions.
///
/// Supports arbitrary nesting: `and=(or(a.eq.1,b.eq.2),c.eq.3)`
///
/// PostgREST equivalent: `LogicTree` in `ApiRequest/Types.hs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicTree {
    And(Vec<LogicNode>),
    Or(Vec<LogicNode>),
}

/// A node in the boolean logic tree — either a leaf filter or a nested tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicNode {
    Filter(Filter),
    Tree(LogicTree),
    /// Negation of a tree: `not.and=(...)`.
    Not(Box<LogicNode>),
}

/// Range/pagination specification parsed from `limit`, `offset`, and `Range` header.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RangeSpec {
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

/// Cursor pagination specification parsed from `cursor_column` + `cursor_value`.
///
/// When `column` is `None`, the planner defaults to the table's primary key.
/// When `value` is `None`, this indicates the first page (no seek condition).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorSpec {
    /// The column to use as the cursor. `None` means "use the table's primary key".
    pub column: Option<String>,
    /// The cursor value to seek after (exclusive).
    /// `None` means "start from the beginning" (first page).
    pub value: Option<String>,
}
