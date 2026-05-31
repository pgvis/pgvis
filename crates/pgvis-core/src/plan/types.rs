//! # Plan layer types — fully-resolved, I/O-free action plans.
//!
//! These types represent the output of the planning phase. The plan layer takes
//! a parsed `ApiRequest`, the `SchemaCache`, and the `Dialect`, and produces an
//! `ActionPlan` that the SQL builder can translate directly without further
//! lookups or decisions.

use crate::cache::{Cardinality, IsolationLevel, QualifiedIdentifier, Volatility};
use crate::preferences::Preferences;
use crate::query_params::types::{
    CursorSpec, Filter, FilterValue, LogicTree, NullsOrder, Operator, OrderDirection, OrderTerm,
    RangeSpec,
};
use crate::select_ast::{AggregateFunction, JsonOperation, SelectItem};

// ---------------------------------------------------------------------------
// ApiRequest — adapter-agnostic input to the plan layer
// ---------------------------------------------------------------------------

/// The adapter-agnostic representation of an incoming request.
/// Both REST handlers and MCP tool handlers produce this.
#[derive(Debug, Clone)]
pub struct ApiRequest {
    /// The resolved schema name (from URL path or header).
    pub schema: String,
    /// The target table, view, or function name.
    pub target: String,
    /// HTTP method (or MCP verb equivalent).
    pub method: RequestMethod,
    /// Whether this request targets an RPC function (as opposed to a table).
    /// Set by the adapter layer (REST routes `/rpc/{fn}` vs `/{table}`, MCP verb `call` vs others).
    pub is_rpc: bool,
    /// Parsed `select` parameter items.
    pub select: Vec<SelectItem>,
    /// Parsed filter expressions.
    pub filters: Vec<Filter>,
    /// Parsed `order` parameter.
    pub order: Vec<OrderTerm>,
    /// Parsed range (limit/offset).
    pub range: Option<RangeSpec>,
    /// Parsed `Prefer` header values.
    pub preferences: Preferences,
    /// Request body (for POST/PATCH/PUT).
    pub body: Option<RequestBody>,
    /// On-conflict column for upsert (from `Prefer: resolution=merge-duplicates` + `on_conflict` param).
    pub on_conflict: Option<String>,
    /// Columns specification for upsert conflict target.
    pub columns: Option<Vec<String>>,
    /// Logic tree filters (and/or grouping).
    pub logic_filters: Vec<LogicTree>,
    /// Cursor pagination specification (alternative to offset-based).
    pub cursor: Option<CursorSpec>,
}

/// HTTP method or MCP verb equivalent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestMethod {
    Get,
    Head,
    Post,
    Patch,
    Put,
    Delete,
}

/// The request body — parsed into its semantic shape.
#[derive(Debug, Clone)]
pub enum RequestBody {
    /// Single JSON object.
    Single(serde_json::Value),
    /// Array of JSON objects (bulk insert).
    Bulk(Vec<serde_json::Value>),
    /// Raw text body (for RPC with text content type).
    Raw(String),
}

// ---------------------------------------------------------------------------
// ActionPlan — the top-level output of plan_request()
// ---------------------------------------------------------------------------

/// The fully-resolved action plan — output of `plan_request()`.
/// The SQL builder pattern-matches on this to generate SQL.
#[derive(Debug, Clone)]
pub enum ActionPlan {
    /// SELECT query with optional embedding.
    Read(ReadPlan),
    /// INSERT / UPDATE / DELETE with optional returning.
    Mutate(MutatePlan),
    /// RPC function call.
    Call(CallPlan),
    /// OpenAPI spec request (no SQL needed).
    Inspect(InspectPlan),
}

// ---------------------------------------------------------------------------
// ReadPlan
// ---------------------------------------------------------------------------

/// A fully-resolved read plan — maps to a SELECT query.
#[derive(Debug, Clone)]
pub struct ReadPlan {
    /// The target table/view, fully qualified.
    pub target: QualifiedIdentifier,
    /// Resolved table metadata reference (insertable, columns, etc.).
    pub table_info: ResolvedTableInfo,
    /// What columns/expressions to emit.
    pub select: Vec<ResolvedSelect>,
    /// Embedded resources (joins).
    pub embeds: Vec<EmbeddedResource>,
    /// Resolved filter conditions.
    pub filters: Vec<ResolvedFilter>,
    /// Resolved ordering.
    pub order: Vec<ResolvedOrder>,
    /// Limit/offset with server-side cap applied.
    pub range: ResolvedRange,
    /// Logic tree filters (and/or grouping).
    pub logic_filters: Vec<ResolvedLogicTree>,
    /// Aggregate columns requiring GROUP BY synthesis.
    pub aggregates: Vec<ResolvedAggregate>,
    /// Whether to include total count (and which method).
    pub count: Option<CountStrategy>,
    /// Response preferences (singular, representation, etc.).
    pub preferences: Preferences,
}

// ---------------------------------------------------------------------------
// ResolvedTableInfo
// ---------------------------------------------------------------------------

/// Snapshot of relevant table metadata from the schema cache.
/// Avoids keeping a reference to SchemaCache in the plan.
#[derive(Debug, Clone)]
pub struct ResolvedTableInfo {
    /// Whether this is a view (as opposed to a base table).
    pub is_view: bool,
    /// Whether INSERT is allowed.
    pub insertable: bool,
    /// Whether UPDATE is allowed.
    pub updatable: bool,
    /// Whether DELETE is allowed.
    pub deletable: bool,
    /// Primary key columns (for conflict detection and singular responses).
    pub primary_key_columns: Vec<String>,
}

// ---------------------------------------------------------------------------
// ResolvedSelect
// ---------------------------------------------------------------------------

/// A resolved column/expression in the SELECT list.
#[derive(Debug, Clone)]
pub enum ResolvedSelect {
    /// A concrete column from the table.
    Column(ResolvedColumn),
    /// A computed/aggregate expression.
    Aggregate(ResolvedAggregate),
    /// A star expansion (all columns of the table).
    Star,
    /// An embedded resource's columns (represented as a sub-select or lateral join).
    /// The string is a name reference to an `EmbeddedResource` in the parent plan.
    Embed(String),
}

/// A resolved column reference — validated against the schema cache.
#[derive(Debug, Clone)]
pub struct ResolvedColumn {
    /// Column name.
    pub name: String,
    /// Column alias (from `select=col:alias` syntax).
    pub alias: Option<String>,
    /// JSON path operations (for JSONB columns).
    pub json_path: Vec<JsonOperation>,
    /// The column's data type (from schema cache).
    pub data_type: String,
    /// Whether this column is nullable.
    pub nullable: bool,
}

/// A resolved aggregate function call.
#[derive(Debug, Clone)]
pub struct ResolvedAggregate {
    /// The aggregate function.
    pub function: AggregateFunction,
    /// The column to aggregate (None for count(*)).
    pub column: Option<String>,
    /// Alias for the result.
    pub alias: Option<String>,
}

// ---------------------------------------------------------------------------
// EmbeddedResource
// ---------------------------------------------------------------------------

/// A child resource embedded via a relationship (join).
#[derive(Debug, Clone)]
pub struct EmbeddedResource {
    /// Name used in the select parameter (e.g., "posts" in `select=posts(title)`).
    pub name: String,
    /// Alias if renamed (e.g., `select=my_posts:posts(title)` → alias = "my_posts").
    pub alias: Option<String>,
    /// The fully-resolved join strategy.
    pub join: ResolvedJoin,
    /// The child's own read plan (recursive).
    pub plan: ReadPlan,
    /// Whether this is a spread embed (`...posts(title)` flattens into parent).
    pub is_spread: bool,
}

// ---------------------------------------------------------------------------
// ResolvedJoin
// ---------------------------------------------------------------------------

/// A fully-resolved join — the SQL builder doesn't need to look up anything.
/// This is one of pgvis's key improvements over PostgREST, which passes raw
/// `Relationship` references and re-resolves join shape in the SQL builder.
#[derive(Debug, Clone)]
pub enum ResolvedJoin {
    /// Direct FK relationship (M2O or O2M).
    Direct {
        /// FK columns on the source side.
        source_columns: Vec<String>,
        /// FK columns on the target side.
        target_columns: Vec<String>,
        /// The target table identifier.
        target_table: QualifiedIdentifier,
        /// The relationship cardinality.
        cardinality: Cardinality,
    },
    /// Many-to-many via junction table.
    Junction {
        /// First leg: parent → junction — source columns in the parent table.
        source_columns: Vec<String>,
        /// The junction (join) table.
        junction_table: QualifiedIdentifier,
        /// Junction table columns pointing to the source table.
        junction_source_columns: Vec<String>,
        /// Junction table columns pointing to the target table.
        junction_target_columns: Vec<String>,
        /// Second leg: junction → target — target columns in the target table.
        target_columns: Vec<String>,
        /// The target table identifier.
        target_table: QualifiedIdentifier,
    },
    /// Computed relationship via a function (Postgres-only).
    Computed {
        /// The function name.
        function_name: String,
        /// The function schema.
        function_schema: String,
        /// The target table identifier.
        target_table: QualifiedIdentifier,
        /// The relationship cardinality.
        cardinality: Cardinality,
    },
}

// ---------------------------------------------------------------------------
// ResolvedFilter
// ---------------------------------------------------------------------------

/// A resolved filter condition with dialect-specific rewrite hints.
/// The SQL builder uses the `rewrite` field to generate dialect-appropriate SQL.
#[derive(Debug, Clone)]
pub struct ResolvedFilter {
    /// Column name.
    pub column: String,
    /// The operator.
    pub operator: Operator,
    /// The filter value.
    pub value: FilterValue,
    /// Whether this is negated (`not.eq.5`).
    pub negated: bool,
    /// Dialect-specific rewrite hint (if the operator needs special handling).
    pub rewrite: Option<FilterRewrite>,
}

/// Dialect-specific rewrite hints for filter operators.
/// Instead of the SQL builder checking dialect capabilities at generation time,
/// the plan layer pre-computes the necessary rewrites.
#[derive(Debug, Clone)]
pub enum FilterRewrite {
    /// Use JSON extraction function instead of native array operator.
    /// e.g., `cs` on SQLite → `json_each()` subquery.
    JsonArrayContains,
    /// Use LIKE pattern instead of native regex.
    /// e.g., `match` on SQLite → LIKE approximation.
    LikePattern(String),
    /// Use `json_extract()` instead of `->` / `->>` operators.
    JsonExtractFunction,
    /// Use `INSTR()` instead of `ILIKE`.
    InstrFallback,
    /// Use `GLOB` instead of `SIMILAR TO`.
    GlobPattern,
}

// ---------------------------------------------------------------------------
// ResolvedOrder
// ---------------------------------------------------------------------------

/// A resolved ORDER BY term.
#[derive(Debug, Clone)]
pub struct ResolvedOrder {
    /// Column name.
    pub column: String,
    /// Sort direction.
    pub direction: OrderDirection,
    /// Nulls ordering.
    pub nulls: Option<NullsOrder>,
}

// ---------------------------------------------------------------------------
// ResolvedRange
// ---------------------------------------------------------------------------

/// Resolved range with server-side cap applied.
#[derive(Debug, Clone)]
pub struct ResolvedRange {
    /// Effective limit (min of client limit and max_rows config).
    pub limit: Option<u64>,
    /// Offset (from client). Ignored when cursor is present.
    pub offset: Option<u64>,
    /// Resolved cursor for keyset pagination (alternative to offset).
    pub cursor: Option<ResolvedCursor>,
    /// The resolved cursor column name (set even on first page when cursor_value is absent).
    /// Used for extracting the X-Next-Cursor response header.
    pub cursor_column: Option<String>,
}

// ---------------------------------------------------------------------------
// ResolvedCursor
// ---------------------------------------------------------------------------

/// A resolved cursor condition for keyset (cursor-based) pagination.
///
/// Generated by `resolve_cursor()` after validating the cursor column
/// exists in the table and determining the comparison direction from
/// the ORDER BY clause.
#[derive(Debug, Clone)]
pub struct ResolvedCursor {
    /// The cursor column name (validated against schema).
    pub column: String,
    /// The cursor value to compare against (as a string; cast at execution time).
    pub value: String,
    /// The comparison operator (`Gt` for ASC order, `Lt` for DESC order).
    pub operator: Operator,
    /// The column's data type (for proper casting in SQL).
    pub data_type: String,
}

// ---------------------------------------------------------------------------
// ResolvedLogicTree
// ---------------------------------------------------------------------------

/// A resolved logic tree (AND/OR grouping of filters).
#[derive(Debug, Clone)]
pub enum ResolvedLogicTree {
    /// All child nodes must be true.
    And(Vec<ResolvedLogicNode>),
    /// At least one child node must be true.
    Or(Vec<ResolvedLogicNode>),
}

/// A node in a resolved logic tree — either a leaf filter or a nested tree.
#[derive(Debug, Clone)]
pub enum ResolvedLogicNode {
    /// A leaf filter condition.
    Filter(ResolvedFilter),
    /// A nested logic tree.
    Tree(ResolvedLogicTree),
    /// A negated node (NOT).
    Not(Box<ResolvedLogicNode>),
}

// ---------------------------------------------------------------------------
// CountStrategy
// ---------------------------------------------------------------------------

/// How to compute the total count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountStrategy {
    /// Exact count via COUNT(*) (can be expensive).
    Exact,
    /// Use query planner's estimate.
    Planned,
    /// Use statistics-based estimate.
    Estimated,
}

// ---------------------------------------------------------------------------
// MutatePlan
// ---------------------------------------------------------------------------

/// A fully-resolved mutation plan (INSERT/UPDATE/DELETE).
#[derive(Debug, Clone)]
pub struct MutatePlan {
    /// The target table.
    pub target: QualifiedIdentifier,
    /// Table metadata.
    pub table_info: ResolvedTableInfo,
    /// The mutation type.
    pub mutation: MutationType,
    /// Columns to return after mutation (from `select` + `Prefer: return=representation`).
    pub returning: Vec<ResolvedSelect>,
    /// Filter conditions (for UPDATE/DELETE).
    pub filters: Vec<ResolvedFilter>,
    /// Logic tree filters.
    pub logic_filters: Vec<ResolvedLogicTree>,
    /// Ordering (for UPDATE/DELETE with RETURNING).
    pub order: Vec<ResolvedOrder>,
    /// Range (for UPDATE/DELETE with LIMIT).
    pub range: ResolvedRange,
    /// Embeds on the RETURNING result.
    pub embeds: Vec<EmbeddedResource>,
    /// Count strategy.
    pub count: Option<CountStrategy>,
    /// Response preferences.
    pub preferences: Preferences,
    /// The request body (used for INSERT/UPDATE values, and RPC arguments).
    pub body: Option<RequestBody>,
}

/// The specific mutation operation.
#[derive(Debug, Clone)]
pub enum MutationType {
    /// INSERT (single or bulk).
    Insert {
        /// Columns in the payload.
        payload_columns: Vec<String>,
        /// Whether this is a bulk insert.
        is_bulk: bool,
        /// Conflict resolution (upsert).
        on_conflict: Option<ResolvedConflict>,
    },
    /// UPDATE (PATCH semantics — partial update).
    Update {
        /// Columns being updated.
        payload_columns: Vec<String>,
    },
    /// DELETE.
    Delete,
}

/// Resolved upsert conflict target.
#[derive(Debug, Clone)]
pub struct ResolvedConflict {
    /// The constraint or columns to use for conflict detection.
    pub columns: Vec<String>,
    /// Whether to merge duplicates or ignore them.
    pub resolution: ConflictResolution,
}

/// How to handle conflicting rows during upsert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// `ON CONFLICT DO UPDATE` — merge incoming data with existing rows.
    MergeDuplicates,
    /// `ON CONFLICT DO NOTHING` — skip rows that conflict.
    IgnoreDuplicates,
}

// ---------------------------------------------------------------------------
// CallPlan — RPC
// ---------------------------------------------------------------------------

/// A fully-resolved RPC function call plan.
#[derive(Debug, Clone)]
pub struct CallPlan {
    /// Function identifier.
    pub function: QualifiedIdentifier,
    /// Resolved function metadata.
    pub function_info: ResolvedFunctionInfo,
    /// Resolved parameters (matched to function signature).
    pub params: Vec<ResolvedParam>,
    /// Columns to return from the result.
    pub returning: Vec<ResolvedSelect>,
    /// Whether to return a single object or an array.
    pub is_singular: bool,
    /// Response preferences.
    pub preferences: Preferences,
    /// The request body (used for argument values).
    pub body: Option<RequestBody>,
}

/// Snapshot of resolved function metadata.
#[derive(Debug, Clone)]
pub struct ResolvedFunctionInfo {
    /// Whether this function is volatile (affects caching and transaction handling).
    pub volatility: Volatility,
    /// Return type description.
    pub return_type: String,
    /// Whether it returns a set (SETOF).
    pub returns_set: bool,
    /// Whether it returns a table type (composite).
    pub returns_table: bool,
    /// Custom isolation level (if set by function config).
    pub isolation_level: Option<IsolationLevel>,
}

/// A resolved function parameter matched to the call arguments.
#[derive(Debug, Clone)]
pub struct ResolvedParam {
    /// Parameter name.
    pub name: String,
    /// Parameter type.
    pub param_type: String,
    /// Whether a value was provided (vs using the default).
    pub has_value: bool,
    /// Whether this param is variadic.
    pub is_variadic: bool,
}

// ---------------------------------------------------------------------------
// InspectPlan
// ---------------------------------------------------------------------------

/// Plan for metadata/inspection endpoints (no SQL query needed).
#[derive(Debug, Clone)]
pub enum InspectPlan {
    /// Generate OpenAPI spec for a specific schema.
    OpenApi {
        /// The schema to inspect.
        schema: String,
    },
    /// Root endpoint — list available schemas.
    Root,
}
