//! # Schema cache types — the introspected database metadata snapshot.
//!
//! These types represent the **complete picture** of a database's exposed surface:
//! tables, views, columns, relationships, stored functions, data representations,
//! and media handlers.
//!
//! ## Lifecycle
//!
//! 1. A [`Backend`](crate::Backend) runs introspection queries at startup
//! 2. Results are assembled into a [`SchemaCache`]
//! 3. The cache is stored in an `ArcSwap` for lock-free reads on every request
//! 4. On schema change notifications, the cache is rebuilt and atomically swapped
//!
//! ## PostgREST Equivalent
//!
//! Maps to the `SchemaCache` record in `SchemaCache.hs` — `dbTables`, `dbRelationships`,
//! `dbRoutines`, `dbRepresentations`, `dbMediaHandlers`, `dbTimezones`.
//!
//! ## Multi-Backend Design
//!
//! All types use string-based type names (not Postgres OIDs) so they work for
//! both Postgres (`int4`, `text`, `timestamptz`) and SQLite (`INTEGER`, `TEXT`, `REAL`).
//! Backend-specific metadata lives in the driver crates, not here.

use std::collections::HashMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

/// A schema-qualified identifier (e.g. `public.users`, `api.get_user`).
///
/// Used as map keys throughout the cache. On SQLite (single namespace),
/// `schema` is set to `"main"` by convention.
///
/// # PostgREST equivalent
///
/// `QualifiedIdentifier` in `SchemaCache/Identifiers.hs`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedIdentifier {
    /// The schema name (e.g. `"public"`, `"api"`, `"main"` for SQLite).
    pub schema: String,
    /// The object name (table, view, function, type).
    pub name: String,
}

impl QualifiedIdentifier {
    /// Create a new qualified identifier.
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for QualifiedIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

// ---------------------------------------------------------------------------
// SchemaCache — the top-level container
// ---------------------------------------------------------------------------

/// The full introspected schema cache — everything the REST/MCP layers need to
/// build routes, generate OpenAPI specs, and plan queries.
///
/// Rebuilt on every schema reload. Stored behind `ArcSwap` for lock-free access.
///
/// # PostgREST equivalent
///
/// The `SchemaCache` record containing `dbTables`, `dbRelationships`, `dbRoutines`,
/// `dbRepresentations`, `dbMediaHandlers`.
///
/// # Fields
///
/// Each field is an ordered map or vec. `IndexMap` preserves insertion order
/// (introspection query order), which gives deterministic OpenAPI output.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaCache {
    /// When this cache was built (for ETag/Last-Modified and debugging).
    #[serde(default)]
    pub built_at: Option<std::time::SystemTime>,

    /// Backend-specific schema version string for staleness detection.
    ///
    /// - Postgres: result of `pg_catalog.version()` or txid at introspection time
    /// - SQLite: `PRAGMA schema_version` value
    #[serde(default)]
    pub schema_version: Option<String>,

    /// All exposed tables and views, keyed by `schema.name`.
    ///
    /// Includes both base tables and views. The [`Table::is_view`] flag
    /// distinguishes them. Views may have [`Table::insertable`] = true
    /// if they have INSTEAD OF triggers or are simple enough for auto-update.
    pub tables: IndexMap<QualifiedIdentifier, Table>,

    /// Foreign-key relationships between tables (M2O, O2M, O2O, M2M).
    ///
    /// Discovered from `pg_constraint` (Postgres) or `PRAGMA foreign_key_list` (SQLite).
    /// Used by the plan layer to resolve resource embedding (`select=orders(*)`).
    pub relationships: Vec<Relationship>,

    /// Computed (function-based) relationships.
    ///
    /// Single-argument functions that accept a table's row type and return
    /// another table's row type are treated as embeddable virtual relationships.
    /// Postgres only — SQLite has no row-type functions.
    ///
    /// # PostgREST equivalent
    ///
    /// `allComputedRels` in `SchemaCache.hs` — functions discovered from `pg_proc`
    /// where `proargtypes[0]` is a composite type matching a cached table.
    pub computed_relationships: Vec<ComputedRelationship>,

    /// Stored functions/procedures, keyed by `schema.name`.
    ///
    /// The value is a `Vec<Routine>` because PostgreSQL supports **function overloading** —
    /// multiple functions with the same name but different parameter signatures.
    /// The plan layer uses argument matching to select the correct overload.
    ///
    /// On SQLite: this map is empty (`dialect.has_routines = false`).
    pub routines: IndexMap<QualifiedIdentifier, Vec<Routine>>,

    /// Data representation transforms (domain type ↔ JSON/text conversions).
    ///
    /// Maps source type → list of available representations. Used by the plan layer
    /// to auto-apply formatting on output and parsing on input.
    ///
    /// # PostgREST equivalent
    ///
    /// `RepresentationsMap` populated by `dataRepresentations` SQL query.
    /// Enables transparent serialisation of domain types (e.g. `money` → custom JSON format).
    pub representations: HashMap<String, Vec<DataRepresentation>>,

    /// Custom media type handler functions.
    ///
    /// Maps `(schema, media_type)` → handler function. When a client requests a
    /// custom `Accept` type (e.g. `text/csv`, `application/geo+json`), the matching
    /// handler function is called as an aggregate to produce the response body.
    ///
    /// # PostgREST equivalent
    ///
    /// `MediaHandlerMap` populated by `mediaHandlers` SQL query.
    /// Postgres only — SQLite has no aggregate function discovery.
    pub media_handlers: HashMap<(String, String), MediaHandler>,

    /// Pre-built index mapping each table to the indices of its relationships.
    ///
    /// Built by [`crate::cache_post_process::build_relationship_index`] after all
    /// relationships (including inverse and M2M) have been added. Converts the
    /// O(n) linear scan in `find_relationships` to O(1) lookup.
    ///
    /// Skipped during serialisation — rebuilt on cache load.
    #[serde(skip)]
    pub relationship_index: HashMap<QualifiedIdentifier, Vec<usize>>,
}

impl SchemaCache {
    /// Find a table by schema and name.
    pub fn find_table(&self, schema: &str, name: &str) -> Option<&Table> {
        let key = QualifiedIdentifier::new(schema, name);
        self.tables.get(&key)
    }

    /// Find all relationships where `table` is either the source or target.
    ///
    /// Uses the pre-built `relationship_index` for O(1) lookup instead of
    /// scanning all relationships. Returns an empty slice if no relationships exist.
    pub fn find_relationships(&self, table: &QualifiedIdentifier) -> Vec<&Relationship> {
        match self.relationship_index.get(table) {
            Some(indices) => indices.iter().map(|&i| &self.relationships[i]).collect(),
            None => Vec::new(),
        }
    }

    /// Find routines by name (across all schemas).
    ///
    /// Returns all overloads. The plan layer narrows by argument matching.
    pub fn find_routines(&self, schema: &str, name: &str) -> Option<&Vec<Routine>> {
        let key = QualifiedIdentifier::new(schema, name);
        self.routines.get(&key)
    }
}

// ---------------------------------------------------------------------------
// Tables & Columns
// ---------------------------------------------------------------------------

/// A database table or view.
///
/// # PostgREST equivalent
///
/// `Table` in `SchemaCache/Identifiers.hs` — `tableSchema`, `tableName`,
/// `tableDescription`, `tableInsertable`, `tableUpdatable`, `tableDeletable`,
/// `tableColumns`, `tablePKCols`.
///
/// # Multi-backend notes
///
/// - On SQLite, `is_view` is determined from `sqlite_master.type = 'view'`.
/// - On SQLite, all non-view tables are insertable/updatable/deletable.
/// - `pk_cols` on views uses PostgREST's view-key-dependency tracing (Postgres)
///   or is empty (SQLite views have no discoverable PK).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    /// Schema-qualified table identifier.
    pub ident: QualifiedIdentifier,

    /// Human-readable description from `COMMENT ON TABLE`.
    ///
    /// Used as the OpenAPI schema description.
    pub description: Option<String>,

    /// Whether this is a view (as opposed to a base table).
    pub is_view: bool,

    /// Whether INSERT is allowed.
    ///
    /// False for: views without INSTEAD OF INSERT triggers, generated-always tables.
    /// Drives whether POST is registered in the router.
    pub insertable: bool,

    /// Whether UPDATE is allowed.
    ///
    /// Drives whether PATCH is registered in the router.
    pub updatable: bool,

    /// Whether DELETE is allowed.
    ///
    /// Drives whether DELETE is registered in the router.
    pub deletable: bool,

    /// Primary key column names.
    ///
    /// Used for:
    /// - `Location` header generation on INSERT
    /// - M2M relationship inference (PK ⊇ FK columns)
    /// - OpenAPI `required` fields on upsert
    ///
    /// For views, populated via view-key-dependency analysis (Postgres)
    /// or empty (SQLite).
    pub pk_cols: Vec<String>,

    /// All unique constraints (including the primary key).
    ///
    /// Used for:
    /// - `ON CONFLICT` target resolution in upsert
    /// - OpenAPI documentation of conflict targets
    /// - `Location` header when PK differs from upsert target
    pub unique_constraints: Vec<UniqueConstraint>,

    /// All columns in ordinal order.
    ///
    /// `IndexMap` preserves insertion order matching `ordinal_position`.
    pub columns: IndexMap<String, Column>,
}

impl Table {
    /// Shorthand for the unqualified table name.
    pub fn name(&self) -> &str {
        &self.ident.name
    }

    /// Shorthand for the schema name.
    pub fn schema(&self) -> &str {
        &self.ident.schema
    }
}

/// A single column in a table or view.
///
/// # PostgREST equivalent
///
/// `Column` in `SchemaCache/Identifiers.hs` — `colName`, `colType`, `colNullable`,
/// `colDefault`, `colEnum`, `colMaxLen`, `colNominalType`, `colDescription`.
///
/// # Multi-backend notes
///
/// - **Postgres:** `typ` is the base type name from `pg_type` (e.g. `int4`, `text`, `jsonb`).
/// - **SQLite:** `typ` is the declared type affinity (e.g. `INTEGER`, `TEXT`, `REAL`, `BLOB`).
/// - `nominal_type` captures the full declared type (e.g. `character varying(255)`) for
///   OpenAPI format hints, while `typ` is the resolved base type for SQL operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name.
    pub name: String,

    /// Human-readable description from `COMMENT ON COLUMN`.
    ///
    /// Used as the OpenAPI property description.
    pub description: Option<String>,

    /// Whether the column accepts NULL values.
    ///
    /// Drives OpenAPI `nullable` and request validation.
    pub nullable: bool,

    /// Whether this is a generated column (`GENERATED ALWAYS AS ... STORED`
    /// or `GENERATED ALWAYS AS IDENTITY`).
    ///
    /// Generated columns must be excluded from INSERT/UPDATE payloads.
    /// OpenAPI marks them as `readOnly`.
    ///
    /// Supported on both Postgres and SQLite 3.31+.
    #[serde(default)]
    pub is_generated: bool,

    /// Whether this column is updatable.
    ///
    /// False for generated columns, identity columns with `GENERATED ALWAYS`,
    /// and view columns without INSTEAD OF triggers.
    /// Used to gate PATCH inclusion per-column.
    #[serde(default = "default_true")]
    pub updatable: bool,

    /// The resolved base type name.
    ///
    /// - Postgres: `int4`, `text`, `timestamptz`, `jsonb`, `uuid`, etc.
    /// - SQLite: `INTEGER`, `TEXT`, `REAL`, `BLOB`, `NUMERIC`
    ///
    /// Used by the SQL builder for cast operations and by OpenAPI for
    /// JSON Schema type mapping.
    pub typ: String,

    /// The full declared type name (before resolution to base type).
    ///
    /// Examples: `character varying(255)`, `numeric(10,2)`, `my_domain_type`.
    /// Used for OpenAPI `format` hints. `None` if same as `typ`.
    pub nominal_type: Option<String>,

    /// Maximum character length (`character_maximum_length` in `information_schema`).
    ///
    /// Used for OpenAPI `maxLength` constraint. `None` for non-character types.
    pub max_len: Option<i32>,

    /// Column default expression (e.g. `nextval('id_seq')`, `now()`, `'active'`).
    ///
    /// Used for:
    /// - OpenAPI `default` value annotation
    /// - `Prefer: missing=default` semantics (omitted columns get their default)
    pub default: Option<String>,

    /// Enum value labels for enum-typed columns.
    ///
    /// Non-empty only for PostgreSQL enum types. Used for OpenAPI `enum` constraint.
    /// Empty vec for non-enum columns and all SQLite columns.
    pub enum_values: Vec<String>,

    /// Whether this column is part of the primary key.
    pub is_pk: bool,

    /// Whether this column participates in any foreign key (as source).
    pub is_fk: bool,

    /// Column ordinal position (1-based, from `ordinal_position` in information_schema).
    ///
    /// Used to maintain stable column ordering across serialisation.
    pub ordinal: i32,
}

// ---------------------------------------------------------------------------
// Relationships
// ---------------------------------------------------------------------------

/// Relationship cardinality between two tables.
///
/// Determines how resource embedding renders:
/// - `M2O` → embedded as a single JSON object
/// - `O2M` → embedded as a JSON array
/// - `O2O` → embedded as a single JSON object
/// - `M2M` → embedded as a JSON array (via junction table)
///
/// # PostgREST equivalent
///
/// `Cardinality` in `SchemaCache/Identifiers.hs`.
///
/// # Multi-backend notes
///
/// All cardinalities work on both Postgres and SQLite — foreign keys are
/// universal SQL. The only difference is discovery mechanism (pg_constraint vs PRAGMA).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Cardinality {
    /// Many-to-one: child table → parent table via FK.
    ///
    /// Example: `orders.customer_id → customers.id`
    /// Embedding direction: from orders, embed customer as object.
    M2O,

    /// One-to-many: parent table → child table(s) via FK.
    ///
    /// Example: `customers → orders` (reverse of the FK direction)
    /// Embedding direction: from customers, embed orders as array.
    O2M,

    /// One-to-one: unique foreign key constraint.
    ///
    /// Like M2O but the FK column has a UNIQUE constraint, guaranteeing
    /// at most one related row. Embedded as a single object (not array).
    O2O,

    /// Many-to-many: via a junction (join) table.
    ///
    /// Example: `students ←→ courses` via `enrollments(student_id, course_id)`
    ///
    /// Inferred when a junction table's PK columns are a superset of the
    /// union of its FK columns pointing to both related tables.
    M2M {
        /// The junction table connecting source and target.
        junction_table: QualifiedIdentifier,
        /// FK columns in the junction table pointing to the source table.
        junction_cols_source: Vec<String>,
        /// FK columns in the junction table pointing to the target table.
        junction_cols_target: Vec<String>,
    },
}

/// A foreign-key relationship between two tables.
///
/// # PostgREST equivalent
///
/// `Relationship` in `SchemaCache/Identifiers.hs`.
///
/// # Usage
///
/// - **Plan layer:** Resolves `select=relation(*)` to the correct join conditions
/// - **SQL builder:** Generates `json_agg` subqueries with WHERE clause from FK columns
/// - **OpenAPI:** Documents embeddable resources in schema descriptions
/// - **Disambiguation:** `constraint_name` resolves ambiguity when multiple FKs
///   connect the same pair of tables (user hints via `!constraint` syntax)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relationship {
    /// The table where the FK columns live (the "child" in M2O).
    pub source_table: QualifiedIdentifier,

    /// The table being referenced (the "parent" in M2O).
    pub target_table: QualifiedIdentifier,

    /// Column names in the source table participating in the FK.
    pub source_columns: Vec<String>,

    /// Column names in the target table being referenced.
    pub target_columns: Vec<String>,

    /// The cardinality of this relationship.
    pub cardinality: Cardinality,

    /// The constraint name (e.g. `orders_customer_id_fkey`).
    ///
    /// Used for disambiguation when multiple FKs connect the same table pair.
    /// Users hint the desired relationship via `!constraint_name` in `select`.
    pub constraint_name: String,

    /// Whether this is a self-referential FK (source_table == target_table).
    ///
    /// Example: `employees.manager_id → employees.id`
    pub is_self: bool,
}

/// A computed (function-based) relationship — virtual embedding via a function.
///
/// When a function accepts a single composite-type argument matching a cached table
/// and returns a set of another table's row type, it can be used as an embeddable
/// relationship in `select`.
///
/// # PostgREST equivalent
///
/// `ComputedRelationship` from `allComputedRels` — discovered via `pg_proc` where
/// the function's first argument is a composite type matching a table.
///
/// # Multi-backend notes
///
/// **Postgres only.** SQLite has no composite types or set-returning functions.
/// This vec will always be empty for SQLite backends.
///
/// # Example
///
/// ```sql
/// CREATE FUNCTION full_name(users) RETURNS text AS $$
///   SELECT $1.first_name || ' ' || $1.last_name
/// $$ LANGUAGE sql;
///
/// -- Then: GET /users?select=*,full_name
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedRelationship {
    /// The table whose row type is the function's input.
    pub source_table: QualifiedIdentifier,

    /// The function that computes the relationship.
    pub function_name: String,

    /// The function's schema.
    pub function_schema: String,

    /// The return type — either a scalar or another table's row type.
    ///
    /// If this matches a cached table's composite type, the result is
    /// embeddable as a sub-resource.
    pub target_table: Option<QualifiedIdentifier>,

    /// Whether the function returns a set (SETOF) — determines array vs object embedding.
    pub returns_set: bool,
}

// ---------------------------------------------------------------------------
// Routines (stored functions/procedures)
// ---------------------------------------------------------------------------

/// A stored function or procedure.
///
/// Drives `/rpc/<name>` routes. Only available when `dialect.has_routines = true`
/// (Postgres). SQLite backends have an empty routines map.
///
/// # PostgREST equivalent
///
/// `Routine` in `SchemaCache/Identifiers.hs` — `pdName`, `pdDescription`,
/// `pdParams`, `pdReturnType`, `pdVolatility`, etc.
///
/// # Overloading
///
/// PostgreSQL supports function overloading. Multiple `Routine` entries may share
/// the same `ident`. The plan layer resolves the correct overload by matching
/// the caller's argument names against each overload's `params`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Routine {
    /// Schema-qualified function identifier.
    pub ident: QualifiedIdentifier,

    /// Human-readable description from `COMMENT ON FUNCTION`.
    pub description: Option<String>,

    /// Function parameters in declaration order.
    pub params: Vec<RoutineParam>,

    /// Return type name (e.g. `int4`, `json`, `SETOF orders`, composite type name).
    pub return_type: String,

    /// Whether the function returns a set (`SETOF`).
    ///
    /// When true, the function is "table-valued" — all read parameters
    /// (select, order, limit, filters) apply to its output.
    pub return_type_is_set: bool,

    /// Whether the return type is a composite (row) type.
    ///
    /// When true, the function returns structured rows (like a table).
    /// When false + `return_type_is_set`, returns a set of scalars.
    pub return_type_is_composite: bool,

    /// Function volatility category.
    ///
    /// Controls which HTTP methods are allowed:
    /// - `Immutable`/`Stable` → GET and POST
    /// - `Volatile` → POST only
    pub volatility: Volatility,

    /// Whether the function has a VARIADIC parameter.
    pub is_variadic: bool,

    /// Transaction isolation level for this function (from `proconfig`).
    ///
    /// If set, the transaction is opened with this isolation level.
    /// Discovered from function-level `SET default_transaction_isolation` setting.
    pub isolation_level: Option<IsolationLevel>,

    /// Function-level GUC settings to "hoist" into the transaction.
    ///
    /// Discovered from `proconfig` — settings like `search_path`, `work_mem`, etc.
    /// that should be applied at the transaction level before calling the function.
    ///
    /// PostgREST equivalent: `configDbHoistedTxSettings`.
    pub settings: Vec<(String, String)>,
}

/// A routine parameter.
///
/// # PostgREST equivalent
///
/// `RoutineParam` in `SchemaCache/Identifiers.hs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineParam {
    /// Parameter name (empty string for unnamed positional params).
    pub name: String,

    /// Parameter type name (e.g. `int4`, `text`, `json`).
    pub typ: String,

    /// Whether the parameter is required (has no DEFAULT value).
    ///
    /// Required params must be supplied by the caller. Optional params
    /// (those with defaults) can be omitted from the request.
    pub required: bool,

    /// Whether this is a VARIADIC parameter.
    pub is_variadic: bool,
}

/// Function volatility category.
///
/// PostgreSQL classifies functions by their side-effect guarantees.
/// This affects HTTP method routing and cacheability.
///
/// # HTTP Method Mapping
///
/// | Volatility | GET | POST | Cacheable |
/// |---|---|---|---|
/// | `Immutable` | ✓ | ✓ | Yes (result depends only on arguments) |
/// | `Stable` | ✓ | ✓ | Within-transaction (reads but doesn't modify) |
/// | `Volatile` | ✗ | ✓ | No (may modify data) |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Volatility {
    /// Result depends only on arguments — `2 + 2` always equals `4`.
    Immutable,
    /// Result depends on database state but doesn't modify it.
    Stable,
    /// May modify database state (INSERT, UPDATE, DELETE, etc.).
    Volatile,
}

/// Transaction isolation level for per-function override.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// `READ COMMITTED` — default PostgreSQL isolation.
    ReadCommitted,
    /// `REPEATABLE READ` — snapshot isolation.
    RepeatableRead,
    /// `SERIALIZABLE` — full serialisation.
    Serializable,
}

// ---------------------------------------------------------------------------
// Unique Constraints
// ---------------------------------------------------------------------------

/// A unique constraint on a table (includes PK).
///
/// Used for upsert (`ON CONFLICT`) target resolution and OpenAPI documentation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueConstraint {
    /// Constraint name (e.g. `users_pkey`, `users_email_key`).
    pub name: String,
    /// Columns participating in the constraint.
    pub columns: Vec<String>,
    /// Whether this is the primary key constraint.
    pub is_pk: bool,
}

// ---------------------------------------------------------------------------
// Data Representations (domain type transforms)
// ---------------------------------------------------------------------------

/// A data representation transform — implicit cast between types.
///
/// Enables transparent serialisation of domain types. When a column's type has
/// a registered representation, the SQL builder applies the transform function
/// automatically on output (formatting) or input (parsing).
///
/// # PostgREST equivalent
///
/// `DataRepresentation` in `SchemaCache.hs` (populated by `dataRepresentations` query).
///
/// # Example
///
/// A domain type `money_usd` with a cast to `json`:
/// ```sql
/// CREATE DOMAIN money_usd AS numeric(10,2);
/// CREATE CAST (money_usd AS json) WITH FUNCTION money_to_json(money_usd);
/// ```
///
/// The SQL builder wraps output columns: `money_to_json(col) AS col`
/// and input values: `money_from_json($1::json)`.
///
/// # Multi-backend notes
///
/// **Postgres only.** SQLite has no domain types or custom casts.
/// This map will always be empty for SQLite backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRepresentation {
    /// The source type being transformed from.
    pub source_type: String,

    /// The target type being transformed to (`json` or `text`).
    pub target_type: String,

    /// The function that performs the transformation.
    pub function_name: String,

    /// The schema containing the transform function.
    pub function_schema: String,
}

// ---------------------------------------------------------------------------
// Media Handlers (custom response formats)
// ---------------------------------------------------------------------------

/// A custom media type handler function.
///
/// When clients send `Accept: <media_type>`, and a matching handler exists for
/// the target table's schema, the handler function is used as an aggregate to
/// produce the response body in the requested format.
///
/// # PostgREST equivalent
///
/// `MediaHandler` from `mediaHandlers` query — discovers aggregate functions
/// returning domain types whose names match media type patterns
/// (e.g. `text/csv`, `application/geo+json`).
///
/// # Example
///
/// ```sql
/// CREATE DOMAIN "text/csv" AS text;
/// CREATE FUNCTION to_csv(anyelement) RETURNS "text/csv" AS $$...$$;
///
/// -- Then: GET /users -H "Accept: text/csv" uses to_csv as aggregate
/// ```
///
/// # Multi-backend notes
///
/// **Postgres only.** SQLite has no user-defined aggregate functions discoverable
/// at runtime. This map will always be empty for SQLite backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaHandler {
    /// The media type this handler produces (e.g. `"text/csv"`, `"application/geo+json"`).
    pub media_type: String,

    /// The aggregate function to call.
    pub function_ident: QualifiedIdentifier,

    /// The schema this handler applies to.
    ///
    /// Media handlers are schema-scoped — a handler in schema `api` doesn't
    /// apply to tables in schema `internal`.
    pub target_schema: String,
}
