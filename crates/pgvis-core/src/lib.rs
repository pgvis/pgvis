//! # `pgvis-core` — Backend-agnostic database narration primitives
//!
//! This crate is the **I/O-free foundation** of the pgvis workspace. It defines:
//!
//! - The [`Backend`] trait that database drivers (`pgvis-postgres`, `pgvis-sqlite`) implement
//! - The [`SchemaCache`] and related types describing introspected database metadata
//! - The [`Dialect`] struct parametrising SQL generation for different databases
//! - The [`Error`] type used throughout the stack
//! - Shared [`Config`] types consumed by backends and adapters
//! - The [`select_ast`] types for parsed `select=` parameter AST
//! - The [`query_params`] parsers for the PostgREST query-string DSL
//! - The [`preferences`] module for `Prefer` header parsing
//! - The [`plan`] layer types — fully-resolved, I/O-free action plans
//!
//! ## Architectural Role
//!
//! ```text
//! pgvis-core (this crate)
//!   │
//!   ├── defines trait Backend ──► implemented by pgvis-postgres, pgvis-sqlite
//!   ├── defines SchemaCache   ──► consumed by pgvis-rest, pgvis-mcp
//!   ├── defines Dialect       ──► used by SQL builder (pgvis-core::query)
//!   ├── defines select_ast    ──► parser output, plan layer input
//!   ├── defines query_params  ──► winnow parsers for PostgREST DSL
//!   ├── defines plan          ──► resolved action plans for SQL builder
//!   └── defines Error/Config  ──► shared across all crates
//! ```
//!
//! ## No I/O Guarantee
//!
//! This crate has **zero runtime I/O dependencies**:
//! - No `tokio-postgres`, no `sqlx`, no `rusqlite`
//! - No `axum`, no `hyper`, no HTTP framework
//! - No filesystem access, no network calls
//!
//! All async operations are defined as trait methods returning [`futures::future::BoxFuture`],
//! making the trait **object-safe** (`dyn Backend` works). The actual I/O happens in the
//! implementing crates.
//!
//! ## Extensibility for Multiple Backends
//!
//! Every type is designed to work with both Postgres and SQLite from day one:
//! - [`Dialect`] captures capability differences as boolean flags
//! - [`SchemaCache`] types use string-based type names (not Postgres OIDs)
//! - [`Backend`] methods accept generic [`serde_json::Value`] params
//! - Optional fields (e.g. schema namespacing) gracefully degrade

pub mod backend;
pub mod cache;
pub mod cache_post_process;
pub mod config;
pub mod dialect;
pub mod error;
pub mod plan;
pub mod preferences;
pub mod pubsub;
pub mod query;
pub mod query_params;
pub mod select_ast;

// Re-export primary types for ergonomic use
pub use backend::{Backend, ExecContext, IntrospectConfig, QueryResult, SchemaChangeStream};
pub use cache::{
    Cardinality, Column, ComputedRelationship, DataRepresentation, MediaHandler,
    QualifiedIdentifier, Relationship, Routine, RoutineParam, SchemaCache, Table, UniqueConstraint,
    Volatility,
};
pub use config::{CacheConfig, Config, RoutingConfig};
pub use dialect::{Dialect, POSTGRES, Placeholder, SQLITE};
pub use error::{Error, ErrorCode};
pub use pubsub::{PubSubBackend, PubSubConfig, PubSubMessage, PubSubStream};
pub use plan::{
    ActionPlan, ApiRequest, CallPlan, MutatePlan, PlanConfig, ReadPlan, RequestMethod, plan_request,
};
pub use preferences::Preferences;
pub use select_ast::{
    AggregateFunction, FieldSelect, JoinType, JsonOperand, JsonOperation, RelationSelect,
    SelectItem, SpreadSelect,
};
