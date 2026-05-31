//! # Query parameter parsing — PostgREST DSL → typed AST.
//!
//! Parses PostgREST's query-string DSL into strongly-typed Rust structures
//! using the `winnow` parser combinator library.
//!
//! ## Sub-modules
//!
//! - [`common`] — shared parsers (field names, JSON paths, identifiers, operators)
//! - [`select`] — `select=` parameter parser → `Vec<SelectItem>`
//! - [`filter`] — filter operator expressions → `Filter` (with typed values)
//! - [`order`] — `order=` parameter → `Vec<OrderItem>`
//! - [`logic`] — `and=`/`or=` logic trees → `LogicNode`
//! - [`types`] — AST output types

pub mod common;
pub mod filter;
pub mod logic;
pub mod order;
pub mod select;
pub mod types;

pub use filter::parse_filter;
pub use logic::parse_logic_tree;
pub use order::{OrderItem, OrderRelationTerm, parse_order};
pub use select::parse_select;
pub use types::{
    CursorSpec, Filter, FilterValue, IsKind, LogicNode, LogicTree, NullsOrder, Operator,
    OrderDirection, OrderTerm, Quantifier, RangeSpec,
};
