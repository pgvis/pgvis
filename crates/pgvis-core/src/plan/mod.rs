//! # Plan layer — transforms parsed requests + schema cache into resolved action plans.
//!
//! The plan layer is the **I/O-free decision engine** of pgvis. It takes:
//! - A parsed `ApiRequest` (from the REST or MCP adapter)
//! - The `SchemaCache` (introspected database metadata)
//! - The `Dialect` (backend capability flags)
//!
//! And produces an `ActionPlan` — a fully-resolved tree that the SQL builder
//! can translate directly to SQL without further lookups or decisions.

pub mod planner;
pub mod resolve;
pub mod routing;
pub mod types;
pub mod validate;
pub use routing::replica_read_eligible;

pub use planner::{PlanConfig, plan_request};
pub use types::*;
