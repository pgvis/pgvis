//! `pgvis-router` — Embeddable REST + OpenAPI router for pgvis.
//!
//! Provides [`build_app`] which takes a [`SchemaCache`](pgvis_core::SchemaCache) and produces an
//! axum Router with schema-driven routes. Mount it into any axum application.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use arc_swap::ArcSwap;
//! use pgvis_core::{Backend, Config, SchemaCache, dialect::POSTGRES};
//! use pgvis_router::build_app;
//!
//! let cache = Arc::new(ArcSwap::new(Arc::new(SchemaCache::default())));
//! let config = Arc::new(Config::default());
//! let dialect = Arc::new(POSTGRES.clone());
//! let backend: Arc<dyn Backend> = /* your backend here */;
//!
//! let app = build_app(cache, config, dialect, backend);
//! // app is ready to serve with axum::serve(...)
//! ```
//!
//! ## Embedding in an existing app
//!
//! ```rust,ignore
//! use axum::Router;
//! use axum::routing::get;
//! use std::sync::Arc;
//! use arc_swap::ArcSwap;
//! use pgvis_core::{Backend, Config, SchemaCache, dialect::POSTGRES};
//! use pgvis_router::build_app;
//!
//! let cache = Arc::new(ArcSwap::new(Arc::new(SchemaCache::default())));
//! let config = Arc::new(Config::default());
//! let dialect = Arc::new(POSTGRES.clone());
//! let backend: Arc<dyn Backend> = /* your backend here */;
//!
//! let pgvis_api = build_app(cache, config, dialect, backend);
//! let my_app = Router::new()
//!     .nest("/db", pgvis_api)
//!     .route("/health", get(|| async { "ok" }));
//! ```

pub mod data_cache;
pub mod openapi;
pub mod pubsub;
pub mod response;
pub mod routing;

pub use data_cache::{CacheStats, DataCache};
pub use pubsub::{PubSubHub, build_pubsub_router};
pub use routing::{AppState, CallerIdentity, build_app, build_router};
