# 07 — Design Decisions

The significant architectural choices, each with the problem it solves, why this
option won, and what it costs. Format per entry: **Decision · Context ·
Rationale · Consequences · Status.**

## 1. I/O-free core behind a `Backend` trait

- **Context.** PostgREST hardcodes Postgres throughout; pgvis needs multiple
  databases and multiple surfaces without forked logic.
- **Decision.** All parsing, planning, SQL building, schema-cache types, error,
  config live in [pgvis-core](../crates/pgvis-core) with *zero* I/O
  dependencies. Databases implement one `Backend` trait
  ([backend.rs](../crates/pgvis-core/src/backend.rs)).
- **Rationale.** A pure core is unit-testable without a database and reusable by
  every surface; the trait is the only seam to mock.
- **Consequences.** + Fast, deterministic tests; clean layering. − Backends must
  conform to a single `execute` contract (the CTE shape); a leaky abstraction
  there would be costly.
- **Status.** `[Implemented]`.

## 2. Hand-rolled SQL builder (no query-builder crate)

- **Context.** The PostgREST result pattern wraps each query in a CTE that
  `json_agg`s rows, with correlated JSON-aggregated subqueries for embedding and
  dialect-specific JSON functions.
- **Decision.** A single-pass string builder with a `RenderContext`
  ([query/mod.rs](../crates/pgvis-core/src/query/mod.rs)), no intermediate SQL
  AST, no `sea-query`/`diesel`/`sqlx` query DSL.
- **Rationale.** General builders cannot express correlated `json_agg`
  subqueries or per-dialect JSON functions cleanly; hand-rolling is fewer
  allocations, full control, and snapshot-testable.
- **Consequences.** + Exact output control; trivial to diff per dialect. − We
  own SQL-injection safety (mitigated: values are *always* positional params via
  `push_param`, never interpolated) and all escaping.
- **Status.** `[Implemented]`.

## 3. `Dialect` as a data struct, not a trait

- **Context.** Postgres vs SQLite differ in placeholders, JSON functions, and
  feature availability; these decisions occur thousands of times per query.
- **Decision.** `Dialect` is a flat `&'static` struct of syntax fields + boolean
  flags ([dialect.rs](../crates/pgvis-core/src/dialect.rs)); `POSTGRES`/`SQLITE`
  are constants in core.
- **Rationale.** A `dyn Dialect` trait would add a virtual call per fragment. A
  flat struct is branch-prediction-friendly and zero-cost to pass; constants
  living in core keep the SQL builder testable without driver crates.
- **Consequences.** + Hot-path-free of dynamic dispatch. − Adding a backend
  edits a core file; the struct is not yet `#[non_exhaustive]`
  ([08-future-scope.md](08-future-scope.md)).
- **Status.** `[Implemented]`.

## 4. Plan-time capability gating + `FilterRewrite` bridge

- **Context.** The SQL builder must not contain `if postgres { … } else { … }`
  business logic, or multi-DB support rots.
- **Decision.** The plan layer rejects unsupported operations
  (`validate_dialect_support`) and annotates expressible-but-different operators
  with a `FilterRewrite` hint on the `ResolvedFilter`
  ([plan/types.rs](../crates/pgvis-core/src/plan/types.rs)).
- **Rationale.** Capability knowledge stays in one place (the planner); the
  builder only formats. Errors are raised before any SQL is generated.
- **Consequences.** + Builder stays dialect-mechanical; clear error timing.
  − Every new rewrite needs a `FilterRewrite` variant + a builder arm.
- **Status.** `[Implemented]` (the SQLite backend exercises the rewrites; their
  SQL is emitted in [query/fragment.rs](../crates/pgvis-core/src/query/fragment.rs)).

## 5. One CTE-wrapped result shape

- **Context.** Reads, mutations, and RPC otherwise decode differently;
  PostgREST-style response status/header GUCs need readback.
- **Decision.** `wrap_cte` wraps every statement so the driver decodes one row:
  `body`, `page_total`, optional `total_count`, and (Postgres only)
  `response_status`/`response_headers`
  ([query/cte.rs](../crates/pgvis-core/src/query/cte.rs) →
  [`QueryResult`](../crates/pgvis-core/src/backend.rs)).
- **Rationale.** The backend's `execute` becomes trivial and uniform; GUC
  readback is free on Postgres and cleanly absent on SQLite via
  `supports_set_local`.
- **Consequences.** + Uniform decode path. − Backends that cannot express the
  CTE/GUC pattern need a translation: SQLite skips the envelope
  (`render_inner`) and its executor builds the same `QueryResult` in Rust.
- **Status.** `[Implemented]`.

## 6. Object-safe `Backend` via `BoxFuture`

- **Context.** Adapters want `Arc<dyn Backend>` and must not name a concrete
  driver type.
- **Decision.** Trait methods return `futures::future::BoxFuture` instead of
  `async fn` ([backend.rs](../crates/pgvis-core/src/backend.rs)).
- **Rationale.** `async fn` in traits is not object-safe for this use; an
  explicit boxed future keeps `dyn` working without the `async_trait` macro.
- **Consequences.** + Clean dynamic dispatch, swappable backends. − One heap
  allocation per call (negligible vs network I/O).
- **Status.** `[Implemented]`.

## 7. One `ApiRequest`, shared by every surface

- **Context.** REST and MCP (and future surfaces) must not each re-implement
  resolution and SQL.
- **Decision.** Every surface lowers input to
  [`ApiRequest`](../crates/pgvis-core/src/plan/types.rs) and calls the same
  `plan_request`.
- **Rationale.** Schema resolution, disambiguation, dialect gating, and SQL gen
  are written once; a surface is just two translation functions.
- **Consequences.** + New surfaces are cheap and behaviorally consistent.
  − `ApiRequest` must stay surface-neutral (no HTTP-isms leaking in).
- **Status.** `[Implemented]` (REST + MCP both prove it).

## 8. winnow for the query DSL parser

- **Context.** PostgREST's query language (nested `select` with embedding,
  operator filters, `and`/`or` trees, permissive field names) needs a real
  parser, not ad-hoc splitting.
- **Decision.** Parser combinators with `winnow`
  ([query_params](../crates/pgvis-core/src/query_params/mod.rs),
  [Cargo.toml](../Cargo.toml)).
- **Rationale.** Combinators map directly onto the recursive grammar and are
  unit-testable per production; winnow is a maintained, ergonomic combinator
  library that fits a hand-written tokenizer-free grammar. (Earlier exploration
  weighed a Parsec-style library; the as-built choice is winnow, consistent with
  the rest of the dependency set.)
- **Consequences.** + Grammar lives in typed Rust, snapshot-testable. − Parser
  combinator learning curve; grammar must track PostgREST's permissive rules.
- **Status.** `[Implemented]`.

## 9. Schema in the URL (vs header-only profiles)

- **Context.** PostgREST selects schema via `Accept-Profile`/`Content-Profile`
  headers — invisible in URLs, easy for proxies/caches to drop.
- **Decision.** Default routing is `/{prefix}/{schema}/{table}`; a
  header/flat-compat mode remains for drop-in PostgREST replacement
  ([config.rs](../crates/pgvis-core/src/config.rs),
  [routing.rs](../crates/pgvis-router/src/routing.rs)).
- **Rationale.** Path-based schema is bookmarkable, proxy/CDN-safe, and
  self-documenting; compat mode preserves migration.
- **Consequences.** + Clear multi-schema URLs. − Two routing modes to maintain
  and document.
- **Status.** `[Implemented]` — routing modes are live on REST, and MCP tool
  names follow the same `RoutingConfig`.

## 10. Routes and OpenAPI from one source, one pass

- **Context.** Spec/route drift is a chronic API-server problem.
- **Decision.** `generate_spec` iterates the same `SchemaCache` +
  `RoutingConfig` that `build_app` uses
  ([openapi.rs](../crates/pgvis-router/src/openapi.rs),
  [routing.rs](../crates/pgvis-router/src/routing.rs)).
- **Rationale.** If both are derived from one source, they cannot disagree.
- **Consequences.** + Spec always matches reality. − Spec richness is bounded by
  cache metadata richness (improves as introspection does).
- **Status.** `[In progress]`.

## 11. `openapiv3` over derive/macro spec crates

- **Context.** pgvis builds the spec from *runtime* introspection, not
  compile-time route handlers.
- **Decision.** Use `openapiv3` (pure serde data model)
  ([Cargo.toml](../Cargo.toml)).
- **Rationale.** Derive-first crates (`utoipa`/`aide`/`okapi`) assume
  compile-time routes; pgvis has none — a data model serialized at runtime fits.
- **Consequences.** + Direct control over emitted document. − Manual
  construction of schema objects.
- **Status.** `[In progress]`.

## 12. Dual license, forbid `unsafe`, clippy pedantic

- **Context.** Rust-ecosystem norms and a security-sensitive (SQL-emitting)
  codebase.
- **Decision.** `Apache-2.0 OR MIT`; workspace `unsafe_code = "forbid"`; clippy
  `all` + `pedantic` at warn ([Cargo.toml](../Cargo.toml)).
- **Rationale.** Maximizes adoption; removes an entire class of memory bugs;
  keeps the SQL-generating code under strict lint.
- **Consequences.** + Broad reuse, strong baseline. − Pedantic noise (a few
  lints explicitly allowed in the manifest).
- **Status.** `[Implemented]`.
