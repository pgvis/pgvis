# 08 — Future Scope and Known Gaps

What is designed but not built, and the sharp edges to address. The goal is an
honest map of the road ahead, grounded in the current code.

## Milestone roadmap

| Milestone | Surfaces | Backends | Theme |
| ----------- | ---------- | ---------- | ------- |
| 0.1 | REST + OpenAPI | Postgres | Execute boundary closed; first end-to-end queries (REST, integration-tested) |
| 0.2 | + MCP (stdio) | Postgres | LLM tool exposure on real data |
| 0.3 | + SQLite | Postgres + SQLite | Second backend validates the dialect abstraction |
| 0.4 | MCP over SSE | both | Hosted-agent transport |
| 1.0 | stable API | both | Semver-pin `build_app` + `Config` + `Backend` |

## Closed seams (previously gaps, now implemented)

These were previously listed as future work but are now fully operational:

- **Query execution `[Closed]`** — `PgBackend::execute` and `SqliteBackend::execute`
  both run the full plan→render→execute path. On Postgres, BEGIN, one batched
  `set_config` (role, claims, timeout), the pre-request call and the statement
  go out as one pipelined flight, then COMMIT/ROLLBACK
  ([execute.rs](../crates/pgvis-postgres/src/execute.rs)); SQLite has no
  roles or GUCs.
- **Mutation guards `[Closed]`** — `Prefer: max-affected` (with
  `handling=strict`) and singular-object mutations are checked before COMMIT
  and roll back on violation; `limit`/`offset` bound PATCH/DELETE.
- **JWT/Auth enforcement `[Closed]`** — `verify_jwt()` in
  [routing.rs](../crates/pgvis-router/src/routing.rs) decodes the JWT (checking
  `jwt_aud` when set), extracts role + claims, and threads them into
  `ExecContext` via `build_exec_context()`. REST and in-process calls (via
  `CallerIdentity`) apply role switching; MCP runs as `anon_role`.
- **Logic-tree query parsing `[Closed]`** — `and=`/`or=`/`not.and=`/`not.or=`
  parameters are parsed by `parse_logic_filters_from_params` in routing.rs and
  passed through to the planner and SQL builder.
- **MCP execution `[Closed]`** — `McpServer` holds `Arc<dyn Backend>` and
  `handle_tool_call` renders SQL, executes, and returns real rows.
- **MCP select/order `[Closed]`** — `parse_mcp_order()` and select-string parsing
  are implemented in [tools.rs](../crates/pgvis-mcp/src/tools.rs).
- **Exact count `[Closed]`** — A separate `render_read_count_source` CTE counts
  all matching rows pre-LIMIT when `Prefer: count=exact` is set.
- **M2M embedding SQL `[Closed]`** — Junction-table two-hop joins are fully
  emitted in [read.rs](../crates/pgvis-core/src/query/read.rs).
- **SQLite backend `[Closed]`** — `pgvis-sqlite` provides full `Backend`
  implementation (introspection + execution).
- **Read replica support `[Closed]`** — `PgReplicaBackend` in
  [replica.rs](../crates/pgvis-postgres/src/replica.rs) distributes reads with
  lag-aware round-robin and primary fallback.
- **Config layering `[Closed]`** — `load_config` in
  [main.rs](../crates/pgvis-server/src/main.rs) uses figment with
  TOML file + `PGVIS_*` env vars + CLI flag overrides.
- **Data cache `[Closed]`** — Table-scoped generation-based invalidation; keys
  hashed with SipHash under a random per-process key. See
  [09-data-cache.md](09-data-cache.md).
- **Function overload resolution `[Closed]`** — `resolve_overload` in
  [plan/planner.rs](../crates/pgvis-core/src/plan/planner.rs) picks the
  overload by argument names (`PGRST202` none, `PGRST203` ambiguous).
- **`LISTEN/NOTIFY` hot reload `[Closed]`** — `watch_schema` listens on
  `pgrst`; `SchemaReloader` re-introspects and swaps the cache (also on
  `reload()`, `reload_now()` and `SIGUSR1`). See
  [05-schema-cache.md](05-schema-cache.md).
- **MCP over HTTP `[Closed]`** — besides stdio, MCP is served over Streamable
  HTTP at `/mcp` (`pgvis serve --mcp-http`).
- **Pass-through JSON bytes `[Closed]`** — table reads forward the database's
  JSON text (`ExecContext::raw_body`) without a parse and re-serialize, and
  the data cache stores those bytes.

## Core engine gaps

- **`planned`/`estimated` count.** Exact count is implemented, but
  `Prefer: count=planned` and `count=estimated` need `EXPLAIN` parsing
  (Postgres-only) to extract the planner's row estimate without a full count.
- **Computed relationship embedding.** `ResolvedJoin::Computed` exists in the plan
  layer but the SQL builder emits a placeholder (`TRUE /* computed via fn */`).
  Requires subquery wrapping: `LATERAL (SELECT fn(parent.*)) AS alias`.
- **Relation-scoped ordering.** REST `order` does not yet support
  `order=embed.column.asc` for ordering the parent by a child relation aggregate.
- **Parsed but not applied.** `Prefer: timezone`, `missing` and `params` are
  parsed but nothing consumes them; the
  `plan_enabled` (EXPLAIN media type) and `tx_rollback_all` config flags are
  likewise accepted but not acted on
  ([preferences.rs](../crates/pgvis-core/src/preferences.rs),
  [config.rs](../crates/pgvis-core/src/config.rs)).

## Introspection gaps

Some fields are still populated empty
([05-schema-cache.md](05-schema-cache.md),
[introspect/mod.rs](../crates/pgvis-postgres/src/introspect/mod.rs)):

- **Computed relationships** (`allComputedRels`) — function-as-relationship
  embedding; `ComputedRelationship` + `ResolvedJoin::Computed` exist but the
  introspection pass is a TODO.
- **Media handlers** — custom `Accept` types via aggregate functions
  (`MediaHandler` defined; query TODO).
- **Data representations** — introspection is **done**
  (`query_representations`); wiring the casts into the SQL builder for
  transparent (de)serialization is what remains.
- **`schema_version`** — needed for ETag/staleness; currently `None`.
- **View primary keys** — view-key-dependency tracing so embedding/`Location`
  works on views.

## Backend and surface gaps

- **OpenAPI richness.** Request/response JSON Schemas, per-column filter
  parameters, RPC bodies, and `openapi_mode = FollowPrivileges` filtering remain
  ([openapi.rs](../crates/pgvis-router/src/openapi.rs)).
- **MCP caller identity.** MCP tool calls run as `anon_role` with no claims
  ([tools.rs](../crates/pgvis-mcp/src/tools.rs)); passing a caller's token
  through to `ExecContext` (as REST does) is not implemented, so hosted
  deployments need an auth proxy in front of `/mcp`.

## Performance — remaining opportunities

These are lower-priority items from the performance audit that were not
implemented:

- **SQL builder string allocations.** `quote_ident` and `format!` calls allocate
  ~50-100 small `String`s per complex query. An append-to-buffer API would
  reduce this but requires significant API changes.
- **HashMap query parameters.** `Query<HashMap<String, String>>` allocates
  per-parameter. A zero-copy extractor borrowing from the URI would eliminate
  these, but fights axum's extraction model.

## Extensibility notes

- **`Dialect` is not `#[non_exhaustive]`.** Adding a backend currently means
  editing a core file and every struct literal. Marking `Dialect`
  `#[non_exhaustive]` with a constructor/builder would let backend crates extend
  capability without core churn ([dialect.rs](../crates/pgvis-core/src/dialect.rs)).
- **Catalog databases (DuckDB/MySQL).** `QualifiedIdentifier` is two-part
  (`schema.name`); a `catalog.schema.name` database needs either a third
  component or a convention. MySQL adds backtick quoting and
  `LIMIT offset,count` — anticipated by `Dialect` syntax fields but needs new
  `FilterRewrite` variants for regex/upsert syntax.
- **New surfaces (gRPC/GraphQL).** The recipe is fixed
  ([04-surfaces.md](04-surfaces.md), [07-design-decisions.md](07-design-decisions.md)):
  translate input → `ApiRequest`, reuse `plan_request`/`render`/`execute`,
  translate `QueryResult`/`Error` back. No engine changes required.
- **Multi-statement execution.** The `Backend` contract is one statement per
  call (the Postgres backend already pipelines that call's session setup with
  it); running several statements in one transaction would be an additive
  trait method.

## Verification path as features land

The intended end-to-end check: run the `pgvis` binary against a known schema and
exercise it with PostgREST's own HTTP-level expectations, asserting parity on
query DSL, `Prefer` semantics, and `PGRST*` error codes. The parser, plan layer,
and SQL builder remain independently testable without a database
([02-core-pipeline.md](02-core-pipeline.md)).
