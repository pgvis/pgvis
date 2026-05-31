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

## The critical seam: query execution `[Closed for Postgres/REST]`

This was 0.1's defining gap and is now **closed for Postgres**.
`PgBackend::execute` ([lib.rs](../crates/pgvis-postgres/src/lib.rs)) calls
`execute::execute_query`
([execute.rs](../crates/pgvis-postgres/src/execute.rs)), which:

- binds `serde_json::Value` parameters via a `TextParam` `ToSql` wrapper
  (text protocol; Postgres coerces by the inferred parameter type),
- applies `ExecContext` (BEGIN, `SET LOCAL role`, `request.jwt.claims` +
  per-claim GUCs, `statement_timeout`, `pre_request`, `tx_end`
  COMMIT/ROLLBACK),
- decodes the CTE result row into `QueryResult`
  (`body`/`page_total`/`total_count`/`response_status`/`response_headers`).

`pgvis-router` (REST) now runs the full `plan_request → render → execute` path
and is covered by the integration suite in
[crates/pgvis-server/tests](../crates/pgvis-server/tests). **Still open:**
`pgvis-mcp` returns a *plan summary* because `pgvis-lib` builds `McpServer`
without a backend, so MCP never reaches `render`/`execute`
([04-surfaces.md](04-surfaces.md)); and JWT verification + threading claims
into `ExecContext` is not yet wired on either surface.

## Core engine gaps

- **Function overload resolution.** `plan_call`
  ([plan/planner.rs](../crates/pgvis-core/src/plan/planner.rs)) takes the first
  routine under a name. PostgreSQL allows overloading; a scoring algorithm over
  `Routine.params` vs supplied argument names/types is needed
  (`PGRST203 AmbiguousFunction` already exists for the unresolved case).
- **Logic-tree query parsing in adapters.** `parse_logic_tree` exists in core
  ([query_params/logic.rs](../crates/pgvis-core/src/query_params/logic.rs)) but
  the REST `build_api_request` leaves `logic_filters` empty and MCP likewise —
  `and=`/`or=` query parameters are not yet wired through the adapters.
- **Relation ordering / select-string in MCP.** REST drops `order` relation
  terms; MCP's `select` argument is stubbed to `Star`
  ([tools.rs](../crates/pgvis-mcp/src/tools.rs)).
- **Exact count.** `wrap_cte`
  ([query/cte.rs](../crates/pgvis-core/src/query/cte.rs)) reuses the page count
  as `total_count`; a true pre-LIMIT exact count needs a separate counting CTE.
  `planned`/`estimated` need `EXPLAIN` parsing (Postgres).
- **Embedding SQL breadth.** `Direct`/`Junction`/`Computed` joins are modelled in
  the plan; full SQL emission for M2M two-hop and computed-relationship
  subqueries in [query/read.rs](../crates/pgvis-core/src/query/read.rs) is still
  being filled in.

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

- **Read replica support. `[Closed]`** `PgReplicaBackend`
  ([replica.rs](../crates/pgvis-postgres/src/replica.rs)) distributes reads
  across replicas with lag-aware round-robin, falling back to primary when all
  replicas are unhealthy. Configuration via `Config.replica` (`replica_dsns`,
  `max_replication_lag_bytes`, `health_check_interval_ms`, `primary_reads`).
  Failover is delegated to external tools; `deadpool` handles reconnection
  transparently.
- **`LISTEN/NOTIFY` hot reload.** `PgBackend::watch_schema` returns `None`; the
  reload pipeline ([05-schema-cache.md](05-schema-cache.md)) needs the push
  signal on a dedicated connection with reconnect/backoff.
- **SQLite backend. `[Closed]`** The `pgvis-sqlite` crate now provides
  `SqliteBackend` implementing `Backend` (introspection + execution). DSN
  detection in `pgvis-lib` auto-selects it for `sqlite:` URIs or `.db`/`.sqlite3`
  file paths.
- **MCP execution. `[Closed]`** `McpServer` now holds an `Arc<dyn Backend>` and
  `handle_tool_call` renders SQL, calls `backend.execute()`, and returns real
  rows (or error) as MCP tool results.
- **`pgvis-server` config layering.** `serve`/`mcp`/`openapi`/`inspect` are
  wired through `pgvis-lib`, but `load_config` still returns `Config::default()`
  — figment TOML/`PGVIS_*` layering is stubbed
  ([pgvis-server/src/main.rs](../crates/pgvis-server/src/main.rs)).
- **OpenAPI richness.** Request/response JSON Schemas, per-column filter
  parameters, RPC bodies, and `openapi_mode = FollowPrivileges` filtering remain
  ([openapi.rs](../crates/pgvis-router/src/openapi.rs)).
- **Auth enforcement.** `Config` carries JWT/role settings; the execute path
  already applies `ExecContext.role`/`claims`, but verifying the JWT and
  populating those fields from it is not yet wired (REST passes `claims: None`).

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
- **Batch/pipelined execution.** The `Backend` contract is one statement per
  call; bulk/pipelined execution would be an additive trait method.

## Verification path as features land

The intended end-to-end check once the execute seam closes: run the `pgvis`
binary against a known schema and exercise it with PostgREST's own
HTTP-level expectations, asserting parity on query DSL, `Prefer` semantics, and
`PGRST*` error codes. The parser, plan layer, and SQL builder remain
independently testable without a database
([02-core-pipeline.md](02-core-pipeline.md)).
