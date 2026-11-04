# 03 — Backends and Dialects

How pgvis stays database-agnostic. There are two distinct mechanisms, and the
split between them is the central design idea:

- **`Backend` trait** — the *async I/O boundary*. Connection pooling, query
  execution, schema introspection, change notifications. One trait, implemented
  per database. **Status: `[Implemented]`** for Postgres (`introspect`,
  `execute`, `watch_schema` via `LISTEN pgrst`, `pool_status`) and SQLite
  (`introspect`, `execute`; no schema watch).
- **`Dialect` struct** — *pure data* describing SQL syntax and capability
  differences. No I/O, no trait, no dynamic dispatch. **Status: `[Implemented]`**
  (`POSTGRES` and `SQLITE` constants both defined).

The planner and SQL builder only read the `Dialect` data; surfaces only see
`Arc<dyn Backend>`, never a concrete driver.

```mermaid
flowchart TB
    plan["<b>plan layer</b><br/>capability gate + FilterRewrite"]
    sqlb["<b>query::render</b><br/>syntax fields"]
    dl["<b>Dialect</b><br/>&amp;'static data, no I/O"]
    adp["<b>Surfaces</b><br/>hold Arc&lt;dyn Backend&gt;"]
    pgb["<b>PgBackend</b><br/>one pool"]
    rep["<b>PgReplicaBackend</b><br/>primary + replica pools"]
    lite["<b>SqliteBackend</b><br/>writer + reader connections"]
    pg[("PostgreSQL")]
    sq[("SQLite")]

    plan --> dl
    sqlb --> dl
    adp --> pgb
    adp --> rep
    adp --> lite
    pgb --> pg
    rep --> pg
    lite --> sq
```

## The `Backend` trait

Defined in [pgvis-core/src/backend.rs](../crates/pgvis-core/src/backend.rs):

```rust
pub trait Backend: Send + Sync + 'static {
    fn introspect(&self, cfg: &IntrospectConfig)
        -> BoxFuture<'_, Result<SchemaCache, Error>>;

    fn execute(&self, ctx: &ExecContext, sql: &str, params: &[Value])
        -> BoxFuture<'_, Result<QueryResult, Error>>;

    fn watch_schema(&self) -> BoxFuture<'_, Option<SchemaChangeStream>> { /* None */ }

    fn dialect(&self) -> &'static Dialect;

    fn pool_status(&self) -> Option<PoolStatus> { /* None */ }
}
```

Design choices baked into this signature (see also
[07-design-decisions.md](07-design-decisions.md)):

- **Object-safe via `BoxFuture`.** Methods return `futures::future::BoxFuture`
  rather than using `async fn`, so adapters can hold `Arc<dyn Backend>` and
  never name a concrete driver type. The one allocation per call is negligible
  next to network I/O.
- **`serde_json::Value` params.** The SQL builder emits generic JSON values; the
  driver converts them to native wire types. This keeps the builder
  driver-free.
- **One `execute`, not per-operation methods.** Because the SQL builder fully
  renders the CTE-wrapped statement, the backend just runs the string and
  decodes the uniform [`QueryResult`](../crates/pgvis-core/src/backend.rs).
- **Synchronous `dialect()`.** Returns `&'static Dialect` — cheap, no I/O,
  callable from the hot SQL path.

### Supporting types

All in [backend.rs](../crates/pgvis-core/src/backend.rs):

| Type | Role |
| ------ | ------ |
| `IntrospectConfig` | which schemas to expose + `extra_search_path` for type/function resolution |
| `ExecContext` | per-request session setup: `role`, JWT `claims`, `pre_request`, `statement_timeout`, `tx_end`; routing hint `is_mutation`; `raw_body`; mutation guards `max_affected` and `single_row` |
| `QueryResult` | decoded CTE result: `body` (or `raw_body`, the database's JSON text), `total_count`, `page_total`, `response_status`, `response_headers`, `was_insert` |
| `SchemaChangeStream` | `Pin<Box<dyn Stream<Item=()> + Send>>` — push schema-reload signal |
| `PoolStatus` | pool occupancy (`max_size`, `size`, `available`, `waiting`), reported by `pool_status()` |

`ExecContext` is how row-level security and PostgREST-style GUC behaviour reach
the database: on Postgres the backend opens a transaction and sets `role`,
`request.jwt.claims` and `statement_timeout` with transaction-local
`set_config(..., true)` (the same effect as `SET LOCAL`), optionally calls the
pre-request function, then runs the statement. Per-claim
`request.jwt.claim.<key>` GUCs are not set; `request.jwt.claims` carries every
claim, as in current PostgREST. On SQLite the role and claims are ignored.

### The Postgres implementations

#### `PgBackend` — single-server

[`PgBackend`](../crates/pgvis-postgres/src/lib.rs) wraps a
`deadpool-postgres::Pool` (lazy connections; created from a DSN). It:

- `introspect()` — gets a pooled client and calls
  `introspect::load_schema_cache` ([05-schema-cache.md](05-schema-cache.md)).
- `execute()` — checks out a connection as a `Checkout` guard and calls
  `execute::execute_query`
  ([execute.rs](../crates/pgvis-postgres/src/execute.rs)); see the sequence
  below. A failed checkout (pool exhausted or database unreachable) is 503
  `PGRST000`, as in PostgREST.
- `watch_schema()` — `LISTEN pgrst` on a dedicated connection
  ([schema_watch.rs](../crates/pgvis-postgres/src/schema_watch.rs)); see
  [05-schema-cache.md](05-schema-cache.md).
- `dialect()` — returns `&pgvis_core::dialect::POSTGRES`.
- `pool_status()` — occupancy of the request pool.

**The pipelined executor.** `execute_query` sends BEGIN, one batched
`SELECT set_config($1, $2, true), …` for every GUC, the optional pre-request
call and the CTE-wrapped statement as one pipelined flight (`futures::join!`
over unnamed `query_typed` statements: no per-request PREPARE, safe behind
transaction-mode poolers). Parameters are bound by a `TextParam` `ToSql`
wrapper in the text format, so Postgres coerces them by inferred type. The
mutation guards are checked against the CTE's `page_total` before the
transaction ends, so a violation rolls the writes back. That makes two round
trips per request regardless of how many claims the JWT carries.

```mermaid
sequenceDiagram
    participant B as PgBackend
    participant C as Checkout
    participant PG as Postgres
    B->>C: pool.get() (503 PGRST000 on failure)
    Note over C,PG: one pipelined flight
    C->>PG: BEGIN
    C->>PG: SELECT set_config(role, claims, timeout)
    C->>PG: SELECT pre_request() (if configured)
    C->>PG: CTE-wrapped statement
    PG-->>C: results (the first error wins)
    C->>C: guards: max-affected, singular row
    alt error, guard violated, or tx=rollback
        C->>PG: ROLLBACK
    else success
        C->>PG: COMMIT
    end
    C->>C: decode the CTE row into QueryResult
    C-->>B: QueryResult or Error
```

There is no RAII transaction, so `Checkout` guards the connection instead: if
the request future is dropped mid-flight (client disconnect, timeout), its
`Drop` detaches the connection from the pool and closes it, and the server
rolls the open transaction back. A connection still inside a transaction, with
the caller's role set, is never handed to the next request. The body is
decoded only after COMMIT/ROLLBACK, so locks and the connection are not held
while JSON is parsed; table reads skip parsing entirely and forward the
database's JSON text (`ExecContext::raw_body`).

**Pool lifecycle configuration:** Pool creation is centralized in
[`create_pool()`](../crates/pgvis-postgres/src/lib.rs) which applies all
settings from [`PoolConfig`](../crates/pgvis-core/src/config.rs):

| Layer | Settings | Purpose |
|-------|----------|---------|
| Connection-level | `keepalives`, `keepalives_idle`, `connect_timeout` | TCP health; detect dead sockets through NATs/LBs |
| Pool-level | `max_size`, `wait`/`create`/`recycle` timeouts | Bound resource usage and prevent unbounded waits |
| Manager-level | `recycling_method` (Fast/Verified/Clean) | Validate connections on checkout; trade latency for safety |

Both `PgBackend` and `PgReplicaBackend` share the same `create_pool` function,
so identical pool settings apply uniformly to the primary and all replica pools
(the replica health probes reuse it with `size = 1` and short timeouts).

#### `PgReplicaBackend` — primary + read replicas `[Implemented]`

[`PgReplicaBackend`](../crates/pgvis-postgres/src/replica.rs) extends the
single-server model with replica-aware routing:

Eligible plans select a reader from a bounded snapshot of stable pool entries.
Writes, unverified read plans, pre-request hooks and explicit primary-consistency
requests stay on the primary. All adapters use `plan::replica_read_eligible`.

```mermaid
flowchart TB
    exec(["Backend::execute(ctx, sql, params)"])
    write{"mutation, require_primary,<br/>or pre_request?"}
    pick{"eligible reader?"}
    get{"reader checkout ok?"}
    reader["<b>Reader pool</b><br/>replica, or primary if primary_reads"]
    primary["<b>Primary pool</b>"]

    exec --> write
    write -->|"yes: write, volatile RPC, pub/sub"| primary
    write -->|no| pick
    pick -->|"yes: round-robin"| get
    pick -->|none| primary
    get -->|yes| reader
    get -.->|"no: try the next reader once"| pick
    get -->|"no, twice"| primary
```

**Routing logic:**
- `is_mutation = true` → always the primary pool. The router sets it for
  mutations *and* for calls to `VOLATILE` functions (`plan_writes`), and the
  pub/sub handlers set it for their statements (NOTIFY, the authorize
  function). Unknown table metadata, views, foreign/nonpermanent tables, inheritance parents, RLS and computed
  relationships set `require_primary`. Volatility is not proof of side-effect
  freedom: reader transactions enforce READ ONLY and never replay execution errors.
- Eligible read plans without a hook or `require_primary` → round-robin across readers (replicas +
  optionally the primary); on a checkout failure the next eligible reader is
  tried once.
- No eligible reader, or both tries failed → the primary.

**Health monitoring:** A background task runs every `health_check_interval_ms`
(default 5 s). It probes over its **own one-connection pools** with short
timeouts (the interval clamped to 0.5–5 s), never the request pools: probing
through a saturated request pool made a healthy but busy replica look
unreachable, excluding it and piling its load onto the rest. Each tick reads
`pg_current_wal_lsn()` on the primary, then checks every replica concurrently,
each under its own deadline, comparing `pg_last_wal_replay_lsn()` (the
position visible to queries) with the primary's. Replicas more than
`max_replication_lag_bytes` (default 10 MB) behind, unreachable, or not in
recovery are excluded. Unknown primary LSN excludes lag-checked readers. Static
DSNs with explicitly disabled lag checking retain connectivity-only probes.
A short mutex protects the topology snapshot; each entry has its own eligibility
and retirement gate. No lock is held across network I/O and no old bitmap can
select a different replacement pool. At most 64 reader slots are supported.

```mermaid
flowchart TB
    tick(["every health_check_interval_ms"])
    lsn["<b>Primary WAL position</b><br/>own 1-connection probe pool"]
    probe["<b>Probe every replica</b><br/>concurrently, each with a deadline"]
    lag{"reachable, in recovery,<br/>lag within the limit?"}
    ok["<b>eligible</b>"]
    out["<b>excluded</b><br/>lagging or unreachable"]
    bits[("Stable reader entries + eligibility")]

    tick --> lsn --> probe --> lag
    lag -->|yes| ok --> bits
    lag -->|no| out --> bits
```

`PgReplicaBackend::eligible_readers()` reports how many readers passed the last
check; `pool_status()` reports the primary's pool, which takes every write.
Introspection runs on the primary, and `watch_schema()` listens on the
primary, where DDL is made and announced.

**Failover:** pgvis does NOT handle promotion. External tools (Patroni,
pg_auto_failover, DNS/VIP) handle that. `deadpool-postgres` evicts broken
connections; when the DSN resolves to a new primary, the next pool checkout
reconnects automatically.

**Configuration:** See [`ReplicaConfig`](../crates/pgvis-core/src/config.rs)
in `Config.replica`:
- `replica_dsns` — list of replica connection strings
- `max_replication_lag_bytes` — lag threshold (0 disables checking)
- `health_check_interval_ms` — monitor tick interval
- `primary_reads` — whether primary participates in read load balancing
- `discovery` — opt-in bounded physical streaming discovery, trusted networks,
  listening port, refresh/stale intervals and capacity. See the
  [configuration reference](../website/src/content/docs/reference/configuration.mdx).

**Activation:** Automatic via the Builder. When `config.replica.replica_dsns`
is non-empty or discovery is configured, and the DSN is Postgres, `pgvis-lib` constructs
`PgReplicaBackend` instead of `PgBackend`. No surface-layer changes needed.

## The `Dialect` struct

Defined in [pgvis-core/src/dialect.rs](../crates/pgvis-core/src/dialect.rs). It
is a flat struct of syntax fields and boolean capability flags, *not* a trait.

Rationale: a `dyn Dialect` trait would mean a virtual call for every SQL
fragment — thousands per complex query. A flat `&'static` struct is
branch-prediction-friendly and zero-cost to pass around. Capability decisions
are made *once* in the plan layer, not re-derived per fragment.

### Syntax fields

| Field | Postgres | SQLite |
| ------- | ---------- | -------- |
| `identifier_quote` | `"` | `"` |
| `placeholder` | `Numbered` (`$1`) | `Question` (`?`) |
| `json_array_agg` | `json_agg` | `json_group_array` |
| `json_object` | `json_build_object` | `json_object` |

`Placeholder::render(n)` produces the placeholder text; `RenderContext`
([02-core-pipeline.md](02-core-pipeline.md)) calls it.

### Capability flag matrix

From the `POSTGRES` and `SQLITE` constants in
[dialect.rs](../crates/pgvis-core/src/dialect.rs):

| Flag | Postgres | SQLite | Effect when false |
| ------ | :--------: | :------: | ------------------- |
| `supports_returning` | ✓ | ✓ | no `RETURNING`; mutation return needs a re-select |
| `supports_roles` | ✓ | ✗ | `ExecContext.role` ignored (no RLS) |
| `supports_listen_notify` | ✓ | ✗ | no push schema reload; poll/watch instead |
| `supports_set_local` | ✓ | ✗ | no GUC readback; `response_status/headers` always `None` |
| `schema_namespacing` | ✓ | ✗ | table refs drop the schema qualifier |
| `has_routines` | ✓ | ✗ | no `/rpc/*` routes, no `call_*` MCP tools |
| `supports_aggregates` | ✓ | ✓ | `select=col.sum()` rejected |
| `supports_ilike` | ✓ | ✗ | rewrite `ILIKE` → `LOWER() LIKE LOWER()` |
| `supports_regex_match` | ✓ | ✗ | `match`/`imatch` rejected or rewritten |
| `supports_fts` | ✓ | ✓ | full-text search rejected (different syntax per DB) |
| `supports_array_ops` | ✓ | ✗ | `cs`/`cd`/`ov` rejected |
| `supports_range_ops` | ✓ | ✗ | range operators rejected |
| `supports_estimated_count` | ✓ | ✗ | `count=estimated` falls back to `exact` |
| `supports_quantifiers` | ✓ | ✗ | `op(any)`/`op(all)` rejected or fanned out |
| `supports_set_timezone` | ✓ | ✗ | `Prefer: timezone` ignored |
| `supports_is_distinct` | ✓ | ✗ (conservative) | `IS DISTINCT FROM` avoided |
| `supports_row_to_json` | ✓ | ✗ | embeds serialize with explicit `json_object(...)` |
| `supports_json_recordset` | ✓ | ✗ | INSERT binds one parameter per value instead of one JSON parameter (`json_populate_recordset`) |
| `escape_string_literals` | ✓ | ✗ | JSON-path keys rendered as plain `'...'` instead of `E'...'` |

`row_identifier` (`ctid` on Postgres, `rowid` on SQLite) is the system column
used for limited PATCH/DELETE (`WHERE <id> IN (SELECT <id> … LIMIT n)`).

### How dialect gating works

Two phases, never re-checked in the SQL builder:

1. **Plan-time rejection.** `validate::validate_dialect_support`
   ([plan/validate.rs](../crates/pgvis-core/src/plan/validate.rs)) returns an
   `Error::Unsupported` (HTTP 400, code `PGV001`) when a request needs a
   capability the dialect lacks — e.g. an array operator on SQLite, or an
   `/rpc/*` call when `has_routines` is false.
2. **Plan-time rewrite annotation.** When an operator is *expressible
   differently* rather than impossible, the planner attaches a `FilterRewrite`
   to the `ResolvedFilter` (`InstrFallback` for ILIKE as
   `LOWER() LIKE LOWER()`, `JsonArrayContains`, `GlobPattern`,
   `JsonExtractFunction`, `LikePattern`). The SQL builder reads
   the hint and emits the dialect-appropriate fragment with no capability
   logic of its own.

`FilterRewrite` is defined alongside `ResolvedFilter` in
[plan/types.rs](../crates/pgvis-core/src/plan/types.rs). It is the explicit
bridge that keeps the plan layer and SQL builder decoupled while still producing
correct multi-dialect SQL.

## SQLite backend `[Implemented]`

[`SqliteBackend`](../crates/pgvis-sqlite/src/lib.rs) (`rusqlite` +
`tokio-rusqlite`) opens one writer connection, serialized by a mutex, plus a
set of reader connections in WAL mode; `ExecContext::is_mutation` picks the
writer. It differs from Postgres along the lines the flags anticipate:

- single namespace (`schema_namespacing = false`; cache uses `"main"` by
  convention — see [05-schema-cache.md](05-schema-cache.md))
- no CTE envelope: the router renders with `query::render_inner` and the
  executor assembles the JSON body in Rust
  ([execute.rs](../crates/pgvis-sqlite/src/execute.rs))
- no roles / GUCs (`ExecContext` role and claims are ignored; no response GUCs)
- `ILIKE`/regex/array/range/quantifier rewrites or rejections per the matrix
- no push schema reload: `watch_schema()` returns `None`, so a reload comes
  from `SchemaReloader::reload()` / `reload_now()`

## Adding a new database (e.g. MySQL/DuckDB) `[Planned]`

The architecture is designed so a new backend touches only its own crate:

1. Add a `&'static Dialect` constant (quote char, placeholder style, JSON
   function names, capability flags).
2. If an operator needs different syntax, add a `FilterRewrite` variant and emit
   it from the SQL builder's rewrite match.
3. Implement `Backend` (pool, introspection against the DB's catalog,
   execute, optional `watch_schema`).
4. No changes to the parser, plan layer, or any surface adapter.

This is examined further, with the known sharp edges (e.g. `Dialect` is not yet
`#[non_exhaustive]`, three-part names for catalog databases), in
[08-future-scope.md](08-future-scope.md).


## PostgreSQL admission and adaptive sizing

The default PostgreSQL pool has a fixed ceiling. `pool.max_waiters` (default
1024) bounds pending checkouts; excess requests fail with 503. `pool.timeout_ms`
covers the whole checkout, including connect and recycle. A client-side
`pool.query_timeout_ms` (default 30000) also bounds stalled queries and schema
introspection. An explicit SQL statement timeout gets an extra second to return
its server error. Setting a deadline to zero disables it; adaptive mode requires
both deadlines. A cancelled or timed-out request discards its connection so
transaction state cannot reach another caller. A timeout does not prove that a
write failed: pgvis never automatically replays a submitted query.

Optional AIMD controls the raw pool size within its configured ceiling:

```toml
[pool]
size = 16
max_waiters = 256
timeout_ms = 5000
query_timeout_ms = 30000

[pool.adaptive]
min_size = 2
target_query_time_ms = 100
adjustment_interval_ms = 1000
```

At most once per interval, healthy completed work with queued demand adds one
connection; slow queries, connection failures or database overload halve the
limit down to the minimum. Shrinking retires excess connections on return; it
does not cancel active work. Mixed long/short workloads need a suitable latency
target. This is application-local admission, not shared transaction multiplexing
across processes. No equivalence to PgBouncer capacity has been established.
Each primary/replica request pool has its own ceiling; reserve a total budget
across **all instances and all pools**, plus one health probe connection per
monitored endpoint, one schema listener and the separate optional pub/sub pool
and listener. Direct raw-pool callers are responsible for their own admission
and must not resize beyond their budget.

Replica lag checks use replay LSN (not received WAL) and start with unverified
replicas excluded. If primary LSN cannot be obtained, lag cannot be proven and
readers are excluded for that check; static DSNs with `max_replication_lag_bytes = 0`
explicitly opt into connectivity-only checks. Discovery always verifies cluster
identity, receiving timeline and recovery, even when the byte limit is disabled. Probes run immediately at startup, then at the
configured positive interval. Health can change between probe and query;
requests still need error handling. Promotion and writer endpoint selection
belong to the PostgreSQL HA system; pgvis does not elect or promote a primary.
Schema listeners reconnect with backoff and coalesce notifications. Dropping a
backend/reloader stops its monitoring tasks and dedicated connections.


The opt-in `failover` integration test starts and removes its own primary and
streaming standby (`initdb`, `pg_basebackup`, `pg_ctl` required). It exercises
paused replay, catch-up, primary loss, external promotion, a multi-host
`target_session_attrs=read-write` writer DSN, and a timed-out mutation whose
nontransactional attempt counter must remain one. A promoted node retains its
last replay LSN, so lag-based readers also require `pg_is_in_recovery()`.

```sh
cargo test -p pgvis-server --test failover -- --ignored --nocapture
cargo test -p pgvis-server --test pool compare_fixed_and_adaptive -- --ignored --nocapture
```

A local PostgreSQL 18.4 run on 2026-11-04, with 2048 concurrent callers, 8192
2 ms sleeping queries per mode, and a 16-connection ceiling, completed without
checkout/query errors or exceeding that ceiling. Fixed sizing took 1.296 s
(p95 request latency 325 ms); adaptive 2→16 took 1.419 s (p95 402 ms), including
its ramp-up. This synthetic single-process run validates bounds and records
ramp-up cost; it does not model a multi-node production workload, network
partitions, or compare PgBouncer throughput. A separate regression verifies
that shrinking preserves active work and retires excess connections on return.

The opt-in `discovery` integration test needs an isolated Linux Ubuntu host,
passwordless sudo, iproute2 and PostgreSQL tools in the Nix development shell:

```sh
PGVIS_DISCOVERY_NETNS=1 cargo test -p pgvis-server --test discovery -- --ignored --nocapture
```

It creates and cleans up a private bridge, two network namespaces and three
PostgreSQL clusters. Namespaces make each WAL receiver originate from its query
address, unlike multiple loopback listeners. It checks plan routing and schema
reload, discovery membership and privilege failure, draining reads, bounded
flapping, restoration, pooled-session promotion safety and task cleanup. It never
uses an existing PostgreSQL database. Do not run the privileged fixture on a
shared production host.
