# Data Cache

`[Implemented]` — opt-in; keyed by role + claims + rendered query + table generation; table-scoped invalidation on writes, global on volatile RPC and schema reload

An optional **in-memory response cache** for read queries. When enabled, a
read whose response is cacheable is stored under a key that hashes the security
context and the rendered query, and subsequent identical reads are served from
memory without touching the backend. A write to a table bumps that table's
generation counter, causing subsequent key computations to produce different keys
— effectively invalidating stale entries without clearing the store.

The cache is **off by default**. It is created inside
[`build_app()`](../crates/pgvis-router/src/routing.rs) only when
`config.cache.enabled == true`, so a disabled cache allocates nothing and adds
no per-request cost beyond a single `Option` check.

- Cache module: [pgvis-router/src/data_cache.rs](../crates/pgvis-router/src/data_cache.rs)
- Dispatch integration: [pgvis-router/src/routing.rs](../crates/pgvis-router/src/routing.rs)
- Config struct: [pgvis-core/src/config.rs](../crates/pgvis-core/src/config.rs) (`CacheConfig`)
- Underlying store: the [`svcache`](https://crates.io/crates/svcache) crate, 0.1.1 (TTL + SIEVE eviction over a sharded `DashMap`)

## Where it sits in the request lifecycle

The cache is a wrapper around two points in `dispatch_request`: a read-side
lookup before backend execution, and store/invalidate hooks after it. Parsing,
planning, and SQL building all run unchanged — the cache key is computed *from*
the finished `ReadPlan` plus the rendered SQL, so it never alters what would be
executed on a miss.

```mermaid
flowchart TB
    req(["Request, planned and rendered"])
    kind{"plan type?"}
    key{"cacheable read?<br/>compute_key"}
    hit{"cache hit?"}
    fmt["<b>format_response</b><br/>from cached bytes"]
    exec["<b>Backend::execute</b><br/>cache miss"]
    store["<b>store</b><br/>body bytes under key"]
    plain["<b>Backend::execute</b><br/>not cached"]
    wexec["<b>Backend::execute</b><br/>write"]
    inv["<b>invalidate</b><br/>table or global generation"]
    resp(["HTTP response"])

    req --> kind
    kind -->|Read| key
    kind -->|"Mutate or volatile RPC"| wexec --> inv --> resp
    kind -->|"other RPC"| plain
    key -->|"no, or pre_request set"| plain --> resp
    key -->|yes| hit
    hit -->|yes| fmt --> resp
    hit -->|no| exec --> store --> resp
```

Source: the read lookup, store, and invalidate blocks are steps 4b / 5b / 5c in
[`dispatch_request`](../crates/pgvis-router/src/routing.rs). The hit path
reconstructs a `QueryResult` from the cached entry and runs it through the same
[`response::format_response`](../crates/pgvis-router/src/response.rs) as a live
result, so singular (`Accept: application/vnd.pgrst.object`), pagination, and
cursor headers are applied identically on hits and misses.

## What gets cached

`compute_key` ([data_cache.rs](../crates/pgvis-router/src/data_cache.rs)) decides
cacheability and returns `Some(key)` or `None`:

| Query shape | Cached? |
| ------------- | --------- |
| PK lookup — every primary-key column filtered with `eq` (non-negated, single value) | Always (when cache enabled) |
| List / collection query | Only when `cache_lists = true` |
| Any query with embeds | Never |
| Mutations (`POST`/`PATCH`/`PUT`/`DELETE`), RPC | Never read-cached |
| Any read while `pre_request` is configured | Never (the hook must see every request) |

Embeds are excluded because a join pulls rows from multiple tables; only the
top-level table is tracked.

### Key identity

The PK-vs-list distinction decides *cacheability* only — it is **not** the key.
Every cacheable read gets the same key form:

```text
{role}:{pk|list}:{schema}.{table}:g{generation}:{hash}
```

where `generation` is the table's generation counter plus the global one
(`table_generation()`; the table counter is bumped by writes to that table, the
global one by volatile RPCs and schema reloads), and `hash` is a 64-bit keyed
SipHash (random per-process key) of, in order:

1. the JWT **claims** (streamed directly from the `serde_json::Value` tree
   without allocating an intermediate String), then
2. the **rendered SQL**, then
3. the **bound parameters** (also streamed from the Value tree).

`role` is the resolved DB role (or `"anon"`) and prefixes the key, so different
roles never collide. Folding claims into the hash means two users sharing a role
but differing in identity (e.g. `sub`) get **distinct** entries — a hit never
serves one user's RLS-filtered rows to another. Folding the rendered SQL +
params in means the same PK requested with a different `select`, an extra
filter, a different order, or different pagination also gets a distinct entry —
the key can never alias two responses of different shape. Including the generation
counter means a write automatically causes subsequent reads to compute a
different key — a cache miss — without needing to scan or clear the store.
(The `pk`/`list` label is purely for readability when inspecting keys.)

### Hashing strategy

The hash is SipHash keyed with a random per-process key (`std::hash::RandomState`,
held by the `DataCache`). The hashed inputs include request data (bound
parameters, claims), so an unkeyed hash such as FNV would let a caller construct
inputs whose key collides with another caller's entry and read or plant it; with
a secret key that can't be targeted.
The `hash_json_value()` helper walks the JSON value tree recursively, feeding
type-discriminant tags and raw bytes directly into the hasher, avoiding
the allocation that a `claims.to_string()` + `DefaultHasher` approach would
incur on every cacheable read.

## Invalidation model

Invalidation is **generation-based**: writes bump a per-table generation counter
stored in `table_generations: RwLock<HashMap<String, u64>>`. Since the generation
is embedded in every cache key, bumping it causes all subsequent
`compute_key()` calls for that table to produce keys that don't match any
existing stored entry — effectively a cache miss without clearing the store. Old
entries are eventually evicted by SIEVE pressure or TTL expiry.

Three events trigger invalidation; the first two in step 5c of
`dispatch_request` ([routing.rs](../crates/pgvis-router/src/routing.rs)):

- **Mutations** (`ActionPlan::Mutate` — INSERT/UPDATE/DELETE) call
  `invalidate_table(target)`, which bumps only that table's generation.
- **Volatile RPC** (`ActionPlan::Call` whose `function_info.volatility ==
  Volatile`) calls `invalidate_all`, which bumps a global generation counter
  (`AtomicU64`). Since `table_generation()` returns
  `global_gen + table_gen`, this invalidates all tables.
- **Schema reload** — before each lookup the router calls
  `sync_schema(cache.built_at)`; when the `SchemaCache` was rebuilt since the
  last call it runs `invalidate_all`, so no response cached against the old
  schema is served ([05-schema-cache.md](05-schema-cache.md)).

This approach avoids the thundering-herd problem of whole-store clearing: a write
to table `T` only invalidates entries for `T`, while entries for unrelated tables
remain hot. For cascading FK writes, the current model is still optimistic (only
the directly-mutated table is invalidated). If a future FK/trigger dependency
graph is added, `invalidate_table` can be extended to bump related tables too.

### Remaining staleness edge

A function that actually modifies data but is mislabeled `STABLE`/`IMMUTABLE` in
the catalog will not trigger invalidation (only `VOLATILE` does). That is a
schema-definition error on the database side; `ttl_seconds` still bounds the
resulting staleness.

The cache is also per process: writes made through another pgvis instance, or
directly in the database (triggers, jobs, `psql`), do not bump this instance's
generations. Only `ttl_seconds` bounds that staleness.

## TTL and capacity

Backed by `svcache::SvCache::with_ttl_and_limit(ttl, max_entries)`:

- **TTL** (`ttl_seconds`, default 60) — an entry older than the TTL is
  invisible to lookups at once and reclaimed lazily (by a lookup, the eviction
  hand, or a bounded sweep). A miss on an expired entry re-queries the backend.
- **Capacity** (`max_entries`, default 10000) — when full, svcache evicts with
  SIEVE: a hand sweeps from the oldest entry, clearing the "visited" bit set by
  hits and evicting the first entry whose bit is clear (an expired entry
  regardless). O(1) amortized; without hits it is plain FIFO.

TTL is the backstop for every staleness gap: even when an invalidation is
missed (the mislabeled-volatility edge above), no entry outlives `ttl_seconds`.

## Caveats before enabling

1. **Per-identity RLS is handled by the key, not by the security boundary.**
   Because claims are folded into the key, two users sharing a role get distinct
   entries, so a hit cannot leak one user's rows to another. The flip side is hit
   rate: highly-personalized data (a distinct claims set per request) produces a
   distinct entry per user and benefits little from caching. PK lookups on
   shared, role-gated data are the sweet spot.
2. **`cache_lists` cardinality.** List keys hash the full SQL+params, so every
   distinct filter/order/pagination combination is a separate entry. High-variety
   list traffic yields low hit rates and high memory churn; it is off by default
   for this reason.

## Observability

`GET /pgvis/cache` returns the current settings, a stats snapshot, and the
backend's pool occupancy (handler: `handle_cache_info` in
[routing.rs](../crates/pgvis-router/src/routing.rs)). The endpoint is always
registered and authenticates like the data API; `stats` is `null` when caching
is disabled, and `pool` is `null` for a backend without a pool (SQLite).

```json
{
  "settings": { "enabled": true, "ttl_seconds": 60, "max_entries": 10000, "cache_lists": false },
  "stats":    { "hits": 1280, "misses": 240, "invalidations": 12, "entries": 305, "hit_rate": 84.21 },
  "pool":     { "max_size": 16, "size": 4, "available": 3, "waiting": 0 }
}
```

`entries` is read live from `SvCache::len()`, so it reflects the actual stored
set rather than a running counter; it may briefly include entries that are
expired-but-not-yet-reclaimed.
`hits`/`misses`/`hit_rate`/`invalidations` are exact counters.

## Configuration

See [06-errors-and-config.md](06-errors-and-config.md) for the config system as a
whole. The cache is the `[cache]` table / `CacheConfig` struct:

| Field | Env (any command) | Default | Meaning |
| ------- | ----- | --------- | --------- |
| `cache.enabled` | `PGVIS_CACHE__ENABLED` | `false` | Master switch. Nothing is allocated when off. |
| `cache.ttl_seconds` | `PGVIS_CACHE__TTL_SECONDS` | `60` | Entry lifetime before expiry. |
| `cache.max_entries` | `PGVIS_CACHE__MAX_ENTRIES` | `10000` | Capacity; SIEVE eviction beyond it. |
| `cache.cache_lists` | `PGVIS_CACHE__CACHE_LISTS` | `false` | Cache list queries, not just PK lookups. |

CLI flags on `pgvis serve` (`--cache-enabled`, `--cache-ttl`,
`--cache-max-entries`, `--cache-lists`) override the loaded config; each flag
also reads its own env var (`PGVIS_CACHE_ENABLED`, `PGVIS_CACHE_TTL`,
`PGVIS_CACHE_MAX_ENTRIES`, `PGVIS_CACHE_LISTS`), which therefore applies to
`serve` only ([pgvis-server/src/main.rs](../crates/pgvis-server/src/main.rs)).
