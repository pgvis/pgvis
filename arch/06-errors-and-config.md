# 06 — Errors, Configuration, Preferences

The three cross-cutting concerns that every surface and the core share.
**Status: `[Implemented]`** for the types and parsing. REST and MCP both
consume them at the execute boundary; a few `Prefer` tokens and config flags
are parsed but not applied yet (marked below).

## Errors

Module [pgvis-core/src/error.rs](../crates/pgvis-core/src/error.rs). One unified
`Error` enum plus a machine-readable `ErrorCode`, both PostgREST-compatible so
existing clients (e.g. `supabase-js`, `postgrest-py`) handle pgvis errors
unchanged.

### Shape

Every error serializes to the PostgREST JSON shape:

```json
{ "code": "PGRST200", "message": "...", "details": "...", "hint": "..." }
```

`Error` variants: `Introspection`, `Execution { message, db_code, detail, hint }`,
`Parse { message, detail, code }`, `Plan { message, detail, hint, code }`,
`Config`, `Auth { message, code }`, `Unsupported`, `Internal`,
`PubSub { message, code }`. Convenience
constructors (`invalid_select`, `invalid_filter`, `ambiguous_relationship`,
`not_found`, `unsupported`) build the right `code` + `hint` so call sites stay
terse and messages stay actionable.

### Codes and HTTP mapping

`ErrorCode::as_str()` yields the `PGRST*` (or pgvis-specific `PGV*`) string;
`http_status()` yields the status:

| Code | `ErrorCode` | HTTP |
| ------ | ----------- | ------ |
| `PGRST000` | `ConnectionError` (introspection failure, pool exhausted or database unreachable) | 503 |
| `PGRST100` | `InvalidSelect` / `InvalidFilter` / `InvalidOrder` | 400 |
| `PGRST102` | `InvalidBody` | 400 |
| `PGRST103` | `InvalidRange` | 416 |
| `PGRST109` | `StatementTimeout` | 504 |
| `PGRST116` | `NotSingular` (singular response, not exactly one row) | 406 |
| `PGRST119` | `SpreadOnToMany` | 400 |
| `PGRST122` | `InvalidPreference` (`handling=strict`) | 400 |
| `PGRST123` | `AggregatesDisabled` | 400 |
| `PGRST124` | `MaxAffectedExceeded` | 400 |
| `PGRST200` | `RelationshipNotFound` | 400 |
| `PGRST201` | `AmbiguousRelationship` | 300 |
| `PGRST202` | `FunctionNotFound` | 404 |
| `PGRST203` | `AmbiguousFunction` | 300 |
| `PGRST204` | `ColumnNotFound` | 400 |
| `PGRST205` | `NotFound` (table) | 404 |
| `PGRST301` | `JwtInvalid` / `JwtExpired` | 401 |
| `PGRST302` | `JwtMissing` (anonymous access disabled) | 401 |
| `PGRST303` | `InsufficientPrivilege` | 403 |
| `PGRST400` | `DatabaseError` (refined by `db_code`, below) | 500 |
| `PGV001` | `UnsupportedOperation` | 400 |
| `PGV002` | `ConfigError` | 500 |
| `PGV500` | `Internal` | 500 |
| `PGVIS_PUBSUB_*` | `PubSub(..)` ([10-pubsub.md](10-pubsub.md)) | 400 / 403 / 501 / 503 |

`Error::http_status()` additionally special-cases the database `db_code` on
`Execution` errors so constraint/permission failures map precisely —
`23505`/`23503` → 409; `23502`/`23514` and invalid input (`22P02`, `22003`,
`22007`) → 400; `42501` → 403; `42P01`/`42883` → 404; `57014` (statement
timeout) → 504; `25006` (read-only transaction) → 405; `P0001` (`RAISE
EXCEPTION`) → 400; `08xxx` → 503; and PostgREST's `PTxyz` convention lets a
function choose status `xyz` — before falling back to the code's default
status.

Adapters consume this directly: the REST handler sets the response status from
`err.http_status()` and the body from `err.code().as_str()` + `err.to_string()`
([response.rs](../crates/pgvis-router/src/response.rs)); for a database error
the body's `code` is the SQLSTATE, as in PostgREST. MCP returns the same
`code`/`message`/`details`/`hint` object as a tool error
([types.rs](../crates/pgvis-mcp/src/types.rs)). Plan-time
dialect rejections surface here as `Unsupported`/`PGV001`
([03-backends-and-dialects.md](03-backends-and-dialects.md)).

## Configuration

Module [pgvis-core/src/config.rs](../crates/pgvis-core/src/config.rs).
`Config` is the one struct every crate reads; subsystems get nested
sections (`routing`, `pool`, `replica`, `cache`, `pubsub`). Process concerns —
the bind address, and the figment/`clap` layering — stay in the binary, so a
library consumer builds a `Config` value directly.

### `Config`

Shared knobs, each mapped to its PostgREST equivalent in the source doc-comments:

- **Schema selection** — `schemas`, `extra_search_path`.
- **Auth** — `jwt_secret`, `jwt_algo` (`HS256`/`HS384`/`HS512`/`RS256`/`EdDSA`),
  `jwt_aud` (required audience; unchecked when unset), `anon_role`,
  `role_claim_key`. With `jwt_secret` set and no `anon_role`, a request without
  a token is rejected rather than run as the DSN's role.
- **Feature gates** — `aggregates_enabled`, `tx_allow_override`, `read_only`
  (MCP exposes read tools only). `plan_enabled` (EXPLAIN media type) and
  `tx_rollback_all` are accepted but not acted on yet.
- **Query limits** — `max_rows` (server-side cap applied in the plan layer via
  `PlanConfig`), `statement_timeout_ms`.
- **Hooks** — `pre_request` (function called after role switch, before the main
  query; can abort the request).
- **OpenAPI** — `openapi_title`, `openapi_server_url`, `openapi_mode`
  (`IgnorePrivileges` default / `FollowPrivileges` / `Disabled`).
- **Routing** — a nested `RoutingConfig`.
- **Nested sections** — `pool` (`PoolConfig`, applied to every Postgres pool,
  [03-backends-and-dialects.md](03-backends-and-dialects.md)), `replica`
  (`ReplicaConfig`), `cache` (`CacheConfig`,
  [09-data-cache.md](09-data-cache.md)), `pubsub` (`PubSubConfig`,
  [10-pubsub.md](10-pubsub.md)).

### `RoutingConfig`

Controls URL structure *and* MCP tool naming from one place so the two surfaces
stay parallel:

- `prefix` — route prefix (`"api"` default, `""` for PostgREST compat).
- `schema_in_path` — `true` → `/{prefix}/{schema}/{table}`; `false` → schema
  from `Accept-Profile`/`Content-Profile` header or `default_schema`.
- `default_schema` — used when `schema_in_path = false`.
- `mcp_separator` — char joining schema and verb in MCP tool names (`/` default).
- Helpers: `mcp_tool_name(schema, verb, target)`, `schema_path_prefix(schema)`,
  `normalized_prefix()` — used by both [routing.rs](../crates/pgvis-router/src/routing.rs)
  and [tools.rs](../crates/pgvis-mcp/src/tools.rs). The routing modes are
  tabulated in [04-surfaces.md](04-surfaces.md).

### Layering

The standalone binary layers config with `figment` and `clap`
([pgvis-server/src/main.rs](../crates/pgvis-server/src/main.rs)); each layer
overrides the one before:

1. built-in `Config::default()`
2. the TOML file from `--config` / `PGVIS_CONFIG`
3. `PGVIS_*` environment variables (`__` separates nested keys, e.g.
   `PGVIS_CACHE__TTL_SECONDS`; `PGVIS_SCHEMAS` takes a comma-separated list)
4. CLI flags (`--schema`, `--replica-dsn`, `--cache-*`, `--pubsub-*`,
   `--read-only`); most also read their own env var (e.g. `--schema` from
   `PGVIS_SCHEMAS`)

Missing fields take their `#[serde(default)]`, so partial files are valid.
The file is strict: a missing file or an unknown key (e.g. PostgREST's
`jwt-secret` spelling) stops startup instead of silently running with
defaults.

## Preferences (the `Prefer` header)

Module [pgvis-core/src/preferences.rs](../crates/pgvis-core/src/preferences.rs).
`Preferences::parse(header) -> (Preferences, Vec<String>)` returns typed
preferences plus a list of unrecognized tokens (for `handling=strict`
validation). `applied_header()` produces the `Preference-Applied` response
echoing only honored preferences. RFC 7240 comma-separated and repeated headers
are both accepted.

| `Prefer` token | Type | Consumed by |
| ---------------- | ------ | ------------- |
| `return=representation\|minimal\|headers-only\|none` | `PreferReturn` | mutation response shape |
| `count=exact\|planned\|estimated` | `PreferCount` | `CountStrategy` in the plan; CTE `total_count` (estimated needs `supports_estimated_count`) |
| `resolution=merge-duplicates\|ignore-duplicates` | `PreferResolution` | upsert `ON CONFLICT` (`ResolvedConflict`) |
| `handling=strict\|lenient` | `PreferHandling` | whether unknown prefs 400 |
| `timezone=<tz>` | `String` | parsed only; not applied yet |
| `missing=default\|null` | `PreferMissing` | parsed only; not applied yet |
| `tx=commit\|rollback` | `PreferTx` | `ExecContext.tx_end`, gated by `Config::tx_allow_override` (dropped otherwise) |
| `max-affected=N` | `u64` | with `handling=strict`: `ExecContext.max_affected`, checked before COMMIT (`PGRST124`, rolled back) |
| `params=single-object\|multiple-objects` | `PreferParams` | parsed only; not applied yet |

`Preferences` flows unchanged through `ApiRequest` into every `ActionPlan`
variant ([02-core-pipeline.md](02-core-pipeline.md)); the dialect gates marked
above are enforced in the plan layer
([03-backends-and-dialects.md](03-backends-and-dialects.md)). On REST, `tx`,
`max-affected`, role/claims and `statement_timeout` flow through `ExecContext`
into the backend's `execute`
([execute.rs](../crates/pgvis-postgres/src/execute.rs)); `return` and `count`
shape the SQL and the response. MCP tool calls carry no `Prefer` header: they
run with the defaults, as `anon_role`.
