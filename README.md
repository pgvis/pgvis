# pgvis

**Turn any Postgres database into MCP tools for LLM agents — plus a
PostgREST-compatible REST API and an OpenAPI 3.0 document — from one Rust
engine.**

> **Status:** `v0.1.0` · early development · **Postgres & SQLite** ·
> MCP-over-SSE is on the [roadmap](#roadmap). REST, MCP, and OpenAPI work
> today on both backends, and the build-from-source quick start below is
> real and runnable.

---

## What it is

Point pgvis at a database. It introspects the schema once at startup and then
serves that schema **three ways from a single pipeline** — one query parser,
one planner, one SQL builder:

- **MCP** — every table and function becomes a typed [Model Context
  Protocol](https://modelcontextprotocol.io) tool an LLM agent can call. No
  glue code, no hand-written tool schemas.
- **REST** — a PostgREST-compatible HTTP API: the same query DSL, the same
  `Prefer` semantics, the same `PGRST*` error codes, so existing PostgREST
  clients work unchanged.
- **OpenAPI** — an OpenAPI 3.0 document generated from the same introspected
  schema.

Because all three surfaces lower into the same `ApiRequest` → plan → SQL
pipeline, their behavior never diverges. The core engine
([`pgvis-core`](crates/pgvis-core)) does **no I/O** — database drivers
implement a single `Backend` trait — which is what makes pgvis embeddable as a
library and backend-agnostic by design (Postgres and SQLite ship today, from
the same core).

How it differs from PostgREST: an I/O-free backend-agnostic core, a
first-class MCP surface for LLM agents, an embeddable Rust library (not just a
server), and schema-in-URL routing (`/api/{schema}/{table}`) with a
PostgREST-compatible flat mode for drop-in replacement.

## Quick start

**Prerequisites:** a stable Rust toolchain (Rust edition 2024 — see
[rust-toolchain.toml](rust-toolchain.toml)) and a running PostgreSQL 17 or 18 instance.

```bash
# Build the CLI (the `mcp` feature is on by default)
cargo build --release --bin pgvis

# Point it at your database and serve (binds 127.0.0.1:3000).
# Without a JWT secret or anon role the server refuses to start;
# --insecure-no-auth is for local development only.
export PGVIS_DSN="postgres://user@localhost/mydb"
./target/release/pgvis serve --insecure-no-auth

# First request — list rows from a table in the `public` schema
curl "http://localhost:3000/api/public/your_table"
```

Nix users can `nix build` or drop into a dev shell with `nix develop` (see
[flake.nix](flake.nix); the dev shell pre-sets `PGVIS_DSN` and `RUST_LOG`).

The CLI is the source of truth for flags and subcommands
([crates/pgvis-server/src/main.rs](crates/pgvis-server/src/main.rs)):

```text
pgvis --dsn <DSN> [--config <FILE>] <COMMAND>

  serve     Start the HTTP server (REST + optional MCP over Streamable HTTP)
              --bind <ADDR>     default 127.0.0.1:3000 (env PGVIS_BIND)
              --insecure-no-auth  start without jwt_secret/anon_role (local dev)
              --schema <NAME>   repeatable / comma-sep  (env PGVIS_SCHEMAS)
              --mcp-http        also serve MCP at /mcp
  mcp       Run an MCP server over stdio (for Claude Desktop / agents)
              --schema <NAME>   repeatable / comma-sep  (env PGVIS_SCHEMAS)
              --read-only       expose only read tools (no create/update/delete/RPC)
  openapi   Print the OpenAPI 3.0 document and exit
  inspect   Dump the introspected schema cache as JSON

env: PGVIS_DSN (required), PGVIS_CONFIG
```

## Usage

### MCP — database as agent tools

```bash
# stdio transport, e.g. for a Claude Desktop MCP server entry
pgvis --dsn "postgres://user@localhost/mydb" mcp

# read-only: catalogue contains only list_/select tools, mutations are rejected
pgvis --dsn "postgres://user@localhost/mydb" mcp --read-only

# or expose MCP over Streamable HTTP at /mcp alongside the REST API
PGVIS_ANON_ROLE=web_anon pgvis --dsn "postgres://user@localhost/mydb" serve --mcp-http
```

The stdio MCP server logs to **stderr** (stdout carries the JSON-RPC stream),
shuts down cleanly on SIGINT/SIGTERM, treats client-side broken pipe as a
clean exit, bounds every tool call by `statement_timeout_ms`, and returns
errors as a structured JSON object (`code`/`message`/`details`/`hint`)
matching the REST surface's PostgREST-compatible shape.

Tools are generated per table/function — `list_<table>`, `create_<table>`,
`update_<table>`, `delete_<table>`, `call_<function>` — and discovery
resources (`pgvis://schemas`, `pgvis://{schema}/schema`) describe the
available surface. Tool calls run the same plan → SQL path as REST. See
[crates/pgvis-mcp/src/tools.rs](crates/pgvis-mcp/src/tools.rs).

### REST — PostgREST-compatible query DSL

Routes default to `/{prefix}/{schema}/{table}` with `prefix = "api"` (a flat
PostgREST-compatible mode is available via routing config). Examples, all
exercised by the integration tests in
[crates/pgvis-server/tests/query.rs](crates/pgvis-server/tests/query.rs):

```bash
# Select specific columns
curl "http://localhost:3000/api/public/items?select=id,name,price"

# Filters: equality, comparison, pattern, null, set membership, negation
curl "http://localhost:3000/api/public/items?name=eq.Widget"
curl "http://localhost:3000/api/public/items?price=gte.99.99"
curl "http://localhost:3000/api/public/items?name=ilike.*widget*"
curl "http://localhost:3000/api/public/items?description=is.null"
curl "http://localhost:3000/api/public/items?category=in.(gadgets,toys)"
curl "http://localhost:3000/api/public/items?name=neq.Widget"

# Ordering and pagination
curl "http://localhost:3000/api/public/items?order=category.asc,price.desc"
curl "http://localhost:3000/api/public/items?order=id.asc&limit=3&offset=3"

# Combined
curl "http://localhost:3000/api/public/items?select=name,price&category=eq.gadgets&order=price.desc"

# Call a function (RPC)
curl "http://localhost:3000/api/public/rpc/your_function"
```

Writes use `POST` / `PATCH` / `DELETE` on the same routes; the `Prefer` header
controls return representation, count strategy, and transaction behavior.

### OpenAPI

```bash
pgvis --dsn "postgres://user@localhost/mydb" openapi   # prints the 3.0 spec
```

The spec is also served at the API root for clients sending
`Accept: application/openapi+json`.

### Embed in a Rust app

`pgvis-lib` is the single authoritative way to construct the stack — the CLI
uses it too ([crates/pgvis-lib/src/lib.rs](crates/pgvis-lib/src/lib.rs)):

```rust
use pgvis_lib::Builder;

// REST + MCP-over-HTTP router, ready for `axum::serve`
let router = Builder::new("postgres://localhost/mydb")
    .schemas(vec!["public"])
    .with_mcp_http()
    .build()
    .await?;

// Or a stdio MCP server
let mcp = Builder::new("postgres://localhost/mydb")
    .schemas(vec!["public"])
    .build_mcp_server()
    .await?;
pgvis_lib::pgvis_mcp::serve_stdio(mcp).await?;
```

### Configuration

Configuration is the `Config` struct in
[crates/pgvis-core/src/config.rs](crates/pgvis-core/src/config.rs) (PostgREST
config keys map directly — see the table in that file). Most-used fields:

| Field | Purpose | Default |
|---|---|---|
| `schemas` | Schemas exposed as routes / tools | `["public"]` |
| `jwt_secret` / `jwt_algo` | JWT verification (symmetric or asymmetric) | none |
| `jwt_aud` | Required JWT audience, when set | none |
| `anon_role` | Role used for unauthenticated requests | none |
| `aggregates_enabled` | Allow `sum()`/`avg()`/… in `select` | disabled |
| `max_rows` | Server-side cap on returned rows | unlimited |
| `routing.prefix` | URL prefix | `"api"` |
| `routing.schema_in_path` | `/{prefix}/{schema}/{table}` vs flat | `true` |

> Layering is defaults → TOML file (`--config` / `PGVIS_CONFIG`) → `PGVIS_*`
> env vars (`__` for nested keys, e.g. `PGVIS_POOL__SIZE`) → CLI flags. The
> config file is strict: an unknown key stops startup.

## Advantages

- **Database → LLM tools with zero glue.** Every table and function becomes a
  typed MCP tool automatically — keep the schema as the contract.
- **One engine, three surfaces.** REST, OpenAPI, and MCP share the same
  parser, planner, and SQL builder, so behavior can't drift between them.
- **PostgREST-compatible.** Same query DSL, `Prefer` semantics, and `PGRST*`
  error codes — existing PostgREST clients work unchanged.
- **Backend-agnostic, I/O-free core.** A single `Backend` trait; Postgres
  and SQLite share the core (dialect capability flags drive feature gating).
- **A library, not just a server.** Add a database API to any Rust app with
  the `pgvis-lib` `Builder`.
- **Safe by construction.** Parameterized SQL, JWT auth, role switching / RLS,
  statement timeouts, and hot schema reload.

## Architecture

pgvis is a seven-crate Rust workspace; all dependencies point inward to the
I/O-free core.

| Crate | Role |
|---|---|
| [pgvis-core](crates/pgvis-core) | I/O-free engine: query parser, plan layer, SQL builder, schema cache, `Backend`/`Dialect`/`Config` |
| [pgvis-postgres](crates/pgvis-postgres) | Postgres `Backend`: connection pool, introspection, execution |
| [pgvis-sqlite](crates/pgvis-sqlite) | SQLite `Backend`: introspection and execution via `rusqlite` |
| [pgvis-router](crates/pgvis-router) | axum REST router + OpenAPI generator |
| [pgvis-mcp](crates/pgvis-mcp) | MCP tools & resources (stdio + Streamable HTTP) |
| [pgvis-lib](crates/pgvis-lib) | `Builder` facade — the one way to assemble the stack |
| [pgvis-server](crates/pgvis-server) | The `pgvis` CLI binary |

The authoritative architecture reference lives in **[arch/](arch/README.md)**:

1. [Overview — goals, principles, request lifecycle](arch/01-overview.md)
2. [Core pipeline — parse → plan → SQL](arch/02-core-pipeline.md)
3. [Backends and dialects](arch/03-backends-and-dialects.md)
4. [Surfaces — REST, OpenAPI, MCP](arch/04-surfaces.md)
5. [Schema cache and introspection](arch/05-schema-cache.md)
6. [Errors, configuration, preferences](arch/06-errors-and-config.md)
7. [Design decisions](arch/07-design-decisions.md)
8. [Future scope and known gaps](arch/08-future-scope.md)

> Note: the `arch/` docs predate a rename and may refer to `pgvis-rest` /
> `pgvis-embed`; in the code these are `pgvis-router` / `pgvis-lib`.

## Roadmap

| Milestone | Scope |
|---|---|
| 0.1 | REST + OpenAPI on Postgres & SQLite; MCP tools wired; execution on both backends |
| **0.2** *(current)* | MCP over stdio hardened: stderr logging, graceful shutdown, EPIPE-tolerant, per-call deadline, structured PGRST errors, `--read-only` mode, `--schema`/env-overridable CLI |
| 0.3 | MCP over SSE |
| 1.0 | Stable embed API |

## License

Apache-2.0 OR MIT — see [LICENSE](LICENSE).
