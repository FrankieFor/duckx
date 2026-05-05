# Redshift DuckDB Extension — Design Spec

- Date: 2026-05-05
- Topic slug: `redshift-extension`
- Status: approved (brainstorming complete, awaiting plan)

## Summary

Ship `duckx`, a Rust-built DuckDB extension that lets a user query Amazon Redshift from inside a DuckDB SQL session via a `redshift_scan` table function. The extension uses [connectorx](https://github.com/sfu-db/connector-x) directly from Rust to extract Redshift query results as Arrow record batches and stream them into DuckDB. Supports optional partitioned parallel reads, with credentials sourced from a DuckDB Secret or environment variables.

## Goals

- A DuckDB user can run `SELECT * FROM redshift_scan('SELECT * FROM sales WHERE dt > current_date - 7')` in any DuckDB session that has loaded the extension and gets the rows back.
- Credentials live in a DuckDB Secret or in environment variables — never in the SQL string.
- Partitioned parallel reads are available as opt-in arguments and produce identical results to an unpartitioned scan.
- The extension streams results (no full materialization in extension memory) and propagates Redshift errors with clear, PII-safe messages.
- The architecture is structured so that `ATTACH 'redshift://…'` and IAM auth can be added later as new modules without rewriting the v1 code paths.

## Non-Goals

- Replacing `UNLOAD … TO 's3://'` for very large extracts. README will document that for >100M-row extracts, `UNLOAD` + `read_parquet` is faster; `redshift_scan` targets ad-hoc / interactive use up to tens of millions of rows.
- Writes / DML to Redshift.

## Out of Scope (v1)

Listed explicitly so v1 reviewers know not to ask for them; each is a named follow-up below.

- `ATTACH 'redshift://…'` catalog integration (`SHOW TABLES`, cross-attach joins, filter & projection pushdown).
- IAM-based temporary credentials (`GetClusterCredentials`, IAM Identity Center).
- Auto-detected partition column.
- `UNLOAD` fast path for large queries.
- DuckDB Community Extensions registry submission and signed binaries.

## User-Facing Surface

### The table function

```sql
SELECT *
FROM redshift_scan(
  'SELECT * FROM public.sales WHERE dt > current_date - 7',
  secret => 'redshift_prod',
  partition_on => 'id',
  partition_num => 8
);
```

Arguments:

| Name | Type | Required | Default | Notes |
|------|------|----------|---------|-------|
| (positional 1) `query` | `VARCHAR` | yes | — | Arbitrary Redshift `SELECT`. |
| `secret` | `VARCHAR` | no | — | Name of a DuckDB `SECRET` of `TYPE REDSHIFT`. |
| `partition_on` | `VARCHAR` | no | — | Integer column for parallel partitioning. |
| `partition_num` | `BIGINT` | no | `1` | Number of parallel connections. Requires `partition_on`. |
| `partition_min` | `BIGINT` | no | auto | If omitted, discovered via `SELECT MIN(col)` on the user query. |
| `partition_max` | `BIGINT` | no | auto | If omitted, discovered via `SELECT MAX(col)`. |

Unknown named args are rejected at bind time. `partition_num` without `partition_on` errors. `partition_min > partition_max` errors.

### Credentials

The extension registers a Secret type `REDSHIFT`:

```sql
CREATE SECRET redshift_prod (
  TYPE REDSHIFT,
  HOST 'cluster.xxx.us-east-1.redshift.amazonaws.com',
  PORT 5439,
  USER 'analyst',
  PASSWORD 'hunter2',
  DATABASE 'analytics'
);
```

Resolution order on each call:

1. Explicit `secret => 'name'` arg — looked up via the DuckDB Secrets Manager.
2. Environment variables: `REDSHIFT_HOST`, `REDSHIFT_PORT` (default `5439`), `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_DATABASE`.
3. Error: `MissingCredential { fields: [...] }` listing exactly which fields could not be resolved.

There is no plain DSN-with-embedded-password path.

## Architecture

Single Cargo crate `duckx` building a `cdylib` — the `.duckdb_extension` shared library. Pinned to one DuckDB minor version, recorded in `rust-toolchain.toml`, `Cargo.toml`, and a `DUCKDB_VERSION` const verified at extension load.

### Module layout

```
src/
  lib.rs           # Extension entry: registers redshift_scan + REDSHIFT secret type
  secret.rs        # REDSHIFT secret type registration + lookup
  config.rs        # Config struct; resolve(secret_name | env) -> Config or ConfigError
  scan.rs          # redshift_scan VTab (bind/init/func)
  partition.rs     # Partition arg validation + bound discovery
  pipeline.rs      # connectorx Source/Destination/Dispatcher wiring
  types.rs         # Arrow → DuckDB type mapping; explicit unsupported-type errors
  error.rs         # DuckxError (thiserror); PII-safe Display
tests/
  unit/            # Config resolution, partition validation, type-mapping table
  integration_pg/  # Postgres-backed end-to-end (testcontainers, runs in CI)
  integration_rs/  # Real-Redshift, gated by REDSHIFT_TEST_DSN + --features redshift-integration
```

### Module boundaries

- `config` knows nothing about DuckDB or connectorx — it just resolves credentials.
- `pipeline` knows nothing about DuckDB — it produces an `impl Iterator<Item = Result<RecordBatch>>`.
- `scan` is the only module that talks to the DuckDB extension API.
- `secret` is DuckDB-only and does not import `connectorx`.

This is also the seam for v2 features: `ATTACH` becomes a new module alongside `scan`, reusing `config` + `pipeline` unchanged. IAM auth becomes a new variant in `secret.rs` resolving to the same `Config`.

### Crate dependencies

| Crate | Role |
|-------|------|
| `duckdb` (features `vtab`, `extension-loadable`) | DuckDB Rust bindings + extension framework. |
| `connectorx` | Extraction engine (Rust API; not Python bindings). |
| `arrow` | Record-batch interchange. |
| `url` | DSN parsing. |
| `secrecy` | Wrap passwords; redact in `Display`. |
| `thiserror` | Error enum derive. |
| `tracing`, `tracing-subscriber` | Structured logging gated behind `DUCKX_LOG`. |
| `testcontainers` (dev) | Postgres container in CI. |
| `pretty_assertions` (dev) | Better diff output in tests. |

## Data Flow

1. **Bind.** Parse positional and named args. Reject unknown named args. Resolve auth via `config::resolve`. Build a `BindData { config, query, partition_spec }`.
2. **Schema discovery (still in bind).** Issue a `LIMIT 0` of the user query against Redshift via connectorx, take the resulting Arrow schema, map each column through `types::arrow_to_duckdb`, and return that to DuckDB as the table function's output schema.
3. **Init.** Construct the connectorx `Dispatcher`. If `partition_on` is set without explicit bounds, run `SELECT MIN(col), MAX(col) FROM (<user query>) t`. Spawn the dispatcher; receive an `mpsc::Receiver<RecordBatch>`. Stash the receiver on `InitData`.
4. **Func.** On each call, pull the next `RecordBatch` from the channel, copy each column into the DuckDB `DataChunk` via the Arrow C-data interface, emit. End-of-scan when the channel closes.
5. **Cancellation.** On DuckDB interrupt, `InitData` drops; the receiver drops; connectorx senders error on send; worker connections close cleanly.

### Memory model

No batch is ever fully materialized in extension memory. At most `partition_num × 1` Arrow batches are in flight (channel bound = N). Default Arrow batch size is connectorx's default (~64K rows).

## Type Mapping

connectorx returns Arrow types via the Postgres-protocol path. We map Arrow → DuckDB through `duckdb-rs`'s built-in C-data interface (verified against the type matrix in tests).

Redshift-specific types not representable in Arrow error explicitly with the column name and the offending Redshift type:

- `SUPER`
- `GEOMETRY` / `GEOGRAPHY`
- `HLLSKETCH`
- `VARBYTE` (revisit if connectorx adds support)

Error variant: `UnsupportedType { column, type_name }`.

## Error Handling

Single typed `DuckxError` enum (`thiserror`):

```
MissingCredential { fields: Vec<String> }
BadDsn(String)
RedshiftError(String)              // wraps connectorx / wire-protocol errors
UnsupportedType { column: String, type_name: String }
PartitionBoundsInvalid { reason: &'static str }
BatchDecode(String)
```

Rules:

- Passwords wrapped in `secrecy::Secret<String>` and never appear in `Display`.
- `From<DuckxError> for duckdb::Error` produces a `duckdb::Error::DuckDBFailure` with our message.
- All errors logged at `error` level via `tracing` before being returned, gated behind `DUCKX_LOG=trace|debug|info`.
- Every entry from DuckDB into our code is wrapped in `std::panic::catch_unwind` and panics are converted to `duckdb::Error`. A panic across the FFI boundary is UB; this is non-negotiable.

## Testing

### Unit (always run)

- `config::resolve` truth table: secret present, env present, both, neither, partial env.
- DSN parsing edge cases.
- Partition arg validation: `partition_num` without `partition_on`; `partition_min > partition_max`; non-integer `partition_on`.
- Type-mapping table covering every Postgres / Redshift type we claim to support, plus the explicit failure cases (`SUPER`, `GEOMETRY`, `HLLSKETCH`, `VARBYTE`).

### Postgres integration (CI)

`testcontainers`-launched Postgres 15. Seed fixtures:
- A "wide" table covering every supported type (one column each).
- A "tall" table for partitioning: 10M rows, integer `id` partition key.

Tests:
- End-to-end scan returns expected rows.
- `LIMIT 0` schema discovery returns correct DuckDB schema.
- Partitioned scan (N=8) returns the same multiset as unpartitioned.
- Secret-based auth.
- Env-based auth.
- Missing-credential error names every missing field.
- Unsupported-type error names the column and type (simulated with a Postgres extension type that maps to a synthetic unsupported case).
- Mid-stream connection drop surfaces a `RedshiftError` with the underlying message.

Each test runs against a real DuckDB in-process, loading the freshly built extension via `LOAD '/path/to/redshift.duckdb_extension';`.

### Real Redshift (pre-release)

`--features redshift-integration` gates a small suite that runs only when `REDSHIFT_TEST_DSN` (or the equivalent env quad) is set. Coverage:

- `SUPER` errors cleanly.
- Partitioned read across multiple compute nodes.
- Leader-node-only query.
- `VARCHAR(MAX)` very-wide rows.

Not run in default CI. Documented as a release checklist step.

### Manual smoke (release checklist)

Build extension, start `duckdb`, `LOAD`, run `redshift_scan('SELECT 1')` against a real cluster, eyeball the result.

### What we explicitly do not mock

connectorx and the DuckDB extension surface are never mocked in unit tests. Postgres-backed integration tests are the contract — mocks here would diverge from the real protocol behavior, and the `arrow ↔ DuckDB` C-data interface only meaningfully tests against a real DuckDB.

## Build & Distribution (v1)

- `cargo build --release` produces `target/release/libduckx.{dylib,so,dll}`.
- A small `xtask` (or `build.rs` post-step) renames it to `redshift.duckdb_extension` and verifies the DuckDB-required metadata footer (version + platform tags) added by the extension framework.
- CI matrix: `linux-amd64`, `linux-arm64`, `macos-arm64` × pinned DuckDB version. Artifacts uploaded per release tag.
- Install: manual download, then `SET allow_unsigned_extensions = true; LOAD '/abs/path/to/redshift.duckdb_extension';`.
- README documents prerequisites, build steps, the four env vars, a `CREATE SECRET` example, and the unsigned-load incantation.

## Top Risks (and how the plan de-risks them)

1. **arrow / arrow2 crate split.** `connectorx` has historically used `arrow2`; `duckdb-rs` consumes the `arrow` crate. *De-risk in plan's Task 1*: spike `connectorx::destinations::arrow::ArrowDestination` and verify it produces the `arrow` crate's `RecordBatch` directly against the pinned DuckDB version. If not, choose between (a) a small re-wrap layer (~5% overhead) or (b) forking connectorx's destination. **Stop the workflow** if neither is viable and re-brainstorm.
2. **DuckDB extension Rust API maturity.** `duckdb-rs`'s extension features are less covered than the C++ API. *De-risk in plan's Task 2*: build a hello-world extension exposing `redshift_scan('SELECT 1')` returning a hard-coded row, before any connectorx work.
3. **Schema discovery cost.** A `LIMIT 0` round-trip on every call adds latency for fast Redshift queries. Acceptable for v1; documented. Future: cache schema by `(secret_name, query_hash)` for the connection lifetime.
4. **DuckDB version pinning.** Extensions break on minor DuckDB upgrades. We pin one version, document it, and add a CI matrix slot only when DuckDB updates.
5. **Unsigned-extension friction.** Local `LOAD` requires `SET allow_unsigned_extensions = true;`. Document in README.

## Future Work (named so v1 architecture stays honest)

- **`ATTACH 'redshift://…' AS rs`** — implement DuckDB `Catalog` / `StorageExtension`; expose Redshift schemas/tables; add filter & projection pushdown via `BindReplace`. Reuses `config` and `pipeline` unchanged.
- **IAM auth** — new `secret.rs` variant `TYPE REDSHIFT_IAM` plus AWS SDK call to `GetClusterCredentials`; resolves to a `Config` with a 15-minute-lived password. Plumbing above `config` does not change.
- **Schema cache** keyed on `(secret_name, query_hash)`.
- **Community Extensions submission** + signed binaries.
- **`UNLOAD` fast path** — for queries above a row threshold, transparently use `UNLOAD … TO 's3://…' FORMAT PARQUET` and read back via `httpfs`; behind `via => 's3'` arg.

## Design Decisions

- **Extension over CLI.** User wants to query Redshift *from* a DuckDB session, not pipe data into one.
- **Table function in v1, `ATTACH` in v2.** Table function is a few hundred lines; `ATTACH` with pushdown is a multi-week project. Architecture is structured so v2 reuses v1 modules unchanged.
- **DuckDB Secret + env, no in-DSN password.** Removes the most common credential leak vector; matches DuckDB conventions.
- **Opt-in partitioning.** Connectorx's headline feature; making it opt-in keeps the simple case simple.
- **Postgres for CI, real Redshift for pre-release.** Redshift speaks Postgres wire protocol, so Postgres covers ~90% of plumbing; Redshift-only quirks (`SUPER`, multi-node partitioning) are caught by a gated suite before tagging.
- **Streaming, never buffering.** Bound the channel at `partition_num`; emit Arrow batches chunk-at-a-time into DuckDB.
- **No mocks of connectorx or DuckDB.** Mocks at integration boundaries diverge from reality; the Arrow C-data interface only meaningfully tests against a real DuckDB.

## Open Questions

None. All decisions settled in brainstorming.
