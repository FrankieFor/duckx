# Redshift DuckDB Extension — Design Spec

- Date: 2026-05-05
- Topic slug: `redshift-extension`
- Status: approved (brainstorming complete, awaiting plan)

## Summary

Ship `duckx`, a Rust-built DuckDB extension that lets a user query Amazon Redshift from inside a DuckDB SQL session via a `redshift_scan` table function. The extension uses [connectorx](https://github.com/sfu-db/connector-x) directly from Rust to extract Redshift query results as Arrow record batches and stream them into DuckDB. Supports optional partitioned parallel reads, with credentials sourced from a DuckDB Secret or environment variables.

## Goals

- A DuckDB user can run `SELECT * FROM redshift_scan('SELECT * FROM sales WHERE dt > current_date - 7')` in any DuckDB session that has loaded the extension and gets the rows back.
- Credentials are sourced from a DuckDB Secret or from environment variables. Users cannot supply a connection DSN directly through the table function; the DSN is constructed internally from resolved fields.
- Connections to Redshift use TLS by default (`sslmode=require`). Plaintext connections are opt-in only.
- Partitioned parallel reads are available as opt-in arguments and produce identical results to an unpartitioned scan.
- The extension streams results (no full materialization in extension memory) and propagates Redshift errors with clear, PII-safe messages.
- The architecture is structured so that `ATTACH 'redshift://…'` and IAM auth can be added later as new modules without rewriting the v1 code paths.

### Performance budgets

- Schema discovery (`LIMIT 0` round-trip) adds < 500 ms p50 over a direct `psql` query when the cluster is in the same AWS region as the client.
- Bound discovery (`MIN/MAX` round-trip when `partition_on` is set without explicit bounds) adds the cost of one extra `MIN/MAX(col) FROM (<user_query>) t` execution. Documented limitation: this re-executes the user query and can be expensive for aggregation-heavy queries; users with such queries should pass explicit `partition_min` / `partition_max`.
- Total `redshift_scan` overhead over a direct `psql` extract of the same query is < 1.0× wall-clock for queries returning ≥ 100 k rows (i.e., the extension does not more than double extract time).
- Partitioned reads (`partition_num=8`) achieve ≥ 3× wall-clock speedup over `Single` for queries returning ≥ 1 M rows. Lower speedups indicate broken parallelism (e.g., serialization on a connection mutex) and are tested in the Postgres integration suite.

## Non-Goals

- Replacing `UNLOAD … TO 's3://'` for very large extracts. README will document that for extracts above ~50 M rows, `UNLOAD` + `read_parquet` is faster; `redshift_scan` targets ad-hoc / interactive use up to tens of millions of rows.
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

Unknown named args are rejected at bind time. `partition_num` without `partition_on` errors. `partition_min > partition_max` errors. `partition_num = 1` is treated as the unpartitioned case (single connection); `partition_num` must be in `[1, 64]`. `partition_on` must match `^[A-Za-z_][A-Za-z0-9_]*$` (validated at bind time) and is double-quoted when interpolated into partition SQL to support mixed-case identifiers safely. `partition_on` must reference an integer-typed column; non-numeric columns (e.g. `DATE`, `VARCHAR`) error at the `MIN/MAX` discovery step.

### Credentials

The extension registers a Secret type `REDSHIFT`:

```sql
CREATE SECRET redshift_prod (
  TYPE REDSHIFT,
  HOST 'cluster.xxx.us-east-1.redshift.amazonaws.com',
  PORT 5439,
  USER 'analyst',
  PASSWORD 'hunter2',
  DATABASE 'analytics',
  SSLMODE 'require'
);
```

Resolution order on each call:

1. Explicit `secret => 'name'` arg — looked up via the DuckDB Secrets Manager.
2. Environment variables: `REDSHIFT_HOST`, `REDSHIFT_PORT` (default `5439`), `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_DATABASE`, `REDSHIFT_SSLMODE` (default `require`).
3. Error: `MissingCredential { fields: [...] }` listing exactly which fields could not be resolved.

Users do not pass a DSN string to the table function. Internally, the resolved `Config` is rendered into a libpq DSN with URL-encoded password and `sslmode` parameter for connectorx; that internal DSN never leaves the process and is never logged.

### TLS

`sslmode` accepts `disable`, `prefer`, `require`, `verify-ca`, `verify-full`. Default is `require` — Redshift clusters reject plaintext by default and AWS recommends TLS. Setting `sslmode=disable` is permitted for local Postgres test fixtures only and emits a `tracing::warn!` log.

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

No batch is ever fully materialized in extension memory. At most `partition_num × 1` Arrow batches are in flight (channel bound = N). Default Arrow batch size is connectorx's default (~64 K rows). Worst-case peak in-flight rows: `64 × 64 K ≈ 4 M rows`. For typical row sizes of ~256 B, that's ~1 GB of in-flight data — documented as the practical upper bound and the reason `partition_num` is capped at 64.

### Concurrency and connection model

Each `redshift_scan` call opens a fresh set of Redshift connections — `partition_num` connections per call (so the `Single` case opens 1, `Parallel { num: 8 }` opens 8). Connections close when the scan finishes or the iterator is dropped (cancellation path).

There is no connection pool in v1; sessions issuing many small `redshift_scan` calls will repeatedly establish and tear down connections. Pooling is named in Future Work; v1 callers running tight loops should partition by hand or batch their queries.

## Type Mapping

connectorx returns Arrow types via the Postgres-protocol path. We map Arrow → DuckDB through `duckdb-rs`'s built-in C-data interface (verified against the type matrix in tests).

| Arrow type | DuckDB logical type |
|---|---|
| `Boolean` | `BOOLEAN` |
| `Int8`, `Int16` | `SMALLINT` |
| `Int32`, `UInt8`, `UInt16` | `INTEGER` |
| `Int64`, `UInt32` | `BIGINT` |
| `UInt64` | `HUGEINT` |
| `Float32` | `FLOAT` |
| `Float64` | `DOUBLE` |
| `Utf8`, `LargeUtf8` | `VARCHAR` |
| `Binary`, `LargeBinary`, `FixedSizeBinary(_)` | `BLOB` |
| `Date32`, `Date64` | `DATE` |
| `Time32(_)`, `Time64(_)` | `TIME` |
| `Timestamp(_, None)` | `TIMESTAMP` |
| `Timestamp(_, Some(tz))` | `TIMESTAMPTZ` |
| `Decimal128(p, s)`, `Decimal256(p, s)` | `DECIMAL(p, s)` |

Anything else falls through to `UnsupportedType { column, type_name }` — the mapping is best-effort, not exhaustive. Known Redshift / Postgres types that will hit this fallthrough today:

- `SUPER` (Redshift JSON / semistructured)
- `GEOMETRY` / `GEOGRAPHY`
- `HLLSKETCH`
- `VARBYTE` (revisit if connectorx adds support)
- `INTERVAL`
- `TIME WITH TIME ZONE` (`TIMETZ`)
- `OID` and other Postgres system types
- Any Arrow `List`, `Struct`, `Map`, `Union` arrays returned by future connectorx changes

Workaround for users: `CAST(col AS VARCHAR)` in the user query.

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
- **Connection pool** — keyed by resolved `Config`; reuses connections across `redshift_scan` calls within a DuckDB session.
- **`statement_timeout_ms` arg** — bounds runaway Redshift queries from inside DuckDB. Deferred from v1 because the cleanest implementation requires a connectorx pre-execution hook that 0.4 doesn't expose; embedding `SET statement_timeout` ahead of the user query breaks under partition wrapping (`SELECT … FROM (SET …; SELECT …)` is invalid).
- **Cooperative cancellation** — propagate DuckDB `Ctrl-C` / interrupt to in-flight Redshift connections. v1 closes connections on iterator drop, but does not respond to mid-stream interrupts within a single `RecordBatch`.
- **Schema cache** keyed on `(secret_name, query_hash)`.
- **Community Extensions submission** + signed binaries.
- **`UNLOAD` fast path** — for queries above a row threshold, transparently use `UNLOAD … TO 's3://…' FORMAT PARQUET` and read back via `httpfs`; behind `via => 's3'` arg.

## Design Decisions

- **Extension over CLI.** User wants to query Redshift *from* a DuckDB session, not pipe data into one.
- **Table function in v1, `ATTACH` in v2.** Table function is a few hundred lines; `ATTACH` with pushdown is a multi-week project. Architecture is structured so v2 reuses v1 modules unchanged.
- **No user-supplied DSN.** Users go through Secret or env; the internal libpq DSN never leaves the process. Removes the most common credential leak vector and matches DuckDB conventions.
- **TLS by default.** `sslmode=require` unless explicitly downgraded. Redshift's posture demands it.
- **Opt-in partitioning.** Connectorx's headline feature; making it opt-in keeps the simple case simple. `partition_num=1` is treated as `Single` so the arg space is uniform.
- **Postgres for CI, real Redshift for pre-release.** Redshift speaks Postgres wire protocol, so Postgres covers ~90% of plumbing; Redshift-only quirks (`SUPER`, multi-node partitioning) are caught by a gated suite before tagging.
- **Streaming, never buffering.** Bound the channel at `partition_num`; emit Arrow batches chunk-at-a-time into DuckDB.
- **No connection pool in v1.** Pooling adds non-trivial state and lifecycle bugs; ship without it and add pooling once usage patterns are observed.
- **Best-effort type mapping.** Spec lists known mappings and known unsupported types; everything else falls through to a clear error. Adding a new mapping requires adding a test.
- **No mocks of connectorx or DuckDB.** Mocks at integration boundaries diverge from reality; the Arrow C-data interface only meaningfully tests against a real DuckDB.

## Open Questions

None. All decisions settled in brainstorming.
