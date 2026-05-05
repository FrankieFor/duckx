# arrow_compat spike

Verifies that `connectorx::destinations::arrow::ArrowDestination` produces
`arrow`-crate `RecordBatch`es directly (not `arrow2`), and that the
`PostgresSource` / `Dispatcher` API used in the production `pipeline.rs`
typechecks against the pinned versions.

## Result

- **connectorx version:** `0.4.5` (resolved by Cargo).
- **arrow version:** `54` (pinned to match the `arrow-array 54.x` that
  connectorx 0.4.5 transitively brings in; pinning `arrow=53` produced a
  multi-version `arrow_array` mismatch).
- **compat:** **OK** — `cargo check` passes; `_proof` coercion confirms
  `ArrowDestination::arrow()` returns `Vec<arrow::record_batch::RecordBatch>`
  from the same `arrow_array` 54.x crate.
- **destination type:** `Vec<RecordBatch>` (materialized). connectorx 0.4.5
  buffers the full result in the destination during `Dispatcher::run()`,
  then `arrow()` returns the buffer. There is no streaming destination on
  0.4.x.

## Production API findings (used by Task 9 plan correction)

Real `PostgresSource::new` signature on connectorx 0.4.5:

```rust
PostgresSource::<P, C>::new(
    config: postgres::config::Config,   // sync postgres crate, NOT tokio_postgres
    tls: C,                             // C: MakeTlsConnect<Socket> + Clone + 'static + Send + Sync
    nconn: usize,
) -> Result<Self, _>
```

The plan's earlier `(cfg_url, queries.len())` form was wrong. `pipeline.rs`
must:

1. Build `postgres::Config` from the URL via `FromStr`.
2. Pass an explicit TLS connector (`tokio_postgres::NoTls` for
   `sslmode=disable`, or `postgres_native_tls::MakeTlsConnector` for
   `require`/`verify-*`).
3. Pass `nconn = queries.len()` as a separate `usize` arg.

Bonus finding: `PostgresSource` exposes a `pre_execution_queries: Option<Vec<String>>`
field. This means `statement_timeout_ms` (deferred to Future Work) IS
implementable via a pre-execution `SET statement_timeout = N` injected per
connection — worth revisiting post-v1.

## Implications for spec

The Memory Model section in
`docs/superpowers/specs/2026-05-05-redshift-extension-design.md` already
reflects "destination materializes full result set" — no further spec
change needed from this spike. Plan corrections (arrow=54, real
PostgresSource signature) are committed in
`d1ec711` ("fix(plan): correct connectorx 0.4.5 API per Task 1 spike findings").

## Decision

**Proceed to Task 2.** Compat is OK; main implementation depends on
`arrow = "54"` and constructs `PostgresSource` per the corrected
signature.
