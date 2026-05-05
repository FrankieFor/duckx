# Resuming the redshift-extension workflow

This branch (`workflow/redshift-extension`) was paused on 2026-05-05 with
the design + plan reviewed and the first two de-risk spikes attempted.

## Where things are

| Artifact | Status |
|---|---|
| `docs/superpowers/specs/2026-05-05-redshift-extension-design.md` | Approved, three review passes |
| `docs/superpowers/plans/2026-05-05-redshift-extension-plan.md` | Approved with status snapshot section at top |
| `spikes/arrow_compat/` | Builds cleanly. `cargo check` confirms `arrow=54` + connectorx 0.4.5 + correct `PostgresSource::new` signature |
| `spikes/hello_extension/` | Builds cleanly. **Cannot LOAD** — see `spikes/hello_extension/README.md` |
| beads epic `duckx-hrj` | 15 tasks seeded with dependencies. `duckx-hrj.1` closed; rest open |
| `.workflow-state.json` | `phase: "paused"` |

## To resume

1. **Resolve the `.duckdb_extension` metadata footer.** Pick one of:
   a. Vendor DuckDB upstream's `scripts/append_extension_metadata.py` and
      invoke from `xtask::package`.
   b. Write a Rust footer appender (~few hundred lines based on
      DuckDB's `extension_install.cpp` format).
   c. Switch the `duckdb` dep to `quack-rs` and re-evaluate.
   Then run `cargo build --release && cargo xtask package` and verify
   `duckdb -unsigned -c "LOAD '...'; SELECT * FROM hello_redshift();"`
   returns `hello, redshift`.
2. **Close `duckx-hrj.2`** (`bd update duckx-hrj.2 --status closed`).
3. **Set `.workflow-state.json` `phase: "dispatch"`** and re-run
   `/workflow` to continue. Or implement Tasks 3–14 sequentially.
4. **Mirror `spikes/hello_extension/src/lib.rs` when writing Task 10's
   `scan.rs`** — that is the canonical 1.10502.0 VTab pattern.

## Verified API surface (don't relitigate)

- `arrow = "54"` (matches connectorx 0.4.5's transitive `arrow-array 54.x`)
- `duckdb = "=1.10502.0"` with feature `loadable-extension` (not
  `extension-loadable`); `duckdb-loadable-macros = "=1.10502.0"`
- `PostgresSource::<P, C>::new(config: postgres::Config, tls: C, nconn: usize)`
  — uses the SYNC `postgres::Config`, NOT `tokio_postgres::Config`
- `VTab` trait in 1.10502.0:
  ```rust
  fn bind(&BindInfo) -> Result<Self::BindData, Box<dyn Error>>
  fn init(&InitInfo) -> Result<Self::InitData, Box<dyn Error>>
  fn func(&TableFunctionInfo<Self>, &mut DataChunkHandle) -> Result<(), _>
  // BindData/InitData: Sized + Send + Sync
  // get_init_data() returns &Self::InitData (immutable) — use AtomicBool/Mutex
  ```
- `Connection::register_table_function::<T>(name: &str)`; static
  `parameters()` / `named_parameters()` methods on the VTab define the
  signature
- `duckdb::core::Inserter` must be in scope to call `vector.insert(idx, value)`

## Bonus finding worth revisiting

`PostgresSource` exposes a `pre_execution_queries: Option<Vec<String>>`
field. The plan deferred `statement_timeout_ms` to Future Work assuming
no pre-exec hook existed; that was wrong. Could be added in a follow-up
without breaking v1's API.
