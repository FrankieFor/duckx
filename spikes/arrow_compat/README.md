# arrow_compat spike

Verifies that `connectorx::destinations::arrow::ArrowDestination` produces
`arrow`-crate `RecordBatch`es directly (not `arrow2`), AND records whether
`arrow()` returns a fully-materialized `Vec` or a streaming iterator.

## Result (filled in at run time)

- connectorx version: UNVERIFIED — `cargo search` / `cargo check` could not be
  executed in the agent's sandbox (Bash permission denied for cargo and network
  access denied for crates.io). Plan-recommended version `0.4` left in
  `Cargo.toml`; orchestrator must pin the exact 0.4.x after running
  `cargo search connectorx`.
- arrow version: UNVERIFIED — left at plan default `53`.
- compat: UNVERIFIED — see blocker note below.
- destination type: UNVERIFIED at runtime, but the spike asserts at compile
  time that `ArrowDestination::arrow()` returns `Vec<arrow::record_batch::RecordBatch>`
  (see `src/main.rs`'s `_proof` coercion). If `cargo check` succeeds with the
  pinned versions, the answer is `Vec<RecordBatch>` (materialized).

## Blocker note (agent run on 2026-05-05)

The spike sources have been written exactly per the plan, but compile-time
verification (`cargo check`) was NOT performed because the executing agent's
sandbox blocked `cargo`, `bd`, and outbound HTTP. The spike's value is gated on
that compile step; running `cargo check` here is mandatory before declaring
"OK".

Operator action required:

```bash
cd spikes/arrow_compat
cargo search connectorx --limit 10   # confirm latest 0.4.x
# update Cargo.toml's connectorx version to the pinned 0.4.x if newer
cargo check
```

If `cargo check` fails with a type mismatch on the `_proof` coercion, the
arrow-vs-arrow2 split is real and the workflow must STOP per the plan's
Task 1 stop condition.

## Implications

If destination type is `Vec<RecordBatch>` (likely on 0.4): the spec's
Memory Model section's claim "no batch is ever fully materialized" is
WRONG and must be updated to:

> connectorx materializes the full result set in `ArrowDestination`
> during the dispatcher's `run()`. After dispatch, our iterator walks
> the resulting `Vec<RecordBatch>` and emits batches into DuckDB chunk
> at a time. Peak memory is therefore the full result set size, not
> `partition_num × batch`. For very large extracts, prefer `UNLOAD` +
> `read_parquet` (already documented as a non-goal).

If a streaming destination is available, prefer it.

## Decision

If compat is OK: proceed to main implementation, depend on `arrow` crate only.
If REWRAP: add a small `arrow2 -> arrow` shim in `pipeline.rs` (~5% perf cost).
If BLOCKED: stop the workflow and re-brainstorm.

If destination is `Vec`: update `docs/superpowers/specs/2026-05-05-redshift-extension-design.md`
Memory Model section before continuing to Task 2.
