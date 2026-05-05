# hello_extension spike

De-risk spike for Task 2 of the redshift-extension plan.

## What this proves

Against `duckdb-rs 1.10502.0` and DuckDB CLI `v1.5.2`:

- Cargo can build a `cdylib` that links against `libduckdb-sys`.
- The new VTab trait surface compiles end-to-end: `bind`/`init` returning
  owned data (no `*mut` parameter, no `Free` trait), `func` receiving
  `&TableFunctionInfo<Self>`, init data borrowed immutably (so per-scan
  mutable state needs interior mutability — we use `AtomicBool`).
- The Cargo feature is `loadable-extension`, not `extension-loadable`.
- The `Inserter` trait must be imported for `vector.insert(...)`.

This file (`src/lib.rs`) is the canonical reference for Task 10's
`scan.rs` rewrite.

## What this does NOT prove (BLOCKER for load test)

`cargo build --release` succeeds, but `duckdb -c "LOAD '...'; SELECT ..."`
fails with:

> Invalid Input Error: ... The file is not a DuckDB extension. The
> metadata at the end of the file is invalid

`.duckdb_extension` files require a metadata footer (signature
placeholder + DuckDB version vector + platform tag) that
`duckdb-loadable-macros 1.10502.0` does NOT generate. Upstream DuckDB
ships a Python script (`scripts/append_extension_metadata.py` under the
duckdb GitHub repo) that adds it during the community-extension build
flow.

Resolution paths (none implemented here):

1. Vendor `append_extension_metadata.py` from DuckDB upstream and
   invoke it from `xtask::package` after `cargo build`.
2. Reimplement the footer appender in Rust (a few hundred lines based
   on DuckDB's `src/main/extension/extension_install.cpp` format).
3. Switch to the alternative crate `quack-rs` ("Production-grade Rust
   SDK for building DuckDB loadable extensions") which may handle the
   footer natively. Not evaluated.

Resume by picking one of those before any further task lands.

## Reproducing the build

    cd spikes/hello_extension
    cargo build --release
    cp target/release/libhello_extension_spike.dylib \
       target/release/hello_redshift.duckdb_extension
    duckdb -unsigned -c "LOAD '$(pwd)/target/release/hello_redshift.duckdb_extension'; \
                         SELECT greeting FROM hello_redshift();"
    # ↑ this fails today because of the missing metadata footer.
