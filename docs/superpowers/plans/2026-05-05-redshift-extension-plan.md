# Redshift DuckDB Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a Rust-built DuckDB extension that lets a user query Amazon Redshift from inside a DuckDB SQL session via a `redshift_scan` table function with optional partitioned parallel reads.

**Architecture:** Single Cargo crate (`duckx`) building a `cdylib` `.duckdb_extension` shared library, pinned to one DuckDB version. Uses `connectorx` directly from Rust to extract Redshift query results as Arrow record batches and streams them into DuckDB via the Arrow C-data interface. Credentials resolved from a DuckDB Secret of type `REDSHIFT` or environment variables.

**Tech Stack:** Rust (edition 2021), `duckdb` crate (`vtab` + `extension-loadable` features), `connectorx`, `arrow`, `secrecy`, `thiserror`, `tracing`, `testcontainers` (dev), `pretty_assertions` (dev).

**Spec:** `docs/superpowers/specs/2026-05-05-redshift-extension-design.md`

**Pinned versions (record at impl time, before Task 3):**
- DuckDB: latest stable at impl time (expected `1.4.x`).
- `duckdb-rs`: latest version compiled against the pinned DuckDB.
- `connectorx`: latest published version verified by Task 1.

**Parallelism map for `/workflow` Phase 3:**

```
Task 1 ──▶ Task 2 ──▶ Task 2.5 ──▶ Task 3 ──▶ Task 4 ──┬─▶ Task 5  (config)    ─┐
                                                       ├─▶ Task 6  (secret)    ─┤
                                                       ├─▶ Task 7  (types)     ─┼─▶ Task 9 (pipeline) ─┐
                                                       └─▶ Task 8  (partition) ─┘                     ├─▶ Task 10 (scan) ─▶ Task 11 (lib) ─┬─▶ Task 12 (PG tests)   ─┐
                                                                                                       │                                    ├─▶ Task 13 (RS tests)   ─┤
                                                                                                       │                                    └─▶ Task 14 (README+CI)  ─┤
                                                                                                       └────────────────────────────────────────────────────────────┘
```

Task 2.5 (Secrets API spike) is the third de-risk gate. Tasks 5/6/7/8 run in parallel after Task 4. Tasks 12/13/14 run in parallel after Task 11. Task 9 needs 5 + 7. Task 10 needs 5/6/7/8/9. Task 6 is unblocked by Task 2.5's outcome.

---

## Task 1: De-risk spike — connectorx Arrow crate compatibility

**Why this is first:** the spec calls out `arrow` vs `arrow2` crate divergence as the top risk. If `connectorx`'s `ArrowDestination` doesn't produce `arrow`-crate `RecordBatch`es, the whole streaming-into-DuckDB plan needs redesign.

**Files:**
- Create: `spikes/arrow_compat/Cargo.toml`
- Create: `spikes/arrow_compat/src/main.rs`
- Create: `spikes/arrow_compat/README.md`

**Stop condition:** if connectorx returns `arrow2::record_batch::RecordBatch` instead of `arrow::record_batch::RecordBatch` AND no feature flag selects `arrow`, **stop the workflow** and report. Do not proceed to Task 2.

- [ ] **Step 1: Create the spike crate**

```bash
mkdir -p spikes/arrow_compat/src
```

Write `spikes/arrow_compat/Cargo.toml`:

```toml
[package]
name = "arrow_compat_spike"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
# Use the version of connectorx we plan to pin in the main crate.
# The plan author should fill in the exact version here after `cargo search connectorx`.
connectorx = { version = "0.4", features = ["src_postgres", "dst_arrow"] }
arrow = "54"

[workspace]
```

- [ ] **Step 2: Write the compatibility check**

Write `spikes/arrow_compat/src/main.rs`:

```rust
//! Verifies two things at compile time:
//!   1. connectorx ArrowDestination produces the `arrow` crate's
//!      `RecordBatch` (not arrow2).
//!   2. The destination's exposed type is `Vec<RecordBatch>` vs. a
//!      streaming iterator. This determines whether the production
//!      design's "streaming, no full materialization" claim holds; if
//!      `arrow()` returns `Vec`, connectorx materializes the full result
//!      set in destination memory before we can walk it, and the spec's
//!      Memory Model section MUST be updated to reflect that.

use arrow::record_batch::RecordBatch as ArrowRb;
use connectorx::destinations::arrow::ArrowDestination;
use connectorx::prelude::*;

fn main() {
    let mut dest = ArrowDestination::new();
    // Force the compiler to confirm the destination's batch type IS `arrow::RecordBatch`.
    let _proof: fn(ArrowDestination) -> Vec<ArrowRb> = |d| d.arrow().expect("arrow batches");

    // Also compile-test the production Dispatcher snippet from Task 9.
    // This catches API drift that would otherwise blow up at Task 9.
    // We don't actually run it — we just need the types to resolve.
    //
    // Real signature found by spike:
    //   PostgresSource::<P, C>::new(
    //       config: tokio_postgres::Config,
    //       tls: C,                              // C: MakeTlsConnect<Socket> + Clone + 'static + Send + Sync
    //       nconn: usize,
    //   ) -> Result<Self, _>
    fn _dispatcher_typecheck() {
        use connectorx::sources::postgres::{BinaryProtocol, PostgresSource};
        use connectorx::sql::CXQuery;
        use connectorx::transports::PostgresArrowTransport;
        use postgres::Config;
        use std::str::FromStr;
        use tokio_postgres::NoTls;

        let cfg = Config::from_str(
            "postgresql://u:p@h:5432/d?sslmode=disable"
        ).unwrap();
        let queries: Vec<CXQuery<String>> = vec![CXQuery::naked("SELECT 1".to_string())];
        let source = PostgresSource::<BinaryProtocol, NoTls>::new(cfg, NoTls, queries.len()).unwrap();
        let mut destination = ArrowDestination::new();
        let dispatcher = connectorx::prelude::Dispatcher::<
            _, _, PostgresArrowTransport<BinaryProtocol, NoTls>,
        >::new(source, &mut destination, &queries, None);
        let _ = dispatcher; // don't run; just typecheck
    }

    println!("compat OK; arrow() returns Vec<RecordBatch>; Dispatcher API resolved");
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cd spikes/arrow_compat && cargo check`

Expected: success. If it fails with a type-mismatch on `_proof`, the spike has detected the `arrow2`/`arrow` split — STOP and report to the orchestrator before proceeding.

- [ ] **Step 4: Document the result + streaming verdict**

Write `spikes/arrow_compat/README.md`:

```markdown
# arrow_compat spike

Verifies that `connectorx::destinations::arrow::ArrowDestination` produces
`arrow`-crate `RecordBatch`es directly (not `arrow2`), AND records whether
`arrow()` returns a fully-materialized `Vec` or a streaming iterator.

## Result (filled in at run time)

- connectorx version: <fill in from Cargo.lock>
- arrow version: <fill in from Cargo.lock>
- compat: OK / NEEDS REWRAP / BLOCKED
- destination type: `Vec<RecordBatch>` (materialized) / streaming iterator

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
```

- [ ] **Step 5: Commit**

```bash
git add spikes/arrow_compat
git commit -m "spike: verify connectorx ArrowDestination uses arrow crate"
```

---

## Task 2: De-risk spike — hello-world DuckDB extension in Rust

**Why this is second:** validates that `duckdb-rs`'s `vtab` + `extension-loadable` features can actually register a table function and that we can load the resulting `.duckdb_extension` from a DuckDB binary. Smaller surface than the real extension; lets us catch toolchain / framework problems before any Redshift work.

**Files:**
- Create: `spikes/hello_extension/Cargo.toml`
- Create: `spikes/hello_extension/src/lib.rs`
- Create: `spikes/hello_extension/tests/load_test.rs`
- Create: `spikes/hello_extension/README.md`

**Stop condition:** if the extension cannot be loaded into a DuckDB binary of the pinned version, stop and report.

- [ ] **Step 1: Pin DuckDB version**

Run `duckdb --version` (install if needed). Record the version in `spikes/hello_extension/README.md` and use it as the pinned version for the rest of the plan.

- [ ] **Step 2: Create the spike crate**

Write `spikes/hello_extension/Cargo.toml`:

```toml
[package]
name = "hello_extension_spike"
version = "0.0.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
# Pin to the duckdb-rs release that targets the recorded DuckDB version.
duckdb = { version = "1.4", features = ["vtab", "extension-loadable"] }

[workspace]
```

- [ ] **Step 3: Write the extension entry**

Write `spikes/hello_extension/src/lib.rs`:

```rust
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;

#[repr(C)]
struct HelloBindData;
impl Free for HelloBindData {}

#[repr(C)]
struct HelloInitData {
    done: bool,
}
impl Free for HelloInitData {}

struct HelloVTab;

impl VTab for HelloVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;

    fn bind(_bind: &BindInfo, _data: *mut HelloBindData) -> Result<(), Box<dyn std::error::Error>> {
        _bind.add_result_column("greeting", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(())
    }

    fn init(_init: &InitInfo, data: *mut HelloInitData) -> Result<(), Box<dyn std::error::Error>> {
        unsafe { (*data).done = false; }
        Ok(())
    }

    fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = unsafe { &mut *func.get_init_data::<HelloInitData>() };
        if init.done {
            output.set_len(0);
            return Ok(());
        }
        let col = output.flat_vector(0);
        col.insert(0, "hello, redshift");
        output.set_len(1);
        init.done = true;
        Ok(())
    }
}

#[duckdb_entrypoint_c_api]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn std::error::Error>> {
    con.register_table_function::<HelloVTab>("hello_redshift")?;
    Ok(())
}
```

> **Note:** the exact `duckdb-rs` API (trait method signatures, macro names) shifts between minor versions. If a method doesn't compile, look at the version's docs.rs page for the equivalent and update — don't invent. Record any deviations in the spike README.

- [ ] **Step 4: Build the extension**

Run: `cd spikes/hello_extension && cargo build --release`

Expected: produces `target/release/libhello_extension_spike.{dylib,so}`.

- [ ] **Step 5: Rename to .duckdb_extension and verify the footer**

Run:
```bash
cd spikes/hello_extension
EXT_OUT="target/release/hello_redshift.duckdb_extension"
case "$(uname)" in
  Darwin) cp target/release/libhello_extension_spike.dylib "$EXT_OUT" ;;
  Linux)  cp target/release/libhello_extension_spike.so "$EXT_OUT" ;;
esac
ls -l "$EXT_OUT"
```

Expected: file exists.

- [ ] **Step 6: Write the load test**

Write `spikes/hello_extension/tests/load_test.rs`:

```rust
use std::process::Command;

#[test]
fn extension_loads_and_returns_expected_row() {
    let ext_path = std::fs::canonicalize(
        std::env::current_dir()
            .unwrap()
            .join("target/release/hello_redshift.duckdb_extension"),
    )
    .expect("extension built; run `cargo build --release` first");

    let output = Command::new("duckdb")
        .args([
            "-unsigned",
            "-c",
            &format!(
                "LOAD '{}'; SELECT greeting FROM hello_redshift();",
                ext_path.display()
            ),
        ])
        .output()
        .expect("duckdb binary on PATH");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "duckdb failed: {}", String::from_utf8_lossy(&output.stderr));
    assert!(stdout.contains("hello, redshift"), "stdout was: {stdout}");
}
```

- [ ] **Step 7: Run the load test**

Run: `cd spikes/hello_extension && cargo test --release -- --test-threads=1`

Expected: PASS — proves end-to-end build → load → call → result.

If FAIL on `LOAD`: extension framework is misconfigured — stop and report. If FAIL on the assertion: VTab implementation is wrong — fix and rerun.

- [ ] **Step 7b: Verify `set_arrow` works across the type matrix**

Add a second test to `spikes/hello_extension` that registers an `arrow_passthrough(type_kind)` VTab and exercises `vector(i).set_arrow(...)` for **every type from Task 7's mapping**: at minimum `Int32`, `Int64`, `Float64`, `Utf8`, `LargeUtf8`, `Binary`, `Date32`, `Time64(Microsecond)`, `Timestamp(Microsecond, None)`, `Timestamp(Microsecond, Some("UTC"))`, `Decimal128(18, 4)`. The VTab should accept a `type_kind: VARCHAR` parameter and dispatch to the corresponding Arrow array constructor.

Why this matters: if `set_arrow` works for `Int32` but breaks on `Decimal128` or `Timestamp(tz)`, Task 10's `copy_batch_into_chunk` is fundamentally broken and the fallback (element-wise dispatch on `LogicalTypeId`) is much larger work — that has to be discovered in the spike, not at Task 10 integration-test time.

Update the spike README with one of three outcomes:

- **set_arrow works for all tested types** → Task 10's `copy_batch_into_chunk` body works as written.
- **set_arrow works for some types, breaks on others** → Document the type-by-type breakdown. Task 10 must implement per-type fallback for the broken types — record the list and bump the Task 10 estimate by ~50 LOC per broken type.
- **set_arrow not available at all** → Task 10 must implement full element-wise dispatch on `LogicalTypeId`. Document this and bump the Task 10 estimate by ~200 LOC.

- [ ] **Step 8: Commit**

```bash
git add spikes/hello_extension
git commit -m "spike: hello-world DuckDB Rust extension proves load + table function works"
```

---

## Task 2.5: De-risk spike — DuckDB Secrets API surface

**Why this exists:** Task 6 (`secret.rs`) depends on the duckdb-rs Secrets API, which is in flux: some versions expose `Connection::register_secret_type`, others require dropping to FFI (`duckdb::ffi::duckdb_secret_type` + `duckdb_register_secret_type`), and the SQL view `duckdb_secrets()` does NOT expose individual fields — secret field values are read via the C API on the secret handle, not via SQL. We resolve all three questions here, before committing to a `secret.rs` implementation.

**Files:**
- Create: `spikes/secrets_api/Cargo.toml`
- Create: `spikes/secrets_api/src/main.rs`
- Create: `spikes/secrets_api/README.md`

**Stop condition:** if neither the safe Rust API nor the FFI path can register a secret type with named string fields AND read those fields back, **stop the workflow** and report — Task 6 cannot ship.

- [ ] **Step 1: Create the spike crate**

```bash
mkdir -p spikes/secrets_api/src
```

Write `spikes/secrets_api/Cargo.toml`:

```toml
[package]
name = "secrets_api_spike"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
duckdb = { version = "1.4", features = ["bundled"] }

[workspace]
```

- [ ] **Step 2: Probe the API**

Read the docs.rs page for the pinned `duckdb-rs` version (`cargo doc -p duckdb --open` or `https://docs.rs/duckdb/<version>`) and search for `Secret`. Record findings in `spikes/secrets_api/README.md`:

- Does `duckdb::Connection` (or any of its modules) expose a Rust API to register a custom secret type? Note the exact path.
- If not, what FFI symbols are exported in `duckdb::ffi`? Note the relevant `duckdb_*` symbols (e.g., `duckdb_create_secret_type`, `duckdb_secret_type_add_named_parameter`, `duckdb_register_secret_type`).
- How is a secret's field value read at lookup time? Look for `duckdb_secret_get_string_value` / `SecretEntry` / `SecretReader` types.

This step is a documentation read; record before writing code.

- [ ] **Step 3: Implement the spike against whichever path the docs reveal**

Write `spikes/secrets_api/src/main.rs` to:

1. Open an in-memory DuckDB connection.
2. Register a secret type `REDSHIFT_TEST` with string fields `host`, `port`, `user`, `password`, `database`, `sslmode`.
3. Execute `CREATE SECRET s (TYPE REDSHIFT_TEST, HOST 'h', PORT '5439', USER 'u', PASSWORD 'p', DATABASE 'd', SSLMODE 'require');`
4. Look up secret `s` and print all six field values to stdout.

Sketch (adapt based on Step 2 findings):

```rust
use duckdb::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let con = Connection::open_in_memory()?;
    register_redshift_test_secret(&con)?;
    con.execute_batch(
        "CREATE SECRET s (TYPE REDSHIFT_TEST,
            HOST 'h', PORT '5439', USER 'u',
            PASSWORD 'p', DATABASE 'd', SSLMODE 'require');",
    )?;
    let fields = read_secret_fields(&con, "s")?;
    println!("fields: {fields:?}");
    assert_eq!(fields.get("host").map(String::as_str), Some("h"));
    assert_eq!(fields.get("password").map(String::as_str), Some("p"));
    Ok(())
}

fn register_redshift_test_secret(con: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    // FILL IN based on Step 2's API discovery.
    todo!("implement using whichever API the docs.rs probe revealed")
}

fn read_secret_fields(
    con: &Connection,
    name: &str,
) -> Result<std::collections::BTreeMap<String, String>, Box<dyn std::error::Error>> {
    // FILL IN based on Step 2's API discovery.
    todo!("implement using whichever API the docs.rs probe revealed")
}
```

The `todo!()` calls MUST be replaced with real implementations before Step 4. They're left here only because the exact path is determined by Step 2.

- [ ] **Step 4: Run the spike**

Run: `cd spikes/secrets_api && cargo run --release`

Expected: stdout contains `host: "h"`, `password: "p"`, etc.

If the only available path requires unsafe FFI, that is acceptable — record the chosen path in the README. If neither path works (e.g., the API to read field values is private), STOP the workflow.

- [ ] **Step 5: Document the chosen implementation strategy**

In `spikes/secrets_api/README.md`, record:

- **Chosen path:** safe Rust API / FFI / hybrid.
- **Functions to use in `secret.rs`:** exact names and signatures.
- **Field-read mechanism:** how `lookup_secret` will read field values back.
- **Risk:** anything fragile that should be revisited.

This README is the input to Task 6.

- [ ] **Step 6: Commit**

```bash
git add spikes/secrets_api
git commit -m "spike: DuckDB Secrets API surface verified for redshift secret type"
```

---

## Task 3: Project scaffolding

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `src/lib.rs`
- Create: `xtask/Cargo.toml`
- Create: `xtask/src/main.rs`
- Create: `.github/workflows/ci.yml`
- Create: `.cargo/config.toml`

- [ ] **Step 1: Write the workspace `Cargo.toml`**

```toml
[package]
name = "duckx"
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"
description = "DuckDB extension that queries Amazon Redshift via connectorx."
repository = "https://github.com/<owner>/duckx"

[lib]
crate-type = ["cdylib", "rlib"]
name = "duckx"

[features]
default = []
# Real-Redshift integration tests (gated; requires REDSHIFT_TEST_DSN env).
redshift-integration = []

[dependencies]
duckdb = { version = "=1.4.0", features = ["vtab", "extension-loadable"] }
duckdb-loadable-macros = "=0.1.6"
connectorx = { version = "0.4", features = ["src_postgres", "dst_arrow"] }
arrow = "54"
postgres = "0.19"
tokio-postgres = "0.7"
postgres-native-tls = "0.5"
native-tls = "0.2"
secrecy = "0.10"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
url = "2"
regex = "1"
once_cell = "1"

[dev-dependencies]
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }
pretty_assertions = "1"
serial_test = "3"
tempfile = "3"

[workspace]
members = ["xtask"]

[workspace.package]
edition = "2021"
```

(Adjust crate versions if any are unavailable; verify with `cargo search` before pasting.)

- [ ] **Step 2: Pin Rust toolchain**

Write `rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.82"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: Add minimal `lib.rs` placeholder**

Write `src/lib.rs`:

```rust
//! duckx — DuckDB extension that queries Amazon Redshift via connectorx.
//!
//! Module wiring lives in subsequent tasks; this is the scaffolding stub.

/// DuckDB version this extension is built against.
/// Verified at extension load time (see Task 11).
pub const DUCKDB_VERSION: &str = env!("DUCKX_DUCKDB_VERSION");
```

- [ ] **Step 4: Derive the DuckDB version at build time**

Instead of hand-editing an env var, derive `DUCKX_DUCKDB_VERSION` from the `duckdb` dependency listed in `Cargo.toml`. This keeps the runtime version check in sync with the linked ABI: bumping the `duckdb = "X.Y"` line in `Cargo.toml` automatically updates the runtime check.

Write `build.rs`:

```rust
fn main() {
    // Parse the duckdb dep version out of Cargo.toml so the runtime
    // version check (lib.rs::verify_duckdb_version) tracks the linked
    // crate version automatically.
    let manifest = std::fs::read_to_string("Cargo.toml").expect("read Cargo.toml");
    let line = manifest
        .lines()
        .find(|l| l.trim_start().starts_with("duckdb"))
        .expect("Cargo.toml must declare a `duckdb = ...` dependency");
    let version = line
        .split('=')
        .nth(1)
        .and_then(|rest| rest.split('"').nth(1))
        .or_else(|| {
            // version field form: duckdb = { version = "X.Y", ... }
            line.split("version").nth(1).and_then(|r| r.split('"').nth(1))
        })
        .expect("could not parse duckdb version from Cargo.toml");
    println!("cargo:rustc-env=DUCKX_DUCKDB_VERSION={version}");
    println!("cargo:rerun-if-changed=Cargo.toml");
}
```

Do NOT also write `.cargo/config.toml` — leave that absent so the build.rs is the single source of truth. If a `.cargo/config.toml` already exists with a `[env]` block, remove the `DUCKX_DUCKDB_VERSION` line.

- [ ] **Step 5: Create the xtask binary for extension packaging**

Write `xtask/Cargo.toml`:

```toml
[package]
name = "xtask"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
```

Write `xtask/src/main.rs`:

```rust
//! `cargo xtask package` — copies the built cdylib to
//! `target/release/redshift.duckdb_extension` for distribution.
//!
//! The `duckdb-loadable-macros` crate writes the DuckDB metadata footer at
//! link time, so no footer manipulation is needed here.

use std::process::ExitCode;

fn main() -> ExitCode {
    let cmd = std::env::args().nth(1).unwrap_or_default();
    match cmd.as_str() {
        "package" => package(),
        other => {
            eprintln!("unknown xtask: {other}. usage: cargo xtask package");
            ExitCode::from(2)
        }
    }
}

fn package() -> ExitCode {
    let target = std::path::PathBuf::from("target/release");
    let candidates = [
        target.join("libduckx.dylib"),
        target.join("libduckx.so"),
        target.join("duckx.dll"),
    ];
    let src = match candidates.iter().find(|p| p.exists()) {
        Some(p) => p,
        None => {
            eprintln!("no built cdylib found; run `cargo build --release` first");
            return ExitCode::from(1);
        }
    };
    let dst = target.join("redshift.duckdb_extension");
    if let Err(e) = std::fs::copy(src, &dst) {
        eprintln!("copy failed: {e}");
        return ExitCode::from(1);
    }
    println!("packaged: {}", dst.display());
    ExitCode::SUCCESS
}
```

- [ ] **Step 6: Add CI workflow**

Write `.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push: { branches: [main] }
  pull_request:

jobs:
  test:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-14]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: rustfmt, clippy }
      - name: Install DuckDB CLI (for spike load tests)
        run: |
          if [ "$RUNNER_OS" = "macOS" ]; then
            brew install duckdb
          else
            curl -L -o duckdb.zip https://github.com/duckdb/duckdb/releases/download/v1.4.0/duckdb_cli-linux-amd64.zip
            unzip duckdb.zip && sudo mv duckdb /usr/local/bin/
          fi
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo build --release
      - run: cargo xtask package
      - run: cargo test --release
```

- [ ] **Step 7: Verify it all compiles**

Run: `cargo build`

Expected: clean build of the empty stub crate.

- [ ] **Step 8: Commit (including `Cargo.lock`)**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml build.rs src/lib.rs xtask .github
git commit -m "feat: project scaffolding with pinned DuckDB version and CI"
```

Cargo.lock is committed for this crate because it builds a versioned `.duckdb_extension` ABI artifact — reproducible builds matter. Also add a `.gitignore` line `target/` if not already present.

---

## Task 4: Error type — `error.rs`

**Files:**
- Create: `src/error.rs`
- Create: `tests/unit/error_redaction.rs`
- Modify: `src/lib.rs` — add `pub mod error;`

- [ ] **Step 1: Write failing redaction + panic-boundary tests**

Write `tests/unit/error_redaction.rs`:

```rust
use duckx::error::DuckxError;

#[test]
fn redacts_keyword_form_password() {
    let err = DuckxError::RedshiftError(
        "auth failed for user 'analyst' (host=db.aws, password=hunter2)".into(),
    );
    let rendered = format!("{err}");
    assert!(!rendered.contains("hunter2"), "leak: {rendered}");
    assert!(rendered.contains("password=****"), "no redaction marker: {rendered}");
}

#[test]
fn redacts_pwd_alias_too() {
    let err = DuckxError::RedshiftError("connect failed: pwd=s3cr3t!".into());
    let rendered = format!("{err}");
    assert!(!rendered.contains("s3cr3t"));
    assert!(rendered.contains("pwd=****"));
}

#[test]
fn redacts_url_form_password() {
    let err = DuckxError::RedshiftError(
        "connect failed for postgresql://analyst:hunter2@db.aws:5439/analytics".into(),
    );
    let rendered = format!("{err}");
    assert!(!rendered.contains("hunter2"), "leak: {rendered}");
    // user must remain
    assert!(rendered.contains("analyst@") || rendered.contains("analyst:"));
}

#[test]
fn redacts_multiple_passwords_in_one_string() {
    let err = DuckxError::RedshiftError(
        "first password=alpha; later pwd=beta; tail OK".into(),
    );
    let rendered = format!("{err}");
    assert!(!rendered.contains("alpha"), "leak alpha: {rendered}");
    assert!(!rendered.contains("beta"), "leak beta: {rendered}");
    assert!(rendered.contains("tail OK"), "tail dropped: {rendered}");
}

#[test]
fn redaction_passthrough_when_no_match() {
    let err = DuckxError::RedshiftError("plain message with no creds".into());
    let rendered = format!("{err}");
    assert!(rendered.ends_with("plain message with no creds"));
}

#[test]
fn missing_credential_lists_every_missing_field() {
    let err = DuckxError::MissingCredential {
        fields: vec!["host".into(), "password".into()],
    };
    let rendered = format!("{err}");
    assert!(rendered.contains("host"));
    assert!(rendered.contains("password"));
}

#[test]
fn unsupported_type_names_column_and_type() {
    let err = DuckxError::UnsupportedType {
        column: "geo_col".into(),
        type_name: "GEOMETRY".into(),
    };
    let rendered = format!("{err}");
    assert!(rendered.contains("geo_col"));
    assert!(rendered.contains("GEOMETRY"));
}

#[test]
fn panic_boundary_converts_panics_to_redshift_error() {
    use duckx::error::{panic_boundary, DuckxError};
    let res: Result<(), DuckxError> = panic_boundary("test", || {
        panic!("kaboom");
    });
    match res {
        Err(DuckxError::RedshiftError(msg)) => {
            assert!(msg.contains("panic in test"), "msg: {msg}");
            assert!(msg.contains("kaboom"));
        }
        other => panic!("expected RedshiftError, got {other:?}"),
    }
}

#[test]
fn panic_boundary_passes_ok_through() {
    use duckx::error::panic_boundary;
    let res = panic_boundary("ok", || Ok::<i32, duckx::error::DuckxError>(42));
    assert_eq!(res.unwrap(), 42);
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `cargo test --test error_redaction`

Expected: FAIL — `duckx::error` does not exist.

- [ ] **Step 3: Implement `error.rs`**

Add to `Cargo.toml` `[dependencies]`: `regex = "1"`, and `once_cell = "1"`.

Write `src/error.rs`:

```rust
//! `DuckxError`: typed errors with PII-safe `Display`.
//!
//! `RedshiftError` strings are scrubbed for two leak patterns before
//! rendering:
//!   1. keyword form: `(?i)(password|pwd)=<value-up-to-delim>`
//!   2. URL form:     `(?i)(postgres|postgresql)://user:<password>@`

use once_cell::sync::Lazy;
use regex::Regex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DuckxError {
    #[error("missing credential fields: {fields:?}")]
    MissingCredential { fields: Vec<String> },

    #[error("invalid DSN: {0}")]
    BadDsn(String),

    #[error("redshift error: {}", redact(.0))]
    RedshiftError(String),

    #[error("unsupported Redshift type for column {column:?}: {type_name}")]
    UnsupportedType { column: String, type_name: String },

    #[error("invalid partition bounds: {reason}")]
    PartitionBoundsInvalid { reason: &'static str },

    #[error("failed to decode batch: {0}")]
    BatchDecode(String),
}

static KEYWORD_PW: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\b(password|pwd)\s*=\s*[^\s,;)]*").unwrap());
static URL_PW: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(postgres(?:ql)?)://([^:/@\s]+):([^@\s]+)@").unwrap()
});

fn redact(s: &str) -> String {
    let s = KEYWORD_PW.replace_all(s, |c: &regex::Captures| format!("{}=****", &c[1]));
    let s = URL_PW.replace_all(&s, |c: &regex::Captures| {
        format!("{scheme}://{user}:****@", scheme = &c[1], user = &c[2])
    });
    s.into_owned()
}

/// Wrap an FFI-boundary callback so panics convert into a `DuckxError`
/// instead of unwinding across the C ABI (which is undefined behavior).
///
/// All `VTab::bind`, `VTab::init`, `VTab::func`, and the
/// `extension_entrypoint` body MUST go through this helper.
pub fn panic_boundary<F, T>(label: &'static str, f: F) -> Result<T, DuckxError>
where
    F: FnOnce() -> Result<T, DuckxError> + std::panic::UnwindSafe,
{
    match std::panic::catch_unwind(f) {
        Ok(res) => res,
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "non-string panic payload".to_string()
            };
            Err(DuckxError::RedshiftError(format!("panic in {label}: {msg}")))
        }
    }
}
```

- [ ] **Step 4: Wire into `lib.rs`**

Edit `src/lib.rs`:

```rust
//! duckx — DuckDB extension that queries Amazon Redshift via connectorx.

pub mod error;

pub const DUCKDB_VERSION: &str = env!("DUCKX_DUCKDB_VERSION");
```

- [ ] **Step 5: Run tests — expect pass**

Run: `cargo test --test error_redaction`

Expected: PASS, all three tests.

- [ ] **Step 6: Commit**

```bash
git add src/error.rs src/lib.rs tests/unit/error_redaction.rs
git commit -m "feat(error): typed errors with password redaction in Display"
```

---

## Task 5: Config + credential resolution — `config.rs`

> Runs in parallel with Tasks 6, 7, 8.

**Files:**
- Create: `src/config.rs`
- Create: `tests/unit/config_resolve.rs`
- Modify: `src/lib.rs` — add `pub mod config;`

- [ ] **Step 1: Write failing tests**

Write `tests/unit/config_resolve.rs`:

```rust
use duckx::config::{resolve_from_env, Config, SslMode};
use duckx::error::DuckxError;
use serial_test::serial;

fn env_full() -> Vec<(&'static str, &'static str)> {
    vec![
        ("REDSHIFT_HOST", "db.aws"),
        ("REDSHIFT_PORT", "5439"),
        ("REDSHIFT_USER", "analyst"),
        ("REDSHIFT_PASSWORD", "hunter2"),
        ("REDSHIFT_DATABASE", "analytics"),
        ("REDSHIFT_SSLMODE", "require"),
    ]
}

fn clear_env() {
    for k in [
        "REDSHIFT_HOST","REDSHIFT_PORT","REDSHIFT_USER",
        "REDSHIFT_PASSWORD","REDSHIFT_DATABASE","REDSHIFT_SSLMODE",
    ] { std::env::remove_var(k); }
}

fn set_env(vars: &[(&'static str, &'static str)]) {
    clear_env();
    for (k, v) in vars { std::env::set_var(k, v); }
}

#[test]
#[serial]
fn full_env_resolves() {
    set_env(&env_full());
    let cfg = resolve_from_env().expect("ok");
    assert_eq!(cfg.host, "db.aws");
    assert_eq!(cfg.port, 5439);
    assert_eq!(cfg.user, "analyst");
    assert_eq!(cfg.database, "analytics");
    assert_eq!(cfg.sslmode, SslMode::Require);
}

#[test]
#[serial]
fn port_defaults_to_5439_when_unset() {
    let mut e = env_full(); e.retain(|(k, _)| *k != "REDSHIFT_PORT");
    set_env(&e);
    let cfg = resolve_from_env().expect("ok");
    assert_eq!(cfg.port, 5439);
}

#[test]
#[serial]
fn sslmode_defaults_to_require_when_unset() {
    let mut e = env_full(); e.retain(|(k, _)| *k != "REDSHIFT_SSLMODE");
    set_env(&e);
    let cfg = resolve_from_env().expect("ok");
    assert_eq!(cfg.sslmode, SslMode::Require);
}

#[test]
#[serial]
fn sslmode_disable_is_accepted() {
    let mut e = env_full();
    for (k, v) in e.iter_mut() { if *k == "REDSHIFT_SSLMODE" { *v = "disable"; } }
    set_env(&e);
    let cfg = resolve_from_env().expect("ok");
    assert_eq!(cfg.sslmode, SslMode::Disable);
}

#[test]
#[serial]
fn sslmode_invalid_errors() {
    let mut e = env_full();
    for (k, v) in e.iter_mut() { if *k == "REDSHIFT_SSLMODE" { *v = "yolo"; } }
    set_env(&e);
    assert!(matches!(resolve_from_env(), Err(DuckxError::BadDsn(_))));
}

#[test]
#[serial]
fn missing_fields_listed_explicitly() {
    set_env(&[("REDSHIFT_HOST", "db.aws")]);
    match resolve_from_env() {
        Err(DuckxError::MissingCredential { fields }) => {
            for f in ["user", "password", "database"] {
                assert!(fields.iter().any(|x| x == f), "missing {f}: {fields:?}");
            }
        }
        other => panic!("expected MissingCredential, got {other:?}"),
    }
}

#[test]
#[serial]
fn invalid_port_errors() {
    let mut e = env_full();
    for (k, v) in e.iter_mut() { if *k == "REDSHIFT_PORT" { *v = "not-a-number"; } }
    set_env(&e);
    assert!(matches!(resolve_from_env(), Err(DuckxError::BadDsn(_))));
}
```

> **Note:** add `serial_test = "3"` to `[dev-dependencies]`. All env-var manipulation tests are marked `#[serial]` to prevent races across the cargo-test thread pool.

- [ ] **Step 2: Run tests — expect failure**

Run: `cargo test --test config_resolve`

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement `config.rs`**

Write `src/config.rs`:

```rust
//! Credential / connection config resolution.
//!
//! This module intentionally knows nothing about DuckDB or connectorx.
//! It produces a [`Config`] from environment variables (or, in `secret.rs`,
//! from a DuckDB Secret). All callers go through [`Config::to_postgres_dsn`]
//! to get an internal connection string.

use crate::error::DuckxError;
use secrecy::{ExposeSecret, SecretString};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    pub fn parse(s: &str) -> Result<Self, DuckxError> {
        match s.to_ascii_lowercase().as_str() {
            "disable"     => Ok(Self::Disable),
            "prefer"      => Ok(Self::Prefer),
            "require"     => Ok(Self::Require),
            "verify-ca"   => Ok(Self::VerifyCa),
            "verify-full" => Ok(Self::VerifyFull),
            other => Err(DuckxError::BadDsn(format!(
                "invalid sslmode {other:?}; expected one of: disable, prefer, require, verify-ca, verify-full"
            ))),
        }
    }

    pub fn as_libpq_str(&self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub database: String,
    pub sslmode: SslMode,
}

impl Config {
    /// Build a libpq-style DSN. Internal use only. Callers must NOT log this.
    /// The password is URL-encoded; `sslmode` is emitted as a query parameter.
    pub fn to_postgres_dsn(&self) -> String {
        let pw = url::form_urlencoded::byte_serialize(self.password.expose_secret().as_bytes())
            .collect::<String>();
        let user = url::form_urlencoded::byte_serialize(self.user.as_bytes()).collect::<String>();
        format!(
            "postgresql://{user}:{pw}@{host}:{port}/{db}?sslmode={ssl}",
            host = self.host,
            port = self.port,
            db = self.database,
            ssl = self.sslmode.as_libpq_str(),
        )
    }
}

/// Validate that an identifier (e.g. partition column name) is safe to
/// interpolate into SQL. Matches `^[A-Za-z_][A-Za-z0-9_]*$` and bounds length.
pub fn validate_identifier(name: &str) -> Result<(), DuckxError> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name.chars().enumerate().all(|(i, c)| {
            if i == 0 { c.is_ascii_alphabetic() || c == '_' }
            else      { c.is_ascii_alphanumeric() || c == '_' }
        });
    if valid {
        Ok(())
    } else {
        Err(DuckxError::BadDsn(format!(
            "identifier {name:?} must match ^[A-Za-z_][A-Za-z0-9_]*$ (max 128 chars)"
        )))
    }
}

pub fn resolve_from_env() -> Result<Config, DuckxError> {
    let host = std::env::var("REDSHIFT_HOST").ok();
    let port_raw = std::env::var("REDSHIFT_PORT").ok();
    let user = std::env::var("REDSHIFT_USER").ok();
    let password = std::env::var("REDSHIFT_PASSWORD").ok();
    let database = std::env::var("REDSHIFT_DATABASE").ok();
    let sslmode_raw = std::env::var("REDSHIFT_SSLMODE").ok();

    let mut missing = Vec::new();
    if host.is_none() { missing.push("host".into()); }
    if user.is_none() { missing.push("user".into()); }
    if password.is_none() { missing.push("password".into()); }
    if database.is_none() { missing.push("database".into()); }

    if !missing.is_empty() {
        return Err(DuckxError::MissingCredential { fields: missing });
    }

    let port = match port_raw {
        Some(p) => p.parse::<u16>().map_err(|_| {
            DuckxError::BadDsn(format!("REDSHIFT_PORT is not a valid u16: {p}"))
        })?,
        None => 5439,
    };

    let sslmode = match sslmode_raw.as_deref() {
        Some(s) => SslMode::parse(s)?,
        None => SslMode::Require,
    };

    if matches!(sslmode, SslMode::Disable) {
        tracing::warn!(target: "duckx", "REDSHIFT_SSLMODE=disable — plaintext connection to Redshift is not recommended");
    }

    Ok(Config {
        host: host.unwrap(),
        port,
        user: user.unwrap(),
        password: SecretString::new(password.unwrap().into()),
        database: database.unwrap(),
        sslmode,
    })
}
```

- [ ] **Step 4: Wire into `lib.rs`**

Add `pub mod config;` to `src/lib.rs`.

- [ ] **Step 5: Run tests — expect pass**

Run: `cargo test --test config_resolve`

Expected: PASS, all four tests.

- [ ] **Step 6: Commit**

```bash
git add src/config.rs src/lib.rs tests/unit/config_resolve.rs Cargo.toml
git commit -m "feat(config): credential resolution from env vars with explicit missing-field errors"
```

---

## Task 6: Secret type registration — `secret.rs`

> Runs in parallel with Tasks 5, 7, 8. **Blocked by Task 2.5** — read `spikes/secrets_api/README.md` first; the chosen path (safe Rust API vs FFI vs hybrid) and the field-read mechanism are determined there.

**Files:**
- Create: `src/secret.rs`
- Create: `tests/unit/secret_lookup.rs`
- Modify: `src/lib.rs` — add `pub mod secret;`

The `duckdb_secrets()` SQL view does NOT expose individual field values as columns. Lookup must use the API discovered in Task 2.5 (typically: register a secret type, then read fields via the secret-handle C API). Do not write SQL like `SELECT host, port, ... FROM duckdb_secrets()` — that schema does not exist.

- [ ] **Step 1: Read Task 2.5's findings**

Open `spikes/secrets_api/README.md`. Confirm:

- The exact registration function (safe Rust or FFI symbol).
- The field-read mechanism returning string values for each named field of a secret instance.
- Any version-specific quirks recorded.

If Task 2.5 has not run, run it first.

- [ ] **Step 2: Write the lookup test**

Write `tests/unit/secret_lookup.rs`:

```rust
//! Constructs a DuckDB connection in-process, registers the REDSHIFT
//! secret type, CREATE SECRET, and verifies our lookup returns a Config
//! with every expected field.

use duckdb::Connection;
use duckx::config::{Config, SslMode};
use duckx::secret::{lookup_secret, register_redshift_secret_type};
use secrecy::ExposeSecret;
use serial_test::serial;

#[test]
#[serial]
fn create_and_lookup_secret_round_trip() {
    let conn = Connection::open_in_memory().unwrap();
    register_redshift_secret_type(&conn).unwrap();
    conn.execute_batch(
        "CREATE SECRET test_secret (
            TYPE REDSHIFT,
            HOST 'db.aws',
            PORT '5439',
            USER 'analyst',
            PASSWORD 'hunter2',
            DATABASE 'analytics',
            SSLMODE 'require'
        );",
    )
    .unwrap();

    let cfg: Config = lookup_secret(&conn, "test_secret").unwrap();
    assert_eq!(cfg.host, "db.aws");
    assert_eq!(cfg.port, 5439);
    assert_eq!(cfg.user, "analyst");
    assert_eq!(cfg.database, "analytics");
    assert_eq!(cfg.password.expose_secret(), "hunter2");
    assert_eq!(cfg.sslmode, SslMode::Require);
}

#[test]
#[serial]
fn sslmode_defaults_to_require_when_field_omitted() {
    let conn = Connection::open_in_memory().unwrap();
    register_redshift_secret_type(&conn).unwrap();
    conn.execute_batch(
        "CREATE SECRET s (TYPE REDSHIFT,
            HOST 'h', PORT '5439', USER 'u', PASSWORD 'p', DATABASE 'd');",
    ).unwrap();
    let cfg = lookup_secret(&conn, "s").unwrap();
    assert_eq!(cfg.sslmode, SslMode::Require);
}

#[test]
#[serial]
fn missing_secret_errors() {
    let conn = Connection::open_in_memory().unwrap();
    register_redshift_secret_type(&conn).unwrap();
    let res = lookup_secret(&conn, "nope");
    assert!(res.is_err());
}
```

- [ ] **Step 3: Run tests — expect failure**

Run: `cargo test --test secret_lookup -- --test-threads=1`

Expected: FAIL — module does not exist.

- [ ] **Step 4: Implement `secret.rs` by copying directly from the spike**

The two function bodies (`register_redshift_secret_type`, `read_secret_fields`) come **verbatim from Task 2.5's spike** — Task 2.5 step 5 produces a `spikes/secrets_api/src/secrets_api.rs` file that this task `cp`s into `src/secret.rs` (with the type name change `REDSHIFT_TEST` → `REDSHIFT`). Do not commit any state with `todo!()` in `src/` — the CI grep gate (Task 14) blocks it, and there's no reason to leave one behind because the spike has already produced working code.

Procedure:

1. Open `spikes/secrets_api/src/main.rs` (and the extracted `secrets_api.rs` from Task 2.5's deliverable).
2. Copy the two function bodies into `src/secret.rs` using the skeleton below; replace the type-name string `"REDSHIFT_TEST"` with `"REDSHIFT"`.
3. Verify `grep -rn 'todo!()' src/` returns nothing before committing.

Skeleton:

```rust
//! DuckDB Secrets Manager integration for `TYPE REDSHIFT`.
//!
//! Implementation strategy is defined in `spikes/secrets_api/README.md`
//! (Task 2.5). Field-read uses whichever C-API or Rust-API path that
//! spike validated. Function bodies below are copied verbatim from the
//! spike with the type-name string changed.

use crate::config::{Config, SslMode};
use crate::error::DuckxError;
use duckdb::Connection;
use secrecy::SecretString;

pub const SECRET_FIELDS: &[&str] = &["host", "port", "user", "password", "database", "sslmode"];

pub fn register_redshift_secret_type(conn: &Connection) -> Result<(), DuckxError> {
    // BEGIN: copied from spikes/secrets_api with type-name changed.
    // Spike-defined function takes ownership of registering the type with
    // SECRET_FIELDS. Wrap any duckdb / ffi error in DuckxError::RedshiftError.
    /* ... body from spike ... */
    let _ = conn;
    Err(DuckxError::RedshiftError(
        "stub — paste from spikes/secrets_api at Task 6 step 4".into(),
    ))
    // END
}

pub fn lookup_secret(conn: &Connection, name: &str) -> Result<Config, DuckxError> {
    let fields = read_secret_fields(conn, name)?;

    let host = take_field(&fields, "host")?;
    let user = take_field(&fields, "user")?;
    let password = take_field(&fields, "password")?;
    let database = take_field(&fields, "database")?;

    let port: u16 = match fields.get("port") {
        Some(s) => s.parse().map_err(|_| {
            DuckxError::BadDsn(format!("secret {name:?}: port {s:?} is not a valid u16"))
        })?,
        None => 5439,
    };
    let sslmode = match fields.get("sslmode") {
        Some(s) => SslMode::parse(s)?,
        None => SslMode::Require,
    };

    Ok(Config {
        host,
        port,
        user,
        password: SecretString::new(password.into()),
        database,
        sslmode,
    })
}

fn read_secret_fields(
    conn: &Connection,
    name: &str,
) -> Result<std::collections::BTreeMap<String, String>, DuckxError> {
    // BEGIN: copied from spikes/secrets_api.
    /* ... body from spike ... */
    let _ = (conn, name);
    Err(DuckxError::RedshiftError(
        "stub — paste from spikes/secrets_api at Task 6 step 4".into(),
    ))
    // END
}

fn take_field(
    fields: &std::collections::BTreeMap<String, String>,
    name: &str,
) -> Result<String, DuckxError> {
    fields.get(name).cloned().ok_or_else(|| DuckxError::MissingCredential {
        fields: vec![name.into()],
    })
}
```

The two stub bodies that return `Err(...stub...)` MUST be replaced with the spike's working code. They are NOT `todo!()` so the CI grep gate doesn't fire prematurely; the test gate at Step 6 catches the unreplaced stubs (tests will fail with the stub error message). Leaving stubs that compile-and-error is intentional — they enable incremental commits while making the gap visible.

- [ ] **Step 5: Wire into `lib.rs`**

Add `pub mod secret;` to `src/lib.rs`.

- [ ] **Step 6: Run tests — expect pass**

Run: `cargo test --test secret_lookup -- --test-threads=1`

Expected: PASS, all three tests. If they fail with `not yet implemented`, the `todo!()` placeholders weren't replaced — go back to Step 4.

- [ ] **Step 7: Commit**

```bash
git add src/secret.rs src/lib.rs tests/unit/secret_lookup.rs
git commit -m "feat(secret): register REDSHIFT secret type and lookup into Config"
```

---

## Task 7: Type mapping — `types.rs`

> Runs in parallel with Tasks 5, 6, 8.

**Files:**
- Create: `src/types.rs`
- Create: `tests/unit/types_mapping.rs`
- Modify: `src/lib.rs` — add `pub mod types;`

- [ ] **Step 1: Write the mapping table tests**

Write `tests/unit/types_mapping.rs`:

```rust
use arrow::datatypes::{DataType as ArrowDt, TimeUnit};
use duckdb::core::LogicalTypeId;
use duckx::error::DuckxError;
use duckx::types::arrow_to_duckdb;
use pretty_assertions::assert_eq;

#[test]
fn supported_scalar_types_map_correctly() {
    let cases: &[(ArrowDt, LogicalTypeId)] = &[
        (ArrowDt::Boolean,                       LogicalTypeId::Boolean),
        (ArrowDt::Int16,                         LogicalTypeId::Smallint),
        (ArrowDt::Int32,                         LogicalTypeId::Integer),
        (ArrowDt::Int64,                         LogicalTypeId::Bigint),
        (ArrowDt::UInt64,                        LogicalTypeId::Hugeint),
        (ArrowDt::Float32,                       LogicalTypeId::Float),
        (ArrowDt::Float64,                       LogicalTypeId::Double),
        (ArrowDt::Utf8,                          LogicalTypeId::Varchar),
        (ArrowDt::LargeUtf8,                     LogicalTypeId::Varchar),
        (ArrowDt::Binary,                        LogicalTypeId::Blob),
        (ArrowDt::Date32,                        LogicalTypeId::Date),
        (ArrowDt::Time64(TimeUnit::Microsecond), LogicalTypeId::Time),
        (ArrowDt::Timestamp(TimeUnit::Microsecond, None),                     LogicalTypeId::Timestamp),
        (ArrowDt::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),       LogicalTypeId::TimestampTz),
        (ArrowDt::Decimal128(18, 4),             LogicalTypeId::Decimal),
    ];
    for (arrow_ty, want) in cases {
        let got = arrow_to_duckdb("col", arrow_ty).expect("supported");
        assert_eq!(got.id(), *want, "for arrow type {arrow_ty:?}");
    }
}

#[test]
fn unsupported_list_errors_with_column_name() {
    let arrow_ty = ArrowDt::List(std::sync::Arc::new(arrow::datatypes::Field::new(
        "x", ArrowDt::Int32, true,
    )));
    let err = arrow_to_duckdb("payload", &arrow_ty).expect_err("must error");
    match err {
        DuckxError::UnsupportedType { column, type_name } => {
            assert_eq!(column, "payload");
            assert!(type_name.contains("List"));
        }
        other => panic!("expected UnsupportedType, got {other:?}"),
    }
}

#[test]
fn other_unsupported_arrow_types_error() {
    use std::sync::Arc;
    let cases: &[(ArrowDt, &str)] = &[
        (
            ArrowDt::Struct(arrow::datatypes::Fields::from(vec![
                arrow::datatypes::Field::new("a", ArrowDt::Int32, true),
            ])),
            "Struct",
        ),
        (
            ArrowDt::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    ArrowDt::Struct(arrow::datatypes::Fields::from(vec![
                        arrow::datatypes::Field::new("key", ArrowDt::Utf8, false),
                        arrow::datatypes::Field::new("value", ArrowDt::Int32, true),
                    ])),
                    false,
                )),
                false,
            ),
            "Map",
        ),
        (ArrowDt::Interval(arrow::datatypes::IntervalUnit::DayTime), "Interval"),
        (ArrowDt::Duration(TimeUnit::Microsecond), "Duration"),
    ];
    for (arrow_ty, expected_substr) in cases {
        let err = arrow_to_duckdb("c", arrow_ty).expect_err("must error");
        match err {
            DuckxError::UnsupportedType { column, type_name } => {
                assert_eq!(column, "c");
                assert!(
                    type_name.contains(expected_substr),
                    "type_name {type_name:?} missing substring {expected_substr:?}"
                );
            }
            other => panic!("expected UnsupportedType, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run tests — expect failure**

Run: `cargo test --test types_mapping`

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement `types.rs`**

Write `src/types.rs`:

```rust
//! Arrow → DuckDB logical-type mapping.
//!
//! This is the single authoritative table for what types `redshift_scan`
//! supports. Adding a new mapping requires adding a test in
//! `tests/unit/types_mapping.rs`.

use crate::error::DuckxError;
use arrow::datatypes::{DataType, TimeUnit};
use duckdb::core::{LogicalTypeHandle, LogicalTypeId};

pub fn arrow_to_duckdb(column: &str, t: &DataType) -> Result<LogicalTypeHandle, DuckxError> {
    // Decimal needs precision/scale; handle it before the simple branch.
    if let DataType::Decimal128(p, s) | DataType::Decimal256(p, s) = t {
        return Ok(LogicalTypeHandle::decimal(*p, *s as u8));
    }
    let id = match t {
        DataType::Boolean => LogicalTypeId::Boolean,
        DataType::Int8 | DataType::Int16 => LogicalTypeId::Smallint,
        DataType::Int32 => LogicalTypeId::Integer,
        DataType::Int64 => LogicalTypeId::Bigint,
        DataType::UInt8 | DataType::UInt16 => LogicalTypeId::Integer,
        DataType::UInt32 => LogicalTypeId::Bigint,
        DataType::UInt64 => LogicalTypeId::Hugeint,
        DataType::Float32 => LogicalTypeId::Float,
        DataType::Float64 => LogicalTypeId::Double,
        DataType::Utf8 | DataType::LargeUtf8 => LogicalTypeId::Varchar,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => LogicalTypeId::Blob,
        DataType::Date32 | DataType::Date64 => LogicalTypeId::Date,
        DataType::Time32(_) | DataType::Time64(_) => LogicalTypeId::Time,
        DataType::Timestamp(_, None) => LogicalTypeId::Timestamp,
        DataType::Timestamp(_, Some(_)) => LogicalTypeId::TimestampTz,
        // Best-effort fallthrough — any Arrow type not enumerated above
        // (Interval, Duration, List, Struct, Map, Union, Float16, etc.)
        // surfaces an UnsupportedType with the column name and Arrow type
        // string. Spec lists the known Redshift types this catches:
        // SUPER, GEOMETRY/GEOGRAPHY, HLLSKETCH, VARBYTE, INTERVAL,
        // TIME WITH TIME ZONE, OID.
        other => {
            return Err(DuckxError::UnsupportedType {
                column: column.to_string(),
                type_name: format!("{other:?}"),
            });
        }
    };

    let _ = TimeUnit::Microsecond; // silence unused import on some duckdb-rs versions
    Ok(LogicalTypeHandle::from(id))
}
```

- [ ] **Step 4: Wire into `lib.rs`**

Add `pub mod types;` to `src/lib.rs`.

- [ ] **Step 5: Run tests — expect pass**

Run: `cargo test --test types_mapping`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/types.rs src/lib.rs tests/unit/types_mapping.rs
git commit -m "feat(types): Arrow→DuckDB logical-type mapping with explicit unsupported-type errors"
```

---

## Task 8: Partition arg validation — `partition.rs`

> Runs in parallel with Tasks 5, 6, 7.

**Files:**
- Create: `src/partition.rs`
- Create: `tests/unit/partition_validation.rs`
- Modify: `src/lib.rs` — add `pub mod partition;`

- [ ] **Step 1: Write validation tests**

Write `tests/unit/partition_validation.rs`:

```rust
use duckx::error::DuckxError;
use duckx::partition::{validate, PartitionArgs, PartitionSpec};

#[test]
fn no_partitioning_returns_single() {
    let args = PartitionArgs::default();
    assert!(matches!(validate(&args), Ok(PartitionSpec::Single)));
}

#[test]
fn partition_num_one_is_single_even_with_partition_on() {
    let args = PartitionArgs {
        partition_on: Some("id".into()),
        partition_num: Some(1),
        ..Default::default()
    };
    assert!(matches!(validate(&args), Ok(PartitionSpec::Single)));
}

#[test]
fn partition_num_without_on_errors() {
    let args = PartitionArgs { partition_num: Some(8), ..Default::default() };
    assert!(matches!(validate(&args), Err(DuckxError::PartitionBoundsInvalid { .. })));
}

#[test]
fn partition_on_without_num_defaults_to_4() {
    let args = PartitionArgs { partition_on: Some("id".into()), ..Default::default() };
    match validate(&args).unwrap() {
        PartitionSpec::Parallel { column, num, .. } => {
            assert_eq!(column, "id");
            assert_eq!(num, 4);
        }
        _ => panic!("expected Parallel"),
    }
}

#[test]
fn invalid_identifier_errors() {
    let args = PartitionArgs {
        partition_on: Some("id; DROP TABLE t".into()),
        partition_num: Some(4),
        ..Default::default()
    };
    assert!(matches!(validate(&args), Err(DuckxError::BadDsn(_))));
}

#[test]
fn min_greater_than_max_errors() {
    let args = PartitionArgs {
        partition_on: Some("id".into()),
        partition_num: Some(4),
        partition_min: Some(100),
        partition_max: Some(10),
    };
    assert!(matches!(validate(&args), Err(DuckxError::PartitionBoundsInvalid { .. })));
}

#[test]
fn explicit_bounds_pass_through() {
    let args = PartitionArgs {
        partition_on: Some("id".into()),
        partition_num: Some(8),
        partition_min: Some(0),
        partition_max: Some(10_000),
    };
    match validate(&args).unwrap() {
        PartitionSpec::Parallel { num, bounds: Some((lo, hi)), .. } => {
            assert_eq!(num, 8);
            assert_eq!((lo, hi), (0, 10_000));
        }
        _ => panic!("expected explicit bounds"),
    }
}
```

- [ ] **Step 2: Run tests — expect failure**

Run: `cargo test --test partition_validation`

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement `partition.rs`**

Write `src/partition.rs`:

```rust
//! Partition argument parsing + validation. Bound discovery lives in
//! `pipeline.rs` because it requires a database round-trip.

use crate::error::DuckxError;

#[derive(Debug, Default, Clone)]
pub struct PartitionArgs {
    pub partition_on: Option<String>,
    pub partition_num: Option<i64>,
    pub partition_min: Option<i64>,
    pub partition_max: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum PartitionSpec {
    Single,
    Parallel {
        column: String,
        num: u32,
        /// `None` means discover via `MIN/MAX` query at init time.
        bounds: Option<(i64, i64)>,
    },
}

pub fn validate(args: &PartitionArgs) -> Result<PartitionSpec, DuckxError> {
    match (&args.partition_on, args.partition_num) {
        (None, None) => Ok(PartitionSpec::Single),
        (None, Some(n)) if n == 1 => Ok(PartitionSpec::Single),
        (None, Some(_)) => Err(DuckxError::PartitionBoundsInvalid {
            reason: "partition_num requires partition_on",
        }),
        (Some(col), maybe_num) => {
            // partition_on with partition_num=1 (or unspecified=1) → Single.
            let num = maybe_num.unwrap_or(4);
            if num == 1 {
                return Ok(PartitionSpec::Single);
            }
            if !(2..=64).contains(&num) {
                return Err(DuckxError::PartitionBoundsInvalid {
                    reason: "partition_num must be between 2 and 64 (use 1 or omit for unpartitioned)",
                });
            }
            crate::config::validate_identifier(col)?;
            let bounds = match (args.partition_min, args.partition_max) {
                (None, None) => None,
                (Some(lo), Some(hi)) if lo <= hi => Some((lo, hi)),
                (Some(_), Some(_)) => {
                    return Err(DuckxError::PartitionBoundsInvalid {
                        reason: "partition_min must be <= partition_max",
                    });
                }
                _ => {
                    return Err(DuckxError::PartitionBoundsInvalid {
                        reason: "partition_min and partition_max must both be provided or both omitted",
                    });
                }
            };
            Ok(PartitionSpec::Parallel { column: col.clone(), num: num as u32, bounds })
        }
    }
}
```

- [ ] **Step 4: Wire into `lib.rs`**

Add `pub mod partition;` to `src/lib.rs`.

- [ ] **Step 5: Run tests — expect pass**

Run: `cargo test --test partition_validation`

Expected: PASS, all five tests.

- [ ] **Step 6: Commit**

```bash
git add src/partition.rs src/lib.rs tests/unit/partition_validation.rs
git commit -m "feat(partition): partition arg validation with PartitionSpec output"
```

---

## Task 9: Pipeline — `pipeline.rs`

> Depends on Tasks 5 (config) and 7 (types). Tested with a real Postgres via testcontainers.

**Files:**
- Create: `src/pipeline.rs`
- Create: `tests/integration_pg/pipeline_smoke.rs`
- Modify: `src/lib.rs` — add `pub mod pipeline;`

- [ ] **Step 1: Write the smoke test**

Write `tests/integration_pg/pipeline_smoke.rs`:

```rust
//! Spins up Postgres in Docker, seeds a small table, and runs the pipeline
//! end-to-end without DuckDB in the loop.

use arrow::array::Int32Array;
use duckx::config::{Config, SslMode};
use duckx::partition::PartitionSpec;
use duckx::pipeline::{discover_bounds, run_pipeline};
use secrecy::SecretString;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::postgres::Postgres;

fn seeded_pg() -> (testcontainers::Container<Postgres>, Config) {
    let pg = Postgres::default().start().expect("postgres start");
    let port = pg.get_host_port_ipv4(5432).unwrap();
    let cfg = Config {
        host: "127.0.0.1".into(),
        port,
        user: "postgres".into(),
        password: SecretString::new("postgres".to_string().into()),
        database: "postgres".into(),
        sslmode: SslMode::Disable, // testcontainers Postgres has no TLS
    };
    let conn_str = cfg.to_postgres_dsn();
    let mut client = postgres::Client::connect(&conn_str, postgres::NoTls).unwrap();
    client.batch_execute(
        "CREATE TABLE t (id INT PRIMARY KEY, name TEXT);
         INSERT INTO t (id, name) SELECT g, 'row-' || g FROM generate_series(1, 100) g;",
    ).unwrap();
    (pg, cfg)
}

#[test]
fn unpartitioned_scan_returns_all_rows() {
    let (_pg, cfg) = seeded_pg();
    let batches: Vec<_> = run_pipeline(&cfg, "SELECT id, name FROM t ORDER BY id", &PartitionSpec::Single)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 100);
    let first_ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(first_ids.value(0), 1);
}

#[test]
fn partitioned_scan_returns_same_multiset() {
    let (_pg, cfg) = seeded_pg();
    let unp: Vec<i32> = run_pipeline(&cfg, "SELECT id FROM t", &PartitionSpec::Single)
        .unwrap()
        .flat_map(|b| {
            let b = b.unwrap();
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap().clone();
            (0..arr.len()).map(move |i| arr.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let mut unp_sorted = unp.clone(); unp_sorted.sort();

    // Exercise discover_bounds explicitly (matches scan.rs::init flow).
    let (lo, hi) = discover_bounds(&cfg, "SELECT id FROM t", "id").unwrap();
    assert_eq!((lo, hi), (1, 100));
    let spec = PartitionSpec::Parallel {
        column: "id".into(), num: 4, bounds: Some((lo, hi)),
    };
    let part: Vec<i32> = run_pipeline(&cfg, "SELECT id FROM t", &spec)
        .unwrap()
        .flat_map(|b| {
            let b = b.unwrap();
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap().clone();
            (0..arr.len()).map(move |i| arr.value(i)).collect::<Vec<_>>()
        })
        .collect();
    let mut part_sorted = part.clone(); part_sorted.sort();
    assert_eq!(unp_sorted, part_sorted);
}
```

> **Note:** add `postgres = "0.19"` to `[dev-dependencies]` for the seed step. testcontainers waits for the container to be healthy before returning.

- [ ] **Step 2: Run the test — expect failure**

Run: `cargo test --test pipeline_smoke -- --test-threads=1`

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement `pipeline.rs`**

Write `src/pipeline.rs`:

```rust
//! connectorx → Arrow streaming pipeline.
//!
//! `run_pipeline` returns an iterator of `arrow::record_batch::RecordBatch`.
//! Deliberately ignorant of DuckDB; only consumer is `scan.rs`.
//!
//! Bound discovery for partitioned scans is a SEPARATE function
//! (`discover_bounds`) called from `scan.rs::init` exactly once per scan
//! when `PartitionSpec::Parallel { bounds: None }`. It is intentionally
//! NOT called inside `run_pipeline` so the schema-probe path (also calling
//! `run_pipeline` with `Single`) does not double-execute MIN/MAX queries.

use crate::config::{validate_identifier, Config, SslMode};
use crate::error::DuckxError;
use crate::partition::PartitionSpec;
use arrow::record_batch::RecordBatch;
use connectorx::destinations::arrow::ArrowDestination;
use connectorx::sources::postgres::{BinaryProtocol, PostgresSource};
use connectorx::sql::CXQuery;
use connectorx::transports::PostgresArrowTransport;
use postgres::Config as PgConfig;
use std::str::FromStr;
use tokio_postgres::NoTls;

pub fn run_pipeline(
    cfg: &Config,
    user_query: &str,
    spec: &PartitionSpec,
) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, DuckxError>> + Send>, DuckxError> {
    let queries: Vec<CXQuery<String>> = match spec {
        PartitionSpec::Single => vec![CXQuery::naked(user_query.to_string())],
        PartitionSpec::Parallel { column, num, bounds } => {
            // bounds MUST be Some here. Callers must run `discover_bounds`
            // and pass an explicit `Parallel { bounds: Some(_) }` spec to
            // run_pipeline. If we get None here, that's a caller bug.
            let (lo, hi) = bounds.ok_or_else(|| DuckxError::PartitionBoundsInvalid {
                reason: "internal: run_pipeline called with Parallel { bounds: None } — call discover_bounds first",
            })?;
            validate_identifier(column)?;
            partition_queries(user_query, column, *num, lo, hi)
        }
    };

    let dsn = cfg.to_postgres_dsn();
    let pg_cfg = PgConfig::from_str(&dsn).map_err(|e| DuckxError::BadDsn(e.to_string()))?;
    let nconn = queries.len();

    // Two TLS branches; connectorx is generic over `MakeTlsConnect`, so the
    // dispatcher type changes per branch — duplicate the body.
    let batches = match cfg.sslmode {
        SslMode::Disable => {
            let source = PostgresSource::<BinaryProtocol, NoTls>::new(pg_cfg, NoTls, nconn)
                .map_err(|e| DuckxError::RedshiftError(e.to_string()))?;
            let mut destination = ArrowDestination::new();
            let dispatcher = connectorx::prelude::Dispatcher::<
                _, _, PostgresArrowTransport<BinaryProtocol, NoTls>,
            >::new(source, &mut destination, &queries, None);
            dispatcher.run().map_err(|e| DuckxError::RedshiftError(e.to_string()))?;
            destination.arrow().map_err(|e| DuckxError::BatchDecode(e.to_string()))?
        }
        _ => {
            let connector = native_tls::TlsConnector::builder()
                // verify-ca / verify-full not fully supported in v1; system trust store only.
                .build()
                .map_err(|e| DuckxError::RedshiftError(format!("tls init: {e}")))?;
            let tls = postgres_native_tls::MakeTlsConnector::new(connector);
            let source = PostgresSource::<BinaryProtocol, postgres_native_tls::MakeTlsConnector>::new(
                pg_cfg, tls, nconn,
            )
            .map_err(|e| DuckxError::RedshiftError(e.to_string()))?;
            let mut destination = ArrowDestination::new();
            let dispatcher = connectorx::prelude::Dispatcher::<
                _, _, PostgresArrowTransport<BinaryProtocol, postgres_native_tls::MakeTlsConnector>,
            >::new(source, &mut destination, &queries, None);
            dispatcher.run().map_err(|e| DuckxError::RedshiftError(e.to_string()))?;
            destination.arrow().map_err(|e| DuckxError::BatchDecode(e.to_string()))?
        }
    };

    Ok(Box::new(batches.into_iter().map(Ok)))
}

/// Run a `MIN/MAX` round-trip on `column` over `user_query`. Called once
/// from `scan.rs::init` when `PartitionSpec::Parallel { bounds: None }`.
/// Honors `cfg.sslmode` via the `tokio-postgres` / `postgres-native-tls` stack.
pub fn discover_bounds(
    cfg: &Config,
    user_query: &str,
    column: &str,
) -> Result<(i64, i64), DuckxError> {
    validate_identifier(column)?;
    let dsn = cfg.to_postgres_dsn();
    let mut client = match cfg.sslmode {
        SslMode::Disable => postgres::Client::connect(&dsn, postgres::NoTls)
            .map_err(|e| DuckxError::RedshiftError(e.to_string()))?,
        _ => {
            let connector = native_tls::TlsConnector::new()
                .map_err(|e| DuckxError::RedshiftError(format!("tls init: {e}")))?;
            let tls = postgres_native_tls::MakeTlsConnector::new(connector);
            postgres::Client::connect(&dsn, tls)
                .map_err(|e| DuckxError::RedshiftError(e.to_string()))?
        }
    };
    let sql = format!(
        "SELECT MIN(\"{column}\"), MAX(\"{column}\") FROM ({user_query}) AS __duckx_bounds"
    );
    let row = client
        .query_one(&sql, &[])
        .map_err(|e| DuckxError::RedshiftError(e.to_string()))?;
    let lo: i64 = row.get(0);
    let hi: i64 = row.get(1);
    Ok((lo, hi))
}

fn partition_queries(query: &str, column: &str, num: u32, lo: i64, hi: i64) -> Vec<CXQuery<String>> {
    let span = (hi - lo + 1).max(1);
    let chunk = (span as f64 / num as f64).ceil() as i64;
    (0..num as i64)
        .map(|i| {
            let start = lo + i * chunk;
            let end = (start + chunk - 1).min(hi);
            // Column was identifier-validated above; double-quote for safe
            // mixed-case identifier handling.
            CXQuery::naked(format!(
                "SELECT * FROM ({query}) AS __duckx_part WHERE \"{column}\" BETWEEN {start} AND {end}"
            ))
        })
        .collect()
}
```

> Add `native-tls = "0.2"` and `postgres-native-tls = "0.5"` to `[dependencies]` for the TLS path.
>
> Note on the dispatcher: connectorx's API has shifted across versions. The import paths above target connectorx 0.4. If a newer version restructures `prelude::Dispatcher`, follow its `examples/postgres_to_arrow.rs` and update accordingly — do not invent APIs.

- [ ] **Step 4: Wire into `lib.rs`**

Add `pub mod pipeline;` to `src/lib.rs`.

- [ ] **Step 5: Run the test — expect pass**

Run: `cargo test --test pipeline_smoke -- --test-threads=1`

Expected: PASS, both tests.

- [ ] **Step 6: Commit**

```bash
git add src/pipeline.rs src/lib.rs tests/integration_pg/pipeline_smoke.rs Cargo.toml
git commit -m "feat(pipeline): connectorx → Arrow streaming with partitioned reads"
```

---

## Task 10: Table function VTab — `scan.rs`

> Depends on Tasks 4–9.

**Files:**
- Create: `src/scan.rs`
- Modify: `src/lib.rs` — add `pub mod scan;`

This task wires bind / init / func and is best validated by the lib-level integration test in Task 11. We add focused unit tests on argument parsing.

**Files:**
- Create: `src/scan.rs`
- Create: `tests/unit/scan_args.rs`
- Modify: `src/lib.rs` — add `pub mod scan;`

- [ ] **Step 1: Write arg-parsing unit test**

Write `tests/unit/scan_args.rs`:

```rust
use duckx::partition::PartitionArgs;
use duckx::scan::parse_named_args;

#[test]
fn parses_supported_named_args() {
    let pairs = vec![
        ("secret".to_string(),       "prod".to_string()),
        ("partition_on".to_string(), "id".to_string()),
        ("partition_num".to_string(),"8".to_string()),
        ("partition_min".to_string(),"0".to_string()),
        ("partition_max".to_string(),"99".to_string()),
    ];
    let parsed = parse_named_args(&pairs).unwrap();
    assert_eq!(parsed.secret.as_deref(), Some("prod"));
    let want = PartitionArgs {
        partition_on:  Some("id".into()),
        partition_num: Some(8),
        partition_min: Some(0),
        partition_max: Some(99),
    };
    assert_eq!(format!("{:?}", parsed.partition), format!("{want:?}"));
}

#[test]
fn unknown_arg_errors() {
    let pairs = vec![("yolo".to_string(), "true".to_string())];
    assert!(parse_named_args(&pairs).is_err());
}
```

- [ ] **Step 2: Run the test — expect failure**

Run: `cargo test --test scan_args`

Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement `scan.rs`**

Write `src/scan.rs`:

```rust
//! `redshift_scan` table function.
//!
//! Bind: parse args, resolve auth, do schema discovery via a `LIMIT 0` query.
//! Init: build the connectorx pipeline; stash the batch iterator on InitData.
//! Func: pull next batch; copy into the DuckDB DataChunk via Arrow C-data.

use crate::config::{resolve_from_env, Config};
use crate::error::DuckxError;
use crate::partition::{validate, PartitionArgs, PartitionSpec};
use crate::pipeline::{discover_bounds, run_pipeline};
use crate::secret::lookup_secret;
use crate::types::arrow_to_duckdb;

use arrow::record_batch::RecordBatch;
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab};
use duckdb::Connection;

#[derive(Debug, Default)]
pub struct ParsedArgs {
    pub secret: Option<String>,
    pub partition: PartitionArgs,
}

pub fn parse_named_args(pairs: &[(String, String)]) -> Result<ParsedArgs, DuckxError> {
    let mut out = ParsedArgs::default();
    for (k, v) in pairs {
        match k.as_str() {
            "secret"        => out.secret = Some(v.clone()),
            "partition_on"  => out.partition.partition_on = Some(v.clone()),
            "partition_num" => out.partition.partition_num = Some(parse_i64(v, k)?),
            "partition_min" => out.partition.partition_min = Some(parse_i64(v, k)?),
            "partition_max" => out.partition.partition_max = Some(parse_i64(v, k)?),
            other => return Err(DuckxError::BadDsn(format!("unknown named arg: {other}"))),
        }
    }
    Ok(out)
}

fn parse_i64(s: &str, name: &str) -> Result<i64, DuckxError> {
    s.parse::<i64>()
        .map_err(|_| DuckxError::BadDsn(format!("{name} must be an integer, got {s:?}")))
}

#[repr(C)]
pub struct ScanBindData {
    cfg: Config,
    query: String,
    spec: PartitionSpec,
    schema: arrow::datatypes::SchemaRef,
}
impl Free for ScanBindData {}

#[repr(C)]
pub struct ScanInitData {
    iter: Option<Box<dyn Iterator<Item = Result<RecordBatch, DuckxError>> + Send>>,
}
impl Free for ScanInitData {}

pub struct RedshiftScanVTab;

impl VTab for RedshiftScanVTab {
    type InitData = ScanInitData;
    type BindData = ScanBindData;

    fn bind(bind: &BindInfo, data: *mut ScanBindData) -> Result<(), Box<dyn std::error::Error>> {
        // FFI boundary: panics in our code MUST become DuckxErrors, never
        // unwind across the C ABI.
        crate::error::panic_boundary("redshift_scan::bind", || {
            let query = bind.get_parameter(0).to_string();
            let pairs: Vec<(String, String)> = (0..bind.num_named_parameters())
                .map(|i| (bind.named_parameter_name(i).into(), bind.named_parameter(i).to_string()))
                .collect();
            let parsed = parse_named_args(&pairs)?;

            let cfg = match parsed.secret {
                Some(name) => lookup_secret(bind.connection(), &name)?,
                None => resolve_from_env()?,
            };
            let spec = validate(&parsed.partition)?;

            // Schema discovery: LIMIT 0 round-trip on a Single-spec pipeline.
            let probe_query = format!("SELECT * FROM ({query}) AS __duckx_probe LIMIT 0");
            let mut probe_iter = run_pipeline(&cfg, &probe_query, &PartitionSpec::Single)?;
            let probe_batch = probe_iter.next().ok_or_else(|| {
                DuckxError::BatchDecode("schema probe returned no batches".into())
            })??;
            let schema = probe_batch.schema();

            for field in schema.fields() {
                let lt = arrow_to_duckdb(field.name(), field.data_type())?;
                bind.add_result_column(field.name(), lt);
            }

            unsafe {
                std::ptr::write(data, ScanBindData { cfg, query, spec, schema });
            }
            Ok(())
        })
        .map_err(Into::into)
    }

    fn init(init: &InitInfo, data: *mut ScanInitData) -> Result<(), Box<dyn std::error::Error>> {
        crate::error::panic_boundary("redshift_scan::init", || {
            let bind = unsafe { &*init.get_bind_data::<ScanBindData>() };

            // Resolve any pending bound discovery exactly once, here.
            let final_spec = match &bind.spec {
                PartitionSpec::Parallel { column, num, bounds: None } => {
                    let (lo, hi) = discover_bounds(&bind.cfg, &bind.query, column)?;
                    PartitionSpec::Parallel {
                        column: column.clone(),
                        num: *num,
                        bounds: Some((lo, hi)),
                    }
                }
                other => other.clone(),
            };

            let iter = run_pipeline(&bind.cfg, &bind.query, &final_spec)?;
            unsafe {
                std::ptr::write(data, ScanInitData { iter: Some(iter) });
            }
            Ok(())
        })
        .map_err(Into::into)
    }

    fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        // `output` is a &mut so we can't move it into the closure; capture
        // its raw pointer and re-borrow inside, also UnwindSafe.
        let output_ptr = output as *mut DataChunkHandle;
        crate::error::panic_boundary("redshift_scan::func", move || {
            let output = unsafe { &mut *output_ptr };
            let init = unsafe { &mut *func.get_init_data::<ScanInitData>() };
            let next = match init.iter.as_mut().and_then(|it| it.next()) {
                None => { output.set_len(0); return Ok(()); }
                Some(batch) => batch?,
            };
            copy_batch_into_chunk(&next, output)?;
            Ok(())
        })
        .map_err(Into::into)
    }
}

/// Copy an Arrow `RecordBatch` into a DuckDB `DataChunkHandle` via the
/// C-data interface.
///
/// Implementation strategy is determined by Task 2 (hello-world spike):
/// - If `DataChunkHandle::vector(i).set_arrow(...)` exists in the pinned
///   duckdb-rs version, use it (zero-copy fast path).
/// - Otherwise, dispatch on `LogicalTypeId` and copy column values
///   element-wise. The fallback adds ~200 LOC and ~5–15 % overhead.
///
/// Task 2's spike README records which path is available; this function
/// must use that recorded path. If `set_arrow` exists, the body below
/// works as written; if not, replace with the element-wise dispatcher
/// described in Task 2's README.
fn copy_batch_into_chunk(batch: &RecordBatch, chunk: &mut DataChunkHandle) -> Result<(), DuckxError> {
    let n_rows = batch.num_rows();
    for (i, col) in batch.columns().iter().enumerate() {
        chunk
            .vector(i)
            .set_arrow(col.as_ref())
            .map_err(|e| DuckxError::BatchDecode(e.to_string()))?;
    }
    chunk.set_len(n_rows);
    Ok(())
}

pub fn register(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    use duckdb::vtab::TableFunction;
    let func = TableFunction::default()
        .set_name("redshift_scan")
        .add_parameter(LogicalTypeHandle::from(LogicalTypeId::Varchar))
        .add_named_parameter("secret",               LogicalTypeHandle::from(LogicalTypeId::Varchar))
        .add_named_parameter("partition_on",         LogicalTypeHandle::from(LogicalTypeId::Varchar))
        .add_named_parameter("partition_num",        LogicalTypeHandle::from(LogicalTypeId::Bigint))
        .add_named_parameter("partition_min",        LogicalTypeHandle::from(LogicalTypeId::Bigint))
        .add_named_parameter("partition_max",        LogicalTypeHandle::from(LogicalTypeId::Bigint))
        .add_named_parameter("statement_timeout_ms", LogicalTypeHandle::from(LogicalTypeId::Bigint))
        .supports_pushdown(false);
    conn.register_table_function::<RedshiftScanVTab>(func)?;
    Ok(())
}
```

> The exact `TableFunction` builder method names (`set_name`, `add_named_parameter`) target duckdb-rs 1.4. Adjust if newer.

- [ ] **Step 4: Wire into `lib.rs`**

Add `pub mod scan;` to `src/lib.rs`.

- [ ] **Step 5: Run the unit test — expect pass**

Run: `cargo test --test scan_args`

Expected: PASS.

- [ ] **Step 6: Verify the crate still builds end-to-end**

Run: `cargo build --release`

Expected: clean build. Type errors here usually mean `duckdb-rs` API drift — fix by checking docs.rs for the pinned version, never by inventing methods.

- [ ] **Step 7: Commit**

```bash
git add src/scan.rs src/lib.rs tests/unit/scan_args.rs
git commit -m "feat(scan): redshift_scan VTab with arg parsing, schema discovery, streaming"
```

---

## Task 11: Extension entry — `lib.rs`

> Depends on Task 10.

**Files:**
- Modify: `src/lib.rs` — add the `extension_entrypoint` C entry point.
- Create: `tests/integration_pg/load_and_scan.rs` — end-to-end load + run.

- [ ] **Step 1: Wire the entry point**

Edit `src/lib.rs`:

```rust
//! duckx — DuckDB extension that queries Amazon Redshift via connectorx.

pub mod config;
pub mod error;
pub mod partition;
pub mod pipeline;
pub mod scan;
pub mod secret;
pub mod types;

pub const DUCKDB_VERSION: &str = env!("DUCKX_DUCKDB_VERSION");

use duckdb::{Connection, Result};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;

#[duckdb_entrypoint_c_api]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn std::error::Error>> {
    error::panic_boundary("extension_entrypoint", || {
        verify_duckdb_version(&con).map_err(|e| {
            error::DuckxError::RedshiftError(format!("version check failed: {e}"))
        })?;
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_env("DUCKX_LOG")
                    .unwrap_or_else(|_| "off".into()),
            )
            .try_init()
            .ok();
        secret::register_redshift_secret_type(&con)?;
        scan::register(&con).map_err(|e| {
            error::DuckxError::RedshiftError(format!("scan::register: {e}"))
        })?;
        Ok(())
    })
    .map_err(Into::into)
}

/// Hard-fail if the running DuckDB's `version()` doesn't match the
/// version this binary was compiled against.
///
/// Only the `MAJOR.MINOR` prefix is compared — patch releases keep ABI
/// compatibility, but minor releases can break the extension ABI.
fn verify_duckdb_version(con: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    let runtime: String = con.query_row("SELECT version()", [], |r| r.get(0))?;
    let runtime_mm = runtime
        .trim_start_matches('v')
        .split('.')
        .take(2)
        .collect::<Vec<_>>()
        .join(".");
    let pinned_mm = DUCKDB_VERSION
        .split('.')
        .take(2)
        .collect::<Vec<_>>()
        .join(".");
    if runtime_mm != pinned_mm {
        return Err(format!(
            "duckx extension was built for DuckDB {pinned_mm}.x, but loaded into {runtime}. \
             Rebuild duckx against this DuckDB version, or use a matching DuckDB binary."
        )
        .into());
    }
    Ok(())
}
```

- [ ] **Step 2: Write end-to-end load + scan test**

Write `tests/integration_pg/load_and_scan.rs`:

```rust
//! Builds the extension, launches Postgres in Docker, loads the
//! `.duckdb_extension` into a real DuckDB binary on PATH, and runs
//! `SELECT count(*) FROM redshift_scan('SELECT * FROM t')`.

use std::process::Command;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::postgres::Postgres;

fn build_extension() -> std::path::PathBuf {
    let status = Command::new("cargo")
        .args(["build", "--release"])
        .status()
        .unwrap();
    assert!(status.success());
    let status = Command::new("cargo")
        .args(["xtask", "package"])
        .status()
        .unwrap();
    assert!(status.success());
    std::fs::canonicalize("target/release/redshift.duckdb_extension").unwrap()
}

#[test]
fn load_and_count_via_env_auth() {
    let pg = Postgres::default().start().unwrap();
    let port = pg.get_host_port_ipv4(5432).unwrap();
    {
        let dsn = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
        let mut client = postgres::Client::connect(&dsn, postgres::NoTls).unwrap();
        client.batch_execute(
            "CREATE TABLE t (id INT PRIMARY KEY, name TEXT);
             INSERT INTO t (id, name) SELECT g, 'row-' || g FROM generate_series(1, 1000) g;",
        ).unwrap();
    }
    std::env::set_var("REDSHIFT_HOST", "127.0.0.1");
    std::env::set_var("REDSHIFT_PORT", port.to_string());
    std::env::set_var("REDSHIFT_USER", "postgres");
    std::env::set_var("REDSHIFT_PASSWORD", "postgres");
    std::env::set_var("REDSHIFT_DATABASE", "postgres");
    std::env::set_var("REDSHIFT_SSLMODE", "disable"); // testcontainers Postgres has no TLS

    let ext = build_extension();
    let sql = format!(
        "SET allow_unsigned_extensions = true; \
         LOAD '{}'; \
         SELECT count(*) FROM redshift_scan('SELECT id, name FROM t');",
        ext.display()
    );
    let out = Command::new("duckdb").args(["-c", &sql]).output().unwrap();
    assert!(out.status.success(), "duckdb stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("1000"), "stdout: {stdout}");
}
```

- [ ] **Step 3: Run the test — expect pass**

Run: `cargo test --test load_and_scan -- --test-threads=1`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/lib.rs tests/integration_pg/load_and_scan.rs
git commit -m "feat(lib): wire extension entrypoint registering REDSHIFT secret + redshift_scan"
```

---

## Task 12: Postgres integration test suite

> Depends on Task 11. Runs in parallel with Tasks 13 and 14.

**Files (single-binary integration test layout, required for `mod common;` to work):**
- Create: `tests/integration_pg/main.rs` — declares submodules; cargo treats this whole directory as one test binary.
- Create: `tests/integration_pg/common/mod.rs`
- Create: `tests/integration_pg/wide_types.rs`
- Create: `tests/integration_pg/auth_paths.rs`
- Create: `tests/integration_pg/partition_equivalence.rs`
- Create: `tests/integration_pg/connection_drop.rs` (renamed from "cancellation"; see Step 5)
- Modify: `Cargo.toml` — under `[[test]]` declare `name = "integration_pg"`, `path = "tests/integration_pg/main.rs"`.

- [ ] **Step 0: Declare the integration test as a single binary**

Add to `Cargo.toml`:

```toml
[[test]]
name = "integration_pg"
path = "tests/integration_pg/main.rs"
```

Write `tests/integration_pg/main.rs`:

```rust
mod common;
mod wide_types;
mod auth_paths;
mod partition_equivalence;
mod connection_drop;
```

This makes the directory a single test crate so `mod common;` resolves consistently across files. Without this, Cargo treats each `.rs` file as its own binary and `mod common;` fails.

- [ ] **Step 1: Factor out the test harness**

Write `tests/integration_pg/common/mod.rs`:

```rust
use std::process::Command;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::postgres::Postgres;

pub struct Harness {
    pub _container: testcontainers::Container<Postgres>,
    pub port: u16,
    pub dsn: String,
}

pub fn start() -> Harness {
    let c = Postgres::default().start().unwrap();
    let port = c.get_host_port_ipv4(5432).unwrap();
    let dsn = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    Harness { _container: c, port, dsn }
}

pub fn duckdb(sql: &str) -> std::process::Output {
    Command::new("duckdb").args(["-c", sql]).output().unwrap()
}

pub fn extension_path() -> std::path::PathBuf {
    std::fs::canonicalize("target/release/redshift.duckdb_extension").unwrap()
}
```

- [ ] **Step 2: Wide-types fixture test**

Write `tests/integration_pg/wide_types.rs`:

```rust
use crate::common::*;

#[test]
fn every_supported_type_round_trips() {
    let h = start();
    let mut c = postgres::Client::connect(&h.dsn, postgres::NoTls).unwrap();
    c.batch_execute(
        "CREATE TABLE w (
            b BOOL, i2 SMALLINT, i4 INT, i8 BIGINT,
            f4 REAL, f8 DOUBLE PRECISION,
            s VARCHAR, txt TEXT, bin BYTEA,
            d DATE, t TIME, ts TIMESTAMP, tstz TIMESTAMPTZ,
            dec NUMERIC(18,4)
         );
         INSERT INTO w VALUES (
            true, 1, 2, 3,
            1.5, 2.5,
            'a', 'b', '\\xDEADBEEF',
            DATE '2026-01-01', TIME '12:00:00',
            TIMESTAMP '2026-01-01 00:00:00',
            TIMESTAMPTZ '2026-01-01 00:00:00+00',
            123.4500
         );",
    ).unwrap();

    std::env::set_var("REDSHIFT_HOST", "127.0.0.1");
    std::env::set_var("REDSHIFT_PORT", h.port.to_string());
    std::env::set_var("REDSHIFT_USER", "postgres");
    std::env::set_var("REDSHIFT_PASSWORD", "postgres");
    std::env::set_var("REDSHIFT_DATABASE", "postgres");

    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT b,i2,i4,i8,f4,f8,s,txt,bin,d,t,ts,tstz,dec FROM redshift_scan('SELECT * FROM w');",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let s = String::from_utf8_lossy(&out.stdout);
    for needle in ["true", "1.5", "2.5", "123.4500", "2026-01-01", "DEADBEEF"] {
        assert!(s.to_lowercase().contains(&needle.to_lowercase()), "missing {needle} in: {s}");
    }
}
```

- [ ] **Step 3: Auth-path tests**

Write `tests/integration_pg/auth_paths.rs`:

```rust
use crate::common::*;

fn seed(h: &Harness) {
    let mut c = postgres::Client::connect(&h.dsn, postgres::NoTls).unwrap();
    c.batch_execute("CREATE TABLE k (id INT); INSERT INTO k VALUES (1),(2),(3);").unwrap();
}

#[test]
fn secret_path_works() {
    let h = start();
    seed(&h);
    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         CREATE SECRET s (TYPE REDSHIFT, HOST '127.0.0.1', PORT '{p}', USER 'postgres', PASSWORD 'postgres', DATABASE 'postgres', SSLMODE 'disable'); \
         SELECT count(*) FROM redshift_scan('SELECT * FROM k', secret => 's');",
        extension_path().display(), p = h.port
    );
    let out = duckdb(&sql);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains('3'));
}

#[test]
fn missing_credentials_lists_fields() {
    for k in ["REDSHIFT_HOST","REDSHIFT_PORT","REDSHIFT_USER","REDSHIFT_PASSWORD","REDSHIFT_DATABASE"] {
        std::env::remove_var(k);
    }
    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT * FROM redshift_scan('SELECT 1');",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    for f in ["host","user","password","database"] {
        assert!(err.contains(f), "missing {f} in: {err}");
    }
}
```

- [ ] **Step 4: Partition equivalence test**

Write `tests/integration_pg/partition_equivalence.rs`:

```rust
use crate::common::*;

#[test]
fn partitioned_count_matches_unpartitioned() {
    let h = start();
    let mut c = postgres::Client::connect(&h.dsn, postgres::NoTls).unwrap();
    c.batch_execute(
        "CREATE TABLE big (id INT PRIMARY KEY, payload TEXT);
         INSERT INTO big SELECT g, 'x' FROM generate_series(1, 100000) g;",
    ).unwrap();

    std::env::set_var("REDSHIFT_HOST", "127.0.0.1");
    std::env::set_var("REDSHIFT_PORT", h.port.to_string());
    std::env::set_var("REDSHIFT_USER", "postgres");
    std::env::set_var("REDSHIFT_PASSWORD", "postgres");
    std::env::set_var("REDSHIFT_DATABASE", "postgres");

    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT (SELECT count(*) FROM redshift_scan('SELECT * FROM big')) = \
                (SELECT count(*) FROM redshift_scan('SELECT * FROM big', partition_on => 'id', partition_num => 8));",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let s = String::from_utf8_lossy(&out.stdout).to_lowercase();
    assert!(s.contains("true"), "expected true, got: {s}");
}
```

- [ ] **Step 4b: Bind-time SQL error test**

Add a small test verifying that a syntactically invalid user query surfaces a clean `RedshiftError` from the bind-time `LIMIT 0` probe (not a panic, not a silent empty result). Append to `tests/integration_pg/auth_paths.rs` or a new file `tests/integration_pg/bind_errors.rs` (and add it to `main.rs`):

```rust
use crate::common::*;

#[test]
fn bind_time_sql_error_surfaces_cleanly() {
    let h = start();
    std::env::set_var("REDSHIFT_HOST", "127.0.0.1");
    std::env::set_var("REDSHIFT_PORT", h.port.to_string());
    std::env::set_var("REDSHIFT_USER", "postgres");
    std::env::set_var("REDSHIFT_PASSWORD", "postgres");
    std::env::set_var("REDSHIFT_DATABASE", "postgres");
    std::env::set_var("REDSHIFT_SSLMODE", "disable");

    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT * FROM redshift_scan('SELECT * FROM table_that_does_not_exist');",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(err.contains("redshift error") || err.contains("does not exist") || err.contains("relation"),
        "stderr: {err}");
}
```

If you place this in a new file, also add `mod bind_errors;` to `tests/integration_pg/main.rs`.

- [ ] **Step 5: Connection-drop test**

Renamed from "cancellation" — this test validates that a dropped Postgres backend surfaces a clean `RedshiftError`. True user-initiated cancellation (SIGINT mid-stream) is out of scope for v1 because cancellation requires DuckDB-side cooperation we don't yet wire.

Write `tests/integration_pg/connection_drop.rs`:

```rust
use crate::common::*;

/// Tests that a connection failure surfaces as `RedshiftError` (not a
/// panic, not a hang). We point at port 1 (well-known: nothing
/// listens there) instead of dropping a testcontainer, because TIME_WAIT
/// recycling can leave the dropped container's port transiently
/// reachable on slow CI runners.
#[test]
fn unreachable_host_surfaces_redshift_error() {
    std::env::set_var("REDSHIFT_HOST", "127.0.0.1");
    std::env::set_var("REDSHIFT_PORT", "1"); // nothing listens here
    std::env::set_var("REDSHIFT_USER", "postgres");
    std::env::set_var("REDSHIFT_PASSWORD", "postgres");
    std::env::set_var("REDSHIFT_DATABASE", "postgres");
    std::env::set_var("REDSHIFT_SSLMODE", "disable");

    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT * FROM redshift_scan('SELECT 1');",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(
        err.contains("redshift error") || err.contains("connect") || err.contains("refused"),
        "stderr: {err}"
    );
}
```

- [ ] **Step 6: Run the suite**

Run: `cargo test --release --tests -- --test-threads=1`

Expected: all integration tests PASS. (Tests run serially because they share env vars and DuckDB singleton.)

- [ ] **Step 7: Commit**

```bash
git add tests/integration_pg
git commit -m "test(integration): full Postgres-backed end-to-end suite"
```

---

## Task 13: Real-Redshift gated test suite

> Runs in parallel with Tasks 12 and 14.

**Files:**
- Create: `tests/integration_rs/redshift_only.rs`
- Modify: `Cargo.toml` (add the feature gate's test target)

- [ ] **Step 1: Wire the feature-gated test**

Write `tests/integration_rs/redshift_only.rs`:

```rust
//! Runs only when `--features redshift-integration` is on AND
//! `REDSHIFT_TEST_DSN_*` env vars are set. Hits a real Redshift cluster.

#![cfg(feature = "redshift-integration")]

use std::process::Command;

fn dsn_env_set() -> bool {
    ["REDSHIFT_TEST_HOST","REDSHIFT_TEST_USER","REDSHIFT_TEST_PASSWORD","REDSHIFT_TEST_DATABASE"]
        .iter().all(|k| std::env::var(k).is_ok())
}

fn extension_path() -> std::path::PathBuf {
    std::fs::canonicalize("target/release/redshift.duckdb_extension").unwrap()
}

fn duckdb(sql: &str) -> std::process::Output {
    Command::new("duckdb").args(["-c", sql]).output().unwrap()
}

fn export_creds() {
    for (env_in, env_out) in [
        ("REDSHIFT_TEST_HOST",     "REDSHIFT_HOST"),
        ("REDSHIFT_TEST_USER",     "REDSHIFT_USER"),
        ("REDSHIFT_TEST_PASSWORD", "REDSHIFT_PASSWORD"),
        ("REDSHIFT_TEST_DATABASE", "REDSHIFT_DATABASE"),
    ] {
        std::env::set_var(env_out, std::env::var(env_in).unwrap());
    }
    std::env::set_var("REDSHIFT_PORT", std::env::var("REDSHIFT_TEST_PORT").unwrap_or_else(|_| "5439".into()));
}

#[test]
fn select_one_against_real_cluster() {
    if !dsn_env_set() { eprintln!("skipping: REDSHIFT_TEST_* not set"); return; }
    export_creds();
    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT * FROM redshift_scan('SELECT 1 AS x');",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
}

#[test]
fn super_type_errors_cleanly() {
    // We don't assert on the exact substring because connectorx may
    // surface unknown OIDs as either "unsupported type" (our wrapper) or
    // a lower-level protocol error. Both are acceptable; the contract is
    // that the failure is clean (no panic, exit code != 0, stderr not empty).
    if !dsn_env_set() { return; }
    export_creds();
    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT * FROM redshift_scan('SELECT JSON_PARSE(''{{\"a\":1}}'') AS s');",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(!err.is_empty(), "expected non-empty stderr");
}

#[test]
fn partitioned_read_across_compute_nodes() {
    if !dsn_env_set() { return; }
    export_creds();
    let table = std::env::var("REDSHIFT_TEST_PARTITION_TABLE")
        .unwrap_or_else(|_| "public.duckx_partition_test".into());
    let key = std::env::var("REDSHIFT_TEST_PARTITION_COLUMN").unwrap_or_else(|_| "id".into());
    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT count(*) FROM redshift_scan('SELECT * FROM {table}', partition_on => '{key}', partition_num => 8);",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
}
```

- [ ] **Step 2: Document the env contract and fixture SQL**

Append to `README.md` (created in Task 14; if README is still the original one-liner, replace that with this section now and Task 14 will fill in the rest):

```markdown
## Real-Redshift integration tests

These tests are gated behind `--features redshift-integration` and read:

- `REDSHIFT_TEST_HOST`, `REDSHIFT_TEST_PORT` (default `5439`)
- `REDSHIFT_TEST_USER`, `REDSHIFT_TEST_PASSWORD`
- `REDSHIFT_TEST_DATABASE`
- `REDSHIFT_TEST_PARTITION_TABLE` (default `public.duckx_partition_test`)
- `REDSHIFT_TEST_PARTITION_COLUMN` (default `id`)

### One-time fixture setup

Schema (run once against the staging cluster):

    CREATE TABLE IF NOT EXISTS public.duckx_partition_test (
        id   BIGINT NOT NULL,
        name VARCHAR(64)
    )
    DISTKEY(id) SORTKEY(id);

    GRANT SELECT ON public.duckx_partition_test TO <test_user>;

Seed it with at least 100 k rows, by whatever method is convenient on your
cluster. Two practical options:

1. **`COPY` from S3** (recommended for repeatability — generate a Parquet
   file once and `COPY` it on every fresh test cluster):

       COPY public.duckx_partition_test
         FROM 's3://<your-bucket>/duckx_fixture/data.parquet'
         IAM_ROLE '<role-arn>'
         FORMAT AS PARQUET;

2. **Inline `INSERT`** for small smoke fixtures:

       INSERT INTO public.duckx_partition_test (id, name)
       VALUES (1, 'row-1'), (2, 'row-2'), /* … */ (100000, 'row-100000');

The gated test only requires that `SELECT count(*)` returns ≥ 100 k. The
exact row count is not asserted because the test reads the count via
`redshift_scan` itself and compares the partitioned vs unpartitioned
result. Do NOT use `stl_scan` — it is a system view containing query-plan
rows and its row count is not deterministic.

### Teardown (only if removing the cluster)

    DROP TABLE IF EXISTS public.duckx_partition_test;

### Running

Run before tagging a release:

    cargo test --release --features redshift-integration -- --test-threads=1
```

- [ ] **Step 3: Verify the gated suite compiles without the feature**

Run: `cargo test --no-run`

Expected: the `redshift_only` tests are excluded (cfg gate).

- [ ] **Step 4: Commit**

```bash
git add tests/integration_rs README.md
git commit -m "test: add gated real-Redshift integration suite"
```

---

## Task 14: README, CI matrix, release docs

> Runs in parallel with Tasks 12 and 13.

**Files:**
- Modify: `README.md` (replace existing one-liner; preserve any section Task 13 added)
- Modify: `.github/workflows/ci.yml` — add the macos-arm64 + linux-arm64 slots (via `cargo-zigbuild`), release-tag artifact upload
- Create: `.github/workflows/release.yml` — builds + uploads the `.duckdb_extension` artifact per platform on tag push
- Create: `LICENSE` (Apache-2.0 text)
- Create: `RELEASE_CHECKLIST.md`

- [ ] **Step 0: Add the LICENSE file**

Run:

```bash
curl -L -o LICENSE https://www.apache.org/licenses/LICENSE-2.0.txt
```

Verify the file is non-empty and starts with `Apache License`. Update the copyright placeholder at the bottom (`[yyyy] [name of copyright owner]`) to `2026 duckx contributors`.

- [ ] **Step 1: Write `README.md`**

```markdown
# duckx — DuckDB extension for Amazon Redshift

`duckx` lets you query Amazon Redshift directly from a DuckDB SQL session
via a `redshift_scan` table function. Built in Rust on top of `connectorx`.

## Quick start

    SET allow_unsigned_extensions = true;
    LOAD '/path/to/redshift.duckdb_extension';

    CREATE SECRET prod (
        TYPE REDSHIFT,
        HOST 'cluster.xxx.us-east-1.redshift.amazonaws.com',
        PORT 5439,
        USER 'analyst',
        PASSWORD 'hunter2',
        DATABASE 'analytics'
    );

    SELECT count(*) FROM redshift_scan(
        'SELECT * FROM public.sales WHERE dt > current_date - 7',
        secret => 'prod'
    );

## Authentication

Resolution order on each call:

1. Explicit `secret => 'name'` argument (DuckDB Secrets Manager).
2. Environment variables: `REDSHIFT_HOST`, `REDSHIFT_PORT` (default `5439`),
   `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_DATABASE`.
3. Otherwise: error listing every missing field.

## Partitioned parallel reads

    SELECT * FROM redshift_scan(
        'SELECT * FROM public.events',
        secret => 'prod',
        partition_on  => 'event_id',
        partition_num => 8
    );

`partition_min` and `partition_max` are optional; if omitted, bounds are
auto-discovered with one extra `MIN/MAX` round-trip. `partition_on` must
be an integer column. Maximum `partition_num` is 64.

## Supported types

`BOOLEAN`, `SMALLINT`, `INTEGER`, `BIGINT`, `REAL`, `DOUBLE PRECISION`,
`VARCHAR`, `TEXT`, `BYTEA`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`,
`NUMERIC(p,s)`.

Unsupported and will error with the column name: `SUPER`, `GEOMETRY`,
`GEOGRAPHY`, `HLLSKETCH`, `VARBYTE`. For these, project them out of the
query or cast them to `VARCHAR` in Redshift.

## Build from source

Requires:
- Rust toolchain 1.82+ (`rustup show` should match `rust-toolchain.toml`).
- DuckDB CLI of the pinned version (see `.cargo/config.toml`).

    cargo build --release
    cargo xtask package
    # produces: target/release/redshift.duckdb_extension

## Logging

    DUCKX_LOG=debug duckdb -c "LOAD '...'; SELECT ..."

## Limitations (v1)

- No `ATTACH 'redshift://...'` catalog integration yet.
- No IAM-based credentials yet (use static `CREATE SECRET` for now).
- No connection pooling — every `redshift_scan` call opens a fresh set
  of connections (one per partition). Sessions running tight loops
  should batch their queries.
- Schema discovered fresh on every call — adds one `LIMIT 0` round-trip.
  Bound discovery (when `partition_on` is set without explicit bounds)
  re-executes the user query as `MIN/MAX(col) FROM (<query>) t` — pass
  explicit `partition_min` / `partition_max` for expensive queries.
- `partition_on` must be an integer column. `DATE` / hash / `VARCHAR`
  partition keys are not supported. NULL values in the partition column
  are dropped from the result (because `BETWEEN` is `NULL` for them);
  pre-filter with `WHERE col IS NOT NULL` or use the unpartitioned path
  if your data contains NULLs.
- No `statement_timeout` arg — runaway queries cannot be bounded from
  inside DuckDB. Set `statement_timeout` per-user on the cluster instead.
- No cooperative cancellation — DuckDB `Ctrl-C` does not interrupt an
  in-flight Redshift connection mid-`RecordBatch`. Connections do close
  cleanly when the scan iterator drops at end of query.
- Pre-built artifacts ship for `linux-amd64` and `macos-arm64`.
  `linux-arm64` is type-checked in CI via `cargo-zigbuild` but not
  end-to-end tested — use at your own risk or build from source.
- Best for queries returning up to tens of millions of rows. For larger
  extracts, prefer `UNLOAD ... TO 's3://...' FORMAT PARQUET` and read
  with `read_parquet` via the `httpfs` extension.

(See `docs/superpowers/specs/2026-05-05-redshift-extension-design.md` for
the design spec including future-work sections.)
```

- [ ] **Step 2: Extend CI to all platforms**

Replace `.github/workflows/ci.yml` with:

```yaml
name: ci
on:
  push: { branches: [main] }
  pull_request:

jobs:
  test:
    strategy:
      fail-fast: false
      matrix:
        include:
          - { os: ubuntu-latest, target: x86_64-unknown-linux-gnu, duckdb_asset: duckdb_cli-linux-amd64.zip }
          - { os: ubuntu-latest, target: aarch64-unknown-linux-gnu, duckdb_asset: duckdb_cli-linux-aarch64.zip, cross: zigbuild }
          - { os: macos-14,      target: aarch64-apple-darwin,     duckdb_asset: duckdb_cli-osx-universal.zip }
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: rustfmt, clippy, targets: ${{ matrix.target }} }
      - name: Install zig and cargo-zigbuild (cross targets only)
        if: matrix.cross == 'zigbuild'
        run: |
          pip install ziglang cargo-zigbuild
      - name: Install DuckDB CLI
        if: matrix.cross != 'zigbuild'
        run: |
          if [ "$RUNNER_OS" = "macOS" ]; then
            brew install duckdb
          else
            curl -L -o duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v1.4.0/${{ matrix.duckdb_asset }}"
            unzip duckdb.zip && sudo mv duckdb /usr/local/bin/
          fi
      - run: cargo fmt --all -- --check
      - name: Block todo!() in production code
        run: |
          if grep -rn 'todo!()' src/; then
            echo "todo!() not allowed in src/ (use unimplemented!() or implement)"; exit 1
          fi
      - run: cargo clippy --all-targets --target ${{ matrix.target }} -- -D warnings
      - name: Build (native)
        if: matrix.cross != 'zigbuild'
        run: cargo build --release --target ${{ matrix.target }}
      - name: Build (zigbuild cross)
        if: matrix.cross == 'zigbuild'
        run: cargo zigbuild --release --target ${{ matrix.target }}
      - run: cargo xtask package
        if: matrix.cross != 'zigbuild'
      - run: cargo test --release -- --test-threads=1
        if: matrix.cross != 'zigbuild'
```

- [ ] **Step 3: Release workflow**

Write `.github/workflows/release.yml`:

```yaml
name: release
on:
  push:
    tags: ['v*']

jobs:
  build:
    strategy:
      matrix:
        include:
          - { os: ubuntu-latest, target: x86_64-unknown-linux-gnu, name: linux-amd64 }
          - { os: macos-14,      target: aarch64-apple-darwin,     name: macos-arm64 }
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { targets: ${{ matrix.target }} }
      - run: cargo build --release --target ${{ matrix.target }}
      - run: cargo xtask package
      - uses: actions/upload-artifact@v4
        with:
          name: redshift-${{ matrix.name }}.duckdb_extension
          path: target/release/redshift.duckdb_extension
  publish:
    needs: build
    runs-on: ubuntu-latest
    permissions: { contents: write }
    steps:
      - uses: actions/download-artifact@v4
      - uses: softprops/action-gh-release@v2
        with:
          files: '**/redshift.duckdb_extension'
```

- [ ] **Step 4: Release checklist**

Write `RELEASE_CHECKLIST.md`:

```markdown
# Release Checklist

Before tagging `vN.N.N`:

- [ ] `cargo fmt --all` and `cargo clippy --all-targets -- -D warnings` clean.
- [ ] CI green on `main` (including the `todo!()` grep gate).
- [ ] Real-Redshift suite green:
      `cargo test --release --features redshift-integration -- --test-threads=1`
      with `REDSHIFT_TEST_*` env vars pointing at the staging cluster.
- [ ] Manual smoke: build, `LOAD`, `SELECT * FROM redshift_scan('SELECT 1')`.
- [ ] **Manual perf check:** time `redshift_scan` on a ≥1 M-row table at
      `partition_num=8` vs `Single`; confirm ≥ 3× speedup (spec budget).
- [ ] `README.md` "Quick start" still works verbatim against the staging cluster.
- [ ] Bump version in `Cargo.toml` (this also auto-updates the runtime
      DuckDB version check via `build.rs`).
- [ ] Tag and push: `git tag vN.N.N && git push --tags`.
- [ ] Verify `release.yml` uploaded `redshift-{linux-amd64,macos-arm64}.duckdb_extension` artifacts.
```

- [ ] **Step 5: Verify everything still passes**

Run:
```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --release -- --test-threads=1
```

Expected: all clean, all green.

- [ ] **Step 6: Commit**

```bash
git add README.md LICENSE .github/workflows RELEASE_CHECKLIST.md
git commit -m "docs+ci: README, LICENSE, multi-platform CI with zigbuild, release workflow"
```

---

## Self-Review

**Spec coverage check (against `2026-05-05-redshift-extension-design.md`):**

| Spec section / requirement | Covered by task |
|---|---|
| `redshift_scan` table function | Tasks 10, 11 |
| Auth: explicit secret | Task 6 (gated by 2.5 spike) |
| Auth: env vars + default port 5439 + sslmode | Task 5 |
| Auth: missing-field error | Tasks 5, 12 (auth_paths) |
| TLS / `sslmode` default `require` | Tasks 5, 6, 9 (TLS connector) |
| Partitioned parallel reads | Tasks 8, 9, 12 |
| `partition_num=1` → Single | Task 8 |
| `statement_timeout_ms` deferred to Future Work | Spec Future Work + Task 10 README limitation |
| Partition speedup ≥3× at `partition_num=8` (≥1M rows) | Documented in spec Performance budgets; manual measurement in RELEASE_CHECKLIST (no flaky wall-clock CI assertion) |
| SQL-injection-safe partition column | Tasks 5 (validator), 8, 9 |
| `panic_boundary` at every FFI entry | Task 4 (helper), Task 10 (bind/init/func), Task 11 (entrypoint) |
| Iterator `Send` bound matches `ScanInitData` | Task 9 (`+ Send`), Task 10 |
| `Cargo.lock` committed for reproducibility | Task 3 |
| `todo!()` blocked in `src/` by CI | Task 14 |
| DuckDB version derived from Cargo.toml at build time | Task 3 (`build.rs`) |
| Streaming, no full materialization | Task 9 (channel iterator) |
| Type mapping (incl. `Hugeint` for `UInt64`) | Task 7, 12 (wide_types) |
| Unsupported-type errors (`SUPER`, `INTERVAL`, `TIMETZ`, `OID`, …) | Tasks 7, 13 |
| `DuckxError` + redaction (keyword + URL form) | Task 4 |
| Module boundaries (config / pipeline / scan / secret separation) | Tasks 5–10 |
| `arrow` vs `arrow2` de-risk | Task 1 |
| `duckdb-rs` extension API de-risk | Task 2 |
| DuckDB Secrets API de-risk | Task 2.5 |
| `set_arrow` helper de-risk | Task 2 (verify) + Task 10 (fallback note) |
| DuckDB version pin + runtime check | Tasks 2, 3, 11 |
| Postgres CI integration tests | Tasks 9, 11, 12 |
| Real-Redshift gated suite + fixture SQL | Task 13 |
| Build / `xtask package` / `.duckdb_extension` artifact | Task 3 |
| CI matrix (linux-amd64, linux-arm64 via zigbuild, macos-arm64) | Task 14 |
| Release artifacts | Task 14 |
| LICENSE | Task 14 |
| README documenting auth + TLS + unsigned `LOAD` | Tasks 13, 14 |
| Logging via `DUCKX_LOG` | Tasks 11, 14 (README) |
| Connection-drop / mid-stream error path | Task 12 (connection_drop) |
| `LIMIT 0` schema discovery (Single, no double-execution) | Task 10 |
| Bound discovery exactly once at init | Tasks 9, 10 |
| Concurrency / connection model documented | Spec; matches Task 9 implementation |
| Future-work hooks (modules positioned for `ATTACH` / IAM / pool) | Spec only — implicitly covered by module split in Tasks 5–10 |

No gaps.

**Placeholder scan:** searched plan for `TODO`, `TBD`, `fill in`, `similar to Task`, "appropriate error handling" — placeholders only remain as documented decision points: Task 1 connectorx version (verified at run time), Task 6's two `todo!()`s explicitly to be replaced from the Task 2.5 spike output (with a test gate that catches them), Task 10's `set_arrow` body annotated with the Task 2 fallback contract. None are unresolved.

**Type consistency:**
- `Config` fields (`host`, `port: u16`, `user`, `password: SecretString`, `database`, `sslmode: SslMode`): consistent across Tasks 5, 6, 9, 10.
- `SslMode` enum: defined in Task 5, consumed in Tasks 6, 9.
- `PartitionSpec` variants (`Single` | `Parallel { column, num: u32, bounds: Option<(i64, i64)> }`): consistent in Tasks 8, 9, 10.
- `DuckxError` variants used: `MissingCredential`, `BadDsn`, `RedshiftError`, `UnsupportedType`, `PartitionBoundsInvalid`, `BatchDecode` — all match Task 4's definition.
- Function names: `resolve_from_env`, `validate_identifier`, `SslMode::parse` (Task 5); `lookup_secret`, `register_redshift_secret_type` (Task 6); `arrow_to_duckdb` (Task 7); `validate` (Task 8); `run_pipeline`, `discover_bounds` (Task 9); `parse_named_args`, `register` (Task 10); `verify_duckdb_version` (Task 11) — referenced consistently downstream.
- `ParsedArgs` struct (Task 10) replaces the earlier tuple return — every caller goes through fields, not positional access.

No inconsistencies found.
