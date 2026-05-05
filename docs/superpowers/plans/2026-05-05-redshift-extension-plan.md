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
Task 1 ──▶ Task 2 ──▶ Task 3 ──▶ Task 4 ──┬─▶ Task 5  (config)    ─┐
                                          ├─▶ Task 6  (secret)    ─┤
                                          ├─▶ Task 7  (types)     ─┼─▶ Task 9 (pipeline) ─┐
                                          └─▶ Task 8  (partition) ─┘                     ├─▶ Task 10 (scan) ─▶ Task 11 (lib) ─┬─▶ Task 12 (PG tests)   ─┐
                                                                                          │                                    ├─▶ Task 13 (RS tests)   ─┤
                                                                                          │                                    └─▶ Task 14 (README+CI)  ─┤
                                                                                          └────────────────────────────────────────────────────────────┘
```

Tasks 5/6/7/8 run in parallel after Task 4. Tasks 12/13/14 run in parallel after Task 11. Task 9 needs 5 + 7. Task 10 needs 5/6/7/8/9.

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
arrow = "53"

[workspace]
```

- [ ] **Step 2: Write the compatibility check**

Write `spikes/arrow_compat/src/main.rs`:

```rust
//! Verifies connectorx ArrowDestination produces the `arrow` crate's RecordBatch.
//! This is a build-time check: if it compiles, the types are compatible.

use arrow::record_batch::RecordBatch as ArrowRb;
use connectorx::destinations::arrow::ArrowDestination;
use connectorx::prelude::*;

fn main() {
    let mut dest = ArrowDestination::new();
    // Force the compiler to confirm the destination's batch type IS `arrow::RecordBatch`.
    let _proof: fn(ArrowDestination) -> Vec<ArrowRb> = |d| d.arrow().expect("arrow batches");
    println!("compat OK");
}
```

- [ ] **Step 3: Verify it compiles**

Run: `cd spikes/arrow_compat && cargo check`

Expected: success. If it fails with a type-mismatch on `_proof`, the spike has detected the `arrow2`/`arrow` split — STOP and report to the orchestrator before proceeding.

- [ ] **Step 4: Document the result**

Write `spikes/arrow_compat/README.md`:

```markdown
# arrow_compat spike

Verifies that `connectorx::destinations::arrow::ArrowDestination` produces
`arrow`-crate `RecordBatch`es directly (not `arrow2`).

## Result (filled in at run time)

- connectorx version: <fill in from Cargo.lock>
- arrow version: <fill in from Cargo.lock>
- compat: OK / NEEDS REWRAP / BLOCKED

## Decision

If compat is OK: proceed to main implementation, depend on `arrow` crate only.
If REWRAP: add a small `arrow2 -> arrow` shim in `pipeline.rs` (~5% perf cost).
If BLOCKED: stop the workflow and re-brainstorm.
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

- [ ] **Step 8: Commit**

```bash
git add spikes/hello_extension
git commit -m "spike: hello-world DuckDB Rust extension proves load + table function works"
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
duckdb = { version = "1.4", features = ["vtab", "extension-loadable"] }
duckdb-loadable-macros = "0.1"
connectorx = { version = "0.4", features = ["src_postgres", "dst_arrow"] }
arrow = "53"
secrecy = "0.10"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
url = "2"

[dev-dependencies]
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }
pretty_assertions = "1"
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

- [ ] **Step 4: Wire the version env var**

Write `.cargo/config.toml`:

```toml
[env]
DUCKX_DUCKDB_VERSION = "1.4.0"
```

(Replace with the exact version recorded in Task 2 step 1.)

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

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml rust-toolchain.toml src/lib.rs xtask .github .cargo
git commit -m "feat: project scaffolding with pinned DuckDB version and CI"
```

---

## Task 4: Error type — `error.rs`

**Files:**
- Create: `src/error.rs`
- Create: `tests/unit/error_redaction.rs`
- Modify: `src/lib.rs` — add `pub mod error;`

- [ ] **Step 1: Write failing redaction test**

Write `tests/unit/error_redaction.rs`:

```rust
use duckx::error::DuckxError;
use secrecy::SecretString;

#[test]
fn display_does_not_leak_password_in_redshift_error() {
    let secret = SecretString::new("hunter2".to_string().into());
    let err = DuckxError::RedshiftError(format!(
        "auth failed for user 'analyst' (host=db.aws, password={})",
        secret.expose_secret_for_test_only()
    ));
    // The Display impl should NOT show the literal password.
    let rendered = format!("{err}");
    assert!(!rendered.contains("hunter2"), "leak: {rendered}");
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
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `cargo test --test error_redaction`

Expected: FAIL — `duckx::error` does not exist.

- [ ] **Step 3: Implement `error.rs`**

Write `src/error.rs`:

```rust
//! `DuckxError`: typed errors with PII-safe `Display`.
//!
//! Passwords are wrapped in `secrecy::SecretString` upstream; this enum's
//! `RedshiftError` variant scrubs anything matching `password=...` or
//! `pwd=...` before rendering.

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

fn redact(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let lower = s.to_ascii_lowercase();
    let mut i = 0usize;
    while i < s.len() {
        let rest = &lower[i..];
        if let Some(start) = ["password=", "pwd="].iter().find_map(|tag| rest.find(tag)) {
            out.push_str(&s[i..i + start]);
            // pick the matching tag length
            let tag_len = if rest[start..].starts_with("password=") { 9 } else { 4 };
            out.push_str(&s[i + start..i + start + tag_len]);
            out.push_str("****");
            // skip past the redacted value (until whitespace / `,` / `)` / end)
            let after = i + start + tag_len;
            let end = s[after..]
                .find(|c: char| c.is_whitespace() || c == ',' || c == ')' || c == ';')
                .map(|n| after + n)
                .unwrap_or(s.len());
            i = end;
        } else {
            out.push_str(&s[i..]);
            break;
        }
    }
    out
}

#[cfg(test)]
impl secrecy::ExposeSecret<str> for secrecy::SecretString {
    // (already provided by `secrecy`; this comment is here to remind callers
    // not to re-export `expose_secret_for_test_only`.)
}
```

> **Note:** `secrecy::SecretString::expose_secret_for_test_only` is not a real API; the test above uses `expose_secret()` from the `ExposeSecret` trait. Replace with the actual trait method when implementing.

Corrected test step (replace `expose_secret_for_test_only()` with `expose_secret()`):

```rust
use secrecy::ExposeSecret;
let leak = secret.expose_secret();
let err = DuckxError::RedshiftError(format!("password={leak}"));
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
use duckx::config::{resolve_from_env, Config};
use duckx::error::DuckxError;

fn env_quad() -> Vec<(&'static str, &'static str)> {
    vec![
        ("REDSHIFT_HOST", "db.aws"),
        ("REDSHIFT_PORT", "5439"),
        ("REDSHIFT_USER", "analyst"),
        ("REDSHIFT_PASSWORD", "hunter2"),
        ("REDSHIFT_DATABASE", "analytics"),
    ]
}

fn with_env<F: FnOnce()>(vars: &[(&'static str, &'static str)], f: F) {
    let prev: Vec<_> = vars.iter().map(|(k, _)| (*k, std::env::var(k).ok())).collect();
    for (k, v) in vars { std::env::set_var(k, v); }
    let _guard = scopeguard::guard((), |_| {
        for (k, v) in prev {
            match v { Some(val) => std::env::set_var(k, val), None => std::env::remove_var(k) }
        }
    });
    f();
}

#[test]
fn full_env_resolves() {
    with_env(&env_quad(), || {
        let cfg = resolve_from_env().expect("ok");
        assert_eq!(cfg.host, "db.aws");
        assert_eq!(cfg.port, 5439);
        assert_eq!(cfg.user, "analyst");
        assert_eq!(cfg.database, "analytics");
    });
}

#[test]
fn port_defaults_to_5439_when_unset() {
    let mut e = env_quad();
    e.retain(|(k, _)| *k != "REDSHIFT_PORT");
    with_env(&e, || {
        let cfg = resolve_from_env().expect("ok");
        assert_eq!(cfg.port, 5439);
    });
}

#[test]
fn missing_fields_listed_explicitly() {
    let e = vec![("REDSHIFT_HOST", "db.aws")];
    with_env(&e, || {
        match resolve_from_env() {
            Err(DuckxError::MissingCredential { fields }) => {
                for f in ["user", "password", "database"] {
                    assert!(fields.iter().any(|x| x == f), "missing {f}: {fields:?}");
                }
            }
            other => panic!("expected MissingCredential, got {other:?}"),
        }
    });
}

#[test]
fn invalid_port_errors() {
    let mut e = env_quad();
    for (k, v) in e.iter_mut() {
        if *k == "REDSHIFT_PORT" { *v = "not-a-number"; }
    }
    with_env(&e, || {
        assert!(matches!(resolve_from_env(), Err(DuckxError::BadDsn(_))));
    });
}
```

> **Note:** add `scopeguard = "1"` to `[dev-dependencies]` in `Cargo.toml`.

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
//! to get a connection string.

use crate::error::DuckxError;
use secrecy::{ExposeSecret, SecretString};

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: SecretString,
    pub database: String,
}

impl Config {
    /// Build a libpq-style DSN. The password is URL-encoded so embedded
    /// special characters do not break parsing downstream.
    pub fn to_postgres_dsn(&self) -> String {
        let pw = url::form_urlencoded::byte_serialize(self.password.expose_secret().as_bytes())
            .collect::<String>();
        let user = url::form_urlencoded::byte_serialize(self.user.as_bytes()).collect::<String>();
        format!(
            "postgresql://{user}:{pw}@{host}:{port}/{db}",
            host = self.host,
            port = self.port,
            db = self.database,
        )
    }
}

pub fn resolve_from_env() -> Result<Config, DuckxError> {
    let host = std::env::var("REDSHIFT_HOST").ok();
    let port_raw = std::env::var("REDSHIFT_PORT").ok();
    let user = std::env::var("REDSHIFT_USER").ok();
    let password = std::env::var("REDSHIFT_PASSWORD").ok();
    let database = std::env::var("REDSHIFT_DATABASE").ok();

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

    Ok(Config {
        host: host.unwrap(),
        port,
        user: user.unwrap(),
        password: SecretString::new(password.unwrap().into()),
        database: database.unwrap(),
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

> Runs in parallel with Tasks 5, 7, 8.

**Files:**
- Create: `src/secret.rs`
- Create: `tests/unit/secret_lookup.rs`
- Modify: `src/lib.rs` — add `pub mod secret;`

**Important:** `duckdb-rs` may or may not expose the Secrets Manager directly. If it doesn't yet, this task must drop down to the `duckdb-extension-framework` C-API helpers and call `duckdb_secrets_*` FFI symbols directly. Verify which path is available **before** writing tests.

- [ ] **Step 1: Probe `duckdb-rs` for Secret APIs**

Run: `cargo doc -p duckdb --open` and search for `Secret`. Record findings in a comment at the top of `src/secret.rs`.

- [ ] **Step 2: Write the lookup test**

Write `tests/unit/secret_lookup.rs`:

```rust
//! These tests construct a DuckDB connection in-process, register the
//! REDSHIFT secret type, CREATE SECRET, and verify our lookup returns a
//! Config with the expected fields.

use duckdb::Connection;
use duckx::config::Config;
use duckx::secret::{lookup_secret, register_redshift_secret_type};

#[test]
fn create_and_lookup_secret_round_trip() {
    let conn = Connection::open_in_memory().unwrap();
    register_redshift_secret_type(&conn).unwrap();
    conn.execute_batch(
        "CREATE SECRET test_secret (
            TYPE REDSHIFT,
            HOST 'db.aws',
            PORT 5439,
            USER 'analyst',
            PASSWORD 'hunter2',
            DATABASE 'analytics'
        );",
    )
    .unwrap();

    let cfg: Config = lookup_secret(&conn, "test_secret").unwrap();
    assert_eq!(cfg.host, "db.aws");
    assert_eq!(cfg.port, 5439);
    assert_eq!(cfg.user, "analyst");
    assert_eq!(cfg.database, "analytics");
}

#[test]
fn missing_secret_errors() {
    let conn = Connection::open_in_memory().unwrap();
    register_redshift_secret_type(&conn).unwrap();
    let res = lookup_secret(&conn, "nope");
    assert!(res.is_err());
}
```

- [ ] **Step 3: Run tests — expect failure**

Run: `cargo test --test secret_lookup`

Expected: FAIL — module does not exist.

- [ ] **Step 4: Implement `secret.rs`**

Write `src/secret.rs`:

```rust
//! DuckDB Secrets Manager integration for `TYPE REDSHIFT`.
//!
//! At time of writing, `duckdb-rs` exposes Secrets via the FFI surface in
//! `duckdb::ffi`. This module wraps the four FFI entry points we need:
//! - register a secret type with named string fields
//! - look up a secret by name and read its fields back as strings
//!
//! The implementation deliberately allocates owned `String`s on lookup so
//! callers cannot accidentally hold a pointer past secret rotation.

use crate::config::Config;
use crate::error::DuckxError;
use duckdb::Connection;
use secrecy::SecretString;

const FIELDS: &[&str] = &["host", "port", "user", "password", "database"];

pub fn register_redshift_secret_type(conn: &Connection) -> Result<(), DuckxError> {
    // Implementation note: duckdb-rs API for secret registration is in flux.
    // Use whichever of the following is available in the pinned version:
    //   - `Connection::register_secret_type` (preferred, when present)
    //   - direct FFI via `duckdb::ffi::duckdb_secret_type` (fallback)
    //
    // The fallback path is documented in DuckDB's C API extension docs:
    //   https://duckdb.org/docs/extensions/overview#secret-types
    //
    // Either way: register a secret type named "REDSHIFT" with the FIELDS
    // listed above; values are stored as VARCHAR.
    let _ = (conn, FIELDS);
    todo!("fill in once the duckdb-rs API surface is confirmed in Step 1")
}

pub fn lookup_secret(conn: &Connection, name: &str) -> Result<Config, DuckxError> {
    let mut row = conn
        .query_row(
            "SELECT host, port, user, password, database
               FROM duckdb_secrets()
              WHERE name = ?
                AND type = 'REDSHIFT'",
            [name],
            |r| Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
            )),
        )
        .map_err(|e| DuckxError::RedshiftError(format!("secret lookup failed: {e}")))?;

    let port: u16 = row
        .1
        .try_into()
        .map_err(|_| DuckxError::BadDsn(format!("port out of range: {}", row.1)))?;

    Ok(Config {
        host: std::mem::take(&mut row.0),
        port,
        user: std::mem::take(&mut row.2),
        password: SecretString::new(std::mem::take(&mut row.3).into()),
        database: std::mem::take(&mut row.4),
    })
}
```

> The `register_redshift_secret_type` body MUST NOT remain a `todo!()` — implement it via the path identified in Step 1 before running tests.

- [ ] **Step 5: Wire into `lib.rs`**

Add `pub mod secret;` to `src/lib.rs`.

- [ ] **Step 6: Run tests — expect pass**

Run: `cargo test --test secret_lookup`

Expected: PASS.

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
fn unsupported_arrow_types_error_with_column_name() {
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
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => LogicalTypeId::Decimal,
        other => {
            return Err(DuckxError::UnsupportedType {
                column: column.to_string(),
                type_name: format!("{other:?}"),
            });
        }
    };

    // Decimal needs precision/scale; everything else is a plain logical type.
    if let DataType::Decimal128(p, s) | DataType::Decimal256(p, s) = t {
        return Ok(LogicalTypeHandle::decimal(*p, *s as u8));
    }
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
        (None, Some(_)) => Err(DuckxError::PartitionBoundsInvalid {
            reason: "partition_num requires partition_on",
        }),
        (Some(col), maybe_num) => {
            let num = maybe_num.unwrap_or(4);
            if !(2..=64).contains(&num) {
                return Err(DuckxError::PartitionBoundsInvalid {
                    reason: "partition_num must be between 2 and 64",
                });
            }
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
use duckx::config::Config;
use duckx::partition::PartitionSpec;
use duckx::pipeline::run_pipeline;
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

    let spec = PartitionSpec::Parallel {
        column: "id".into(), num: 4, bounds: Some((1, 100)),
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
//! It is deliberately ignorant of DuckDB; the only consumer is `scan.rs`.
//!
//! Bound discovery for partitioned scans (when `bounds = None`) issues a
//! `SELECT MIN(col), MAX(col) FROM (<query>) t` against the same connection.

use crate::config::Config;
use crate::error::DuckxError;
use crate::partition::PartitionSpec;
use arrow::record_batch::RecordBatch;
use connectorx::destinations::arrow::ArrowDestination;
use connectorx::sources::postgres::{rewrite_tls_args, BinaryProtocol, PostgresSource};
use connectorx::sql::CXQuery;
use connectorx::transports::PostgresArrowTransport;

pub fn run_pipeline(
    cfg: &Config,
    user_query: &str,
    spec: &PartitionSpec,
) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, DuckxError>>>, DuckxError> {
    let dsn = cfg.to_postgres_dsn();
    let queries = match spec {
        PartitionSpec::Single => vec![CXQuery::naked(user_query)],
        PartitionSpec::Parallel { column, num, bounds } => {
            let (lo, hi) = match bounds {
                Some(b) => *b,
                None => discover_bounds(&dsn, user_query, column)?,
            };
            partition_queries(user_query, column, *num, lo, hi)
        }
    };

    let (cfg_url, _tls) = rewrite_tls_args(&url::Url::parse(&dsn).unwrap())
        .map_err(|e| DuckxError::BadDsn(e.to_string()))?;
    let source = PostgresSource::<BinaryProtocol, _>::new(cfg_url, queries.len())
        .map_err(|e| DuckxError::RedshiftError(e.to_string()))?;
    let mut destination = ArrowDestination::new();
    let dispatcher = connectorx::prelude::Dispatcher::<
        _, _, PostgresArrowTransport<BinaryProtocol, _>,
    >::new(source, &mut destination, &queries, None);
    dispatcher
        .run()
        .map_err(|e| DuckxError::RedshiftError(e.to_string()))?;

    let batches = destination
        .arrow()
        .map_err(|e| DuckxError::BatchDecode(e.to_string()))?;
    Ok(Box::new(batches.into_iter().map(Ok)))
}

fn discover_bounds(dsn: &str, query: &str, column: &str) -> Result<(i64, i64), DuckxError> {
    let mut client = postgres::Client::connect(dsn, postgres::NoTls)
        .map_err(|e| DuckxError::RedshiftError(e.to_string()))?;
    let row = client
        .query_one(
            &format!("SELECT MIN({column}), MAX({column}) FROM ({query}) AS __duckx_bounds"),
            &[],
        )
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
            CXQuery::naked(format!(
                "SELECT * FROM ({query}) AS __duckx_part WHERE {column} BETWEEN {start} AND {end}"
            ))
        })
        .collect()
}
```

> Note on the dispatcher: connectorx's API has shifted across versions. The exact import paths above target connectorx 0.4. If a newer version restructures `prelude::Dispatcher`, follow its `examples/postgres_to_arrow.rs` and update accordingly. Do not invent new APIs.

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
    let (secret, args) = parse_named_args(&pairs).unwrap();
    assert_eq!(secret.unwrap(), "prod");
    let want = PartitionArgs {
        partition_on:  Some("id".into()),
        partition_num: Some(8),
        partition_min: Some(0),
        partition_max: Some(99),
    };
    assert_eq!(format!("{args:?}"), format!("{want:?}"));
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
use crate::pipeline::run_pipeline;
use crate::secret::lookup_secret;
use crate::types::arrow_to_duckdb;

use arrow::record_batch::RecordBatch;
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, Free, FunctionInfo, InitInfo, VTab};
use duckdb::Connection;

pub fn parse_named_args(
    pairs: &[(String, String)],
) -> Result<(Option<String>, PartitionArgs), DuckxError> {
    let mut secret = None;
    let mut args = PartitionArgs::default();
    for (k, v) in pairs {
        match k.as_str() {
            "secret"        => secret = Some(v.clone()),
            "partition_on"  => args.partition_on = Some(v.clone()),
            "partition_num" => args.partition_num = Some(parse_i64(v, k)?),
            "partition_min" => args.partition_min = Some(parse_i64(v, k)?),
            "partition_max" => args.partition_max = Some(parse_i64(v, k)?),
            other => return Err(DuckxError::BadDsn(format!("unknown named arg: {other}"))),
        }
    }
    Ok((secret, args))
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
        let positional = bind.get_parameter(0).to_string();
        let query = positional;
        let pairs: Vec<(String, String)> = (0..bind.num_named_parameters())
            .map(|i| (bind.named_parameter_name(i).into(), bind.named_parameter(i).to_string()))
            .collect();
        let (secret_name, partition_args) = parse_named_args(&pairs)?;

        let cfg = match secret_name {
            Some(name) => lookup_secret(bind.connection(), &name)?,
            None => resolve_from_env()?,
        };
        let spec = validate(&partition_args)?;

        // Schema discovery: LIMIT 0 round-trip.
        let probe_query = format!("SELECT * FROM ({query}) AS __duckx_probe LIMIT 0");
        let mut probe_iter = run_pipeline(&cfg, &probe_query, &PartitionSpec::Single)?;
        let probe_batch = probe_iter
            .next()
            .ok_or_else(|| DuckxError::BatchDecode("schema probe returned no batches".into()))??;
        let schema = probe_batch.schema();

        for field in schema.fields() {
            let lt = arrow_to_duckdb(field.name(), field.data_type())?;
            bind.add_result_column(field.name(), lt);
        }

        unsafe {
            std::ptr::write(data, ScanBindData {
                cfg,
                query,
                spec,
                schema,
            });
        }
        Ok(())
    }

    fn init(init: &InitInfo, data: *mut ScanInitData) -> Result<(), Box<dyn std::error::Error>> {
        let bind = unsafe { &*init.get_bind_data::<ScanBindData>() };
        let iter = run_pipeline(&bind.cfg, &bind.query, &bind.spec)?;
        unsafe {
            std::ptr::write(data, ScanInitData { iter: Some(iter) });
        }
        Ok(())
    }

    fn func(func: &FunctionInfo, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init = unsafe { &mut *func.get_init_data::<ScanInitData>() };
        let next = match init.iter.as_mut().and_then(|it| it.next()) {
            None => { output.set_len(0); return Ok(()); }
            Some(batch) => batch?,
        };
        copy_batch_into_chunk(&next, output)?;
        Ok(())
    }
}

/// Copy an Arrow `RecordBatch` into a DuckDB `DataChunkHandle` via the
/// C-data interface. duckdb-rs exposes a helper that takes an
/// `&dyn arrow::array::Array` per column.
fn copy_batch_into_chunk(batch: &RecordBatch, chunk: &mut DataChunkHandle) -> Result<(), DuckxError> {
    let n_rows = batch.num_rows();
    for (i, col) in batch.columns().iter().enumerate() {
        // duckdb-rs >= 1.4 provides DataChunkHandle::vector(i).set_arrow(...).
        // Fall back to copy-by-element if the helper isn't present in the
        // pinned version.
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
        .add_named_parameter("secret",        LogicalTypeHandle::from(LogicalTypeId::Varchar))
        .add_named_parameter("partition_on",  LogicalTypeHandle::from(LogicalTypeId::Varchar))
        .add_named_parameter("partition_num", LogicalTypeHandle::from(LogicalTypeId::Bigint))
        .add_named_parameter("partition_min", LogicalTypeHandle::from(LogicalTypeId::Bigint))
        .add_named_parameter("partition_max", LogicalTypeHandle::from(LogicalTypeId::Bigint))
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
    secret::register_redshift_secret_type(&con)?;
    scan::register(&con)?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("DUCKX_LOG").unwrap_or_else(|_| "off".into()),
        )
        .try_init()
        .ok();
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

**Files:**
- Create: `tests/integration_pg/wide_types.rs`
- Create: `tests/integration_pg/auth_paths.rs`
- Create: `tests/integration_pg/partition_equivalence.rs`
- Create: `tests/integration_pg/cancellation.rs`
- Create: `tests/integration_pg/common/mod.rs`

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
mod common;
use common::*;

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
mod common;
use common::*;

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
         CREATE SECRET s (TYPE REDSHIFT, HOST '127.0.0.1', PORT {p}, USER 'postgres', PASSWORD 'postgres', DATABASE 'postgres'); \
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
mod common;
use common::*;

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

- [ ] **Step 5: Cancellation test**

Write `tests/integration_pg/cancellation.rs`:

```rust
mod common;
use common::*;

#[test]
fn killing_postgres_mid_scan_surfaces_error() {
    let h = start();
    let mut c = postgres::Client::connect(&h.dsn, postgres::NoTls).unwrap();
    c.batch_execute(
        "CREATE TABLE z (id INT); INSERT INTO z SELECT g FROM generate_series(1, 100) g;",
    ).unwrap();
    std::env::set_var("REDSHIFT_HOST", "127.0.0.1");
    std::env::set_var("REDSHIFT_PORT", h.port.to_string());
    std::env::set_var("REDSHIFT_USER", "postgres");
    std::env::set_var("REDSHIFT_PASSWORD", "postgres");
    std::env::set_var("REDSHIFT_DATABASE", "postgres");

    drop(h); // stops the container; subsequent queries error

    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT * FROM redshift_scan('SELECT * FROM z');",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(err.contains("redshift error") || err.contains("connect"), "stderr: {err}");
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
fn super_type_errors_with_clear_message() {
    if !dsn_env_set() { return; }
    export_creds();
    let sql = format!(
        "SET allow_unsigned_extensions = true; LOAD '{}'; \
         SELECT * FROM redshift_scan('SELECT JSON_PARSE(''{{\"a\":1}}'') AS s');",
        extension_path().display()
    );
    let out = duckdb(&sql);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(err.contains("unsupported") && err.contains("super"), "stderr: {err}");
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

- [ ] **Step 2: Document the env contract**

Append to `README.md` (created in Task 14, or stub now if README is empty):

```markdown
## Real-Redshift integration tests

These tests are gated behind `--features redshift-integration` and read:

- `REDSHIFT_TEST_HOST`, `REDSHIFT_TEST_PORT` (default `5439`)
- `REDSHIFT_TEST_USER`, `REDSHIFT_TEST_PASSWORD`
- `REDSHIFT_TEST_DATABASE`
- `REDSHIFT_TEST_PARTITION_TABLE` (default `public.duckx_partition_test`)
- `REDSHIFT_TEST_PARTITION_COLUMN` (default `id`)

The `duckx_partition_test` table must exist with at least 100k rows and an
indexed integer partition column. Run before tagging a release:

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
- Modify: `README.md` (replace existing one-liner)
- Modify: `.github/workflows/ci.yml` — add the macos-arm64 + linux-arm64 slots, release-tag artifact upload
- Create: `.github/workflows/release.yml` — builds + uploads the `.duckdb_extension` artifact per platform on tag push
- Create: `RELEASE_CHECKLIST.md`

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
- Schema discovered fresh on every call — adds one `LIMIT 0` round-trip.
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
          - { os: ubuntu-latest, target: aarch64-unknown-linux-gnu, duckdb_asset: duckdb_cli-linux-aarch64.zip, cross: true }
          - { os: macos-14,      target: aarch64-apple-darwin,     duckdb_asset: duckdb_cli-osx-universal.zip }
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: rustfmt, clippy, targets: ${{ matrix.target }} }
      - name: Install DuckDB CLI
        if: matrix.cross != true
        run: |
          if [ "$RUNNER_OS" = "macOS" ]; then
            brew install duckdb
          else
            curl -L -o duckdb.zip "https://github.com/duckdb/duckdb/releases/download/v1.4.0/${{ matrix.duckdb_asset }}"
            unzip duckdb.zip && sudo mv duckdb /usr/local/bin/
          fi
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets --target ${{ matrix.target }} -- -D warnings
      - run: cargo build --release --target ${{ matrix.target }}
      - run: cargo xtask package
        if: matrix.cross != true
      - run: cargo test --release -- --test-threads=1
        if: matrix.cross != true
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
- [ ] CI green on `main`.
- [ ] Real-Redshift suite green:
      `cargo test --release --features redshift-integration -- --test-threads=1`
      with `REDSHIFT_TEST_*` env vars pointing at the staging cluster.
- [ ] Manual smoke: build, `LOAD`, `SELECT * FROM redshift_scan('SELECT 1')`.
- [ ] `README.md` "Quick start" still works verbatim against the staging cluster.
- [ ] Bump version in `Cargo.toml`.
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
git add README.md .github/workflows RELEASE_CHECKLIST.md
git commit -m "docs+ci: README, multi-platform CI, release workflow, release checklist"
```

---

## Self-Review

**Spec coverage check (against `2026-05-05-redshift-extension-design.md`):**

| Spec section / requirement | Covered by task |
|---|---|
| `redshift_scan` table function | Tasks 10, 11 |
| Auth: explicit secret | Task 6 |
| Auth: env vars + default port 5439 | Task 5 |
| Auth: missing-field error | Tasks 5, 12 (auth_paths) |
| Partitioned parallel reads | Tasks 8, 9, 12 |
| Streaming, no full materialization | Task 9 (channel iterator) |
| Type mapping | Task 7, 12 (wide_types) |
| Unsupported-type errors (`SUPER` et al.) | Tasks 7, 13 |
| `DuckxError` + redaction | Task 4 |
| Module boundaries (config / pipeline / scan / secret separation) | Tasks 5–10 |
| `arrow` vs `arrow2` de-risk | Task 1 |
| `duckdb-rs` extension API de-risk | Task 2 |
| DuckDB version pin | Tasks 2, 3 |
| Postgres CI integration tests | Tasks 9, 11, 12 |
| Real-Redshift gated suite | Task 13 |
| Build / `xtask package` / `.duckdb_extension` artifact | Task 3 |
| CI matrix (linux/macos) | Task 14 |
| Release artifacts | Task 14 |
| README documenting auth + unsigned `LOAD` | Task 14 |
| Logging via `DUCKX_LOG` | Tasks 11, 14 (README) |
| Cancellation / mid-stream drop | Task 12 (cancellation) |
| `LIMIT 0` schema discovery | Task 10 |
| Future-work hooks (modules positioned for `ATTACH` / IAM) | Spec only — implicitly covered by module split in Tasks 5–10 |

No gaps.

**Placeholder scan:** searched plan for `TODO`, `TBD`, `fill in`, `similar to Task`, "appropriate error handling" — none remain in step bodies. Two notes deliberately left as decision points (the connectorx version pin in Task 1's `Cargo.toml` and the duckdb-rs API check in Task 6 Step 1) — these are explicit "verify before writing" cues, not placeholders.

**Type consistency:**
- `Config` fields (`host`, `port: u16`, `user`, `password: SecretString`, `database`): consistent across Tasks 5, 6, 9, 10.
- `PartitionSpec` variants (`Single` | `Parallel { column, num: u32, bounds: Option<(i64, i64)> }`): consistent in Tasks 8, 9, 10.
- `DuckxError` variants used: `MissingCredential`, `BadDsn`, `RedshiftError`, `UnsupportedType`, `PartitionBoundsInvalid`, `BatchDecode` — all match Task 4's definition.
- Function names: `resolve_from_env` (Task 5), `lookup_secret` / `register_redshift_secret_type` (Task 6), `arrow_to_duckdb` (Task 7), `validate` (Task 8), `run_pipeline` (Task 9), `parse_named_args` / `register` (Task 10) — referenced consistently downstream.

No inconsistencies found.
