//! Hello-world DuckDB extension.
//!
//! Proves Task 2's de-risk goals against duckdb-rs 1.10502.0 (DuckDB 1.5.2):
//! 1. We can build a `.duckdb_extension` cdylib in Rust.
//! 2. The resulting extension loads in a real DuckDB binary and a
//!    registered table function returns the expected row.
//!
//! Important API note vs. plan's earlier draft: in 1.10502.0, the VTab
//! trait's `bind` and `init` return owned data (no `*mut data` parameter,
//! no `Free` trait). `init_data` is borrowed immutably from `func` so any
//! per-scan mutable state needs interior mutability (we use AtomicBool).

use std::sync::atomic::{AtomicBool, Ordering};

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::{Connection, Result};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;

struct HelloBindData;

struct HelloInitData {
    done: AtomicBool,
}

struct HelloVTab;

impl VTab for HelloVTab {
    type InitData = HelloInitData;
    type BindData = HelloBindData;

    fn bind(bind: &BindInfo) -> Result<HelloBindData, Box<dyn std::error::Error>> {
        bind.add_result_column("greeting", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        Ok(HelloBindData)
    }

    fn init(_init: &InitInfo) -> Result<HelloInitData, Box<dyn std::error::Error>> {
        Ok(HelloInitData {
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init = func.get_init_data();
        if init.done.swap(true, Ordering::SeqCst) {
            output.set_len(0);
            return Ok(());
        }
        let mut col = output.flat_vector(0);
        col.insert(0, "hello, redshift");
        output.set_len(1);
        Ok(())
    }
}

#[duckdb_entrypoint_c_api]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn std::error::Error>> {
    con.register_table_function::<HelloVTab>("hello_redshift")?;
    Ok(())
}
