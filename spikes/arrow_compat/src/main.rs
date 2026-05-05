//! Verifies two things at compile time:
//!   1. connectorx ArrowDestination produces the `arrow` crate's
//!      `RecordBatch` (not arrow2). The Cargo.toml pins `arrow = "54"`
//!      to match the `arrow-array 54.x` that connectorx 0.4.5 transitively
//!      brings in; this `_proof` coercion will fail to compile if the
//!      destination's RecordBatch is from a different crate version.
//!   2. The connectorx 0.4.5 `Dispatcher` / `PostgresSource` API used by
//!      the production `pipeline.rs` resolves to types we can construct.
//!
//! `arrow()` returns `Vec<RecordBatch>` (materialized), not a streaming
//! iterator — recorded in README.md and reflected in the spec's
//! Memory Model section.

use arrow::record_batch::RecordBatch as ArrowRb;
use connectorx::destinations::arrow::ArrowDestination;

fn main() {
    let dest = ArrowDestination::new();
    // Force the compiler to confirm the destination's batch type IS
    // `arrow::record_batch::RecordBatch` from the same arrow version.
    let _proof: fn(ArrowDestination) -> Vec<ArrowRb> =
        |d| d.arrow().expect("arrow batches");
    let _ = dest;

    // Also compile-test the production Dispatcher snippet from Task 9.
    // We don't actually run it — we just need the types to resolve.
    //
    // Real signature (verified against connectorx 0.4.5):
    //   PostgresSource::<P, C>::new(
    //       config: postgres::config::Config,
    //       tls: C,                              // C: MakeTlsConnect<Socket> + Clone + 'static + Send + Sync
    //       nconn: usize,
    //   ) -> Result<Self, _>
    //
    // Note: connectorx exposes the SYNC `postgres::Config`, not
    // `tokio_postgres::Config`. The sync `Config` impls `FromStr`.
    fn _dispatcher_typecheck() {
        use connectorx::sources::postgres::{BinaryProtocol, PostgresSource};
        use connectorx::sql::CXQuery;
        use connectorx::transports::PostgresArrowTransport;
        use postgres::Config;
        use std::str::FromStr;
        use tokio_postgres::NoTls;

        let cfg = Config::from_str("postgresql://u:p@h:5432/d?sslmode=disable").unwrap();
        let queries: Vec<CXQuery<String>> = vec![CXQuery::naked("SELECT 1".to_string())];
        let source =
            PostgresSource::<BinaryProtocol, NoTls>::new(cfg, NoTls, queries.len()).unwrap();
        let mut destination = ArrowDestination::new();
        let dispatcher = connectorx::prelude::Dispatcher::<
            _,
            _,
            PostgresArrowTransport<BinaryProtocol, NoTls>,
        >::new(source, &mut destination, &queries, None);
        let _ = dispatcher; // don't run; just typecheck
    }

    println!("compat OK; arrow() returns Vec<RecordBatch>; Dispatcher API resolved");
}
