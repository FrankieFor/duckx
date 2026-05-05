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
    fn _dispatcher_typecheck() {
        use connectorx::sources::postgres::{rewrite_tls_args, BinaryProtocol, PostgresSource};
        use connectorx::sql::CXQuery;
        use connectorx::transports::PostgresArrowTransport;

        let url = url::Url::parse("postgresql://u:p@h:5432/d?sslmode=disable").unwrap();
        let (cfg_url, _tls): (_, _) = rewrite_tls_args(&url).unwrap();
        let queries: Vec<CXQuery<String>> = vec![CXQuery::naked("SELECT 1")];
        let source = PostgresSource::<BinaryProtocol, _>::new(cfg_url, queries.len()).unwrap();
        let mut destination = ArrowDestination::new();
        let dispatcher = connectorx::prelude::Dispatcher::<
            _, _, PostgresArrowTransport<BinaryProtocol, _>,
        >::new(source, &mut destination, &queries, None);
        let _ = dispatcher; // don't run; just typecheck
    }

    // Keep `dest` alive to suppress unused warning.
    let _ = &mut dest;

    println!("compat OK; arrow() returns Vec<RecordBatch>; Dispatcher API resolved");
}
