# Issue: Example Rot and Duplicative Scripts

Priority: P2
Status: Open
Components: `examples/`

## Summary
Several examples are redundant, non-portable, or overlap with benchmarks and tests. Some reference local absolute paths or replicate existing benchmarking scripts.

## Evidence
- `examples/test_read_real.rs` hard-codes a local path and uses `unwrap()`.
- `examples/test_read_converted.rs` duplicates basic readback functionality covered by tests.
- `examples/show_compression_improvements.rs` overlaps `examples/compare_compression.rs` and benchmarks.
- `examples/compression_test.rs` is effectively a benchmark and overlaps `benches/`.
- `examples/analyze_gains.rs` overlaps benchmark/publication scripts.
- `examples/benchmark_converter.rs` overlaps `benches/conversion.rs`.

## Impact
- Increased maintenance burden.
- Confusing entry points for users (too many similar scripts).
- Non-portable examples break out of the box.

## Root Cause
Examples were added during exploratory development without later consolidation.

## Proposed Fix
- Remove non-portable examples and redundant benchmarks.
- Keep a small curated set of examples that each demonstrate a unique feature.
- Move benchmarking to `benches/` and keep publication scripts in a dedicated `tools/` or `scripts/` directory if still needed.

## Streaming Requirements
- Examples should demonstrate streaming usage once the streaming pipeline is implemented.

## Acceptance Criteria
- `examples/` contains only unique, portable demos.
- No examples reference local absolute paths.

## Tests
- N/A (documentation/examples cleanup).
