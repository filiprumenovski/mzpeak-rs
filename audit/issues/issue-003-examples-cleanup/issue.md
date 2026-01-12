# Issue 003: Example Rot and Duplicative Scripts

Priority: P2
Status: Open
Components: `examples/`

## Summary
Several examples are redundant, non-portable, or overlap with benchmarks. Some reference local absolute paths.

## Evidence
- `examples/test_read_real.rs` hard-codes local paths
- `examples/compression_test.rs` overlaps `benches/`
- `examples/benchmark_converter.rs` duplicates `benches/conversion.rs`
- Multiple compression comparison examples overlap

## Proposed Fix
1. Remove non-portable examples with hardcoded paths
2. Consolidate benchmark-like examples into `benches/`
3. Keep curated set demonstrating unique features:
   - Basic read/write example
   - Streaming conversion example
   - Python interop example

## Acceptance Criteria
- [ ] `examples/` contains only unique, portable demos
- [ ] No examples reference local absolute paths
- [ ] Each example demonstrates a distinct feature
