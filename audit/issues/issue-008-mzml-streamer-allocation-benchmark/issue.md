# Issue 008: No Allocation Benchmark for mzML Streamer

Priority: P3
Status: Open
Components: `src/mzml/streamer/`, `benches/`

## Summary
The mzML streamer buffer reuse fix removed per-spectrum allocations, but there is no benchmark or profiling test to validate allocation counts or prevent regressions.

## Evidence
- No benchmark in `benches/` tracks allocations per spectrum
- No perf guardrails for `MzMLStreamer` parsing loops

## Impact
- Allocation regressions may go unnoticed
- Hard to quantify performance gains from buffer reuse

## Proposed Fix
1. Add a micro-benchmark for `next_spectrum`/`next_raw_spectrum`
2. Track allocations per spectrum (or wall-clock throughput) across versions
3. Add a lightweight CI benchmark (optional, non-gating)

## Acceptance Criteria
- [ ] Benchmark exists for mzML streaming parser
- [ ] Allocation/throughput metrics are recorded and compared over time
