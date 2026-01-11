# Issue: mzML Streamer Reallocates Buffers Per Spectrum

Priority: P2
Status: Open
Components: `src/mzml/streamer.rs`

## Summary
The mzML streaming parser allocates new `Vec`/`String` buffers inside per-spectrum parsing loops. This increases allocation churn and impacts performance on large files.

## Evidence
- `src/mzml/streamer.rs:588-590` creates `current_binary_cv_params`, `current_binary_data`, and `buf` within `parse_raw_spectrum` for each spectrum.
- Multiple other parsing functions allocate fresh `Vec` buffers.

## Impact
- Increased allocations and GC pressure in tight parsing loops.
- Reduced throughput compared to a reuse strategy.

## Root Cause
Scratch buffers are defined inside per-spectrum functions rather than on the `MzMLStreamer` struct for reuse.

## Proposed Fix
- Move scratch buffers to `MzMLStreamer` fields and reuse them across calls.
- Use `Vec::clear()` and `String::clear()` to reuse capacity.
- Ensure functions do not retain references across calls.

## Streaming Requirements
- Buffer reuse must remain compatible with streaming parsing (no full-file buffering).

## Acceptance Criteria
- Allocation counts drop in profiling for large conversions.
- Parsing throughput improves measurably on large files.

## Tests
- Add a benchmark or perf test to track allocations per spectrum.
