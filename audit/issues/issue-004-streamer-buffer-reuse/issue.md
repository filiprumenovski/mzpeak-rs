# Issue 004: mzML Streamer Reallocates Buffers Per Spectrum

Priority: P2
Status: Open
Components: `src/mzml/streamer/spectrum.rs`

## Summary
The mzML streaming parser allocates new `Vec`/`String` buffers inside per-spectrum parsing loops, increasing allocation churn.

## Evidence
- `parse_raw_spectrum` creates fresh `buf`, `current_binary_cv_params`, `current_binary_data` per spectrum
- Multiple parsing functions allocate fresh `Vec` buffers

## Impact
- Increased allocations in tight parsing loops
- Reduced throughput on large files

## Proposed Fix
1. Move scratch buffers to `MzMLStreamer` struct fields
2. Use `Vec::clear()` / `String::clear()` to reuse capacity
3. Ensure no references retained across calls

## Acceptance Criteria
- [ ] Allocation counts drop in profiling
- [ ] Parsing throughput improves on large files
- [ ] Add benchmark tracking allocations per spectrum
