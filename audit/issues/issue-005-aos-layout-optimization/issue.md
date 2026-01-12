# Issue 005: AoS Peak Layout Limits SIMD and Zero-Copy Interop

Priority: P1
Status: In Progress
Components: `src/writer/`, `src/reader/`, `src/python/`, `src/mzml/converter/`

## Summary
Core data structures use array-of-structs layout (`Vec<Peak>` inside `Spectrum`). Suboptimal for SIMD, cache locality, and zero-copy export. `ColumnarBatch` exists but isn't the primary path.

## Evidence
- `Spectrum` contains `peaks: Vec<Peak>` (AoS)
- `Peak { mz, intensity, ion_mobility }` struct layout
- Columnar API (`ColumnarBatch`) exists but not used by reader/Python

## Impact
- SIMD operations limited by AoS layout
- Python requires object conversion, no zero-copy
- Data reshaping needed for analytics

## Proposed Fix
1. Introduce `SpectrumArrays` / `PeaksSoA` with `Vec<f64>` / `Vec<f32>` columns
2. Make SoA internal representation for read/write
3. Convert to AoS only when explicitly requested
4. Expose SoA buffers to Python via NumPy/PyBuffer

## Acceptance Criteria
- [x] Reading builds SoA first and AoS is now a wrapper
- [x] mzML conversion and writer paths use SoA internally
- [ ] Python/Arrow can share buffers without conversion
- [ ] SoA is the primary public API across Rust and Python (writers/builders/tests)
- [ ] Benchmarks show improved throughput

## Progress
- Added `SpectrumArrays`/`PeakArrays` and SoA writer/reader paths
- Python reader exposes `SpectrumArrays` and streaming iterators
- AoS streaming iterator now wraps SoA

## Remaining Gaps
- Python writer and builder are still AoS-only (no NumPy array ingestion)
- Python `SpectrumArrays` arrays are copies, not zero-copy views
- Tests/examples/docs still AoS-centric and lack SoA coverage

## Related
- Issue 001 (Python zero-copy) - depends on this for zero-copy views
