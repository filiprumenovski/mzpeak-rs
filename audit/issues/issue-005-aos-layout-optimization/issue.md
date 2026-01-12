# Issue 005: AoS Peak Layout Limits SIMD and Zero-Copy Interop

Priority: P1
Status: Open
Components: `src/writer/`, `src/reader/`, `src/python/`

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
- [ ] Reading yields SoA batches by default
- [ ] Python/Arrow can share buffers without conversion
- [ ] Benchmarks show improved throughput

## Related
- Issue 001 (Python zero-copy) - depends on this for zero-copy views
