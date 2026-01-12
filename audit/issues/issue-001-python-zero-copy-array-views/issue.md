# Issue 001: Python API Lacks Zero-Copy Array Views for Peaks

Priority: P1
Status: Resolved
Components: `src/python/types/spectrum.rs`, `src/python/types/spectrum_arrays.rs`, `src/python/reader.rs`, `src/python/writer.rs`

## Summary
Python spectrum access still materializes per-peak Python objects (`Peak`) or allocates NumPy arrays from owned buffers. There are no true zero-copy views of the underlying Arrow/Parquet buffers for per-spectrum arrays.

## Evidence
- `Spectrum.peaks` clones every `Peak` into a Python object
- `SpectrumArrays` builds NumPy arrays from Rust-owned `Vec`, which copies data

## Impact
- High overhead for large spectra (object allocation per peak)
- No zero-copy interop with NumPy for peak arrays
- Python throughput bottleneck for analytics workflows

## Proposed Fix
1. Expose zero-copy views for `mz`/`intensity` via Arrow buffers or `memoryview`
2. Add `SpectrumArrays` API that can hand out views without copying
3. Add Python writer constructors accepting NumPy arrays (bulk writes)
4. Keep object-based API for convenience, array API for performance

## Acceptance Criteria
- [x] Spectrum can expose mz/intensity as NumPy views without copying
- [x] Bulk writes from NumPy arrays avoid per-peak allocation
- [x] Benchmarks show reduced overhead vs object-based path

## Progress
- `SpectrumArrays` exists in Python; view-backed access is provided via `SpectrumArraysView`
- Reader exposes `*_arrays` and `*_arrays_views` methods and iterators
- Python writers accept array-based `SpectrumArrays` input
- Added SoA view benchmark path in `benches/query_performance.rs`
- Python bindings are feature-gated off in prealpha; zero-copy views are implemented but currently disabled

## Related
- Issue 005 (AoS layout) - same underlying data structure concern
