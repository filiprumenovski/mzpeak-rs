# Issue 001: Python API Lacks Zero-Copy Array Views for Peaks

Priority: P1
Status: In Progress
Components: `src/python/types/spectrum.rs`, `src/python/types/spectrum_arrays.rs`, `src/python/reader.rs`, `src/python/writer.rs`

## Summary
Python spectrum access still materializes per-peak Python objects (`Peak`) or allocates NumPy arrays from owned buffers. There are no true zero-copy views of the underlying Arrow/Parquet buffers for per-spectrum arrays.

## Evidence
- `Spectrum.peaks` clones every `Peak` into a Python object
- `SpectrumArrays` builds NumPy arrays from Rust-owned `Vec`, which copies data
- Python writers only accept AoS `Spectrum` objects, not NumPy arrays

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
- [ ] Spectrum can expose mz/intensity as NumPy views without copying
- [ ] Bulk writes from NumPy arrays avoid per-peak allocation
- [ ] Benchmarks show reduced overhead vs object-based path

## Progress
- `SpectrumArrays` exists in Python, but array construction still copies data
- Reader exposes `*_arrays` methods and iterators, but views are not zero-copy

## Related
- Issue 005 (AoS layout) - same underlying data structure concern
