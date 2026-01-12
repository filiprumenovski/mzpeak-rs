# Issue 001: Python API Lacks Zero-Copy Array Views for Peaks

Priority: P1
Status: Open
Components: `src/python/types.rs`, `src/python/reader.rs`

## Summary
Python spectrum access materializes per-peak Python objects (`Peak`) and clones data. No NumPy/PyBuffer view exists for peak arrays, defeating zero-copy goals.

## Evidence
- `Spectrum.peaks` clones every `Peak` into a Python object
- `SpectrumBuilder` only accepts `List[Peak]` or per-peak adds
- No NumPy array access for `mz`, `intensity` columns

## Impact
- High overhead for large spectra (object allocation per peak)
- No zero-copy interop with NumPy for peak arrays
- Python throughput bottleneck for analytics workflows

## Proposed Fix
1. Add `Spectrum.mz_array() -> numpy.ndarray` returning memory view
2. Add `Spectrum.intensity_array() -> numpy.ndarray` returning memory view
3. Add constructor accepting NumPy arrays for bulk writes
4. Keep object-based API for convenience, array API for performance

## Acceptance Criteria
- [ ] Spectrum can expose mz/intensity as NumPy views without copying
- [ ] Bulk writes from NumPy arrays avoid per-peak allocation
- [ ] Benchmarks show reduced overhead vs object-based path

## Related
- Issue 005 (AoS layout) - same underlying data structure concern
