# Issue 009: SoA Reader Still Copies Data Per Spectrum

Priority: P1
Status: Open
Components: `src/reader/spectra.rs`, `src/reader/batches.rs`

## Summary
The SoA reader reconstructs `SpectrumArrays` by iterating rows and pushing into new `Vec` buffers. This is SoA in layout but not zero-copy, and it duplicates data already stored in Arrow arrays.

## Evidence
- `batches_to_spectra_arrays` and `StreamingSpectrumArraysIterator` push values into `Vec`
- No view-based `SpectrumArrays` or offset slicing into Arrow arrays

## Impact
- Extra allocations and copies on read
- Limits throughput and negates zero-copy goals
- Python `SpectrumArrays` currently receives copied data

## Proposed Fix
1. Introduce a view type (e.g., `SpectrumArraysView`) referencing Arrow arrays + offsets
2. Make streaming iterator yield views (zero-copy) with optional materialization
3. Expose view-backed NumPy arrays in Python

## Acceptance Criteria
- [ ] SoA streaming can return views without copying peak buffers
- [ ] Materialization is explicit and optional
- [ ] Python can access view-backed arrays without copying
