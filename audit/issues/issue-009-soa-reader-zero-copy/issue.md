# Issue 009: SoA Reader Still Copies Data Per Spectrum

Priority: P1
Status: Resolved
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
- [x] SoA streaming can return views without copying peak buffers
- [x] Materialization is explicit and optional
- [x] Python can access view-backed arrays without copying

## Progress
- Added view-backed SoA types and streaming iterator in Rust
- Added view segmentation test with small batch size to exercise cross-batch spectra
- Python bindings expose `SpectrumArraysView` with zero-copy NumPy array views
