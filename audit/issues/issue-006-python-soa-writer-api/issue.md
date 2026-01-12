# Issue 006: Python Writer Lacks SoA/Array-Based Input

Priority: P1
Status: Resolved
Components: `src/python/writer.rs`, `src/python/types/spectrum_arrays.rs`

## Summary
Python writers only accept AoS `Spectrum` objects (`Peak` lists or `SpectrumBuilder`). There is no Python API to write spectra from NumPy arrays or `SpectrumArrays`, so the SoA pipeline is not reachable from Python ingestion.

## Evidence
- `PyMzPeakWriter.write_spectrum(s)` takes `PySpectrum` only
- No `write_spectrum_arrays` / `write_spectra_arrays` methods exist
- `SpectrumArrays` is read-only in Python

## Impact
- Python users must allocate per-peak objects to write data
- SoA architecture remains internal and inaccessible from Python
- Blocks zero-copy and high-throughput ingestion workflows

## Proposed Fix
1. Add `write_spectrum_arrays` / `write_spectra_arrays` to Python writer APIs
2. Provide a `SpectrumArrays` constructor or helper that accepts NumPy arrays
3. Support optional ion mobility via validity masks for sparse data

## Acceptance Criteria
- [x] Python can write spectra from NumPy arrays without per-peak objects
- [x] Rust writer uses `write_spectra_arrays` under the hood
- [x] A Python test covers array-based write/read

## Progress
- Added `SpectrumArrays` constructor and writer methods for array-based writes
- Added array-based write/read smoke test in `python_tests/test_smoke.py`
- Python bindings are feature-gated off in prealpha; SoA writer APIs are implemented but currently disabled
