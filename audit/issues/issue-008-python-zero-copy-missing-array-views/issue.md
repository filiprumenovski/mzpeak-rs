# Issue: Python API Lacks Zero-Copy Array Views for Peaks

Priority: P1
Status: Open
Components: `src/python/types.rs`, `src/python/writer.rs`, `python/mzpeak.pyi`

## Summary
Python spectrum access materializes per-peak Python objects (`Peak`) and clones data into `Vec<PyPeak>`. There is no NumPy/Arrow/PyBuffer view for peak arrays, which defeats zero-copy goals and increases CPU overhead.

## Evidence
- `src/python/types.rs:168-170` `peaks()` clones every `Peak` into a Python object.
- `src/python/types.rs:100-118` `Spectrum` constructor takes `List[Peak]`, not arrays.
- `src/python/writer.rs:441-509` `SpectrumBuilder` only accepts `Vec<PyPeak>` or per-peak adds.

## Impact
- High overhead for large spectra (object allocation per peak).
- No zero-copy interop with NumPy for peak arrays.
- Python throughput becomes a bottleneck.

## Root Cause
The Python API mirrors the Rust AoS model and lacks a columnar/array interface.

## Proposed Fix
- Expose `mz`, `intensity`, and `ion_mobility` arrays as NumPy views (PyO3 + numpy crate) or via `PyBuffer`.
- Add methods like `Spectrum.mz_array()` and `Spectrum.intensity_array()` returning `memoryview`/NumPy arrays.
- Add constructor or builder methods that accept NumPy arrays or Arrow arrays to avoid per-peak object creation.
- Keep the object-based API for convenience, but make array-based APIs the fast path.

## Streaming Requirements
- Array views should reference underlying Rust buffers without copying.
- Avoid collecting all peaks into Python objects for bulk operations.

## Acceptance Criteria
- A spectrum can be round-tripped to NumPy arrays without copying.
- Bulk writes from NumPy arrays avoid per-peak Python object allocation.
- Benchmarks show reduced overhead vs current object-based path.

## Tests
- Add Python tests verifying NumPy array views share memory (e.g., `np.shares_memory`).
- Add a performance regression test for large spectra round-trips.
