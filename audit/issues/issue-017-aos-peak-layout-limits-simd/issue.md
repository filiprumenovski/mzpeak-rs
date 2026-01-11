# Issue: AoS Peak Layout Limits SIMD and Zero-Copy Interop

Priority: P1
Status: Open
Components: `src/writer.rs`, `src/reader.rs`, `src/python/types.rs`

## Summary
Core data structures use an array-of-structs layout (`Vec<Peak>` inside each `Spectrum`). This is convenient but suboptimal for SIMD, cache locality, and zero-copy export. The codebase already has a columnar API (`ColumnarBatch`), but it is not the primary path.

## Evidence
- `src/writer.rs:451-520` defines `Spectrum` with `peaks: Vec<Peak>`.
- `src/writer.rs:280-320` defines `Peak { mz, intensity, ion_mobility }` (AoS).
- Columnar API exists (`ColumnarBatch`) but is not used by readers or Python.

## Impact
- SIMD and vectorized operations are limited by AoS layout.
- Python access requires object conversion instead of zero-copy columnar views.
- Data needs repeated reshaping for analytics workflows.

## Root Cause
The initial API was designed around ergonomic Rust types, not vectorized processing.

## Proposed Fix
- Introduce a SoA representation (e.g., `SpectrumArrays` or `PeaksSoA`) with `Vec<f64>`/`Vec<f32>` columns.
- Make SoA the internal representation for reading/writing and convert to AoS only when explicitly requested.
- Expose SoA buffers directly to Python via NumPy/PyBuffer and to Arrow via `RecordBatch`.
- Migrate performance-critical code paths to operate on SoA buffers.

## Streaming Requirements
- SoA buffers should be batch-scoped and reusable to maintain bounded memory.

## Acceptance Criteria
- Reading yields SoA batches by default.
- Python and Arrow interop can share buffers without conversion.
- Benchmarks show improved throughput for compute-heavy operations.

## Tests
- Add tests that validate SoA/AoS parity for spectra round-trips.
- Add performance benchmarks comparing AoS vs SoA decode and write paths.
