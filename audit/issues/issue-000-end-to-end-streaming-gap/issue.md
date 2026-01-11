# Issue: End-to-End Pipeline Is Not Fully Streaming

Priority: P0
Status: Open
Components: `src/reader.rs`, `src/python/reader.rs`, container IO

## Summary
The current pipeline does not satisfy full streaming requirements. Multiple layers materialize entire datasets in memory, preventing bounded-memory processing and making large files impractical.

## Evidence
- Container reader buffers `peaks.parquet` in memory (`src/reader.rs:190-195`).
- `read_all_batches` collects all batches into a `Vec` (`src/reader.rs:293-320`).
- `iter_spectra` reconstructs all spectra in memory (`src/reader.rs:324` and `src/python/reader.rs:205-218`).
- Python `to_arrow` builds a list of batches (`src/python/reader.rs:232-257`).

## Impact
- Large datasets exceed memory limits.
- No backpressure or batch-wise processing.
- Incompatible with production streaming pipelines and workflow engines.

## Root Cause
API boundaries were designed around eager data structures (Vec, full materialization), and streaming-friendly abstractions are missing.

## Proposed Fix
- Implement streaming `RecordBatch` and spectrum iterators in Rust.
- Provide a seekable container reader for `.mzpeak` entries (no full buffering).
- Expose streaming Arrow C data interface in Python.
- Ensure Python iterators are lazy and release the GIL between batches.

## Acceptance Criteria
- Every read path can operate in bounded memory proportional to batch size.
- Python `iter_spectra` and `to_arrow` are streaming.
- Container reads do not copy entire `peaks.parquet` into memory.

## Tests
- Add memory-bounded integration tests for large files.
- Add a Python test that iterates through a large file without full materialization.
