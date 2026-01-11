# Issue: Python to_arrow Materializes Entire Dataset

Priority: P1
Status: Open
Components: `src/python/reader.rs`

## Summary
`MzPeakReader.to_arrow()` loads all record batches into memory and converts them one-by-one into PyArrow objects. This defeats streaming and makes Arrow export unsuitable for large files.

## Evidence
- `src/python/reader.rs:232-257` calls `reader.read_all_batches()` then converts every batch.

## Impact
- High memory usage and potential OOM on large datasets.
- No backpressure; the entire dataset must fit in RAM.

## Root Cause
The Python binding uses `read_all_batches` and wraps batches as a list instead of exposing a streaming Arrow C Data interface from a live reader.

## Proposed Fix
- Implement `__arrow_c_stream__` on a Python wrapper that holds a Rust `RecordBatchReader`.
- Avoid pre-materialization; let PyArrow pull batches from the stream.
- Provide a streaming `RecordBatchReader` in Rust backed by the Parquet reader.

## Streaming Requirements
- `to_arrow()` must return a streaming Arrow object that pulls batches on-demand.
- No full-file buffering.

## Acceptance Criteria
- `to_arrow()` can export a large file without loading all batches at once.
- Peak memory remains bounded by the batch size.

## Tests
- Add a Python test that confirms `to_arrow()` works on a large file without excessive memory use (or via a mocked reader that counts batch pulls).
