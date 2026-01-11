# Issue: Reader Materializes All Record Batches

Priority: P1
Status: Open
Components: `src/reader.rs`

## Summary
`MzPeakReader::read_all_batches` loads all Parquet record batches into a `Vec<RecordBatch>`. Any caller that wants to iterate now implicitly materializes the entire dataset, preventing streaming and blowing memory on large files.

## Evidence
- `src/reader.rs:293-320` builds a reader and pushes all batches into a `Vec`.
- `src/reader.rs:324` `iter_spectra` calls `read_all_batches` and reconstructs all spectra in-memory.

## Impact
- Non-streaming I/O for reading and conversion to spectra.
- No backpressure or batch-wise processing; worst-case memory usage is entire dataset.
- Breaks the "full streaming" requirement for the pipeline.

## Root Cause
The current API returns `Vec<RecordBatch>` rather than an iterator/streaming handle. Downstream APIs are layered on this eager load.

## Proposed Fix
- Introduce a streaming API that returns an iterator over `RecordBatch` (or a fallible iterator).
- Refactor `iter_spectra` to be a streaming iterator that yields spectra without requiring all batches in memory.
- Keep `read_all_batches` only as an explicit "eager" API if needed, but do not use it in streaming paths.

## Streaming Requirements
- Reading and iteration must operate in bounded memory proportional to a single batch (or small window).
- Avoid collecting into `Vec` unless a caller explicitly requests it.

## Acceptance Criteria
- A new `iter_batches` API yields batches without materializing all of them.
- A streaming spectra iterator exists and does not allocate all spectra at once.
- `to_arrow` in Python uses streaming rather than `read_all_batches`.

## Tests
- Add a test that iterates batches on a large file without allocating a full dataset.
- Add a test that iterates spectra from a large dataset and verifies memory remains bounded.
