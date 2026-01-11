# Issue: Container Reader Loads Entire Parquet into Memory

Priority: P1
Status: Open
Components: `src/reader.rs`

## Summary
The `.mzpeak` container reader reads the entire `peaks/peaks.parquet` entry into a `Vec<u8>` and then into `Bytes`. This eliminates streaming, inflates memory usage, and can OOM on real data.

## Evidence
- `src/reader.rs:190-195` reads `peaks.parquet` into `parquet_bytes` and `Bytes::from`.
- `src/reader.rs:673-689` (ZIP sub-parquet) reads entire sub-files (chromatograms/mobilograms) into memory before decoding.

## Impact
- Large datasets can crash or be killed by the OS.
- Prevents true streaming access and backpressure.
- Conflicts with the stated design goal of seekable ZIP entries.

## Root Cause
`SerializedFileReader` requires a `ChunkReader` (random access). The current implementation uses a `Bytes` buffer rather than a seekable view of the ZIP entry.

## Proposed Fix
- Implement a seekable reader for stored ZIP entries by using the underlying `.mzpeak` file and the ZIP entry offset.
- For `Stored` (uncompressed) entries, read directly from the container file via a custom `ChunkReader` that maps reads to `[entry_start, entry_start + entry_size)`.
- Keep a lightweight index in memory (entry offset, length) but do not buffer the data itself.
- Fallback behavior if the entry is compressed: either error out with a clear message or stream-decompress into a temp file (explicitly documented). The format spec requires `Stored`, so it is valid to fail fast for compressed entries.

## Streaming Requirements
- Reading a container must not allocate a full copy of `peaks.parquet`.
- `read_all_batches` and Arrow export should iterate over record batches without loading the entire file.

## Acceptance Criteria
- Opening a `.mzpeak` container with a large `peaks.parquet` does not allocate proportional memory.
- `MzPeakReader::open` on container performs no full-file read.
- A test confirms `peaks.parquet` is read in bounded memory (e.g., via a custom reader that counts reads or by a large synthetic file).

## Tests
- Add a test that opens a large container and confirms memory does not scale with file size (or uses a bounded allocator/feature flag for verification).
- Add a test that verifies the reader fails fast if the entry is compressed (since it violates the format spec).
