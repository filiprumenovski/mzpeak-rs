# Issue: Python Error Mapping Drops Root Cause

Priority: P2
Status: Open
Components: `src/python/exceptions.rs`

## Summary
Rust errors are mapped to Python exceptions using `to_string()` and lose the underlying error chain. This removes context (e.g., I/O errors, parse errors) and complicates debugging.

## Evidence
- `src/python/exceptions.rs` uses `MzPeakIOError::new_err(err.to_string())` without preserving `source` or nested error context.

## Impact
- Python users receive generic messages with no causal chain.
- Harder to diagnose file corruption, schema mismatches, or I/O issues.

## Root Cause
PyO3 exception construction does not attach a `__cause__` or nested exception by default.

## Proposed Fix
- Preserve error sources by setting `__cause__` to the underlying error when present.
- Map error categories more precisely (e.g., schema errors vs. IO) and include key context fields (path, entry name, spectrum id).
- Consider a `MzPeakError` data class in Python that exposes structured fields for debugging.

## Streaming Requirements
- Error propagation should work incrementally during streaming reads, with context for the batch/spectrum that failed.

## Acceptance Criteria
- Python exceptions show root cause (via chained exceptions or structured attributes).
- Errors include file path and relevant identifiers when available.

## Tests
- Add a Python test that triggers a nested error and asserts the `__cause__` chain is present.
