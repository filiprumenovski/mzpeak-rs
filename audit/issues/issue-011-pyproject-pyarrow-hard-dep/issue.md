# Issue: pyarrow Is a Mandatory Python Dependency

Priority: P2
Status: Open
Components: `pyproject.toml`

## Summary
`pyarrow` is listed as a mandatory dependency even though only a subset of APIs (`to_arrow`, `to_pandas`, `to_polars`) require it. This inflates installation footprint and may fail on systems without Arrow wheels.

## Evidence
- `pyproject.toml` lists `pyarrow>=14.0.0` under `dependencies`.

## Impact
- Users who only need conversion/writing are forced to install `pyarrow`.
- Increases install size and may block installation on constrained environments.

## Root Cause
Dependencies were aligned with a convenience API rather than minimal core requirements.

## Proposed Fix
- Move `pyarrow` to an optional extra (e.g., `extra = ["pyarrow"]`).
- Keep `pandas` and `polars` extras dependent on `pyarrow` if needed.
- Ensure `to_arrow` and related methods raise a clear `ImportError` when `pyarrow` is missing.

## Streaming Requirements
- None directly, but smaller default dependencies improve portability.

## Acceptance Criteria
- Base install does not require `pyarrow`.
- `pip install mzpeak[arrow]` (or similar extra) provides Arrow support.
- Error messages are clear when Arrow is missing.

## Tests
- Add a Python test that verifies `to_arrow` raises `ImportError` when `pyarrow` is not installed.
