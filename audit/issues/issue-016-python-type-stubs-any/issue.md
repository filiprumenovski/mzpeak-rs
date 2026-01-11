# Issue: Python Type Stubs Use Any Instead of Concrete Types

Priority: P2
Status: Open
Components: `python/mzpeak.pyi`

## Summary
The type stub file uses `Any` for exception parameters in context manager methods and does not fully leverage concrete types. This reduces type safety and obscures correct usage in static analysis.

## Evidence
- `python/mzpeak.pyi` defines `__exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool` for readers and writers.

## Impact
- Static type checking cannot validate context manager usage or exception types.
- Developers lose autocomplete and safety benefits that a precise stub can provide.

## Root Cause
Stubs were generated or written with generic placeholders instead of `Optional[type[BaseException]]`, `Optional[BaseException]`, and `Optional[TracebackType]`.

## Proposed Fix
- Replace `Any` with precise context manager types:
  - `exc_type: Optional[type[BaseException]]`
  - `exc_val: Optional[BaseException]`
  - `exc_tb: Optional[TracebackType]`
- Import `TracebackType` from `types` in the stub.

## Streaming Requirements
- None.

## Acceptance Criteria
- Type checkers (mypy/pyright) accept the stubs and provide correct inference for context managers.

## Tests
- Run mypy/pyright on a small sample using `with mzpeak.MzPeakReader(...)`.
