# Issue: Production unwrap() Calls Can Panic

Priority: P1
Status: Open
Components: `src/main.rs`, `src/reader.rs`, `src/writer.rs`, `src/mzml/simd.rs`

## Summary
Several `unwrap()` calls exist in non-test code paths. These can panic on malformed input or unexpected state, violating production safety and making error handling brittle.

## Evidence
- `src/main.rs:694` and `src/main.rs:753` `partial_cmp().unwrap()` panics on NaN.
- `src/reader.rs:611` and `src/reader.rs:619` downcast unwraps on list values.
- `src/writer.rs:1343` and `src/writer.rs:1352` unwrap `current_writer`.
- `src/mzml/simd.rs:152`, `153`, `154`, `155`, `172`, `197`, `198`, `211` `try_into().unwrap()` on slices.

## Impact
- Malformed or inconsistent data can crash the process.
- Error context is lost (panic instead of structured error).
- Reduced reliability in production pipelines.

## Root Cause
The code assumes invariants that are not enforced or are only implicitly guaranteed by upstream behavior.

## Proposed Fix
- Replace all `unwrap()` in production paths with proper error propagation (`Result<T, E>`).
- For `partial_cmp`, handle NaN by defining a total ordering (e.g., `total_cmp`) or filter NaNs explicitly.
- For slice conversions in SIMD, use checked ranges and return `BinaryDecodeError::InvalidLength`.
- For `current_writer`, return `WriterError::NotInitialized` if missing.

## Streaming Requirements
- Error propagation must not require full-file buffering; errors should surface as soon as they occur.

## Acceptance Criteria
- No `unwrap()` in non-test code under `src/`.
- CI enforces `clippy::unwrap_used` (deny) for production builds.
- All previously panicking paths return structured errors.

## Tests
- Add tests that feed NaN values into sorting logic and verify deterministic behavior.
- Add tests for invalid list column types to ensure `ReaderError::InvalidFormat` is returned.
