# Issue: SIMD Decode Ignores Expected Length

Priority: P1
Status: Open
Components: `src/mzml/simd.rs`

## Summary
The SIMD decoding path ignores the `expected_length` parameter and performs no length validation, allowing truncated or malformed arrays to pass. This can cause subtle data corruption and divergence from scalar decoding behavior.

## Evidence
- `src/mzml/simd.rs:24-28` accepts `_expected_length` but never checks it.
- The scalar path (`src/mzml/binary.rs`) validates length against `expected_length`.

## Impact
- Inconsistent results between SIMD and scalar decode.
- Invalid arrays may be silently accepted in parallel mode.

## Root Cause
The SIMD implementation drops validation to simplify the pipeline; it does not mirror the scalar decoder's checks.

## Proposed Fix
- Add a length check after decoding:
  - Validate `values.len()` against `expected_length` if provided.
  - Return a `BinaryDecodeError::InvalidLength` like the scalar path.
- Ensure error parity between SIMD and scalar paths.

## Streaming Requirements
- Validation must be done per-batch without additional buffering.

## Acceptance Criteria
- SIMD decode rejects incorrect lengths exactly like the scalar decoder.
- SIMD and scalar decoders produce identical error behavior on invalid inputs.

## Tests
- Add a test with a mismatched `expected_length` and ensure SIMD returns `Err`.
- Add a parity test that compares SIMD vs scalar for the same input.
