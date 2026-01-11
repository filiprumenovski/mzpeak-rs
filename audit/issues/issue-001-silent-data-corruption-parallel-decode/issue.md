# Issue: Silent Data Corruption in Parallel Decode

Priority: P0
Status: Open
Components: `src/mzml/converter.rs` (parallel pipeline)

## Summary
The parallel conversion path drops decode errors and silently omits spectra. This produces corrupted output (missing spectra/peaks) without failing the conversion, making it impossible to trust results.

## Evidence
- `src/mzml/converter.rs:547-555` uses `filter_map` with `warn!` and returns `None` on error.
- `src/mzml/converter.rs:590-593` uses `filter_map(|raw| raw.decode().ok())` for the final batch.

## Impact
- Missing spectra/peaks with no error to the caller.
- Conversion statistics become invalid (counts and compression ratios are wrong).
- Any downstream analysis may be incorrect while appearing successful.

## Failure Mode / Repro
- Create or mutate an mzML where one spectrum has malformed Base64, truncated binary arrays, or inconsistent array lengths.
- Run parallel conversion (`parallel-decode` feature or `--parallel` path).
- The conversion completes successfully but silently drops the malformed spectrum.

## Root Cause
The parallel decode path intentionally ignores decode errors by converting `Err` to `None`. The conversion loop continues and writes partial results.

## Proposed Fix
- Make decode errors fatal by returning `Err(ConversionError::MzMLError)` (or a new `ConversionError::DecodeError`).
- Use a fallible parallel collect pattern to preserve error context:
  - `raw_batch.par_drain(..).map(|raw| raw.decode()).collect::<Result<Vec<_>, _>>()`
  - Convert decode errors to a structured error type with spectrum index/id for context.
- Ensure the final batch uses the same error-propagating path.

## Streaming Requirements
- The parallel pipeline must remain streaming-safe: error handling should not require buffering all spectra beyond the configured batch.

## Acceptance Criteria
- Any decode error causes conversion to return `Err`.
- The error includes spectrum index or ID.
- The output is never written in a partially-corrupted state for a failed conversion (or is explicitly documented if partial outputs are allowed).

## Tests
- Add a test mzML with a single corrupted spectrum and verify `convert_parallel` returns `Err`.
- Add a parity test: sequential and parallel conversions produce identical spectrum/peak counts for valid input.
- Add a regression test that ensures no warning-only paths exist for decode errors.
