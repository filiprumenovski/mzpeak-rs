# Issue: Python iter_spectra Is Eager but Documented as Lazy

Priority: P1
Status: Open
Components: `src/python/reader.rs`, `python/mzpeak.pyi`

## Summary
`MzPeakReader.iter_spectra()` is documented as memory-efficient and lazy, but it eagerly loads all spectra into a `Vec` before returning a Python iterator. This is misleading and unsafe for large datasets.

## Evidence
- `src/python/reader.rs:205-218` uses `reader.iter_spectra()` and collects into `Vec<PySpectrum>`.
- `src/python/reader.rs:205-209` docstring claims memory-efficient lazy iteration.
- `python/mzpeak.pyi` echoes the lazy iteration description.

## Impact
- Python users will accidentally load the entire dataset into memory.
- Inconsistent API contract damages trust and leads to unexpected crashes.

## Root Cause
The Rust reader API is eager; Python wrapper mirrors it without clarifying behavior.

## Proposed Fix
- Implement a true streaming iterator in Rust and expose it in Python.
- If streaming is not immediately available, update docs to clearly mark `iter_spectra` as eager and provide a new streaming API.
- Ensure the streaming iterator releases the GIL during batch processing.

## Streaming Requirements
- Python iteration must be lazy and operate in bounded memory.
- Avoid constructing `Vec<PySpectrum>` for large files.

## Acceptance Criteria
- `iter_spectra` yields spectra incrementally without full materialization.
- Documentation and type stubs match actual behavior.

## Tests
- Add a Python test that iterates over a large file and confirms bounded memory (or at least validates that only a small number of spectra are resident at any time).
