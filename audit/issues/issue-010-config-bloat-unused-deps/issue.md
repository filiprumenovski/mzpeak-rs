# Issue: Config Bloat from Unused Dependencies and Features

Priority: P2
Status: Open
Components: `Cargo.toml`

## Summary
There are unused dependencies and features that increase compile time and binary size without providing functionality.

## Evidence
- `Cargo.toml` includes `indicatif = "0.17"` but no usage in `src/` (no progress bars).
- `arrow` dependency enables `prettyprint` but `prettyprint` is not used.

## Impact
- Longer compile times and larger dependency tree.
- Harder maintenance and increased CI times.

## Root Cause
Dependencies were added during prototyping and not removed when code paths changed.

## Proposed Fix
- Remove `indicatif` from `Cargo.toml` or gate it behind a feature used by a CLI progress bar.
- Remove the `prettyprint` feature from `arrow` unless it is actually used.
- Audit `Cargo.toml` for other unused optional features.

## Streaming Requirements
- No direct streaming impact, but smaller deps make CI and iteration faster.

## Acceptance Criteria
- `cargo build` succeeds after removing unused dependencies.
- `cargo tree -d` shows reduced dependency footprint.

## Tests
- Standard `cargo test` and `cargo build --all-features`.
