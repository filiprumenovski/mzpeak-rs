# Issue 007: SoA Path Lacks Test and Documentation Coverage

Priority: P2
Status: Resolved
Components: `README.md`, `tests/`, `examples/`, `benches/`

## Summary
Documentation, examples, and tests still focus on AoS APIs (`SpectrumBuilder`, `Vec<Peak>`). The SoA path is under-tested and not promoted, which risks regressions and slows adoption.

## Evidence
- Integration tests in `tests/` write/read only AoS spectra
- Examples in `examples/` use AoS builders and `Peak` objects
- README does not describe `SpectrumArrays` or array-based usage

## Impact
- SoA regressions may slip into releases
- Users are unaware of the SoA API and benefits
- Harder to evaluate performance improvements

## Proposed Fix
1. Add integration tests that write/read `SpectrumArrays`
2. Update README with SoA usage and performance guidance
3. Add at least one SoA-focused example

## Acceptance Criteria
- [x] At least one integration test covers SoA read/write
- [x] README describes SoA APIs and when to use them
- [x] An example demonstrates SoA usage end-to-end
