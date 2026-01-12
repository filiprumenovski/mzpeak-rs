# Issue 010: Vendor + MS Type Coverage Plan

Priority: P0
Status: Open
Components: vendor adapters, ingestion contract, tests

## Summary
We need a formal thin-waist ingestion contract and a coverage matrix that
maps vendors and MS acquisition types to required fields and tests. This is
the basis for "comprehensive coverage" claims.

## Evidence
- No documented ingestion contract beyond `SpectrumArrays`
- No explicit vendor/mode coverage matrix
- No test fixtures for vendor files

## Impact
- Incomplete or inconsistent vendor support
- Risk of silent metadata loss
- No objective way to track coverage progress

## Proposed Fix
1. Define a thin-waist ingestion contract (required + optional fields)
2. Publish a vendor/mode coverage matrix and track status
3. Add vendor fixture + compliance tests per row in the matrix
4. Require explicit acceptance criteria before marking a vendor/mode as supported

## Acceptance Criteria
- [ ] `audit/vendor_coverage.md` defines the contract and matrix
- [ ] Each vendor/mode row has a fixture, conversion test, and readback test
- [ ] Coverage status reflects real test pass/fail

## Progress
- Added `audit/vendor_coverage.md` with contract and coverage matrix
