# Issue: pytest Test Paths Mismatch

Priority: P2
Status: Open
Components: `pyproject.toml`

## Summary
pytest configuration points to a non-existent directory (`python/tests`) while the repository uses `python_tests`. This can cause tests to be silently skipped.

## Evidence
- `pyproject.toml` sets `testpaths = ["tests", "python/tests"]`.
- The repo contains `python_tests/`, not `python/tests/`.

## Impact
- Python tests may not run locally or in CI depending on invocation.
- Silent test omissions reduce coverage and confidence.

## Root Cause
Directory rename or project restructure not reflected in pytest config.

## Proposed Fix
- Update pytest `testpaths` to include `python_tests`.

## Streaming Requirements
- None.

## Acceptance Criteria
- `pytest` discovers and runs tests from `python_tests/`.

## Tests
- Run `pytest -q` and confirm expected test count includes Python tests.
