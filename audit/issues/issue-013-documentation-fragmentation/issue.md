# Issue: Documentation Fragmentation and Redundancy

Priority: P2
Status: Open
Components: top-level `.md` files

## Summary
Multiple overlapping documentation files cover the same material, which increases maintenance burden and causes divergence.

## Evidence
- `BENCHMARKS.md` duplicates `benches/README.md` and README performance claims.
- `CONTAINER_FORMAT.md` overlaps with `TECHNICAL_SPEC.md`.
- `COMMIT_MESSAGE.txt` is a commit artifact and not user-facing documentation.

## Impact
- Conflicting or stale information.
- Slower onboarding and higher doc maintenance cost.

## Root Cause
Incremental documentation added during development without consolidation.

## Proposed Fix
- Consolidate format specs into a single authoritative document under `docs/`.
- Keep benchmark docs in `benches/README.md` and link from `README.md`.
- Remove `COMMIT_MESSAGE.txt` and any obsolete dev notes.

## Streaming Requirements
- None directly.

## Acceptance Criteria
- One canonical format spec.
- One canonical benchmark guide.
- No stale or duplicate top-level doc files.

## Tests
- N/A (documentation cleanup).
