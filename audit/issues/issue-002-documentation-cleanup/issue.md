# Issue 002: Documentation Fragmentation and Redundancy

Priority: P2
Status: Resolved
Components: top-level `.md` files

## Summary
Multiple overlapping documentation files cover the same material, increasing maintenance burden.

## Evidence
- `BENCHMARKS.md` duplicates `benches/README.md`
- `CONTAINER_FORMAT.md` overlaps with `TECHNICAL_SPEC.md`
- `COMMIT_MESSAGE.txt` is a commit artifact, not user docs

## Proposed Fix
1. Consolidate format specs into single `docs/FORMAT_SPEC.md`
2. Keep benchmark docs in `benches/README.md`, link from main README
3. Remove obsolete files (`COMMIT_MESSAGE.txt`, duplicate specs)

## Acceptance Criteria
- [x] One canonical format specification
- [x] One canonical benchmark guide
- [x] No stale or duplicate top-level doc files

## Progress
- Confirmed no duplicate top-level docs remain (no `BENCHMARKS.md`, `CONTAINER_FORMAT.md`, or `COMMIT_MESSAGE.txt`)
- Benchmarks are documented in `benches/README.md` and linked from `README.md`
