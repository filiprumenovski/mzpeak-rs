# Issue 002: Documentation Fragmentation and Redundancy

Priority: P2
Status: Open
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
- [ ] One canonical format specification
- [ ] One canonical benchmark guide
- [ ] No stale or duplicate top-level doc files
