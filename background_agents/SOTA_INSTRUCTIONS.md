# SOTA Instructions for Background Agents

Goal: make spectrum lookup in mzPeak feel state-of-the-art. The current implementation uses row-group pruning (Parquet stats) but still reads full row groups. We need true random access behavior and benchmarks that prove it.

Context
- Repo: mzpeak-rs
- Current reader: `src/reader/spectra.rs` uses row-group pruning for `get_spectrum_arrays` and `get_spectra_arrays`.
- File format: `.mzpeak` is a ZIP container with `peaks/peaks.parquet` inside.
- Python bindings are feature-gated off; ignore Python.

Primary tasks (in order)
1) Implement a spectrum index file
   - Write a compact index during dataset creation.
   - Prefer adding a new file inside the container: `index/spectrum_index.parquet` (or similar).
   - Index rows should map `spectrum_id` -> `row_group` and (optionally) `row_start`/`row_end` for peak rows.
   - Keep backwards compatibility: if index is absent, fall back to row-group pruning.
2) Update reader to use index when present
   - On open, detect the index file in containers and load it lazily or on demand.
   - For `get_spectrum_arrays`, use the index to select the minimal row group(s).
   - If row offsets are stored, use Parquet row selection to further restrict reads.
3) Add benchmarks for lookup speed
   - Add a Criterion benchmark that measures lookup for early, mid, and late `spectrum_id`.
   - Provide a CLI/example to run this against a real file.
   - Record before/after numbers in `benches/README.md`.

Design guidance
- Keep index small and fast to read. Aim for sequential scan or in-memory hash map.
- Use ASCII-only comments and docs.
- Avoid breaking the existing container format; index is optional.
- Prefer row selection only if it is reliable in parquet-rs; otherwise, row-group selection is enough.

Acceptance criteria
- Single lookup should be in milliseconds, not seconds, on large files.
- Fallback path still works when index is missing.
- New benchmarks run in CI without requiring huge data files.
- Code stays SoA-only; no AoS reintroduction.

Suggested file touchpoints
- `src/dataset/writer_impl.rs` (emit index)
- `src/reader/open.rs` and `src/reader/spectra.rs` (consume index)
- `benches/query_performance.rs` or new `benches/lookup_speed.rs`
- `benches/README.md` (document)

Notes
- The dataset `data/A4_El_etdOT.mzML` can be converted to mzPeak for real-world testing.
- Keep feature flags minimal; use existing defaults unless required.
