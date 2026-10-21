# Reproduction and provenance

This directory publishes measurements and analysis, not a portable converter distribution. The commands below describe the original research checkout; its `code/`, `native-reference/` and `native-validation/` paths are not part of this repository. They are not commands for the `mzpeak-rs` crate. No RAW files, vendor assemblies or private rawr-ms source are included.

To check the published arithmetic without those dependencies, run `python3 benchmarks/raw-integer-2026-10/verify_results.py` from the repository root. This rechecks comparison eligibility, paired speedups, storage reductions and validation limits against the included JSON evidence; it does not rerun RAW conversion.

## Read the result set

The primary comparison is `native-validation/readers-three`, allocation 40651081 on mem1 (eight CPUs, 64 GiB). The separate default/delta comparison is `native-validation/campaign-three`, allocation 40639244 on tta1 (eight CPUs, 32 GiB). The main README uses the latest matched default comparison from mem1 and delta comparison from tta1. Each row retains its own native measurement. The signal binaries differ (`f27b8331` and `53a4ac2e`). Their timings must not be pooled; the later binary has no delta comparison. Both allocations finished and were released after reports and failed outputs were persisted remotely.

Original seven-repeat evidence remains in `native-validation/readers` and the remote original campaign results. The continuation's `protocol-amendment.json` documents the reduction to three measured blocks. Completed cases retain seven; unfinished cases use their first three randomized measured blocks. Each mode has one excluded warmup. No binary changed during the amendment.

Regenerate derived summaries and the publication table from existing reports:

```bash
python3 native-validation/summarize_readers.py native-validation/readers-three
python3 native-validation/summarize.py native-validation/campaign-three
python3 publication/build_results.py
```

The small publication JSONs omit home directories and process arguments. `evidence/source-report-sha256.json` identifies the exact case reports used. Full original reports preserve commands, inputs, binary hashes, runtime settings, per-repeat time/RSS, validation results and failure logs. The public table includes all 39 primary comparison cells, including failures.

## Input and software identity

The selected 13 files total 49,477,787,195 bytes. Dataset accessions, filenames, SHA-256 identities and sizes are in `evidence/conversion-results.json`. The inventory contained 334 files / 548,426,125,080 bytes; it is not a claim that the entire larger RAW collection was benchmarked.

Pinned dependencies:

- mzPeak reference: `40c65521dcfdc3d1df22c16f1ef18e6013fc6ef6`.
- rawr-ms native metadata: `2a0422f6a407aa66bb8146f636a5451eae0966ef`, PR 13.
- Thermo CommonCore oracle: 8.0.42.0; .NET 10 adapters.
- Converter Python environment: Python 3.12, PyArrow 17. Validation/compatibility environment: PyArrow 20. Keep the pinned reference Python package on `PYTHONPATH`; an unrelated installed `mzpeak` package can expose a different API.
- Rust 1.98; individual Cargo.lock files pin the Rust dependency graphs.

The benchmark runtimes are frozen snapshots. Do not infer a converter's identity solely from a version label: `execution.json` records executable SHA-256 values. Local source changes made for documentation are not retroactively part of measured builds.

## Build the custom paths

This is a research checkout, not a one-command distributable package. `code/fast_raw` expects the rawr-ms checkout under `code/vendor/rawr-ms`. `code/native_metadata` also expects the pinned native metadata writer dependency under `native-reference`. Inspect their Cargo manifests before relocating the source. Access to rawr-ms may be required.

```bash
cargo build --release --locked --manifest-path code/fast_raw/Cargo.toml --bin fast_raw
cargo build --release --locked --manifest-path code/native_metadata/Cargo.toml
CARGO_TARGET_DIR=code/fast_raw/target cargo build --release --locked --manifest-path code/native_decoder/Cargo.toml
```

Build the vendor encoder without rawr-ms dependencies:

```bash
CARGO_TARGET_DIR=code/fast_raw/target-vendor \
  cargo build --release --locked --no-default-features \
  --manifest-path code/fast_raw/Cargo.toml --bin fast_raw
```

The vendor mode additionally requires the DirectRawFast adapter and a metadata-only helper built from `code/metadata_only.rs` against the pinned official source with Thermo support. The C# project currently contains lab-specific vendor assembly paths; configure them for a locally supplied CommonCore installation. The experiment does not redistribute those assemblies. See `code/README-mzpeak-compat.md` for DLL overrides and runtime configuration.

## Validate independently

A successful conversion and an internal integer roundtrip are insufficient. Produce a vendor reference for the same input, then run the persisted-output validator with the vendor RAW reader available:

```bash
PYTHONPATH=code:/path/to/pinned-mzPeak/python \
  python3 code/validate_complete.py new-output --numerical \
  --raw input.raw --reference official.mzpeak
```

The result must explicitly report all-spectra/all-points validation, metadata checks, exact zero preservation and total numerical bounds. Source-path relocation is permitted only through the explicit audited mode with unchanged source identity. Physical-event source-summary m/z differences use a separate 0.001 ppm allowance; they do not relax the signal's 0.01 ppm bound.

To measure complete error distributions on a previously validated archive:

```bash
PYTHONPATH=code:/path/to/pinned-mzPeak/python \
  python3 code/measure_fidelity.py input.raw \
  new-output/complete.experimental.zip new-output/validation.json fidelity.json
```

This streams every point, checks source-array identity against the prior validation, preserves zero masks and accumulates bounded histogram percentiles. It also computes TIC from reconstructed arrays. It does not perform peptide identification or downstream biological analysis.

## Interpret comparisons

- A timeout is not a successful 600-second conversion. No ratio is computed against it.
- Speedup uses paired randomized blocks, then a geometric mean across eligible unique files.
- `aggregate_time_speedup` in the existing summary code is a ratio of sums of file medians. The writeup's **total measured-time** ratio is separately computed from all eligible measured runs, including slow repeats.
- Storage reductions use summed bytes on the exact comparison's eligible set.
- Primary sampled tree RSS includes children launched by worker threads. Legacy tree samples from the earlier native campaign are invalid; use that campaign's GNU time maximum single-process RSS instead.
- Query benchmarks are separate experiments with separate fixtures and timing scope. They do not establish SQL performance or conversion throughput.

## Current release gaps

Astral30 complete metadata support, vendor metadata tail latency, native-reader memory and eager index parsing remain open. The source has research-environment paths and is not yet a polished installer. Preserve the original RAW files: this archive is lossy and experimental. A general-purpose release needs a portable setup flow, broader interoperability tests and downstream proteomics validation.

## Successful official Astral query preparation

Allocation 40648979 on mem4 produced a complete official-default Astral archive after the paired campaigns had enforced a 600-second conversion limit. The retained archive is 11,071,619,496 bytes. GNU time records one successful conversion at 1,374.09 seconds and 1,708,998,656 bytes maximum process RSS. This was query preparation, not a matched repeated conversion benchmark. Query-selected points were checked; no full-source official validation is recorded for this output. It is excluded from campaign aggregates. See the [record](evidence/astral-official-single.json) and [sanitized timing log](evidence/astral-official-single.time). The original log hash and query-recorded archive hash are preserved in the record.
