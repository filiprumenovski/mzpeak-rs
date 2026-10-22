# Big Astral Python adapter validation

**All 744,726 scans were checked.** With `filter_profile_zeros=True`, the adapter's **1,392,038,446 retained points** match the saved official-default archive in scan identity, order, count, zero positions, and the declared numerical bounds.

The default adapter preserves all **1,511,811,155 native points**, including **162,582,445 zeros**. Compatibility mode removes **119,772,709 interior profile zeros**, exactly as the official exporter does. Both filtered outputs retain **42,809,736 zeros**. No nonzero point is removed.

| Check | Result |
|:--|:--|
| Maximum m/z difference | **0.00905505 ppm**, below 0.01 ppm |
| Maximum relative intensity difference | **0.0999999%**, below 0.1% |
| Spectrum and scan metadata | All 744,726 rows agree exactly |
| Precursors and selected ions | All 737,461 rows in each table agree exactly |
| XIC: 494 scans, 20–25 min, 524.3 ± 5 ppm | 15,038,451 retained input points checked; no cutoff-membership changes; largest trace difference **0.0981616%** |
| MS2 arrays: 50,208 scans, 20–25 min | All **83,098,323 points** checked |
| Scalar, ID, list, and object slices | 157 spectrum comparisons; 69 explicit peak reads |
| Iteration and peak preference | 120 spectrum comparisons, including file-tail checks |
| Retention time | 85 finite-time queries and three time ranges checked |
| Array-range selections | Six real-file queries; duplicates, order, and slice endpoints checked |
| Chromatograms | TIC, BPC, and both stored chromatograms agree exactly |
| Other checks | Flattened columns, native member checksums, directory input, and repeated close calls |

The full signal comparison used 218 batches. The largest native batch contained 6,818,363 points. Results do not require an all-file signal array or a decoded-signal cache.

## One adapter fix

The adapter previously excluded the stop index in array-range slices. The pinned official reader includes that index. The adapter now follows the official rule. The regression test covers both native and Python dispatch paths against the official range-filter code on a small in-memory fixture.

Array-range slices include stop and ignore step. Spectrum-object slices use the official object's separate slicing behavior. The pinned implementation also treats a zero stop as unspecified: do not use `slice(0, 0)` as an empty selection. The regression fixture checks this without materializing the Astral file.

## Query boundaries and API limits

These checks establish the listed Python operations. They do not establish universal, bit-identical interchangeability.

- **Exact m/z cutoffs:** 176 deliberately narrow windows were tested on eight profile/centroid scans. There were 120 point/window membership differences. Every difference was within the 0.01 ppm quantization band; the largest distance from the cutoff was 0.00546962 ppm. This is expected when a cutoff lies at a value that quantization moves. The production XIC window above had no membership difference.
- **Coordinate argument:** our adapter applies an inclusive `coordinate_range`. The pinned official reader ignored that argument in all 176 probes. Apply the same explicit NumPy mask to each reader's returned arrays when comparing m/z windows.
- **Tail iterators:** four official object-slice, iterator, and time-slice calls reached a 30-second limit. A captured Python stack identifies the reference iterator looping after its profile stream ends. Our native results were then checked against official scalar reads for those exact scan IDs. This validates the returned data; it does not claim that the original official calls completed.
- **Empty selections:** both readers return an empty list for `run[[]]`. An empty array-range query returns empty arrays in our adapter; the pinned official reader raises `ValueError`.
- **Infinite time:** both readers inherit `KeyError(-1)` for `+∞`. Finite times, endpoints, adjacent floating-point values, and midpoint queries passed. Use a finite time or an explicit scan index.

Our arrays are float64. The official archive can expose different dtypes. Use the numerical bounds for values and exact comparisons for indices and zero masks. URI inputs, SQL integration, wavelength data, and the Python fallback decoder were not tested in this Astral run.

## Evidence and targeted retests

The source RAW is `20231206_HAP1_1ug_60min_DIA_2Th_5e4_3p5ms_rep01.raw` from PXD049028. The saved native and official archives have the same SHA-256 identities used by the earlier query benchmark. They were not modified. This is a cross-reader validation, not a new conversion benchmark or an independent official-to-RAW validation. The native archive already has separate all-point vendor-RAW validation.

The original full-run report retains two test-expectation errors. Targeted retests resolve them without changing the adapter, archived data, or signal bounds:

1. The official conversion staged the input as `astral.raw`, which also changed its derived run ID. The strict staging audit checks both known names, the source SHA-1, and every other acquisition field. Only source name/location, derived run ID, converter provenance, and instrument-configuration ordering are normalized.
2. The initial time test assumed `+∞` was supported. A separate test verifies the shared error and completes all finite-time, time-range, empty-selection, and invalid-index checks.

The evidence verifier requires exactly those two recorded errors and their successful supplemental audits. It rejects any other error. The original diagnostic and setup reports remain archived. No output is relabeled to erase an unsuccessful attempt.

- [Derived validation summary](evidence/astral-adapter.json)
- [Original full-run report](evidence/astral-adapter/contract-run.json)
- [Finite-time and empty-query retest](evidence/astral-adapter/time-contract.json)
- [Staged-source metadata audit](evidence/astral-adapter/staging-metadata-audit.json)
- [Reference tail-iterator stack](evidence/astral-adapter/official-tail-stack.txt)
- [Source/report hashes and execution scope](evidence/astral-adapter/provenance.json)
- [Tested adapter and validation-source snapshots](adapter-validation/)

## Install the reader

The [installable adapter and native decoder](../../python-adapter/README.md) now ship in this repository, with a small synthetic example pair and clean-install tests. They need no research checkout or vendor software to read existing integer archives. Package imports, library discovery, and explicit metadata-stream cleanup differ from the frozen benchmark snapshot; the saved Astral measurements remain tied to their recorded sources.

## Check or reproduce

Check the published evidence without RAW files or vendor software:

```bash
python3 benchmarks/raw-integer-2026-10/adapter-validation/verify_evidence.py
```

The validation sources are snapshots of the research checkout. They require its decoder modules, the pinned official Python reader at `40c65521dcfdc3d1df22c16f1ef18e6013fc6ef6`, NumPy, pandas, PyArrow 20, and the compiled native decoder. They are not a standalone adapter distribution or the `mzpeak-rs` Python extension.

Set `MZPEAK_COMPAT_ARCHIVE`, `MZPEAK_OFFICIAL_ARCHIVE`, and `MZLAB_DECODER_LIBRARY` to the retained fixtures and library. Put the research `code/` directory and pinned official Python package on `PYTHONPATH`. The original full-run script reproduces the recorded diagnostic expectations; run the companion staging and time audits to complete their interpretation. Input hashes and signal limits are fixed.

Whole-process RSS in the full-run report includes three simultaneous readers and two additional readers for flattened-column checks. It is a validator measurement, not a per-reader RAM result. A supplemental test ran in a separate Slurm step. No speed or RAM comparison is inferred from this run.
