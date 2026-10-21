# Direct RAW integer archive study — October 2026

This benchmark package is hosted in **mzpeak-rs**. It measures a separate experimental direct-RAW converter with native rawr-ms and Thermo RawFileReader input paths against the pinned HUPO-PSI mzPeak converter. It does **not** measure the current mzpeak-rs crate. The custom signal archive is not official mzPeak and is not readable by this crate merely because the study is hosted here.

| Custom input path vs official default/Zstd 3 | Eligible files | Paired geometric-mean speedup | Aggregate storage reduction |
|---|---:|---:|---:|
| Native rawr-ms | 11/13 | 4.74× | 73.14% |
| Thermo RawFileReader | 10/13 | 1.74× | 74.43% |

The test set contains 49.48 GB in 13 RAW files. Custom output has total limits of **0.01 ppm m/z and 0.1% intensity**, with exact zeros. Official default retains exact values for retained points. Native RAM can be higher, and query startup remains slower. Failed comparisons remain visible and receive no speedup ratio.

- [Technical writeup and quantizer derivation](WRITEUP.md)
- [All per-file speed, RAM and storage results](RESULTS.md)
- [Reproduction scope, revisions and limitations](REPRODUCIBILITY.md)
- [12-slide presentation](raw-integer-archive.pptx)
- [Per-repeat conversion evidence](evidence/conversion-results.json)
- [Failure logs](evidence/failures) (paths relocated; original hashes retained)
- [Independent validation details](evidence/validation.json)
- [HeLa loss distributions](evidence/hela-fidelity.json) and [big-Astral loss distributions](evidence/astral-fidelity.json)
- [Astral query measurements](evidence/astral-queries.json)
- [Separate default/delta campaign](evidence/supplementary-summary.json)
- [Frozen runtime hashes](evidence/execution.json) and [repeat amendment](evidence/protocol-amendment.json)

Run `python3 benchmarks/raw-integer-2026-10/verify_results.py` from the repository root to verify the published arithmetic. Original reports remain archived; their SHA-256 identities are in [source-report-sha256.json](evidence/source-report-sha256.json). Public copies remove process arguments and relocate filesystem paths. They cannot be byte-compared with original-report hashes.

First three cases retain seven measured repeats; remaining cases use three after a documented amendment. Each mode has an excluded warmup. A comparison requires all declared measured repeats and independent validation; timeout durations are never imputed as successful conversions. The supplementary campaign ran on another node and is not pooled into primary results.

Peptide-identification and downstream quantification equivalence have not been established. The archive remains research software; retain the original RAW files.
