# Bounded-error integer storage for Thermo RAW spectra

Technical report · 21 October 2026

This repository hosts the study. The measured custom converter is a separate research pipeline; these timings do not measure the current `mzpeak-rs` crate or establish its compatibility with the experimental archive.

## Abstract

A direct RAW converter separates numerical signal storage from acquisition metadata. It quantizes m/z into block-local linear integers and positive intensities into logarithmic integers, preserves exact zeros, and compresses independently addressable blocks with Zstandard. Two input paths feed the same encoder: native rawr-ms and Thermo RawFileReader. The output is an experimental archive with a Python API adapter, not an official mzPeak file.

On a 13-file, 49.48 GB test set, the native path independently validated 12 files containing 3.355 billion points. Against a pinned official mzPeak converter at Zstd 3, its geometric-mean conversion speedup was **4.74× on 11 eligible files**, with **73.14% less aggregate storage**. The vendor-backed path achieved **1.74× on 10 eligible files** and **74.43% less aggregate storage**. All validated native points met 0.01 ppm m/z and 0.1% intensity limits. Native RAM was often higher; query opening remained slower. Timeouts and unsupported metadata exclude comparisons rather than becoming speedup claims.

## Motivation and scope

mzPeak combines columnar signal data and acquisition metadata in an archive, with a design intended for efficient access and interoperability [1, 2]. This experiment asks a narrower question: what happens when signal storage uses explicit relative-error budgets, compact integers and a dedicated decoder, while acquisition metadata remains accessible through familiar tables?

Numerical compression is established work. MS-Numpress, for example, studies numerical encodings for mass spectrometry [3]. The contribution examined here is the integration and measurement of a particular direct-RAW pipeline, error contract, archive layout and batched reader. These results do not establish a new compression principle or superiority over every mzPeak encoding and implementation.

The reference baseline is the official Rust converter at commit `40c65521dcfdc3d1df22c16f1ef18e6013fc6ef6`, default encoding, Zstd level 3. Both custom paths and the baseline read RAW directly; **there is no mzML bridge in this comparison**. The native metadata integration uses rawr-ms commit `2a0422f6a407aa66bb8146f636a5451eae0966ef` [4]. Its API change and the lossy archive codec are separate projects.

## Storage design

The outer ZIP stores members without an additional ZIP compression layer. `signal.bin` contains individually Zstd-compressed blocks; `signal.index.zst` describes spectra, block offsets, counts, codecs and quantizers. Acquisition metadata and chromatograms occupy separate Parquet members. The manifest records the bounds, selected input reader and member hashes.

| Component | Representation | Purpose |
|---|---|---|
| m/z | Block-local unsigned integer codes | Spend precision according to a relative-error budget |
| Positive intensity | Unsigned logarithmic codes | Bound relative error across the dynamic range |
| Zero intensity | Reserved code zero | Preserve zero positions exactly |
| Signal blocks | Up to 16,384 points per block, per axis | Bound encoding work and allow independent decoding |
| Sorted m/z codes | Delta/base subtraction and byte shuffle | Expose regularity before Zstd |
| Intensity codes | Bit packing; zero-gap/value streams when sparse | Avoid storing float exponent and mantissa redundancy |
| Index | Compressed JSON, compact native scan directory at read time | Locate blocks without scanning all signal |
| Acquisition metadata | Parquet tables and JSON metadata | Preserve scan, precursor, instrument and sample information |

Integer values use the smallest implemented 16-, 32- or 64-bit width; packed streams use the necessary bit count. Unsupported, nonfinite or negative input values are rejected. The measured configuration disables the optional sparse-transpose candidate. The decoder reconstructs float64 arrays and does not keep a decoded-spectrum cache.

### Error bounds from the quantizers

Let `b` be the positive m/z minimum in a block and `εq` the relative quantization budget. With step `Δ = 2 b εq`, define `q = round((x − b)/Δ)` and `x̂ = b + qΔ`. In exact arithmetic, `|x̂ − x| ≤ Δ/2 = b εq ≤ x εq`, because `x ≥ b`. Thus `|x̂ − x|/x ≤ εq`. The implementation uses ties-to-even rounding, a small step margin, exact-integer-range checks and an explicit reconstructed-value bound check.

For positive intensity, let `b` be the smallest positive value and `h = 2 log(1 + εI)`. Encode `q = 1 + round(log(x/b)/h)` and decode `x̂ = b exp((q − 1)h)`. Rounding gives `x̂/x ∈ [exp(−h/2), exp(h/2)] = [1/(1 + εI), 1 + εI]`. The upper relative error is at most `εI`; the lower relative error is `εI/(1 + εI) < εI`. Zero is encoded separately and reconstructed exactly. Again, floating-point reconstruction is explicitly checked rather than assumed to satisfy the exact-arithmetic argument.

The total m/z limit is `ε = 0.01 × 10⁻⁶ = 10⁻⁸`. The encoder reserves `εr = 0.1 ε` for reader differences and uses `εq = (ε − εr)/(1 + εr)`. If reader error is bounded by `εr`, then composition gives `εr + εq + εr εq = ε`. This is a conditional bound, not a universal proof about the RAW reader. Every accepted file is therefore independently checked against vendor-decoded values at the **total** 0.01 ppm limit. Physical-event source-summary m/z differences are audited separately at 0.001 ppm. The vendor path retains the same encoder margin.

For nonnegative intensities with per-point error at most `εI xᵢ`, the triangle inequality also gives `|Σx̂ᵢ − Σxᵢ| ≤ εI Σxᵢ`. A small observed whole-run drift can reflect cancellation and is not a stronger per-peak guarantee.

## Experimental design

The inventory contained **334 RAW files / 548.43 GB**, not the entire approximately 800 GB collection mentioned during planning. The tested set contains 13 files totaling **49.48 GB**: a HeLa control plus 12 selections spanning instrument, dataset and size coverage. Selection preceded timing. Input identity is recorded by SHA-256; repeated content is excluded from aggregates.

The primary three-way campaign ran on one eight-CPU, 64 GiB allocation on an Intel Xeon E5-4627 v4 node. Inputs and runtime were staged on node-local storage. Each variant received an excluded warmup, then randomized measured blocks with a warm filesystem cache. The user reduced repeats during the campaign: the first three completed cases retain seven measured blocks; remaining cases use three. For the interrupted case, the first three randomized blocks are analyzed and extra completed blocks remain in the audit trail. Binaries and numerical bounds were unchanged. This is a documented mid-campaign amendment, not a wholly preregistered three-repeat experiment.

Conversion timing includes reader startup, signal encoding, complete metadata assembly and final archive writes. Independent validation is outside that timing; no fsync durability guarantee is included. Each conversion has a 600-second timeout. A ratio requires every declared measured block to succeed, a stable signal fingerprint and independent all-point validation. Successful retained output alone does not rescue a failed repeat.

Reported speedup is the geometric mean of paired per-block baseline/candidate ratios, then equally weighted across eligible files. Storage reduction uses the sum of output bytes on that same eligible set. The eligible sets differ between native and vendor comparisons. Median seconds, median sampled tree RSS, maximum single-process RSS and output bytes are retained per file. Sampled tree RSS is summed across processes every 50 ms; it can double-count shared pages and miss brief peaks. It is not cgroup memory usage.

## Conversion results

| Path versus official default | Eligible files | Geometric-mean speedup | Total measured-time speedup | Aggregate storage reduction |
|---|---:|---:|---:|---:|
| Ours + rawr-ms | 11/13 | 4.74× | 4.81× | 73.14% |
| Ours + Thermo reader | 10/13 | 1.74× | 1.64× | 74.43% |

The total measured-time ratio divides the sum of all eligible measured baseline times by the corresponding candidate sum; it includes slow successful repeats and weights seven-repeat files more heavily than three-repeat files. It is distinct from a ratio of summed file medians. The complete [per-file results](RESULTS.md) include unsuccessful cells.

Native conversion was faster but did not universally reduce RAM. The geometric mean of native/official median sampled tree RSS was **1.72×** across 11 eligible comparisons. Vendor/official was **0.71×** across 10. On Q Exactive Plus, native used 5,379 MiB versus official's 908 MiB; vendor used 610 MiB. The signal encoder's bounded blocks do not make the entire RAW-reader and metadata pipeline constant-memory.

A vendor Eclipse repeat took 233.65 seconds, with 232.17 seconds in metadata extraction and 5.30 seconds in its overlapping signal stage. It remains in all relevant statistics. This demonstrates a metadata bottleneck for that run, not a complete explanation for every slow conversion.

A separate eight-CPU, 32 GiB campaign on another node compared native with official default and `delta:50`, both Zstd 3. It yielded 5.76× / 73.51% smaller versus default on 10 eligible files, and 6.19× / 61.42% smaller versus delta:50 on 11. These are supplementary, separate-attempt results; their times and RAM measurements are not pooled with the primary campaign.

## Numerical fidelity

Across the 12 accepted native files, validation covered **1,272,981 scans, 3,355,346,983 points and 1,009,282,702 exact zeros**. The largest observed m/z error was **0.00919254 ppm**; the largest intensity error was **0.09999990%**. The metadata checks compare exported acquisition fields and source summaries, with the declared provenance normalization and physical-event summary allowance.

Full error distributions were also computed for HeLa using the vendor path and for the saved big-Astral native archive. They are complete streaming passes, not samples. Intensity-relative statistics exclude exact zeros. Percentiles below are rounded upper endpoints of histogram intervals with widths 0.000001 ppm and 0.00001 percentage points.

| Dataset and axis | Mean absolute error | 99th percentile | Maximum |
|---|---:|---:|---:|
| HeLa m/z | 0.003498 ppm | ≈0.008167 ppm | 0.008999 ppm |
| Astral m/z | 0.001859 ppm | ≈0.007684 ppm | 0.009055 ppm |
| HeLa positive intensity | 0.049950% | ≈0.098950% | 0.09999989% |
| Astral positive intensity | 0.049948% | ≈0.098950% | 0.09999990% |

HeLa whole-run summed intensity changed by **+0.00002199%**, and its worst scan TIC changed by **0.06804%**. Astral changed by **−0.00006401%**, with worst scan TIC error **0.08622%**. Astral preserved all **162,582,445 zeros** among **1,511,811,155 points**. These TIC measurements use decoded values, not stored source summaries.

## Query behavior on the big Astral

The separate query comparison used the same preselected scans, a warm filesystem, one native/Arrow worker and seven randomized fresh-process repeats. Imports were outside the timer. No persistent index or decoded-signal cache was used. The XIC selects 494 scans in RT 20–25 minutes at m/z 524.3 ±5 ppm; the MS2 batch selects 50,208 spectra in the same RT window. That query window is not the archive's quantization limit.

| Query | Native warm query | Official warm query | Native open + first | Official open + first |
|---|---:|---:|---:|---:|
| XIC | 1.068 s | 1.234 s | 5.195 s | 3.243 s |
| MS2 batch | 4.556 s | 7.053 s | 8.693 s | 8.999 s |

The warm MS2 batch is **1.55× faster**, with 3.87 GiB versus 4.75 GiB peak process RSS. Opening takes approximately **4.12 s versus 1.84 s**. The XIC is slower end to end, and the MS2 end-to-end benefit is modest. The selected signal values were independently compared; these are two workloads, not a general SQL or DuckDB claim.

## Failures and limitations

- **Astral30 / PXD046453:** both custom paths completed signal encoding but failed during instrument metadata assembly; the official converter also failed instrument-configuration mapping. No successful complete archive or speed ratio is claimed.
- **Exploris / PXD063416:** the vendor retained output passed numerical validation, but its third measured repeat timed out at 600 seconds. The vendor comparison is excluded.
- **Big Astral / PXD049028:** native completed and validated; vendor and official default timed out during warmup. There is no measured campaign speedup against either. Native median conversion was 268.10 seconds on the primary node; the earlier 98.63-second diagnostic run is not pooled into this result.

The custom archive is lossy and is **not an official mzPeak file**. The shim covers a tested subset of the pinned Python API; it does not provide unmodified official-reader compatibility, Parquet signal querying or demonstrated SQL equivalence. Official default preserves retained values exactly and can omit interior zero runs; the custom archive preserves every source point. This is a measured storage/precision tradeoff, not equal-fidelity compression.

The experiments do not establish peptide-identification, FDR, feature-detection or downstream quantification equivalence. Instrument coverage, metadata tail latency, native-reader memory and index startup require further work. Shared-node scheduling, warm-cache measurement, a single file per selected dataset and the repeat amendment limit generalization. Independent vendor decoding is a reference oracle, not a claim that vendor software is infallible.

## Reproduction and evidence

See [reproduction and provenance](REPRODUCIBILITY.md), the [per-file table](RESULTS.md), and the [machine-readable evidence](evidence/conversion-results.json). Original reports remain immutable. Derived tables retain failed comparisons and never impute timeout durations as successful measurements. The writeup, slides and GitHub summary use the same result set.

## References

1. Van Den Bossche et al. **mzPeak: Designing a Scalable, Interoperable, and Future-Ready Mass Spectrometry Data Format.** *Journal of Proteome Research* 24 (2025), 5329–5335. [doi:10.1021/acs.jproteome.5c00435](https://doi.org/10.1021/acs.jproteome.5c00435).
2. HUPO-PSI. [mzPeak draft specification](https://hupo-psi.github.io/mzPeak-specification/) and [reference implementation at the measured commit](https://github.com/HUPO-PSI/mzPeak/tree/40c65521dcfdc3d1df22c16f1ef18e6013fc6ef6). The benchmark pins a revision, not the latest moving implementation.
3. Teleman et al. [Numerical Compression Schemes for Proteomics Mass Spectrometry Data](https://pmc.ncbi.nlm.nih.gov/articles/PMC4047472/). Numerical compression is prior art; its downstream validation does not validate this codec.
4. [rawr-ms native acquisition metadata PR](https://github.com/filiprumenovski/rawr-ms/pull/13), commit `2a0422f6a407aa66bb8146f636a5451eae0966ef`. Repository access may be required.
