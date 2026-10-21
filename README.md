<div align="center">

# mzpeak-rs

### Store spectra. Measure size, speed, and error.

An independent Rust implementation using Apache Arrow and Parquet.<br>
Includes a benchmark study of direct RAW conversion.

[**Results**](#measured-results) · [**Design**](#integer-storage) · [**Quick start**](#use-the-rust-crate) · [**Writeup**](benchmarks/raw-integer-2026-10/WRITEUP.md) · [**Slides**](benchmarks/raw-integer-2026-10/raw-integer-archive.pptx)

[![License: MIT or Apache-2.0](https://img.shields.io/badge/license-MIT%20or%20Apache--2.0-087f8c)](#attribution-and-license)
[![Evidence: 13 RAW files](https://img.shields.io/badge/evidence-13%20RAW%20files-142630)](benchmarks/raw-integer-2026-10/RESULTS.md)
[![Validation: 3.355 billion points](https://img.shields.io/badge/validation-3.355%20billion%20points-087f8c)](benchmarks/raw-integer-2026-10/evidence/validation.json)

</div>

This project tests how integer storage can reduce file size within specified error limits. The experimental converter reads Thermo RAW files directly. It uses either native **rawr-ms** or **Thermo RawFileReader**. Both readers supply data to the same encoder.

This repository contains the Rust crate and the benchmark study. **The study measures a separate experimental converter. It does not measure the current Rust crate.** The experimental archive is not an official mzPeak file. The [HUPO-PSI mzPeak project](https://github.com/HUPO-PSI/mzPeak) maintains the official implementation.

## Measured results

**13 RAW files · 49.48 GB · complete conversion · independently validated output**

Against official mzPeak default encoding at Zstd level 3:

| Experimental input path | Eligible files | Conversion speedup¹ | Less output storage² | Relative tree RSS³ |
|:--|--:|--:|--:|--:|
| **Native rawr-ms** | **11/13** | **4.74×** | **73.14%** | 1.72× official |
| **Thermo RawFileReader** | **10/13** | **1.74×** | **74.43%** | 0.71× official |

¹ Each file has equal weight in the geometric mean. Each speedup uses paired measurements from the same benchmark block.

² Storage reduction uses the sum of output bytes for eligible files. The two rows use different sets of eligible files.

³ RSS means resident set size. This measurement adds the RSS of each process in the process tree. The ratio uses per-file median RSS values. A lower ratio means less memory use. Native conversion is faster, but it often uses more memory.

<!-- BEGIN INSTRUMENT COMPARISON -->
### Full instrument comparison

All 13 selected files appear below. Each cell uses the order **N / V / O**: native rawr-ms, our Thermo-reader path, and official default.

Our limits are 0.01 ppm for m/z and 0.1% for intensity. Official default preserves retained values exactly.

Times and memory values are medians. Memory is sampled process-tree RSS in GiB. Archive sizes use decimal MB. RAW sizes use decimal GB.

| Instrument / dataset | Acquisition / sample | RAW GB | Time, s<br>N / V / O | RSS, GiB<br>N / V / O | Archive, MB<br>N / V / O | Speedup vs O<br>N / V | Less storage vs O<br>N / V | Status<br>N / V / O |
|:--|:--|--:|--:|--:|--:|--:|--:|:--|
| Q Exactive HF<br>HeLa control | DDA; HeLa control | 0.714 | 14.86<br>34.69<br>61.82 | 1.64<br>0.74<br>1.24 | 240.5<br>240.5<br>1,080.1 | 4.12×<br>1.77× | 77.7%<br>77.7% | Pass<br>Pass<br>Pass |
| Orbitrap Fusion<br>[PXD017803](https://www.ebi.ac.uk/pride/archive/projects/PXD017803) | DDA / FAIMS; TMT, hrMS2 | 0.116 | 3.26<br>14.82<br>16.97 | 0.16<br>0.31<br>0.30 | 26.4<br>26.4<br>45.4 | 5.15×<br>1.14× | 41.9%<br>41.9% | Pass<br>Pass<br>Pass |
| Orbitrap Eclipse<br>[PXD023203](https://www.ebi.ac.uk/pride/archive/projects/PXD023203) | FAIMS / SPS; TMTpro | 0.229 | 7.44<br>34.68<br>40.06 | 0.22<br>0.34<br>0.27 | 63.5<br>63.5<br>107.1 | 5.38×<br>0.88× | 40.8%<br>40.8% | Pass<br>Pass<br>Pass |
| Orbitrap Ascend<br>[PXD043660](https://www.ebi.ac.uk/pride/archive/projects/PXD043660) | TMT16; hrMS2 | 0.689 | 15.33<br>33.62<br>59.79 | 1.54<br>0.44<br>0.64 | 222.7<br>222.7<br>829.9 | 3.94×<br>1.79× | 73.2%<br>73.2% | Pass<br>Pass<br>Pass |
| Q Exactive HF<br>[PXD019848](https://www.ebi.ac.uk/pride/archive/projects/PXD019848) | HeLa; non-FAIMS | 0.689 | 14.99<br>35.43<br>61.07 | 1.58<br>0.80<br>1.31 | 231.6<br>231.6<br>1,032.0 | 4.04×<br>1.72× | 77.6%<br>77.6% | Pass<br>Pass<br>Pass |
| Fusion Lumos<br>[PXD046211](https://www.ebi.ac.uk/pride/archive/projects/PXD046211) | TMTpro; single-cell study | 0.734 | 14.32<br>40.52<br>64.81 | 1.34<br>0.45<br>0.76 | 193.4<br>193.4<br>620.7 | 4.53×<br>1.61× | 68.8%<br>68.8% | Pass<br>Pass<br>Pass |
| LTQ Orbitrap Velos<br>[PXD005780](https://www.ebi.ac.uk/pride/archive/projects/PXD005780) | DDA; gut metaproteome | 0.936 | 14.65<br>29.90<br>68.08 | 2.47<br>0.48<br>1.04 | 351.8<br>351.8<br>1,451.4 | 5.85×<br>2.85× | 75.8%<br>75.8% | Pass<br>Pass<br>Pass |
| Q Exactive HF-X<br>[PXD053241](https://www.ebi.ac.uk/pride/archive/projects/PXD053241) | Wide-window DDA | 1.176 | 26.44<br>72.43<br>109.28 | 1.41<br>0.58<br>0.56 | 347.7<br>347.7<br>1,130.4 | 5.66×<br>2.04× | 69.2%<br>69.2% | Pass<br>Pass<br>Pass |
| Q Exactive<br>[PXD000612](https://www.ebi.ac.uk/pride/archive/projects/PXD000612) | DDA; phosphoproteome | 1.542 | 23.31<br>43.41<br>104.03 | 4.34<br>0.58<br>1.08 | 629.7<br>629.7<br>2,616.0 | 4.46×<br>2.40× | 75.9%<br>75.9% | Pass<br>Pass<br>Pass |
| Q Exactive Plus<br>[PXD002370](https://www.ebi.ac.uk/pride/archive/projects/PXD002370) | Label-free; UPS1 / yeast | 1.930 | 34.33<br>70.40<br>148.04 | 5.25<br>0.60<br>0.89 | 801.6<br>801.6<br>3,246.6 | 4.30×<br>2.07× | 75.3%<br>75.3% | Pass<br>Pass<br>Pass |
| Orbitrap Exploris 480<br>[PXD063416](https://www.ebi.ac.uk/pride/archive/projects/PXD063416) | DIA; HeLa / yeast, 10 m/z | 1.998 | 22.26<br>—<br>115.42 | 0.41<br>—<br>0.45 | 376.8<br>—<br>815.4 | 5.18×<br>— | 53.8%<br>— | Pass<br>Timeout<br>Pass |
| Orbitrap Astral*<br>[PXD046453](https://www.ebi.ac.uk/pride/archive/projects/PXD046453) | DIA; 2 Th, 30 min | 16.948 | —<br>—<br>— | —<br>—<br>— | —<br>—<br>— | —<br>— | —<br>— | Metadata<br>Metadata<br>Metadata |
| Orbitrap Astral<br>[PXD049028](https://www.ebi.ac.uk/pride/archive/projects/PXD049028) | DIA; 2 Th, 60 min | 21.777 | 268.10<br>—<br>— | 3.64<br>—<br>— | 5,467.4<br>—<br>— | —<br>— | —<br>— | Pass<br>Timeout<br>Timeout |

**Pass** requires all declared repeats and independent validation. **Timeout** means that a required run exceeded 600 seconds. **Metadata** means that complete metadata export failed.

A dash means no eligible measurement or ratio. Exploris vendor output passed numerical validation, but one repeat timed out. Its comparison remains excluded.

Speedups use paired geometric means. They can differ from ratios of displayed median times. The slow Eclipse vendor repeat remains in the calculation.

Acquisition and sample labels use selected filenames, stored scan metadata, and linked project records. The [coverage evidence](benchmarks/raw-integer-2026-10/evidence/coverage.json) records each label and its basis.

*The Astral30 instrument name comes from the project record. The benchmark reader returned an unknown instrument model, and all complete conversions failed.

This table covers Thermo RAW files. It does not claim benchmarks for every instrument or acquisition method in each source project.
<!-- END INSTRUMENT COMPARISON -->

![Conversion speedup for each eligible file. The chart compares the native reader and the Thermo reader with official default.](benchmarks/raw-integer-2026-10/conversion-speed.svg)

The native converter passed complete validation on **12/13 files**. The validator examined **3,355,346,983 points**. All **1,009,282,702 zero values** remained zero. The official converter completed only 11 of these files. Thus, only 11 files have a valid speedup comparison.

The total error limits are **0.01 ppm for m/z** and **0.1% for intensity**. The experimental archive keeps every source point. Official default keeps retained values unchanged, but it can remove interior zero runs. The two formats therefore have different precision requirements. The storage comparison includes this difference.

[All per-file times, RAM and sizes →](benchmarks/raw-integer-2026-10/RESULTS.md) · [Per-repeat JSON →](benchmarks/raw-integer-2026-10/evidence/conversion-results.json)

## Integer storage

A floating-point value stores an exponent and a mantissa. The experimental encoder converts signal values to integers within a specified relative-error limit. It then compresses the integer data.

```mermaid
flowchart LR
    RAW[Thermo RAW] --> N[Native rawr-ms]
    RAW --> V[Thermo RawFileReader]
    N --> E[Shared integer encoder]
    V --> E
    E --> M[m/z: linear integer codes]
    E --> I[Intensity: log codes + exact zero]
    M --> B[Bit packing / delta / Zstd blocks]
    I --> B
    B --> A[Experimental archive]
    RAW --> D[Acquisition metadata]
    D --> P[Parquet tables]
    P --> A
```

Both readers read RAW directly. Neither reader requires an intermediate mzML file. The user selects the reader. Native conversion does not automatically switch to the Thermo reader. The independent validator uses the Thermo reader for its reference values.

| Layer | Representation | Reason |
|:--|:--|:--|
| m/z | Linear integer codes relative to each block's minimum | Bound error relative to the original value |
| Positive intensity | Logarithmic integer codes | Allocate relative precision across the dynamic range |
| Zero intensity | Reserved zero code | Preserve every zero and its position |
| Signal | Independently compressed blocks of at most 16,384 points per axis | Decode selected blocks without a full signal scan |
| Index | Compressed block directory | Locate each spectrum's signal |
| Acquisition metadata | Parquet tables and JSON | Keep scan, precursor and instrument context accessible |

For m/z, let `b` be the minimum value in a block. A step of `Δ = 2bεq` gives a rounding error of at most `bεq`. Because `x ≥ b`, the relative error is at most `εq`.

For positive intensity, the logarithmic step is `h = 2 ln(1 + εI)`. The decoded value divided by the source value stays between `1/(1 + εI)` and `1 + εI`.

The encoder reserves a margin for reader error. It also examines the error after floating-point reconstruction. The independent validator examines the saved output.

[Quantization derivation, reader-error composition and archive layout →](benchmarks/raw-integer-2026-10/WRITEUP.md#storage-design)

## Signal error

The big Astral validation includes **all 744,726 scans and 1,511,811,155 points**. All **162,582,445 zero values** remained zero.

| Big Astral error | Mean absolute | 99th percentile⁴ | Maximum | Limit |
|:--|--:|--:|--:|--:|
| m/z, ppm | 0.001859 | ≈0.007684 | 0.009055 | 0.01 |
| Positive intensity, % | 0.049948 | ≈0.098950 | 0.09999990 | 0.1 |

⁴ The percentile values use the rounded upper endpoints of histogram intervals. The calculation includes every point. Relative-intensity statistics exclude zero values.

The sum of intensity across the file changed by **−0.00006401%**. The largest scan TIC error was **0.08622%**. TIC means total ion current. These measurements use decoded arrays. A small change in the total does not give a smaller error limit for each peak.

**The output passed numerical validation. We have not tested whether the changes affect peptide identification, false discovery rate, or downstream quantification.**

[Big Astral distributions →](benchmarks/raw-integer-2026-10/evidence/astral-fidelity.json) · [HeLa distributions →](benchmarks/raw-integer-2026-10/evidence/hela-fidelity.json)

## Query speed and startup time

The native decoder processes spectra in batches. On the big Astral, it reduces warm-query time. Archive opening still takes more time than the official reader.

| Workload | Native warm | Official warm | Native open + first query | Official open + first query |
|:--|--:|--:|--:|--:|
| XIC, 494 scans | 1.068 s | 1.234 s | 5.195 s | 3.243 s |
| MS2 arrays, 50,208 scans | 4.556 s | 7.053 s | 8.693 s | 8.999 s |

The warm MS2 batch is **1.55× faster**. Peak process RSS is **3.87 GiB**, compared with **4.75 GiB** for the official reader. Opening takes approximately **4.12 s**, compared with **1.84 s**. The XIC takes more total time when the measurement includes archive opening.

XIC means extracted ion chromatogram. Each query uses the same selected scans. Seven repeats use a new process and a randomized order. The filesystem cache is warm. Each decoder uses one worker. The benchmark uses no persistent index or decoded-signal cache. These results apply to two queries. They do not measure SQL or DuckDB performance.

[Query protocol and every sample →](benchmarks/raw-integer-2026-10/evidence/astral-queries.json)

## Failures and comparison rules

| Case | Outcome | Treatment |
|:--|:--|:--|
| Astral30 | Both custom paths encoded signal, then failed instrument metadata; official also failed metadata mapping | No complete-archive ratio |
| Exploris, vendor mode | Retained output validated; third measured conversion timed out | Vendor comparison excluded |
| Big Astral | Native completed and validated; vendor and official timed out during warmup | Native median **268.10 s**; no baseline speedup |

The primary benchmark used eight CPUs and a 64 GiB allocation. Each mode had one warmup run. The analysis excludes warmup time. The first three completed cases retain seven measured repeats. The remaining cases use three repeats after a documented protocol change. The binaries and error limits did not change.

A comparison requires all specified repeats to succeed. It also requires matching signal hashes and independent validation of every point. A 600-second timeout is a failed conversion.

A second benchmark compares official default and delta encoding on another node. Its results remain separate. Earlier process-tree RSS samples missed child processes from worker threads. Those memory measurements are invalid. The primary table uses the corrected sampler.

Run the verification script:

```bash
python3 benchmarks/raw-integer-2026-10/verify_results.py
```

The script examines all 39 case/mode cells. It verifies speedups, storage totals, repeat eligibility, signal hashes, and recorded error limits. It does not repeat RAW conversion.

[Protocol and reproduction scope →](benchmarks/raw-integer-2026-10/REPRODUCIBILITY.md) · [Frozen runtime hashes →](benchmarks/raw-integer-2026-10/evidence/execution.json) · [Failure evidence →](benchmarks/raw-integer-2026-10/evidence/failures)

## Use the Rust crate

The Rust crate provides Arrow/Parquet readers and writers. It also provides ZIP containers, mzML conversion, optional Python bindings, and optional vendor integrations. Its Parquet signal layout differs from the experimental integer archive.

### Build and convert mzML

```bash
git clone https://github.com/filiprumenovski/mzpeak-rs.git
cd mzpeak-rs
cargo build --release --locked

./target/release/mzpeak convert input.mzML output.mzpeak
./target/release/mzpeak info output.mzpeak
./target/release/mzpeak validate output.mzpeak
```

The current CLI `convert` command takes mzML input. Thermo and Bruker integrations are available through optional library features and examples. This CLI does not run the experimental converter.

| Capability | Cargo feature / entry point |
|:--|:--|
| mzML parsing and conversion | `mzml` — enabled by default |
| Python extension | `python` — build with maturin |
| Thermo RAW integration | `thermo` — vendor runtime required |
| Bruker TDF integration | `tdf` — vendored `timsrust` fork |
| Parallel mzML decoding | `mzml-parallel` |

### Write spectra from Rust

```rust
use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::MzPeakMetadata;
use mzpeak::writer::{PeakArrays, SpectrumArrays, WriterConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metadata = MzPeakMetadata::new();
    let mut run = MzPeakDatasetWriter::new(
        "example.mzpeak", &metadata, WriterConfig::default(),
    )?;
    let peaks = PeakArrays::new(vec![400.0, 500.0], vec![10_000.0, 25_000.0]);
    let spectrum = SpectrumArrays::new_ms1(0, 1, 60.0, 1, peaks);
    run.write_spectrum_arrays(&spectrum)?;
    run.close()?;
    Ok(())
}
```

This example uses the v1 dataset writer. The v2 writer separates spectrum metadata from peak rows. The ZIP container does not apply additional compression to Parquet members. Thus, readers can seek within the container. Generic Parquet tools can read extracted members. They do not automatically interpret every container or schema.

### Python

Install maturin in a Python virtual environment. Then build the extension:

```bash
maturin develop --release --features python
```

```python
import mzpeak

with mzpeak.MzPeakReader("example.mzpeak") as run:
    for spectrum in run.iter_spectra():
        print(spectrum.scan_number, spectrum.num_peaks)
```

[Python API →](docs/PYTHON_API.md) · [Rust examples →](examples) · [Schema documentation →](docs/SCHEMA_V2.md)

## Documentation and development

| Start here | Contents |
|:--|:--|
| [Technical writeup](benchmarks/raw-integer-2026-10/WRITEUP.md) | Design, mathematical bounds, methods, results and limitations |
| [Presentation](benchmarks/raw-integer-2026-10/raw-integer-archive.pptx) | 12 slides with editable charts and tables |
| [Benchmark package](benchmarks/raw-integer-2026-10/README.md) | Full result set and provenance |
| [Criterion guide](benches/README.md) | Synthetic benchmarks of the Rust crate |
| [Vendor coverage](docs/vendor_coverage.md) | Ingestion contract and coverage requirements |
| [Contributing](CONTRIBUTING.md) | Development and review workflow |

```bash
cargo test --locked
cargo fmt --all -- --check
cargo bench --bench conversion
cargo bench --bench query_performance
```

Further work must improve instrument metadata coverage, reduce native-reader memory, and reduce archive opening time. It must also test downstream proteomics results. The reproduction notes describe the lab setup. There is no portable installer for the experimental converter. This package excludes RAW datasets, proprietary vendor assemblies, and private reader source. Keep the original RAW files.

## Attribution and license

This is an independent project. The [HUPO-PSI mzPeak team](https://github.com/HUPO-PSI/mzPeak) developed the official format and reference implementation. See the [mzPeak paper](https://doi.org/10.1021/acs.jproteome.5c00435). The study uses [rawr-ms](https://github.com/filiprumenovski/rawr-ms), Thermo RawFileReader, Arrow/Parquet, and Zstandard. Earlier work on numerical compression includes [MS-Numpress](https://pmc.ncbi.nlm.nih.gov/articles/PMC4047472/).

Licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option. See vendored dependencies for their own license terms.
