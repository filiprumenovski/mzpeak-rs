# mzpeak-rs

Rust readers and writers for an experimental Parquet mass spectrometry format.
Converts mzML/imzML, Thermo RAW and Bruker TDF; exposes Rust and Python APIs.

This is an independent implementation. Its v1 and v2 layouts are **not the
current HUPO-PSI mzPeak specification**. The official format and reference code
are maintained by [HUPO-PSI](https://github.com/HUPO-PSI/mzPeak).

## Build and run

Requires Rust 1.98+. Initialize the pinned Bruker reader even when you do not
plan to use it: Cargo resolves optional path dependencies during builds.

```sh
git clone --recurse-submodules https://github.com/filiprumenovski/mzpeak-rs.git
cd mzpeak-rs
cargo build --locked --release

# Native RAW ingestion through the bundled rawr executable
./target/release/mzpeak-convert -v convert-thermo run.raw run.mzpeak

# mzML and external-array imzML
./target/release/mzpeak-convert -v convert run.mzML run.mzpeak
./target/release/mzpeak-convert validate run.mzpeak
./target/release/mzpeak-convert info run.mzpeak
```

For an existing checkout, run `git submodule update --init --recursive`.
Use `--profile fast`, `balanced` (default), or `max-compression` with either
conversion command. Rust and Python writers now default to balanced ZSTD level 3;
higher compression levels remain explicit options. `--legacy` selects the old peak-major Parquet layout.
Run the command's `--help` for available options.

## Thermo RAW: bundled rawr-ms

The repository includes a compiled **macOS Apple Silicon** rawr executable.
It is embedded in the mzpeak binary so an installed executable can run outside
the source checkout. No .NET runtime, Rosetta, or private Cargo dependency is
required. The original rawr-ms source repository remains separate.

On another platform, provide a compatible build of rawr:

```sh
MZPEAK_RAWR=/path/to/rawr mzpeak-convert convert-thermo run.raw run.mzpeak
```

The Rust conversion config also accepts `rawr_binary`. An explicit override
is used before the bundled executable; unsupported bundle platforms search
`PATH` for `rawr`. See [binary provenance and rebuild instructions](vendor/rawr-ms/README.md).

RAW conversion streams saved centroids through `rawr convert --format mzml`
directly into the parser. The intermediate is a pipe, not a disk file.
Temporary Parquet output is published only after the child exits successfully
and the declared spectrum count matches. An existing output is never replaced.

The represented m/z and intensity values are preserved at the schema's widths
(f64 m/z, f32 intensity). Native scan numbers, MS order, polarity, retention
time, precursor selection, isolation offsets, injection time and scan
statistics are retained when the exporter supplies them. RAW retention times
are converted from minutes to seconds. Decoder revision and centroid selection
are recorded in provenance.

This selects the source-saved centroid representation. It does not preserve
profile arrays, auxiliary controllers, every vendor field, or the complete RAW
file. Instrument identity is unavailable through the current rawr mzML header.
Collision energy from rawr is normalized energy; provenance records that unit.
Unsupported RAW layouts and decode failures are errors, not skipped scans.
Legacy v1 output rejects empty spectra; v2 preserves them.

## File layouts

The default v2 `.mzpeak` container is a ZIP archive with seekable, uncompressed
Parquet members. Parquet itself supplies compression.

| Member | Contents |
| --- | --- |
| `mimetype` | Container identifier, stored first |
| `manifest.json` | Layout version, modality and counts |
| `metadata.json` | Source and processing provenance |
| `spectra/spectra.parquet` | One metadata row per spectrum, including empty scans |
| `peaks/peaks.parquet` | Contiguous m/z and intensity columns linked by spectrum ID |
| `chromatograms/chromatograms.parquet` | Optional chromatograms |

The reader joins spectrum metadata to peak offsets without copying Arrow
buffers. mzML conversion preserves existing chromatograms and generates TIC/BPC
from MS1 spectra when they are absent. Recorded statistics take precedence over
values recomputed from peaks.

The legacy v1 layout repeats scan metadata on each peak row. Existing v1
readers and writers remain available for compatibility.

## Rust

```rust,no_run
use mzpeak::thermo::{convert_thermo, ThermoConversionConfig};
use mzpeak::reader::MzPeakReader;

convert_thermo("run.raw", "run.mzpeak", &ThermoConversionConfig::default())?;
let reader = MzPeakReader::open("run.mzpeak")?;
for spectrum in reader.iter_spectra_arrays_streaming()? {
    let spectrum = spectrum?;
    println!("scan {}: {} peaks", spectrum.scan_number, spectrum.peak_count());
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

`ThermoStreamer::next_batch` now returns owned `IngestSpectrum` values.
The old thermorawfilereader-specific spectrum converter and reader accessors
have been removed. Python's `ThermoConverter` remains available;
`centroid_spectra=False` now fails explicitly instead of silently ignoring it.

## Features

| Feature | Purpose |
| --- | --- |
| `mzml` | mzML and imzML input |
| `thermo` | RAW ingestion through the rawr executable; enables mzML |
| `tdf` | Pinned Bruker TDF reader |
| `mzml-parallel` | Parallel mzML conversion |
| `parallel-decode` | Compatibility alias for `mzml-parallel` |
| `python` | PyO3 extension; enables mzML |
| `colorized_output` | Colored validation reports |

Default features include mzML, Thermo ingestion and colored output.

## Development

```sh
cargo test --locked
cargo test --locked --features mzml-parallel,tdf
cargo test --locked --no-default-features
cargo fmt --package mzpeak -- --check
cargo clippy --locked --all-targets --features mzml-parallel,tdf
cargo doc --locked --no-deps --features mzml-parallel,tdf

# Optional real RAW corpus check
MZPEAK_TEST_RAW=/path/to/run.raw cargo test --locked --test thermo_integration native_raw_roundtrip -- --ignored

# Reproducible decode and serialization timings
cargo run --locked --release --example io_hotpath
```

Python builds use `maturin develop --features python` in a virtual environment.
The current PyO3 dependency supports Python through 3.12; use Python 3.11 for
the tested development setup. See [contributing](CONTRIBUTING.md).
