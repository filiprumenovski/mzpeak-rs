# mzpeak-rs

Streaming Rust conversion and Parquet storage for mass spectrometry. Native
Thermo RAW ingestion uses the bundled rawr-ms executable. mzML/imzML and Bruker
TDF adapters feed the same v2 dataset writer.

This is an **independent experimental layout**, not the current HUPO-PSI mzPeak
specification. See [the official project](https://github.com/HUPO-PSI/mzPeak)
for that format.

## Build and convert

Requires Rust 1.98+. Initialize the pinned optional Bruker dependency before
running Cargo; Cargo resolves its path even when TDF is disabled.

```sh
git clone --recurse-submodules https://github.com/filiprumenovski/mzpeak-rs.git
cd mzpeak-rs
cargo build --locked --release
./target/release/mzpeak-convert convert run.raw run.mzpeak
./target/release/mzpeak-convert convert run.mzML run.mzpeak
./target/release/mzpeak-convert validate run.mzpeak
./target/release/mzpeak-convert info run.mzpeak
```

`--profile fast|balanced|max-compression` selects ZSTD 1, 3 or 15. The default
is balanced. Explicit `--compression-level`, `--batch-size`, and
`--rawr-binary` options are also available. `convert-thermo` is a command alias.

The bundled rawr executable supports **macOS Apple Silicon**. Other platforms
need a compatible `rawr` on PATH, `MZPEAK_RAWR=/path/to/rawr`, or an explicit
executable path. No .NET runtime or private Cargo dependency is required.
[Binary provenance and rebuild instructions](vendor/rawr-ms/README.md) include
its pinned revision and checksum.

RAW conversion selects source-saved centroids. The mzML intermediate travels
through a pipe. Child failures, malformed arrays, incomplete documents and
scan-count mismatches fail conversion. Completed output is published without
replacing an existing destination; a failed or abandoned writer publishes
nothing. This preserves the represented f64 m/z and f32 intensity values,
including empty spectra. It does not preserve RAW profile arrays, auxiliary
controllers or every vendor field. rawr's current mzML header lacks instrument
identity; its normalized collision-energy unit is recorded in provenance.

## The v2-only API

Version 0.2 removes the v1 format, directory bundles, rolling/async/sharded
writer variants, old TOML configuration and the previous Python wrapper API.
There is one dataset writer and one streaming reader. Existing v2 ZIP
containers remain readable.

The archive contains `mimetype`, `manifest.json`, `metadata.json`,
`spectra/spectra.parquet`, `peaks/peaks.parquet`, and optional
`chromatograms/chromatograms.parquet`. Parquet members are stored without ZIP
compression so readers can seek into them directly; Parquet handles
compression. Spectrum metadata holds peak offsets/counts. The Rust reader
joins slices of Arrow buffers without extracting the archive.

```rust
use mzpeak::reader::MzPeakReader;
let reader = MzPeakReader::open("run.mzpeak")?;
for spectrum in reader.iter_spectra_arrays_streaming()? {
    let spectrum = spectrum?;
    println!("{:?}: {} peaks", spectrum.metadata.scan_number, spectrum.peak_count());
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

`MzPeakDatasetWriterV2` writes `SpectrumMetadata` plus `PeakArraysV2`.
Configuration is checked before output is published. Optional metadata remains
JSON, including original mzML file metadata, rather than a second wrapper type
system. mzML conversion preserves chromatograms and generates MS1 TIC/BPC when
none exist. Recorded scan statistics take precedence over recomputed values.

## Python

Build with `maturin develop --features python` in a Python 3.11 virtual
environment. The current PyO3 dependency supports Python through 3.12 and this
package uses NumPy 1.x.

```python
import mzpeak
import numpy as np

stats = mzpeak.convert("run.raw", "run.mzpeak")
for spectrum in mzpeak.Reader("run.mzpeak"):
    print(spectrum.metadata["scan_number"], spectrum.mz, spectrum.intensity)

with mzpeak.Writer("manual.mzpeak") as writer:
    writer.set_metadata({"provenance": {"method": "manual"}})
    writer.write({"scan_number": 41, "retention_time": 30.0},
                 np.array([400.123456789], dtype=np.float64),
                 np.array([10.5], dtype=np.float32))
```

Python uses dictionaries and NumPy arrays. Omitted spectrum IDs are assigned
in write order; metadata defaults to MS1, positive polarity and RT zero
(seconds). `peak_count` is inferred, or checked if supplied. Arrays must be
contiguous with the shown dtypes and are copied once into owned Rust buffers
at the write boundary. Spectrum iteration materializes one spectrum at a time.
`mzpeak.validate(path)` checks archive CRCs and every spectrum/peak, returning
counts on success and raising an error on failure. Reader batch size and writer
modality can be supplied explicitly; see [the compact stubs](python/mzpeak.pyi).

## Features and checks

Default features are `mzml` and `thermo`. `mzml-parallel` enables ordered batch
decode with Rayon. `tdf` enables the pinned Bruker adapter; calibration and
frame metadata mapping are retained. Decoder errors are never skipped.

```sh
cargo test --locked
cargo test --locked --no-default-features
cargo test --locked --features mzml-parallel,tdf
cargo fmt --package mzpeak -- --check
cargo clippy --locked --all-targets --features mzml-parallel,tdf
python -m pytest python_tests
cargo run --locked --release --example io_hotpath
```

[Validation records](docs/validation/README.md) separate synthetic timings,
real RAW conversion and independent fidelity checks. Historical results record
the exact code revision; they are not blanket performance claims for every
instrument or current API. [Contributing](CONTRIBUTING.md) describes the checks.
