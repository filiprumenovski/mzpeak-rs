# mzPeak

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

A modern, scalable, and interoperable mass spectrometry data format based on Apache Parquet.

## 5-Minute Quickstart

```bash
# 1. Convert - Transform mass spectrometry formats to mzPeak
mzpeak convert input.mzML output.mzpeak
mzpeak convert input.raw output.mzpeak
mzpeak convert input.d output.mzpeak

# 2. Query - Use any Parquet tool (DuckDB example)
duckdb -c "SELECT spectrum_id, mz, intensity
           FROM 'output.mzpeak/peaks/peaks.parquet'
           WHERE ms_level = 2 AND intensity > 1000
           LIMIT 10"
```

## Overview

mzPeak is a reference implementation for a next-generation mass spectrometry data format designed to replace XML-based standards like mzML. It leverages Apache Parquet's columnar storage for:

- **5-10x compression** over mzML files
- **Blazing fast queries** with column pruning and predicate pushdown
- **Universal compatibility** with any Parquet-compatible tool (Python, R, DuckDB, Spark)
- **Single-file container format** (`.mzpeak`) - ZIP archive for easy distribution
- **Self-contained format** with all metadata embedded and human-readable JSON
- **Streaming processing** for arbitrarily large files with minimal memory
- **Multi-format support** for Thermo RAW, Bruker .d, and mzML

### Why Parquet over mzML?

Parquet offers significant advantages for mass spectrometry data:

| Metric | mzML | mzPeak | Improvement |
|--------|------|--------|-------------|
| DDA file size | 2.5 GB | 400 MB | **6.2x smaller** |
| Random spectrum access | Sequential scan | ~100 μs | **Column pruning** |
| MS2 filtering | Load entire file | ~8 ms | **Predicate pushdown** |
| Tool compatibility | XML parsers only | Any Parquet tool | **Universal** |

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    .mzpeak Container (ZIP)                      │
├─────────────────────────────────────────────────────────────────┤
│  mimetype                    application/vnd.mzpeak             │
│  ─────────────────────────── (uncompressed, first entry)        │
├─────────────────────────────────────────────────────────────────┤
│  metadata.json               Human-readable experimental data   │
│  ─────────────────────────── (Deflate compressed)               │
├─────────────────────────────────────────────────────────────────┤
│  peaks/peaks.parquet         Long-table spectral data           │
│  ───────────────────────────  • spectrum_id: Int64              │
│                               • mz: Float64 (MS:1000040)        │
│                               • intensity: Float32 (MS:1000042) │
│                               • retention_time: Float32         │
│                               • ion_mobility: Float64 (opt)     │
│                               (Stored for seekability)          │
├─────────────────────────────────────────────────────────────────┤
│  chromatograms/*.parquet     TIC/BPC traces (wide format)       │
│  ─────────────────────────── (Stored, optional)                 │
└─────────────────────────────────────────────────────────────────┘
```

## v2.0 Schema (NEW)

mzPeak v2.0 introduces a **normalized two-table architecture** that provides 30-40% smaller file sizes:

```
output.mzpeak (v2.0)
├── mimetype                    # "application/vnd.mzpeak+v2"
├── manifest.json               # Schema version and modality
├── spectra/spectra.parquet     # One row per spectrum (metadata)
└── peaks/peaks.parquet         # One row per peak (mz, intensity, ion_mobility)
```

**Key improvements:**
- **30-40% smaller** files through elimination of repeated per-peak metadata
- **Faster metadata queries** (spectra table is small and efficient)
- **Modality-aware columns** (LC-MS, LC-IMS-MS, MSI, MSI-IMS)
- **Optimized encodings**: DELTA_BINARY_PACKED for integers, BYTE_STREAM_SPLIT for floats

See [docs/SCHEMA_V2.md](docs/SCHEMA_V2.md) for full specification.

```rust
use mzpeak::dataset::MzPeakDatasetWriterV2;
use mzpeak::schema::manifest::Modality;
use mzpeak::writer::{SpectrumMetadata, PeakArraysV2};

let mut writer = MzPeakDatasetWriterV2::new("output.mzpeak", Modality::LcMs, None)?;

let metadata = SpectrumMetadata::new_ms1(0, Some(1), 60.0, 1, 100);
let peaks = PeakArraysV2::new(vec![400.0, 500.0], vec![1000.0, 500.0]);
writer.write_spectrum_v2(&metadata, &peaks)?;

let stats = writer.close()?;
```

## Features

- **Container format**: Single `.mzpeak` file (ZIP archive) with embedded Parquet and metadata
- **Two schema versions**: v2.0 (normalized, recommended) and v1.0 (legacy, single table)
- **Long table schema**: Each peak is a row, enabling efficient Run-Length Encoding (RLE) compression on spectrum metadata
- **Ion Mobility support**: Native IM dimension for timsTOF and other IM-MS instruments
- **Multi-format input**: Convert from Thermo RAW, Bruker .d, and mzML files
- **Automatic TIC/BPC generation**: Total Ion Current and Base Peak Chromatograms automatically generated during conversion
- **Chromatogram storage**: Wide-format storage for instant trace visualization without peak table scanning
- **Rolling Writer (sharding)**: Automatic file partitioning for terabyte-scale datasets
- **HUPO-PSI CV integration**: Full controlled vocabulary support for interoperability
- **SDRF-Proteomics metadata**: Sample metadata following community standards
- **Lossless technical metadata**: Preserves instrument settings, LC configurations, pump pressures
- **Streaming mzML parser**: Memory-efficient conversion of large files
- **Human-readable metadata**: Standalone JSON file for quick inspection without Parquet tools
- **Built-in validator**: Comprehensive integrity checks to ensure format compliance and avoid fragmented implementations

## Installation

### From Source

```bash
git clone https://github.com/filiprumenovski/mzpeak-rs.git
cd mzpeak-rs
cargo build --release
```

### As a Library

### Vendor Dependencies
This repository includes a vendored copy of `timsrust` located in `vendor/timsrust`. This is a custom fork patched to support 4D MALDI imaging data, which is not yet available in the upstream crate.

Add to your `Cargo.toml`:

```toml
[dependencies]
mzpeak = "0.1"
```

### Python (Extension Module)

This repository includes optional Python bindings (PyO3 + maturin) under the Cargo feature `python`. 

**Note:** Python bindings are currently disabled in this prealpha and will be reintroduced once core features stabilize.

## Quick Start

### Supported Input Formats

mzPeak supports conversion from the following mass spectrometry data formats:

- **Thermo RAW** - Thermo Fisher Scientific instruments (Orbitrap, Q Exactive, etc.)
- **Bruker .d** - Bruker Daltonics instruments (timsTOF, SolariX, etc.)
- **mzML** - Open community standard (any vendor via conversion)

### Command Line

```bash
# Convert various formats to mzPeak container
mzpeak convert input.raw output.mzpeak
mzpeak convert input.d output.mzpeak
mzpeak convert input.mzML output.mzpeak

# Generate demo data for testing
mzpeak demo demo_run.mzpeak

# Display dataset information
mzpeak info demo_run.mzpeak

# Validate file integrity and compliance
mzpeak validate demo_run.mzpeak
```

### As a Library

```rust
use mzpeak::prelude::*;

// Create a Dataset Bundle (recommended)
let metadata = MzPeakMetadata::new();
let config = WriterConfig::default();
let mut dataset = MzPeakDatasetWriter::new("output.mzpeak", &metadata, config)?;

// Write spectra (SoA)
let peaks = PeakArrays::new(vec![400.0, 500.0], vec![10000.0, 20000.0]);
let spectrum = SpectrumArrays::new_ms1(0, 1, 60.0, 1, peaks);

dataset.write_spectrum_arrays(&spectrum)?;
let stats = dataset.close()?;

println!("Wrote {} spectra, {} peaks", 
    stats.peak_stats.spectra_written, 
    stats.peak_stats.peaks_written);
```



// Convert mzML to Dataset Bundle
let converter = MzMLConverter::new()
    .with_batch_size(1000);

let stats = converter.convert("input.mzML", "output.mzpeak")?;
println!("Converted {} spectra, {} peaks", stats.spectra_count, stats.peak_count);
```

### Reading mzPeak Files

mzPeak supports two output formats:

**Container Format (`.mzpeak`)** - Single ZIP archive (default, recommended for distribution):
```
output.mzpeak (ZIP archive)
├── mimetype                  # "application/vnd.mzpeak" (uncompressed)
├── metadata.json             # Human-readable metadata (compressed)
└── peaks/peaks.parquet       # Spectral data (uncompressed for seekability)
```

**Directory Bundle** (legacy, for development):
```
output.mzpeak/
├── peaks/peaks.parquet           # Spectral data
├── chromatograms/chromatograms.parquet  # TIC/BPC traces
└── metadata.json                  # Human-readable metadata
```

**Quick metadata inspection:**
```bash
# Container format
unzip -p output.mzpeak metadata.json | jq .

# Directory format
cat output.mzpeak/metadata.json | jq .
```

**Read peak data with any Parquet tool:**


**DuckDB**
```sql
-- Query MS2 spectra
SELECT spectrum_id, mz, intensity
FROM read_parquet('output.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 2 AND precursor_mz BETWEEN 500 AND 600;

-- Query TIC chromatogram
SELECT chromatogram_id, unnest(time_array) as time, unnest(intensity_array) as intensity
FROM read_parquet('output.mzpeak/chromatograms/chromatograms.parquet')
WHERE chromatogram_id = 'TIC';

-- Query ion mobility dimension (timsTOF data)
SELECT spectrum_id, mz, intensity, ion_mobility
FROM read_parquet('output.mzpeak/peaks/peaks.parquet')
WHERE ion_mobility IS NOT NULL 
  AND ion_mobility BETWEEN 20 AND 30
  AND mz BETWEEN 400 AND 800;

-- Aggregate statistics by IM bin
SELECT 
  FLOOR(ion_mobility / 5) * 5 AS im_bin,
  COUNT(*) as peak_count,
  AVG(intensity) as avg_intensity
FROM read_parquet('output.mzpeak/peaks/peaks.parquet')
WHERE ion_mobility IS NOT NULL
GROUP BY im_bin
ORDER BY im_bin;
```

## Chromatogram Support

mzPeak automatically generates Total Ion Current (TIC) and Base Peak Chromatogram (BPC) during mzML conversion:

- **Automatic Generation**: When converting mzML files without embedded chromatograms, TIC and BPC are automatically calculated from MS1 spectra during the streaming process
- **Preservation**: If the mzML file already contains chromatograms, they are preserved as-is
- **Wide Format**: Chromatograms are stored in "wide" format (arrays of time and intensity values) for instant visualization without scanning the peak table
- **MS1 Only**: Only MS1 spectra contribute to TIC/BPC calculation; MS2+ spectra are ignored

### Example

```rust
use mzpeak::mzml::MzMLConverter;
use mzpeak::reader::MzPeakReader;

// Convert mzML with automatic TIC/BPC generation
let converter = MzMLConverter::new();
let stats = converter.convert("input.mzML", "output.mzpeak")?;

println!("Converted {} spectra", stats.spectra_count);
println!("Generated {} chromatograms", stats.chromatograms_converted);

// Read back chromatograms
let reader = MzPeakReader::open("output.mzpeak")?;
let chromatograms = reader.read_chromatograms()?;

for chrom in chromatograms {
    println!("{}: {} points", chrom.chromatogram_id, chrom.time_array.len());
}
```

Run the demo:
```bash
cargo run --example chromatogram_generation_demo
```

## Validation

mzPeak includes a comprehensive validator to ensure file integrity and compliance with the format specification. This helps prevent the "fragmented implementation" issues seen in mzML.

### Validation Checks

The validator performs 4 categories of checks:

1. **Structure Check**
   - Validates file/directory structure
   - Checks for required files (`metadata.json`, `peaks/peaks.parquet`)
   - Verifies valid Parquet format

2. **Metadata Integrity**
   - Deserializes and validates `metadata.json`
   - Verifies Parquet footer metadata
   - Checks format version matches specification

3. **Schema Contract**
   - Validates all required columns exist with correct data types
   - Verifies HUPO-PSI CV accessions (e.g., MS:1000040 for m/z)
   - Checks column names match specification

4. **Data Sanity** (samples first 1,000 rows)
   - Asserts m/z > 0
   - Asserts intensity >= 0
   - Asserts ms_level >= 1
   - Verifies retention_time is non-decreasing

### Usage

```bash
# Validate a file
mzpeak validate data.mzpeak.parquet

# Validate a directory bundle
mzpeak validate data.mzpeak/

# Example output
# mzPeak Validation Report
# ========================
# File: data.mzpeak.parquet
#
# [✓] Path exists
# [✓] Format: Single file
# [✓] Valid Parquet file
# [✓] Format version matches (1.0.0)
# [✓] Column: mz
# [✓] CV accession for mz: MS:1000040
# [✓] m/z values positive (sampled 1000 rows)
# ...
# Summary: 21 passed, 0 warnings, 0 failed
#
# Validation PASSED
```

The validator exits with code 0 on success, or 1 if validation fails. This makes it suitable for use in CI/CD pipelines and automated workflows.

## Schema

mzPeak uses a "long" table format where each peak is a row:

| Column | Type | Description |
|--------|------|-------------|
| `spectrum_id` | Int64 | Unique spectrum identifier |
| `scan_number` | Int64 | Original scan number |
| `ms_level` | Int16 | MS level (1, 2, etc.) |
| `retention_time` | Float32 | Retention time in seconds |
| `polarity` | Int8 | 1 for positive, -1 for negative |
| `mz` | Float64 | Mass-to-charge ratio |
| `intensity` | Float32 | Peak intensity |
| `ion_mobility` | Float64? | Ion mobility drift time (ms) |
| `precursor_mz` | Float64? | Precursor m/z (MS2+) |
| `precursor_charge` | Int16? | Precursor charge state |
| `precursor_intensity` | Float32? | Precursor intensity |
| `isolation_window_lower` | Float32? | Lower isolation offset |
| `isolation_window_upper` | Float32? | Upper isolation offset |
| `collision_energy` | Float32? | Collision energy (eV) |
| `total_ion_current` | Float64? | TIC value |
| `base_peak_mz` | Float64? | Base peak m/z |
| `base_peak_intensity` | Float32? | Base peak intensity |
| `injection_time` | Float32? | Ion injection time (ms) |

**Note**: The `ion_mobility` column enables support for ion mobility mass spectrometry (IM-MS) instruments like Bruker timsTOF, Waters SYNAPT, and Agilent IM-QTOF systems.

## Metadata

### Dataset Bundle Metadata

The Dataset Bundle includes a `metadata.json` file in the root directory for quick inspection without Parquet tools:

```json
{
  "format_version": "1.0.0",
  "created": "2026-01-09T10:00:00Z",
  "converter": "mzpeak-rs v0.1.0",
  "sdrf": {
    "source_name": "sample_01",
    "organism": "Homo sapiens",
    "instrument": "Orbitrap Exploris 480"
  },
  "instrument": {
    "model": "Orbitrap Exploris 480",
    "vendor": "Thermo Fisher Scientific"
  },
  "source_file": {
    "name": "sample_01.raw",
    "path": "/data/raw/sample_01.raw",
    "format": "Thermo RAW"
  }
}
```

### Parquet Footer Metadata

All metadata is embedded in the Parquet footer's key-value metadata, making each file self-contained. The footer is the authoritative source of truth for metadata.

**Always Present:**
- `mzpeak:format_version` - Format version (e.g., "1.0.0")
- `mzpeak:conversion_timestamp` - ISO 8601 timestamp of conversion
- `mzpeak:converter_info` - Converter software and version (e.g., "mzpeak-rs v0.1.0")

**Optional (when provided):**
- `mzpeak:sdrf_metadata` - SDRF-Proteomics sample metadata (JSON)
- `mzpeak:instrument_config` - Instrument configuration (JSON)
- `mzpeak:lc_config` - LC system configuration (JSON)
- `mzpeak:run_parameters` - Run parameters and technical settings (JSON)
- `mzpeak:source_file` - Original source file information (JSON)
- `mzpeak:processing_history` - Processing provenance chain (JSON)
- `mzpeak:raw_file_checksum` - SHA-256 checksum of original raw file

## Performance

### Compression Ratios

Typical compression ratios compared to mzML:

| Dataset Type | mzML Size | mzPeak Size | Ratio |
|--------------|-----------|-------------|-------|
| DDA Proteomics | 2.5 GB | 400 MB | 6.2x |
| DIA Proteomics | 4.0 GB | 550 MB | 7.3x |
| Metabolomics | 800 MB | 150 MB | 5.3x |

### Benchmark Results

All benchmarks were run using [Criterion.rs](https://github.com/bheisler/criterion.rs) on a modern workstation. Run `cargo bench` to reproduce these results on your system.

#### Conversion Performance

mzML to mzPeak conversion throughput:

| Dataset Size | Conversion Time | Throughput (peaks/sec) |
|--------------|-----------------|------------------------|
| 10,000 peaks | 5.5 ms | 1.8 million peaks/sec |
| 50,000 peaks | 19.5 ms | 2.6 million peaks/sec |
| 100,000 peaks | 33.9 ms | 2.9 million peaks/sec |

**Per-peak processing overhead**: ~2.5 ms per spectrum (includes metadata parsing, base64 decoding, and Parquet encoding)

#### Write Performance

Direct mzPeak file writing (bypassing mzML parsing):

| Peaks Written | Time | Throughput |
|---------------|------|------------|
| 1,000 | 2.7 ms | 377K peaks/sec |
| 10,000 | 4.8 ms | 2.1M peaks/sec |
| 100,000 | 30.8 ms | 3.2M peaks/sec |

**Scaling**: Write performance scales linearly with dataset size, demonstrating efficient batching and minimal overhead.

#### Query Performance

Random access and filtering operations on a 100,000-peak dataset (1,000 spectra):

| Operation | Time | Description |
|-----------|------|-------------|
| Random access (spectrum by ID) | ~100 μs | Direct seek to spectrum in middle of file |
| MS2 filtering | ~8 ms | Extract all MS2 spectra (66% of data) |
| Retention time range query | ~5 ms | Query 50-second RT window |
| Intensity threshold filter | ~12 ms | Filter peaks above intensity threshold |
| Top-N peak extraction | ~15 ms | Extract top 50 peaks per spectrum |

**Column pruning**: Parquet's columnar format enables reading only needed columns. Metadata-only queries (no peak data) complete in <1 ms.

### Query Performance Benefits

mzPeak leverages Parquet's advanced features for fast queries:

- **Column pruning**: Only read needed columns (e.g., metadata without peak arrays)
- **Predicate pushdown**: Filter before reading (e.g., `WHERE ms_level = 2`)
- **Row group skipping**: Skip irrelevant data blocks entirely
- **Zero-copy reads**: Direct memory mapping for maximum performance

### Scalability

mzPeak is designed for modern large-scale datasets:

| Feature | Capability | Use Case |
|---------|-----------|----------|
| **Rolling Writer** | Billions of peaks | timsTOF PASEF, DIA deep proteomes |
| **Streaming Parser** | Minimal memory footprint | Convert 100+ GB mzML files |
| **Column Pruning** | Read only needed columns | Fast metadata queries |
| **IM Dimension** | Native ion mobility support | 4D proteomics (m/z, RT, IM, intensity) |

**Example**: A 50 GB timsTOF dataset with 2 billion peaks can be:
- Converted in streaming fashion (< 2 GB RAM)
- Automatically sharded into ~25 files (2 GB each)
- Queried efficiently with column/row filtering

### Running Benchmarks

```bash
# Run all benchmarks (takes ~10 minutes)
cargo bench

# Run specific benchmark suite
cargo bench --bench conversion
cargo bench --bench query_performance
cargo bench --bench filtering

# Quick test mode
./run_benchmarks.sh --quick

# Save baseline for comparison
./run_benchmarks.sh --baseline

# Compare against baseline
./run_benchmarks.sh --compare

# View HTML reports
open target/criterion/report/index.html
```

Benchmark suites:
- **conversion**: mzML parsing and conversion throughput
- **query_performance**: Random access, range queries, MS level filtering
- **filtering**: Peak filtering, precursor m/z ranges, intensity thresholds
- **mzml_streamer**: Sequential parsing throughput for decoded and raw spectra



## API Documentation

### Dataset Writer (Recommended)

```rust
use mzpeak::prelude::*;

// Create a container file (.mzpeak) - default when path ends with .mzpeak
let metadata = MzPeakMetadata::new();
let config = WriterConfig::default();
let mut dataset = MzPeakDatasetWriter::new("output.mzpeak", &metadata, config)?;

// Or explicitly choose the output mode:
// Container mode (single ZIP file):
let mut dataset = MzPeakDatasetWriter::new_container("output.mzpeak", &metadata, config)?;
// Directory mode (legacy bundle):
let mut dataset = MzPeakDatasetWriter::new_directory("output_dir", &metadata, config)?;

// Write spectra (SoA)
let peaks = PeakArrays::new(vec![150.0, 250.0], vec![1000.0, 2000.0]);
let mut spectrum = SpectrumArrays::new_ms2(0, 1, 120.5, 1, 500.25, peaks);
spectrum.precursor_charge = Some(2);
spectrum.precursor_intensity = Some(1e6);
spectrum.collision_energy = Some(30.0);

dataset.write_spectrum_arrays(&spectrum)?;
let stats = dataset.close()?;
println!("Wrote {} spectra", stats.peak_stats.spectra_written);
```

**Container Format Notes:**
- The `.mzpeak` container is a ZIP archive
- `mimetype` file is first entry, uncompressed (like `.docx`, `.jar`)
- `metadata.json` is Deflate compressed
- `peaks/peaks.parquet` is stored **uncompressed** within the ZIP for direct seek access

### Legacy Single-File Writer

```rust
// Write to a single Parquet file (legacy format)
let metadata = MzPeakMetadata::new();
let config = WriterConfig::default();
let mut writer = MzPeakWriter::new_file("output.parquet", &metadata, config)?;

let peaks = PeakArrays::new(vec![150.0, 250.0], vec![1000.0, 2000.0]);
let mut spectrum = SpectrumArrays::new_ms2(spectrum_id, scan_number, 120.5, 1, 500.25, peaks);
spectrum.precursor_charge = Some(2);
spectrum.precursor_intensity = Some(1e6);
spectrum.collision_energy = Some(30.0);

writer.write_spectra_arrays(&[spectrum])?;
let stats = writer.finish()?;
```

### mzML Conversion

```rust
use mzpeak::mzml::{MzMLStreamer, MzMLConverter, ConversionConfig};

// Low-level streaming API
let mut streamer = MzMLStreamer::open("input.mzML")?;
let metadata = streamer.read_metadata()?;

while let Some(spectrum) = streamer.next_spectrum()? {
    // Process each spectrum with minimal memory
    println!("Spectrum {}: {} peaks", spectrum.id, spectrum.mz_array.len());
}

// High-level conversion API
let config = ConversionConfig {
    batch_size: 500,
    ..Default::default()
};
let converter = MzMLConverter::with_config(config);
let stats = converter.convert("input.mzML", "output.parquet")?;

// For large datasets: Use sharding (automatic file partitioning)
let mut config = ConversionConfig::default();
config.writer_config.max_peaks_per_file = Some(50_000_000); // 50M peaks per file

let converter = MzMLConverter::with_config(config);
let stats = converter.convert_with_sharding("huge_dataset.mzML", "output.parquet")?;
// Produces: output.parquet, output-part-0001.parquet, output-part-0002.parquet, ...
```

### Ion Mobility Support

mzPeak natively supports ion mobility data from IM-MS instruments:

```rust
use mzpeak::prelude::*;

// Create peaks with ion mobility values
let mut peaks = PeakArrays::new(vec![500.25], vec![1e6]);
peaks.ion_mobility = OptionalColumnBuf::AllPresent(vec![25.3]); // drift time in milliseconds

let spectrum = SpectrumArrays::new_ms1(0, 1, 60.0, 1, peaks);
dataset.write_spectrum_arrays(&spectrum)?;
```

**Supported Instruments:**
- Bruker timsTOF series (TIMS)
- Waters SYNAPT series (TWIMS)
- Any mzML with ion mobility arrays (CV: MS:1002893)

### Rolling Writer (Sharding for Large Datasets)

For terabyte-scale datasets, use `RollingWriter` to automatically partition output:

```rust
use mzpeak::writer::RollingWriter;

// Configure automatic sharding
let mut config = WriterConfig::default();
config.max_peaks_per_file = Some(50_000_000); // 50M peaks per file (~1-2 GB)

let mut writer = RollingWriter::new("output/dataset.parquet", &metadata, config)?;

// Write spectra - files are automatically rotated when threshold is reached
writer.write_spectra_arrays(&spectra)?;

let stats = writer.finish()?;
println!("Wrote {} peaks across {} files", 
    stats.total_peaks_written, 
    stats.files_written);
// Output files: dataset.parquet, dataset-part-0001.parquet, dataset-part-0002.parquet, ...
```

**Benefits:**
- Process datasets with billions of peaks
- Optimal file sizes for cloud storage (1-2 GB per file)
- Each file is self-contained with full metadata
- Parallel processing friendly

## Building from Source

### Requirements

- Rust 1.70 or later
- Cargo

### Build

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test

# Run with verbose output
cargo run -- -vv convert input.mzML output.parquet
```

### Running Tests

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_spectrum_conversion

# Python smoke tests (after `maturin develop`)
python -m unittest -v

# Include the slow mzML conversion smoke test
MZPEAK_RUN_SLOW=1 python -m unittest -v
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy` and address warnings
- Add tests for new functionality
- Update documentation as needed

## References

- [SCHEMA_V2.md](docs/SCHEMA_V2.md) - mzPeak v2.0 schema specification
- [HUPO-PSI MS CV](https://www.ebi.ac.uk/ols/ontologies/ms) - Controlled vocabulary
- [SDRF-Proteomics](https://github.com/bigbio/sdrf-pipelines) - Sample metadata standard
- [Apache Parquet](https://parquet.apache.org/) - File format specification

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Acknowledgments

This implementation follows the mzPeak specification developed by the mass spectrometry community to address the limitations of XML-based data formats and enable modern data science workflows.
