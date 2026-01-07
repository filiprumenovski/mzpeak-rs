# mzPeak

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

A modern, scalable, and interoperable mass spectrometry data format based on Apache Parquet.

## Overview

mzPeak is a reference implementation for a next-generation mass spectrometry data format designed to replace XML-based standards like mzML. It leverages Apache Parquet's columnar storage for:

- **5-10x compression** over mzML files
- **Blazing fast queries** with column pruning and predicate pushdown
- **Universal compatibility** with any Parquet-compatible tool (Python, R, DuckDB, Spark)
- **Self-contained files** with all metadata embedded in the Parquet footer
- **Streaming processing** for arbitrarily large files with minimal memory

## Features

- **Long table schema**: Each peak is a row, enabling efficient Run-Length Encoding (RLE) compression on spectrum metadata
- **HUPO-PSI CV integration**: Full controlled vocabulary support for interoperability
- **SDRF-Proteomics metadata**: Sample metadata following community standards
- **Lossless technical metadata**: Preserves instrument settings, LC configurations, pump pressures
- **Streaming mzML parser**: Memory-efficient conversion of large files

## Installation

### From Source

```bash
git clone https://github.com/your-org/mzpeak.git
cd mzpeak
cargo build --release
```

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
mzpeak = "0.1"
```

## Quick Start

### Command Line

```bash
# Convert mzML to mzPeak format
mzpeak convert input.mzML output.mzpeak.parquet

# Generate demo data for testing
mzpeak demo demo_run.mzpeak.parquet

# Display file information
mzpeak info demo_run.mzpeak.parquet
```

### As a Library

```rust
use mzpeak::prelude::*;
use mzpeak::mzml::MzMLConverter;

// Convert mzML to mzPeak
let converter = MzMLConverter::new()
    .with_batch_size(1000);

let stats = converter.convert("input.mzML", "output.mzpeak.parquet")?;
println!("Converted {} spectra, {} peaks", stats.spectra_count, stats.peak_count);
```

### Reading mzPeak Files

mzPeak files are standard Parquet files and can be read with any compatible tool:

**Python (pyarrow)**
```python
import pyarrow.parquet as pq

table = pq.read_table('output.mzpeak.parquet')
df = table.to_pandas()

# Query MS2 spectra only
ms2 = df[df['ms_level'] == 2]
```

**R (arrow)**
```r
library(arrow)

data <- read_parquet('output.mzpeak.parquet')
ms2_spectra <- data[data$ms_level == 2, ]
```

**DuckDB**
```sql
SELECT spectrum_id, mz, intensity
FROM read_parquet('output.mzpeak.parquet')
WHERE ms_level = 2 AND precursor_mz BETWEEN 500 AND 600;
```

**Polars (Python)**
```python
import polars as pl

df = pl.scan_parquet('output.mzpeak.parquet')
result = df.filter(pl.col('ms_level') == 2).collect()
```

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

## Metadata

All metadata is stored in the Parquet footer's key-value metadata:

- `mzpeak:format_version` - Format version (e.g., "1.0.0")
- `mzpeak:sdrf_metadata` - SDRF-Proteomics sample metadata (JSON)
- `mzpeak:instrument_config` - Instrument configuration (JSON)
- `mzpeak:lc_config` - LC system configuration (JSON)
- `mzpeak:run_parameters` - Run parameters and technical settings (JSON)
- `mzpeak:source_file` - Original source file information (JSON)
- `mzpeak:processing_history` - Processing provenance chain (JSON)

## Performance

Typical compression ratios compared to mzML:

| Dataset Type | mzML Size | mzPeak Size | Ratio |
|--------------|-----------|-------------|-------|
| DDA Proteomics | 2.5 GB | 400 MB | 6.2x |
| DIA Proteomics | 4.0 GB | 550 MB | 7.3x |
| Metabolomics | 800 MB | 150 MB | 5.3x |

Query performance benefits from Parquet's:
- Column pruning (only read needed columns)
- Predicate pushdown (filter before reading)
- Row group skipping (skip irrelevant data)

## API Documentation

### Core Types

```rust
// Create a spectrum
let spectrum = SpectrumBuilder::new(spectrum_id, scan_number)
    .ms_level(2)
    .retention_time(120.5)
    .precursor(500.25, Some(2), Some(1e6))
    .collision_energy(30.0)
    .peaks(vec![
        Peak { mz: 150.0, intensity: 1000.0 },
        Peak { mz: 250.0, intensity: 2000.0 },
    ])
    .build();

// Write spectra to file
let metadata = MzPeakMetadata::new();
let config = WriterConfig::default();
let mut writer = MzPeakWriter::new_file("output.parquet", &metadata, config)?;
writer.write_spectra(&[spectrum])?;
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
```

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

- [mzPeak Whitepaper](./mzPeak_preprint_v02.pdf) - Design rationale and specification
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
