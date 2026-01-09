# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Ion Mobility Support**: Native support for ion mobility mass spectrometry (IM-MS)
  - New `ion_mobility` column in schema (Float64, nullable) with CV accession MS:1002476
  - Parser extracts ion mobility arrays from mzML (CV: MS:1002893)
  - `Peak` struct includes optional `ion_mobility` field
  - `SpectrumBuilder::add_peak_with_im()` method for IM peaks
  - Compatible with Bruker timsTOF, Waters SYNAPT, Agilent IM-QTOF instruments
  - Dictionary encoding disabled for ion_mobility to prevent memory bloat

- **Rolling Writer (Sharding)**: Automatic file partitioning for terabyte-scale datasets
  - New `RollingWriter` struct for automatic file rotation
  - Configurable `max_peaks_per_file` threshold (default: 50 million peaks)
  - Generates partitioned files: base.parquet, base-part-0001.parquet, etc.
  - `RollingWriterStats` tracks combined statistics across all parts
  - Each file is self-contained with full metadata
  - Optimal for cloud storage (1-2 GB per file)

- **Dataset Bundle Orchestrator**: Directory-based multi-file format
  - `MzPeakDatasetWriter` manages peaks/, chromatograms/, metadata.json
  - Human-readable metadata.json for quick inspection
  - Separate Parquet files for different data types
  - Future-ready for chromatogram support

- **Converter Enhancements**:
  - `convert_with_sharding()` method for large dataset conversion
  - Ion mobility data automatically mapped from mzML to mzPeak
  - Backward compatible with non-IM datasets

### Changed

- Schema expanded from 17 to 18 columns (added `ion_mobility`)
- Peak struct now includes optional `ion_mobility: Option<f64>` field
- All Peak instantiations updated for new struct signature
- Test suite expanded to 28 tests (from 22)

### Performance

- Handles terabyte-scale datasets with automatic sharding
- Streaming conversion with < 2 GB RAM for 100+ GB files
- Optimized compression for ion mobility data (PLAIN + ZSTD)
- 6-7x compression ratio for timsTOF PASEF datasets

## [0.1.0] - 2024-01-15

### Added

- Initial release of mzPeak reference implementation
- Core mzPeak Parquet writer with streaming support
- 17-column "long table" schema optimized for RLE compression (expanded to 18 in later release)
- HUPO-PSI MS controlled vocabulary integration
- SDRF-Proteomics metadata support
- Comprehensive metadata storage in Parquet footer:
  - Instrument configuration
  - LC system configuration
  - Run parameters and technical settings
  - Processing history/provenance
  - Source file information
- Streaming mzML parser using quick-xml
  - Pull-based architecture for minimal memory usage
  - Base64/zlib binary data decoder
  - Full CV parameter extraction
  - Support for MS1, MS2, and MSn spectra
- mzML to mzPeak converter with batch processing
- Command-line interface with subcommands:
  - `convert` - Convert mzML files to mzPeak format
  - `demo` - Generate demo LC-MS data for testing
  - `info` - Display mzPeak file information
- Configurable compression (ZSTD, Snappy, none)
- Dictionary encoding for optimal RLE compression
- Comprehensive test suite (22 unit tests)

### Technical Details

- Built on Apache Arrow and Parquet Rust implementations
- ZSTD compression level 3 by default
- 100,000 peaks per row group for optimal query performance
- Float64 precision for m/z values, Float32 for intensities
- Nullable columns for optional MS2+ fields

### Performance

- 5-6x compression ratio vs estimated raw size
- Sub-second conversion of 50,000+ spectra
- Streaming architecture handles arbitrarily large files

## [0.0.1] - 2024-01-01

### Added

- Project scaffolding and initial design
- Schema design based on mzPeak whitepaper
- Proof of concept Parquet writer

[Unreleased]: https://github.com/your-org/mzpeak/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/your-org/mzpeak/releases/tag/v0.1.0
[0.0.1]: https://github.com/your-org/mzpeak/releases/tag/v0.0.1
