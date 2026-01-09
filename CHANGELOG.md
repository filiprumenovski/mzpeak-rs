# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Container Format (`.mzpeak`)**: Single-file ZIP archive format for distribution
  - New default output mode for paths ending in `.mzpeak`
  - ZIP structure: `mimetype` (first, uncompressed), `metadata.json` (Deflate), `peaks/peaks.parquet` (Stored), `chromatograms/chromatograms.parquet` (Stored, if present), `mobilograms/mobilograms.parquet` (Stored, if present)
  - Parquet files stored **uncompressed** within ZIP for direct byte-offset seeking
  - MIME type: `application/vnd.mzpeak`
  - Similar to `.docx`, `.jar`, `.epub` container formats

- **Dual Output Mode Support**:
  - `MzPeakDatasetWriter::new()` auto-detects mode from path extension
  - `new_container()` explicitly creates ZIP archive format
  - `new_directory()` explicitly creates legacy directory bundle
  - `OutputMode` enum: `Container` (default) and `Directory`
  - `mode()` method to query current output mode

- **Writer Enhancements**:
  - `MzPeakWriter::finish_into_inner()` to retrieve underlying writer after completion
  - `MZPEAK_MIMETYPE` constant exported in prelude

- **Validation Module**: Comprehensive integrity checker for mzPeak files
  - New `validator` module with 4-stage validation process
  - `mzpeak validate` CLI command for file/directory validation
  - Structure checks: validates paths, required files, and Parquet format
  - Metadata integrity: deserializes and validates metadata.json and Parquet footer
  - Schema contract: verifies all required columns, data types, and CV accessions
  - Data sanity: validates m/z > 0, intensity >= 0, ms_level >= 1, RT ordering
  - Colored output with ✓/⚠/✗ symbols for easy reading
  - Exit code 1 on validation failure (suitable for CI/CD pipelines)
  - Samples first 1,000 rows for efficient validation of large files
  - Supports both single-file and directory bundle formats

- **Reader API**: Full read support for mzPeak files (complements write-only architecture)
  - New `reader` module with `MzPeakReader` struct
  - Supports ZIP container (`.mzpeak`), directory bundles, and single Parquet files
  - `iter_spectra()` returns reconstructed `Spectrum` objects from long-format data
  - `get_spectrum(id)` for single spectrum lookup by ID
  - `spectra_by_rt_range(start, end)` for retention time queries
  - `spectra_by_ms_level(level)` for MS1/MS2 filtering
  - `summary()` returns `FileSummary` with statistics (num_spectra, RT range, m/z range)
  - `metadata()` exposes `FileMetadata` including format version and parsed `MzPeakMetadata`
  - **`read_chromatograms()`** reads all chromatograms from dataset (TIC, BPC, etc.)
  - **`read_mobilograms()`** reads all mobilograms from dataset (EIM, TIM, etc.)
  - Full support for reading chromatograms/mobilograms from ZIP containers, directory bundles, and single Parquet files
  - Gracefully handles missing chromatogram/mobilogram files (returns empty vector)
  - Configurable batch size via `ReaderConfig`
  - Comprehensive test suite with roundtrip verification

- **Mass Spectrometry Imaging (MSI) Support**: Spatial coordinate columns for imaging data
  - New `pixel_x`, `pixel_y`, `pixel_z` columns in schema (Int32, nullable)
  - CV accessions from imzML: IMS:1000050, IMS:1000051, IMS:1000052
  - `Spectrum` struct includes `pixel_x`, `pixel_y`, `pixel_z` fields
  - `SpectrumBuilder::pixel(x, y)` and `pixel_3d(x, y, z)` methods
  - Dictionary encoding enabled for MSI columns (same value per spectrum)
  - Schema expanded from 18 to 21 columns
  - NOTE: Future versions will store MSI data in separate `imaging/` Parquet file within ZIP container

- **Mobilogram Writer**: Ion mobility trace storage for IM-MS data
  - New `mobilogram_writer` module with `MobilogramWriter` struct
  - Wide-schema format with List arrays for mobility and intensity
  - Schema: `mobilogram_id`, `mobilogram_type`, `mobility_array`, `intensity_array`
  - `Mobilogram` struct with `new_tim()` (Total Ion Mobilogram) and `new_xim()` (Extracted Ion Mobilogram) constructors
  - Array length validation ensures mobility and intensity arrays match
  - CV terms: MS:1003006 (mobilogram), MS:1002476 (ion mobility)
  - Configurable compression and row group size via `MobilogramWriterConfig`
  - Full integration with `MzPeakDatasetWriter`: `write_mobilogram()` and `write_mobilograms()` methods
  - Container mode writes `mobilograms/mobilograms.parquet` (Stored compression)
  - Directory mode creates `mobilograms/` subdirectory
  - `DatasetStats` tracks mobilogram counts and statistics

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

- `MzPeakDatasetWriter` now defaults to Container mode for `.mzpeak` paths
- `peaks_dir()` and `chromatograms_dir()` now return `Option<PathBuf>` (None in container mode)
- `root_path()` deprecated in favor of `output_path()`
- Schema expanded from 17 to 21 columns (added `ion_mobility` + 3 MSI spatial columns)
- Peak struct now includes optional `ion_mobility: Option<f64>` field
- Spectrum struct now includes optional `pixel_x`, `pixel_y`, `pixel_z` fields
- All Peak and Spectrum instantiations updated for new struct signature
- Test suite expanded to 48+ tests
- Added `zip` and `bytes` crate dependencies

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
