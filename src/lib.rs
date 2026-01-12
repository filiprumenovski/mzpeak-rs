//! # mzPeak - A Modern Mass Spectrometry Data Format
//!
//! `mzpeak` is the reference implementation for the mzPeak data format, designed to be
//! a scalable, interoperable replacement for XML-based MS data formats like mzML.
//!
//! ## Key Features
//!
//! - **Dataset Bundle Architecture**: Directory-based format with separate files for
//!   peaks, chromatograms, and human-readable metadata.
//!
//! - **Efficient Storage**: Uses Apache Parquet with ZSTD compression for excellent
//!   compression ratios while maintaining fast random access.
//!
//! - **Long Table Format**: Stores each peak as a separate row, enabling Parquet's
//!   Run-Length Encoding (RLE) to efficiently compress repeated spectrum metadata.
//!
//! - **Human-Readable Metadata**: Standalone JSON file for quick inspection without
//!   Parquet tools, plus embedded metadata in Parquet footer.
//!
//! - **Lossless Technical Metadata**: Unlike other converters, mzPeak preserves
//!   vendor-specific technical parameters like pump pressures and diagnostic data.
//!
//! - **HUPO-PSI CV Integration**: Uses standardized controlled vocabulary terms
//!   for global interoperability.
//!
//! ## Quick Start - Dataset Bundle (Recommended)
//!
//! ```rust,no_run
//! use mzpeak::dataset::MzPeakDatasetWriter;
//! use mzpeak::writer::{SpectrumBuilder, WriterConfig};
//! use mzpeak::metadata::MzPeakMetadata;
//!
//! // Create metadata
//! let metadata = MzPeakMetadata::new();
//!
//! // Create Dataset Bundle writer
//! let mut dataset = MzPeakDatasetWriter::new(
//!     "output.mzpeak",
//!     &metadata,
//!     WriterConfig::default()
//! )?;
//!
//! // Build a spectrum
//! let spectrum = SpectrumBuilder::new(0, 1)
//!     .ms_level(1)
//!     .retention_time(60.0)
//!     .polarity(1)
//!     .add_peak(400.0, 10000.0)
//!     .add_peak(500.0, 20000.0)
//!     .build();
//!
//! // Write the spectrum
//! dataset.write_spectrum(&spectrum)?;
//!
//! // Finalize
//! let stats = dataset.close()?;
//! println!("Wrote {} peaks", stats.peak_stats.peaks_written);
//! # Ok::<(), mzpeak::dataset::DatasetError>(())
//! ```
//!
//! This creates a directory structure:
//! ```text
//! output.mzpeak/
//! ├── peaks/peaks.parquet      # Spectral data
//! ├── chromatograms/            # TIC/BPC traces
//! └── metadata.json             # Human-readable metadata
//! ```
//!
//! ## Legacy Single-File Format
//!
//! ```rust,no_run
//! use mzpeak::writer::{MzPeakWriter, SpectrumBuilder, WriterConfig};
//! use mzpeak::metadata::MzPeakMetadata;
//!
//! let metadata = MzPeakMetadata::new();
//! let mut writer = MzPeakWriter::new_file(
//!     "output.mzpeak.parquet",
//!     &metadata,
//!     WriterConfig::default()
//! )?;
//!
//! let spectrum = SpectrumBuilder::new(0, 1)
//!     .ms_level(1)
//!     .retention_time(60.0)
//!     .polarity(1)
//!     .add_peak(400.0, 10000.0)
//!     .build();
//!
//! writer.write_spectrum(&spectrum)?;
//! let stats = writer.finish()?;
//! # Ok::<(), mzpeak::writer::WriterError>(())
//! ```
//!
//! ## Reading mzPeak Files
//!
//! mzPeak Dataset Bundles are standard Parquet files and can be read with any
//! Parquet-compatible tool:
//!
//! ```python
//! # Python
//! import pyarrow.parquet as pq
//! table = pq.read_table("data.mzpeak/peaks/peaks.parquet")
//! df = table.to_pandas()
//! ```
//!
//! ```r
//! # R
//! library(arrow)
//! df <- read_parquet("data.mzpeak/peaks/peaks.parquet")
//! ```
//!
//! ```sql
//! -- DuckDB
//! SELECT * FROM read_parquet('data.mzpeak/peaks/peaks.parquet')
//! WHERE ms_level = 2 AND precursor_mz BETWEEN 500 AND 600;
//! ```
//!
//! ## Architecture
//!
//! The library is organized into the following modules:
//!
//! - [`dataset`]: Dataset Bundle orchestrator for multi-file output
//! - [`schema`]: Arrow/Parquet schema definitions for the Long table format
//! - [`metadata`]: SDRF parsing and technical metadata structures
//! - [`writer`]: Streaming Parquet writer with RLE optimization
//! - [`controlled_vocabulary`]: HUPO-PSI MS controlled vocabulary terms
//!
//! ## Format Specification
//!
//! ### Schema (Long Table Format)
//!
//! | Column | Type | Required | Description |
//! |--------|------|----------|-------------|
//! | spectrum_id | Int64 | Yes | Unique spectrum identifier |
//! | scan_number | Int64 | Yes | Native scan number |
//! | ms_level | Int16 | Yes | MS level (1, 2, ...) |
//! | retention_time | Float32 | Yes | RT in seconds |
//! | polarity | Int8 | Yes | 1 (pos) or -1 (neg) |
//! | mz | Float64 | Yes | Mass-to-charge ratio |
//! | intensity | Float32 | Yes | Signal intensity |
//! | precursor_mz | Float64 | No | Precursor m/z (MS2+) |
//! | precursor_charge | Int16 | No | Precursor charge |
//! | precursor_intensity | Float32 | No | Precursor intensity |
//! | isolation_window_lower | Float32 | No | Lower isolation offset |
//! | isolation_window_upper | Float32 | No | Upper isolation offset |
//! | collision_energy | Float32 | No | CE in eV |
//! | total_ion_current | Float64 | No | TIC |
//! | base_peak_mz | Float64 | No | Base peak m/z |
//! | base_peak_intensity | Float32 | No | Base peak intensity |
//! | injection_time | Float32 | No | Ion injection time (ms) |
//!
//! ### File Footer Metadata
//!
//! The Parquet file footer contains JSON-serialized metadata:
//!
//! - `mzpeak:format_version`: Format version string
//! - `mzpeak:sdrf_metadata`: SDRF-Proteomics experimental metadata
//! - `mzpeak:instrument_config`: Instrument configuration
//! - `mzpeak:lc_config`: LC configuration and gradient
//! - `mzpeak:run_parameters`: Technical run parameters
//! - `mzpeak:source_file`: Source file provenance
//! - `mzpeak:processing_history`: Data processing audit trail
//!
//! ## Alignment with mzPeak Whitepaper
//!
//! This implementation follows the mzPeak whitepaper specifications:
//!
//! 1. **Binary format**: Uses Parquet for efficient storage (Section 3)
//! 2. **HUPO-PSI CV**: All metadata keys use CV accessions (Section 3)
//! 3. **SDRF integration**: Full SDRF-Proteomics support (Section 3)
//! 4. **Lossless conversion**: Preserves all vendor metadata (Section 3)
//! 5. **Random access**: Parquet enables efficient data retrieval (Section 3)
//! 6. **Single file**: Self-contained archive format (Section 5)
//! 7. **Cross-platform**: Rust implementation with bindings potential (Section 5)

// Documentation lints - enforce complete documentation for publication
#![deny(missing_docs)]
#![deny(rustdoc::missing_crate_level_docs)]
// Allow some patterns common in scientific code
#![allow(clippy::too_many_arguments)]

pub mod controlled_vocabulary;
pub mod chromatogram_writer;
pub mod dataset;
pub mod metadata;
pub mod mobilogram_writer;
pub mod mzml;
pub mod reader;
pub mod schema;
pub mod validator;
pub mod writer;

// Python bindings module (only compiled with the "python" feature)
#[cfg(feature = "python")]
mod python;

/// Re-export commonly used types for convenience
pub mod prelude {
    pub use crate::chromatogram_writer::{
        Chromatogram, ChromatogramWriter, ChromatogramWriterConfig, ChromatogramWriterStats,
    };
    pub use crate::mobilogram_writer::{
        Mobilogram, MobilogramWriter, MobilogramWriterConfig, MobilogramWriterStats,
    };
    pub use crate::controlled_vocabulary::{ms_terms, unit_terms, CvParamList, CvTerm};
    pub use crate::dataset::{DatasetError, DatasetStats, MzPeakDatasetWriter, OutputMode};
    pub use crate::metadata::{
        InstrumentConfig, LcConfig, MzPeakMetadata, RunParameters, SdrfMetadata, SourceFileInfo,
    };
    pub use crate::schema::{
        chromatogram_columns, columns, create_chromatogram_schema, create_mzpeak_schema,
        MZPEAK_FORMAT_VERSION, MZPEAK_MIMETYPE,
    };
    pub use crate::validator::{validate_mzpeak_file, ValidationReport};
    pub use crate::writer::{
        ColumnarBatch, CompressionType, MzPeakWriter, OptionalColumn, OptionalColumnBuf, Peak,
        PeakArrays, Spectrum, SpectrumArrays, SpectrumBuilder, WriterConfig, WriterStats,
    };
    pub use crate::reader::{
        FileSummary, FileMetadata, MzPeakReader, ReaderConfig, ReaderError, SpectrumIterator,
    };
}
