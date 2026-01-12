//! # mzPeak Dataset Module
//!
//! This module provides the `MzPeakDatasetWriter` which orchestrates the creation
//! of mzPeak datasets in two modes:
//!
//! ## Container Mode (`.mzpeak` file - default)
//!
//! A single ZIP archive containing the dataset structure. This is the recommended
//! format for distribution and archival.
//!
//! ```text
//! {name}.mzpeak (ZIP archive)
//! ├── mimetype                  # "application/vnd.mzpeak" (uncompressed, first entry)
//! ├── metadata.json             # Human-readable metadata (Deflate compressed)
//! └── peaks/peaks.parquet       # Spectral data (uncompressed for seekability)
//! ```
//!
//! ## Directory Mode (legacy)
//!
//! A directory-based structure for compatibility and development.
//!
//! ```text
//! {name}.mzpeak/
//! ├── peaks/                    # Spectral data (managed by MzPeakWriter)
//! │   └── peaks.parquet
//! ├── chromatograms/            # TIC/BPC traces (managed by ChromatogramWriter)
//! │   └── chromatograms.parquet
//! └── metadata.json             # Human-readable run summary
//! ```
//!
//! ## Mode Selection
//!
//! - If the path ends with `.mzpeak` and is NOT an existing directory, Container Mode is used
//! - Otherwise, Directory Mode is used
//!
//! ## Performance Notes
//!
//! In Container Mode, the Parquet file is stored **uncompressed** within the ZIP archive.
//! This is critical because:
//! 1. Parquet files already handle their own internal compression (ZSTD/Snappy)
//! 2. Storing uncompressed allows readers to seek directly to byte offsets without
//!    decompressing the entire archive
//!
//! ## Usage
//!
//! ```rust,no_run
//! use mzpeak::dataset::MzPeakDatasetWriter;
//! use mzpeak::metadata::MzPeakMetadata;
//! use mzpeak::writer::{PeakArrays, SpectrumArrays, WriterConfig};
//!
//! let metadata = MzPeakMetadata::new();
//! // Container mode (single .mzpeak file)
//! let mut dataset = MzPeakDatasetWriter::new("output.mzpeak", &metadata, WriterConfig::default())?;
//!
//! // Write spectrum data (SoA)
//! let peaks = PeakArrays::new(vec![400.0], vec![10000.0]);
//! let spectrum = SpectrumArrays::new_ms1(0, 1, 60.0, 1, peaks);
//!
//! dataset.write_spectrum_arrays(&spectrum)?;
//!
//! // Finalize the dataset
//! dataset.close()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod error;
mod stats;
mod types;
mod writer_impl;

#[cfg(test)]
mod tests;

pub use error::DatasetError;
pub use stats::DatasetStats;
pub use types::OutputMode;
pub use writer_impl::MzPeakDatasetWriter;
