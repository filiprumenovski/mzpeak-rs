//! # mzPeak Dataset Module
//!
//! This module provides dataset writers for creating mzPeak containers:
//!
//! - [`MzPeakDatasetWriter`]: v1.0 format writer (single peaks.parquet)
//! - [`MzPeakDatasetWriterV2`]: v2.0 format writer (normalized two-table architecture)
//!
//! ## v1.0 Container Format (legacy)
//!
//! ```text
//! {name}.mzpeak (ZIP archive)
//! ├── mimetype                  # "application/vnd.mzpeak" (uncompressed, first entry)
//! ├── metadata.json             # Human-readable metadata (Deflate compressed)
//! └── peaks/peaks.parquet       # Spectral data (uncompressed for seekability)
//! ```
//!
//! ## v2.0 Container Format (recommended)
//!
//! The v2.0 format uses a normalized two-table architecture that provides
//! 30-40% smaller file sizes through reduced data duplication.
//!
//! ```text
//! {name}.mzpeak (ZIP archive)
//! ├── mimetype                    # "application/vnd.mzpeak+v2"
//! ├── manifest.json               # Schema version and modality declaration
//! ├── metadata.json               # Human-readable metadata
//! ├── spectra/spectra.parquet     # Spectrum-level metadata (one row per spectrum)
//! └── peaks/peaks.parquet         # Peak-level data (one row per peak)
//! ```
//!
//! ## Performance Notes
//!
//! Parquet files are stored **uncompressed** within the ZIP archive because:
//! 1. Parquet files already handle their own internal compression (ZSTD/Snappy)
//! 2. Storing uncompressed allows readers to seek directly to byte offsets
//!
//! ## Usage (v2.0 - recommended)
//!
//! ```rust,ignore
//! use mzpeak::dataset::MzPeakDatasetWriterV2;
//! use mzpeak::schema::manifest::Modality;
//! use mzpeak::writer::types::{SpectrumMetadata, PeakArraysV2};
//!
//! let mut writer = MzPeakDatasetWriterV2::new("output.mzpeak", Modality::LcMs, None)?;
//!
//! let metadata = SpectrumMetadata::new_ms1(0, Some(1), 60.0, 1, 100);
//! let peaks = PeakArraysV2::new(vec![400.0], vec![10000.0]);
//! writer.write_spectrum_v2(&metadata, &peaks)?;
//!
//! let stats = writer.close()?;
//! ```
//!
//! ## Usage (v1.0 - legacy)
//!
//! ```rust,no_run
//! use mzpeak::dataset::MzPeakDatasetWriter;
//! use mzpeak::metadata::MzPeakMetadata;
//! use mzpeak::writer::{PeakArrays, SpectrumArrays, WriterConfig};
//!
//! let metadata = MzPeakMetadata::new();
//! let mut dataset = MzPeakDatasetWriter::new("output.mzpeak", &metadata, WriterConfig::default())?;
//!
//! let peaks = PeakArrays::new(vec![400.0], vec![10000.0]);
//! let spectrum = SpectrumArrays::new_ms1(0, 1, 60.0, 1, peaks);
//! dataset.write_spectrum_arrays(&spectrum)?;
//!
//! dataset.close()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod error;
mod stats;
mod types;
mod writer_impl;
mod writer_v2;

#[cfg(test)]
mod tests;

pub use error::DatasetError;
pub use stats::DatasetStats;
pub use types::OutputMode;
pub use writer_impl::MzPeakDatasetWriter;
pub use writer_v2::{DatasetV2Stats, DatasetWriterV2Config, MzPeakDatasetWriterV2, MZPEAK_V2_MIMETYPE};
