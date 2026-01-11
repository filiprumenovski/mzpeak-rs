//! mzML to mzPeak converter
//!
//! This module provides the high-level conversion pipeline from mzML files
//! to the mzPeak Parquet format, preserving all metadata and numerical precision.

use super::streamer::MzMLError;
use crate::writer::{WriterConfig, WriterError};

/// Errors that can occur during conversion
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    /// Error parsing the input mzML file
    #[error("mzML parsing error: {0}")]
    MzMLError(#[from] MzMLError),

    /// Error writing the output mzPeak file
    #[error("Writer error: {0}")]
    WriterError(#[from] WriterError),

    /// Error from the dataset writer
    #[error("Dataset error: {0}")]
    DatasetError(#[from] crate::dataset::DatasetError),

    /// Error writing chromatograms
    #[error("Chromatogram writer error: {0}")]
    ChromatogramWriterError(#[from] crate::chromatogram_writer::ChromatogramWriterError),

    /// I/O error during file operations
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error processing metadata
    #[error("Metadata error: {0}")]
    MetadataError(#[from] crate::metadata::MetadataError),

    /// Error decoding binary arrays for a spectrum
    #[error("Binary decode error in spectrum {index} ({id}): {source}")]
    BinaryDecodeError {
        /// Spectrum index (0-based)
        index: i64,
        /// Spectrum ID from the mzML
        id: String,
        /// Underlying decode error
        #[source]
        source: super::binary::BinaryDecodeError,
    },
}

/// Configuration for the mzML to mzPeak conversion
#[derive(Debug, Clone)]
pub struct ConversionConfig {
    /// Writer configuration
    pub writer_config: WriterConfig,

    /// Batch size for writing spectra
    pub batch_size: usize,

    /// Batch size for parallel decoding (only used with parallel-decode feature)
    /// Larger batches improve throughput but increase memory usage.
    /// Default: 5000 spectra (~8GB RAM for typical high-res MS data)
    #[cfg(feature = "parallel-decode")]
    pub parallel_batch_size: usize,

    /// Whether to preserve original precision (32/64 bit)
    /// If false, all data is stored as the schema default
    pub preserve_precision: bool,

    /// Whether to include chromatograms
    pub include_chromatograms: bool,

    /// Optional SDRF file path
    pub sdrf_path: Option<String>,

    /// Progress callback interval (spectra count)
    pub progress_interval: usize,
}

impl Default for ConversionConfig {
    fn default() -> Self {
        Self {
            writer_config: WriterConfig::default(),
            batch_size: 100,
            #[cfg(feature = "parallel-decode")]
            parallel_batch_size: 5000,
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
        }
    }
}

impl ConversionConfig {
    /// Configuration optimized for maximum compression (slower conversion)
    /// Best for archival storage or when file size is critical
    /// Expected: 2-3x better compression than default
    pub fn max_compression() -> Self {
        Self {
            writer_config: WriterConfig::max_compression(),
            batch_size: 500,
            #[cfg(feature = "parallel-decode")]
            parallel_batch_size: 5000,
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
        }
    }

    /// Configuration optimized for fast conversion (larger files)
    /// Best for quick prototyping or when write speed is critical
    pub fn fast_write() -> Self {
        Self {
            writer_config: WriterConfig::fast_write(),
            batch_size: 50,
            #[cfg(feature = "parallel-decode")]
            parallel_batch_size: 10000,
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
        }
    }

    /// Balanced configuration (default)
    pub fn balanced() -> Self {
        Self::default()
    }
}

/// Statistics from a conversion
#[derive(Debug, Clone, Default)]
pub struct ConversionStats {
    /// Total spectra converted
    pub spectra_count: usize,
    /// Total peaks converted
    pub peak_count: usize,
    /// Number of MS1 spectra
    pub ms1_spectra: usize,
    /// Number of MS2 spectra
    pub ms2_spectra: usize,
    /// Number of MS3+ spectra
    pub msn_spectra: usize,
    /// Number of chromatograms converted
    pub chromatograms_converted: usize,
    /// Size of source mzML file in bytes
    pub source_file_size: u64,
    /// Size of output mzPeak file in bytes
    pub output_file_size: u64,
    /// Compression ratio (source/output)
    pub compression_ratio: f64,
}

/// Converter from mzML to mzPeak format
pub struct MzMLConverter {
    config: ConversionConfig,
}

impl MzMLConverter {
    /// Create a new converter with default configuration
    pub fn new() -> Self {
        Self {
            config: ConversionConfig::default(),
        }
    }

    /// Create a new converter with custom configuration
    pub fn with_config(config: ConversionConfig) -> Self {
        Self { config }
    }

    /// Set batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }
}

impl Default for MzMLConverter {
    fn default() -> Self {
        Self::new()
    }
}

mod metadata;
mod sequential;
mod spectrum;

#[cfg(feature = "parallel-decode")]
mod parallel;

#[cfg(test)]
mod tests;
