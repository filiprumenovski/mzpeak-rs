//! mzML to mzPeak converter
//!
//! This module provides the high-level conversion pipeline from mzML files
//! to the mzPeak Parquet format, preserving all metadata and numerical precision.

use super::streamer::MzMLError;
use crate::writer::{WriterConfig, WriterError};
use crate::schema::manifest::Modality;

/// Streaming configuration for memory-bounded pipeline operation
///
/// These settings control memory usage throughout the conversion pipeline,
/// ensuring bounded memory proportional to batch size rather than file size.
///
/// # Default Memory Bounds
///
/// | Component | Memory Bound |
/// |-----------|--------------|
/// | Input buffering | `input_buffer_size` (default: 64KB) |
/// | Spectrum batch | `ConversionConfig::batch_size` * spectrum_size |
/// | Row groups | `WriterConfig::row_group_size` * row_size |
/// | Container write | Uses temp file (64KB streaming copy buffer) |
///
/// # Example
///
/// ```rust
/// use mzpeak::mzml::converter::{ConversionConfig, StreamingConfig};
///
/// let config = ConversionConfig {
///     streaming_config: StreamingConfig {
///         input_buffer_size: 128 * 1024,  // 128KB input buffer
///         ..Default::default()
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Size of input buffer for reading source files (default: 64KB)
    ///
    /// Larger buffers can improve throughput for sequential reads,
    /// but increase memory usage during parsing.
    pub input_buffer_size: usize,

    /// Maximum memory for in-memory buffering before using temp files
    ///
    /// When `None` (default), always uses temp files for container writes.
    /// Set to `Some(bytes)` to enable in-memory buffering up to the limit.
    ///
    /// Note: Container write streaming now uses temp files by default,
    /// so this setting is primarily for future optimization paths.
    pub max_container_buffer_bytes: Option<usize>,

    /// Enable streaming mode semantics
    ///
    /// When `true` (default), the pipeline operates in bounded memory:
    /// - Input is read in chunks via BufReader
    /// - Spectra are processed in batches
    /// - Output uses row group streaming
    /// - Container writes use temp files
    ///
    /// When `false`, some operations may buffer more data for performance.
    pub streaming_mode: bool,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            // 64KB is a good default for sequential I/O
            input_buffer_size: 64 * 1024,
            // Always use temp files for container writes (Issue 000 fix)
            max_container_buffer_bytes: None,
            // Default to streaming mode for bounded memory
            streaming_mode: true,
        }
    }
}

impl StreamingConfig {
    /// Create config optimized for low memory usage
    ///
    /// Uses smaller buffers, suitable for memory-constrained environments.
    pub fn low_memory() -> Self {
        Self {
            input_buffer_size: 32 * 1024,  // 32KB
            max_container_buffer_bytes: None,
            streaming_mode: true,
        }
    }

    /// Create config optimized for throughput
    ///
    /// Uses larger buffers for better I/O performance.
    pub fn high_throughput() -> Self {
        Self {
            input_buffer_size: 256 * 1024,  // 256KB
            max_container_buffer_bytes: None,
            streaming_mode: true,
        }
    }
}

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

/// Output format selection for mzML conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// mzPeak v2.0 container (default).
    V2Container,
    /// Legacy v1 Parquet file (.mzpeak.parquet).
    V1Parquet,
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::V2Container
    }
}

/// Configuration for the mzML to mzPeak conversion
#[derive(Debug, Clone)]
pub struct ConversionConfig {
    /// Writer configuration
    pub writer_config: WriterConfig,

    /// Streaming configuration for memory-bounded operation
    pub streaming_config: StreamingConfig,

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

    /// Output format (v2 container or legacy v1 parquet)
    pub output_format: OutputFormat,

    /// Optional modality override for v2 containers (auto-detect when None)
    pub modality: Option<Modality>,
}

impl Default for ConversionConfig {
    fn default() -> Self {
        Self {
            writer_config: WriterConfig::default(),
            streaming_config: StreamingConfig::default(),
            batch_size: 100,
            #[cfg(feature = "parallel-decode")]
            parallel_batch_size: 5000,
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
            output_format: OutputFormat::V2Container,
            modality: None,
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
            streaming_config: StreamingConfig::default(),
            batch_size: 500,
            #[cfg(feature = "parallel-decode")]
            parallel_batch_size: 5000,
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
            output_format: OutputFormat::V2Container,
            modality: None,
        }
    }

    /// Configuration optimized for fast conversion (larger files)
    /// Best for quick prototyping or when write speed is critical
    pub fn fast_write() -> Self {
        Self {
            writer_config: WriterConfig::fast_write(),
            streaming_config: StreamingConfig::high_throughput(),
            batch_size: 50,
            #[cfg(feature = "parallel-decode")]
            parallel_batch_size: 10000,
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
            output_format: OutputFormat::V2Container,
            modality: None,
        }
    }

    /// Configuration optimized for low memory usage
    /// Best for memory-constrained environments or very large files
    pub fn low_memory() -> Self {
        Self {
            writer_config: WriterConfig::default(),
            streaming_config: StreamingConfig::low_memory(),
            batch_size: 50,
            #[cfg(feature = "parallel-decode")]
            parallel_batch_size: 1000,
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
            output_format: OutputFormat::V2Container,
            modality: None,
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
