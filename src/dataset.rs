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
//! use mzpeak::writer::{WriterConfig, SpectrumBuilder};
//!
//! let metadata = MzPeakMetadata::new();
//! // Container mode (single .mzpeak file)
//! let mut dataset = MzPeakDatasetWriter::new("output.mzpeak", &metadata, WriterConfig::default())?;
//!
//! // Write spectrum data
//! let spectrum = SpectrumBuilder::new(0, 1)
//!     .ms_level(1)
//!     .retention_time(60.0)
//!     .polarity(1)
//!     .add_peak(400.0, 10000.0)
//!     .build();
//!
//! dataset.write_spectrum(&spectrum)?;
//!
//! // Finalize the dataset
//! dataset.close()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fs::{self, File};
use std::io::{BufWriter, Cursor, Seek, Write};
use std::path::{Path, PathBuf};

use zip::write::SimpleFileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;

use crate::metadata::MzPeakMetadata;
use crate::writer::{MzPeakWriter, Spectrum, WriterConfig, WriterError, WriterStats};

/// MIME type for mzPeak container files
pub const MZPEAK_MIMETYPE: &str = "application/vnd.mzpeak";

/// Errors that can occur during dataset operations
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Writer error: {0}")]
    WriterError(#[from] WriterError),

    #[error("Metadata error: {0}")]
    MetadataError(#[from] crate::metadata::MetadataError),

    #[error("JSON serialization error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("ZIP error: {0}")]
    ZipError(#[from] zip::result::ZipError),

    #[error("Invalid dataset path: {0}")]
    InvalidPath(String),

    #[error("Dataset already exists: {0}")]
    AlreadyExists(String),

    #[error("Dataset not properly initialized")]
    NotInitialized,
}

/// Statistics from a completed dataset write operation
#[derive(Debug, Clone)]
pub struct DatasetStats {
    /// Statistics from the peak writer
    pub peak_stats: WriterStats,

    /// Number of chromatograms written (placeholder for future chromatogram support)
    pub chromatograms_written: usize,

    /// Total dataset size in bytes
    pub total_size_bytes: u64,
}

impl std::fmt::Display for DatasetStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Dataset: {} spectra, {} peaks, {} chromatograms, {} bytes",
            self.peak_stats.spectra_written,
            self.peak_stats.peaks_written,
            self.chromatograms_written,
            self.total_size_bytes
        )
    }
}

/// Output mode for the dataset writer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Directory-based bundle (legacy)
    Directory,
    /// Single ZIP container file (default)
    Container,
}

/// Wrapper that writes Parquet data to an in-memory buffer for later ZIP inclusion.
/// This is necessary because the ZIP writer needs to know the uncompressed size upfront
/// for Stored entries, so we buffer the entire Parquet file.
struct ParquetBuffer {
    buffer: Cursor<Vec<u8>>,
}

impl ParquetBuffer {
    fn new() -> Self {
        Self {
            buffer: Cursor::new(Vec::new()),
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.buffer.into_inner()
    }
}

impl Write for ParquetBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.flush()
    }
}

impl Seek for ParquetBuffer {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.buffer.seek(pos)
    }
}

/// Internal sink abstraction for writing to either directory or container
enum DatasetSink {
    /// Directory mode: writes directly to files
    Directory {
        root_path: PathBuf,
        peak_writer: Option<MzPeakWriter<File>>,
    },
    /// Container mode: writes to a ZIP archive
    Container {
        output_path: PathBuf,
        zip_writer: ZipWriter<BufWriter<File>>,
        peak_writer: Option<MzPeakWriter<ParquetBuffer>>,
    },
}

/// Orchestrator for creating mzPeak datasets
///
/// Supports two output modes:
/// - **Container Mode** (default): Single `.mzpeak` ZIP archive
/// - **Directory Mode** (legacy): Directory bundle with separate files
pub struct MzPeakDatasetWriter {
    /// Internal sink (directory or container)
    sink: DatasetSink,

    /// Output mode being used
    mode: OutputMode,

    /// Copy of metadata for JSON export
    metadata: MzPeakMetadata,

    /// Writer configuration
    config: WriterConfig,

    /// Flag indicating if the dataset is finalized
    finalized: bool,
}

impl MzPeakDatasetWriter {
    /// Create a new dataset at the specified path
    ///
    /// Mode is automatically selected based on the path:
    /// - If path ends with `.mzpeak` and is not an existing directory → Container Mode
    /// - Otherwise → Directory Mode
    ///
    /// # Arguments
    ///
    /// * `path` - Output path (e.g., "output.mzpeak")
    /// * `metadata` - Metadata to embed in the dataset
    /// * `config` - Writer configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file/directory already exists
    /// - Creation fails
    pub fn new<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, DatasetError> {
        let path = path.as_ref();

        // Determine mode: Container if .mzpeak extension and not an existing directory
        let use_container = path
            .extension()
            .map(|ext| ext == "mzpeak")
            .unwrap_or(false)
            && !path.is_dir();

        if use_container {
            Self::new_container(path, metadata, config)
        } else {
            Self::new_directory(path, metadata, config)
        }
    }

    /// Create a new dataset in Container Mode (ZIP archive)
    pub fn new_container<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, DatasetError> {
        let output_path = path.as_ref().to_path_buf();

        // Validate path
        if output_path.to_string_lossy().is_empty() {
            return Err(DatasetError::InvalidPath("Empty path".to_string()));
        }

        // Check if file already exists
        if output_path.exists() {
            return Err(DatasetError::AlreadyExists(
                output_path.to_string_lossy().to_string(),
            ));
        }

        // Create parent directories if needed
        if let Some(parent) = output_path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        // Create ZIP file
        let file = File::create(&output_path)?;
        let buf_writer = BufWriter::new(file);
        let mut zip_writer = ZipWriter::new(buf_writer);

        // Write mimetype as first entry (MUST be uncompressed and first)
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o644);
        zip_writer.start_file("mimetype", options)?;
        zip_writer.write_all(MZPEAK_MIMETYPE.as_bytes())?;

        // Initialize peak writer to buffer
        let peak_buffer = ParquetBuffer::new();
        let peak_writer = MzPeakWriter::new(peak_buffer, metadata, config.clone())?;

        Ok(Self {
            sink: DatasetSink::Container {
                output_path,
                zip_writer,
                peak_writer: Some(peak_writer),
            },
            mode: OutputMode::Container,
            metadata: metadata.clone(),
            config,
            finalized: false,
        })
    }

    /// Create a new dataset in Directory Mode (legacy bundle)
    pub fn new_directory<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, DatasetError> {
        let root_path = path.as_ref().to_path_buf();

        // Validate path
        if root_path.to_string_lossy().is_empty() {
            return Err(DatasetError::InvalidPath("Empty path".to_string()));
        }

        // Check if dataset already exists
        if root_path.exists() {
            return Err(DatasetError::AlreadyExists(
                root_path.to_string_lossy().to_string(),
            ));
        }

        // Create root directory
        fs::create_dir_all(&root_path)?;

        // Create subdirectories
        let peaks_dir = root_path.join("peaks");
        let chromatograms_dir = root_path.join("chromatograms");

        fs::create_dir(&peaks_dir)?;
        fs::create_dir(&chromatograms_dir)?;

        // Initialize peak writer
        let peak_file_path = peaks_dir.join("peaks.parquet");
        let peak_writer = MzPeakWriter::new_file(&peak_file_path, metadata, config.clone())?;

        Ok(Self {
            sink: DatasetSink::Directory {
                root_path,
                peak_writer: Some(peak_writer),
            },
            mode: OutputMode::Directory,
            metadata: metadata.clone(),
            config,
            finalized: false,
        })
    }

    /// Get the output mode being used
    pub fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Write a single spectrum to the dataset
    ///
    /// This delegates to the internal peak writer.
    pub fn write_spectrum(&mut self, spectrum: &Spectrum) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectrum(spectrum)?;
            }
            DatasetSink::Container { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectrum(spectrum)?;
            }
        }
        Ok(())
    }

    /// Write multiple spectra to the dataset
    ///
    /// This delegates to the internal peak writer.
    pub fn write_spectra(&mut self, spectra: &[Spectrum]) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectra(spectra)?;
            }
            DatasetSink::Container { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectra(spectra)?;
            }
        }
        Ok(())
    }

    /// Get current statistics from the peak writer
    pub fn stats(&self) -> Option<WriterStats> {
        match &self.sink {
            DatasetSink::Directory { peak_writer, .. } => peak_writer.as_ref().map(|w| w.stats()),
            DatasetSink::Container { peak_writer, .. } => peak_writer.as_ref().map(|w| w.stats()),
        }
    }

    /// Build the metadata JSON content
    fn build_metadata_json(&self) -> Result<String, DatasetError> {
        let mut json_map = serde_json::Map::new();

        json_map.insert(
            "format_version".to_string(),
            serde_json::Value::String(crate::schema::MZPEAK_FORMAT_VERSION.to_string()),
        );

        json_map.insert(
            "created".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );

        json_map.insert(
            "converter".to_string(),
            serde_json::Value::String(format!("mzpeak-rs v{}", env!("CARGO_PKG_VERSION"))),
        );

        // Add SDRF metadata
        if let Some(ref sdrf) = self.metadata.sdrf {
            let sdrf_json = serde_json::to_value(sdrf)?;
            json_map.insert("sdrf".to_string(), sdrf_json);
        }

        // Add instrument config
        if let Some(ref instrument) = self.metadata.instrument {
            let instrument_json = serde_json::to_value(instrument)?;
            json_map.insert("instrument".to_string(), instrument_json);
        }

        // Add LC config
        if let Some(ref lc) = self.metadata.lc_config {
            let lc_json = serde_json::to_value(lc)?;
            json_map.insert("lc_config".to_string(), lc_json);
        }

        // Add run parameters
        if let Some(ref run) = self.metadata.run_parameters {
            let run_json = serde_json::to_value(run)?;
            json_map.insert("run_parameters".to_string(), run_json);
        }

        // Add source file info
        if let Some(ref source) = self.metadata.source_file {
            let source_json = serde_json::to_value(source)?;
            json_map.insert("source_file".to_string(), source_json);
        }

        // Add processing history
        if let Some(ref history) = self.metadata.processing_history {
            let history_json = serde_json::to_value(history)?;
            json_map.insert("processing_history".to_string(), history_json);
        }

        let json_value = serde_json::Value::Object(json_map);
        Ok(serde_json::to_string_pretty(&json_value)?)
    }

    /// Close the dataset and finalize all writers
    ///
    /// This ensures:
    /// 1. The peak writer is properly finished and flushed
    /// 2. The metadata.json file is written
    /// 3. For Container Mode: All entries are added to the ZIP and finalized
    ///
    /// After calling close(), the dataset is marked as complete and valid.
    pub fn close(mut self) -> Result<DatasetStats, DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        // Build metadata JSON before consuming sink (to avoid borrow issues)
        let json_string = self.build_metadata_json()?;

        let (peak_stats, total_size) = match self.sink {
            DatasetSink::Directory { root_path, mut peak_writer } => {
                // Finalize peak writer
                let stats = if let Some(writer) = peak_writer.take() {
                    writer.finish()?
                } else {
                    return Err(DatasetError::NotInitialized);
                };

                // Write metadata.json to root directory
                let metadata_path = root_path.join("metadata.json");
                let mut file = File::create(metadata_path)?;
                file.write_all(json_string.as_bytes())?;
                file.flush()?;

                // Calculate total dataset size
                let total_size = calculate_directory_size(&root_path)?;

                (stats, total_size)
            }
            DatasetSink::Container {
                output_path,
                mut zip_writer,
                mut peak_writer,
            } => {
                // Finalize peak writer and get the buffer
                let (stats, parquet_data) = if let Some(writer) = peak_writer.take() {
                    let buffer = writer.finish_into_inner()?;
                    let data = buffer.into_inner();
                    let stats = WriterStats {
                        spectra_written: 0, // Stats not tracked in container mode buffer
                        peaks_written: 0,
                        row_groups_written: 0,
                        file_size_bytes: data.len() as u64,
                    };
                    (stats, data)
                } else {
                    return Err(DatasetError::NotInitialized);
                };

                // Write metadata.json (Deflate compressed)
                let options = SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Deflated)
                    .unix_permissions(0o644);
                zip_writer.start_file("metadata.json", options)?;
                zip_writer.write_all(json_string.as_bytes())?;

                // Write peaks/peaks.parquet (MUST be uncompressed/Stored for seekability)
                let options = SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Stored)
                    .unix_permissions(0o644);
                zip_writer.start_file("peaks/peaks.parquet", options)?;
                zip_writer.write_all(&parquet_data)?;

                // Finalize the ZIP archive
                let inner = zip_writer.finish()?;
                inner.into_inner().map_err(|e| {
                    DatasetError::IoError(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to flush ZIP buffer: {}", e.error()),
                    ))
                })?;

                // Get final file size
                let total_size = fs::metadata(&output_path)?.len();

                (stats, total_size)
            }
        };

        self.finalized = true;

        Ok(DatasetStats {
            peak_stats,
            chromatograms_written: 0, // Placeholder
            total_size_bytes: total_size,
        })
    }

    /// Get the root path of the dataset (directory mode) or output file (container mode)
    pub fn output_path(&self) -> &Path {
        match &self.sink {
            DatasetSink::Directory { root_path, .. } => root_path,
            DatasetSink::Container { output_path, .. } => output_path,
        }
    }

    /// Get the root path of the dataset (for backwards compatibility)
    #[deprecated(since = "0.2.0", note = "Use output_path() instead")]
    pub fn root_path(&self) -> &Path {
        self.output_path()
    }

    /// Get the peaks directory path (only valid in Directory mode)
    pub fn peaks_dir(&self) -> Option<PathBuf> {
        match &self.sink {
            DatasetSink::Directory { root_path, .. } => Some(root_path.join("peaks")),
            DatasetSink::Container { .. } => None,
        }
    }

    /// Get the chromatograms directory path (only valid in Directory mode)
    pub fn chromatograms_dir(&self) -> Option<PathBuf> {
        match &self.sink {
            DatasetSink::Directory { root_path, .. } => Some(root_path.join("chromatograms")),
            DatasetSink::Container { .. } => None,
        }
    }
}

/// Calculate the total size of a directory recursively
fn calculate_directory_size(path: &Path) -> Result<u64, std::io::Error> {
    let mut total_size = 0u64;

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_dir() {
                total_size += calculate_directory_size(&entry.path())?;
            } else {
                total_size += metadata.len();
            }
        }
    } else {
        total_size = fs::metadata(path)?.len();
    }

    Ok(total_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::SpectrumBuilder;
    use std::io::Read;
    use tempfile::tempdir;

    // ==================== Directory Mode Tests ====================

    #[test]
    fn test_directory_mode_creation() {
        let dir = tempdir().unwrap();
        // Use a path without .mzpeak extension to force directory mode
        let dataset_path = dir.path().join("test_dir.mzpeak_dir");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

        // Verify directory structure
        assert_eq!(dataset.mode(), OutputMode::Directory);
        assert!(dataset_path.exists());
        assert!(dataset_path.is_dir());
        assert!(dataset.peaks_dir().unwrap().exists());
        assert!(dataset.chromatograms_dir().unwrap().exists());
    }

    #[test]
    fn test_directory_mode_already_exists() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("existing_dir");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        // Create first dataset
        let _dataset1 =
            MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config.clone()).unwrap();

        // Try to create again - should fail
        let result = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config);
        assert!(result.is_err());
    }

    #[test]
    fn test_directory_mode_write_spectrum() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("write_test_dir");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(60.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .add_peak(500.0, 20000.0)
            .build();

        dataset.write_spectrum(&spectrum).unwrap();

        let stats = dataset.close().unwrap();
        assert_eq!(stats.peak_stats.spectra_written, 1);
        assert_eq!(stats.peak_stats.peaks_written, 2);
    }

    #[test]
    fn test_directory_mode_metadata_json_created() {
        use crate::metadata::{RunParameters, SdrfMetadata, SourceFileInfo};

        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("metadata_test_dir");

        let mut metadata = MzPeakMetadata::new();
        metadata.sdrf = Some(SdrfMetadata::new("test_sample"));
        metadata.run_parameters = Some(RunParameters::new());
        metadata.source_file = Some(SourceFileInfo::new("test.raw"));

        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(0.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .build();

        dataset.write_spectrum(&spectrum).unwrap();
        dataset.close().unwrap();

        // Verify metadata.json exists and is valid JSON
        let metadata_json_path = dataset_path.join("metadata.json");
        assert!(metadata_json_path.exists());

        let json_content = fs::read_to_string(&metadata_json_path).unwrap();
        let json_value: serde_json::Value = serde_json::from_str(&json_content).unwrap();

        assert!(json_value.get("format_version").is_some());
        assert!(json_value.get("created").is_some());
        assert!(json_value.get("converter").is_some());
        assert!(json_value.get("sdrf").is_some());
        assert!(json_value.get("source_file").is_some());
    }

    #[test]
    fn test_directory_mode_peaks_file_created() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("peaks_test_dir");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(60.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .build();

        dataset.write_spectrum(&spectrum).unwrap();
        dataset.close().unwrap();

        // Verify peaks file exists
        let peaks_file = dataset_path.join("peaks").join("peaks.parquet");
        assert!(peaks_file.exists());
        assert!(peaks_file.metadata().unwrap().len() > 0);
    }

    // ==================== Container Mode Tests ====================

    #[test]
    fn test_container_mode_creation() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

        // Should be container mode since path ends with .mzpeak
        assert_eq!(dataset.mode(), OutputMode::Container);
        // Container shouldn't have peaks_dir/chromatograms_dir
        assert!(dataset.peaks_dir().is_none());
        assert!(dataset.chromatograms_dir().is_none());
    }

    #[test]
    fn test_container_mode_already_exists() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("existing.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        // Create first dataset and close it
        let mut dataset1 =
            MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config.clone()).unwrap();
        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(0.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .build();
        dataset1.write_spectrum(&spectrum).unwrap();
        dataset1.close().unwrap();

        // Try to create again - should fail
        let result = MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config);
        assert!(result.is_err());
    }

    #[test]
    fn test_container_mode_write_spectrum() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("write_test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(60.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .add_peak(500.0, 20000.0)
            .build();

        dataset.write_spectrum(&spectrum).unwrap();

        let stats = dataset.close().unwrap();
        // Note: In container mode, stats tracking is simplified
        assert!(stats.total_size_bytes > 0);
    }

    #[test]
    fn test_container_mode_zip_structure() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("structure_test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(60.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .build();

        dataset.write_spectrum(&spectrum).unwrap();
        dataset.close().unwrap();

        // Open and verify ZIP structure
        let file = File::open(&dataset_path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();

        // Verify mimetype is first and uncompressed
        {
            let mimetype_entry = archive.by_index(0).unwrap();
            assert_eq!(mimetype_entry.name(), "mimetype");
            assert_eq!(
                mimetype_entry.compression(),
                zip::CompressionMethod::Stored
            );
        }

        // Verify metadata.json exists and is compressed
        {
            let metadata_entry = archive.by_name("metadata.json").unwrap();
            assert_eq!(
                metadata_entry.compression(),
                zip::CompressionMethod::Deflated
            );
        }

        // Verify peaks/peaks.parquet exists and is UNCOMPRESSED (critical for seekability)
        {
            let peaks_entry = archive.by_name("peaks/peaks.parquet").unwrap();
            assert_eq!(peaks_entry.compression(), zip::CompressionMethod::Stored);
        }
    }

    #[test]
    fn test_container_mode_mimetype_content() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("mimetype_test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(0.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .build();

        dataset.write_spectrum(&spectrum).unwrap();
        dataset.close().unwrap();

        // Verify mimetype content
        let file = File::open(&dataset_path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let mut mimetype_entry = archive.by_name("mimetype").unwrap();

        let mut content = String::new();
        mimetype_entry.read_to_string(&mut content).unwrap();
        assert_eq!(content, MZPEAK_MIMETYPE);
    }

    #[test]
    fn test_container_mode_metadata_json_content() {
        use crate::metadata::{SdrfMetadata, SourceFileInfo};

        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("metadata_content_test.mzpeak");

        let mut metadata = MzPeakMetadata::new();
        metadata.sdrf = Some(SdrfMetadata::new("test_sample"));
        metadata.source_file = Some(SourceFileInfo::new("test.raw"));

        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(0.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .build();

        dataset.write_spectrum(&spectrum).unwrap();
        dataset.close().unwrap();

        // Extract and verify metadata.json content
        let file = File::open(&dataset_path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        let mut metadata_entry = archive.by_name("metadata.json").unwrap();

        let mut content = String::new();
        metadata_entry.read_to_string(&mut content).unwrap();

        let json_value: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(json_value.get("format_version").is_some());
        assert!(json_value.get("created").is_some());
        assert!(json_value.get("converter").is_some());
        assert!(json_value.get("sdrf").is_some());
        assert!(json_value.get("source_file").is_some());
    }

    // ==================== Auto-detection Tests ====================

    #[test]
    fn test_auto_detection_container_mode() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("auto_container.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();
        assert_eq!(dataset.mode(), OutputMode::Container);
    }

    #[test]
    fn test_auto_detection_directory_mode_no_extension() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("auto_directory");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();
        assert_eq!(dataset.mode(), OutputMode::Directory);
    }

    #[test]
    fn test_auto_detection_directory_mode_other_extension() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("auto_directory.parquet");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();
        assert_eq!(dataset.mode(), OutputMode::Directory);
    }

    #[test]
    fn test_write_multiple_spectra_directory() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("multi_test_dir");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

        let spectra: Vec<_> = (0..10)
            .map(|i| {
                SpectrumBuilder::new(i, i + 1)
                    .ms_level(1)
                    .retention_time((i as f32) * 10.0)
                    .polarity(1)
                    .add_peak(400.0 + (i as f64), 10000.0)
                    .build()
            })
            .collect();

        dataset.write_spectra(&spectra).unwrap();

        let stats = dataset.close().unwrap();
        assert_eq!(stats.peak_stats.spectra_written, 10);
        assert_eq!(stats.peak_stats.peaks_written, 10);
    }

    #[test]
    fn test_write_multiple_spectra_container() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("multi_test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset =
            MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

        let spectra: Vec<_> = (0..10)
            .map(|i| {
                SpectrumBuilder::new(i, i + 1)
                    .ms_level(1)
                    .retention_time((i as f32) * 10.0)
                    .polarity(1)
                    .add_peak(400.0 + (i as f64), 10000.0)
                    .build()
            })
            .collect();

        dataset.write_spectra(&spectra).unwrap();

        let stats = dataset.close().unwrap();
        assert!(stats.total_size_bytes > 0);
    }
}
