//! # mzPeak Dataset Bundle Module
//!
//! This module provides the `MzPeakDatasetWriter` which orchestrates the creation
//! of a Dataset Bundle - a directory-based structure that acts as a single logical file.
//!
//! ## Dataset Bundle Structure
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
//! ## Design Goals
//!
//! 1. **Scalability**: Separate files for different data types allow parallel I/O
//! 2. **Random Access**: Users can read just peaks or just chromatograms as needed
//! 3. **Metadata Consolidation**: Single JSON file for quick inspection without Parquet tools
//! 4. **Writer Synchronization**: Ensure all sub-writers complete before marking dataset as valid
//!
//! ## Usage
//!
//! ```rust,no_run
//! use mzpeak::dataset::MzPeakDatasetWriter;
//! use mzpeak::metadata::MzPeakMetadata;
//! use mzpeak::writer::{WriterConfig, SpectrumBuilder};
//!
//! let metadata = MzPeakMetadata::new();
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
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::metadata::MzPeakMetadata;
use crate::writer::{MzPeakWriter, Spectrum, WriterConfig, WriterError, WriterStats};

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

/// Orchestrator for creating mzPeak Dataset Bundles
///
/// A Dataset Bundle is a directory containing:
/// - `peaks/` directory with spectral data
/// - `chromatograms/` directory with TIC/BPC traces (future)
/// - `metadata.json` for human-readable inspection
pub struct MzPeakDatasetWriter {
    /// Root directory path (e.g., "output.mzpeak")
    root_path: PathBuf,

    /// Writer for peak data in the peaks/ subdirectory
    peak_writer: Option<MzPeakWriter<File>>,

    /// Copy of metadata for JSON export
    metadata: MzPeakMetadata,

    /// Writer configuration
    config: WriterConfig,

    /// Flag indicating if the dataset is finalized
    finalized: bool,
}

impl MzPeakDatasetWriter {
    /// Create a new dataset bundle at the specified path
    ///
    /// # Arguments
    ///
    /// * `path` - Root directory path (e.g., "output.mzpeak")
    /// * `metadata` - Metadata to embed in the dataset
    /// * `config` - Writer configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The directory already exists
    /// - Directory creation fails
    /// - Sub-directory creation fails
    pub fn new<P: AsRef<Path>>(
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

        // TODO: Initialize chromatogram writer when ChromatogramWriter is implemented
        // For now, we just create the directory structure

        Ok(Self {
            root_path,
            peak_writer: Some(peak_writer),
            metadata: metadata.clone(),
            config,
            finalized: false,
        })
    }

    /// Write a single spectrum to the dataset
    ///
    /// This delegates to the internal peak writer.
    pub fn write_spectrum(&mut self, spectrum: &Spectrum) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        let writer = self
            .peak_writer
            .as_mut()
            .ok_or(DatasetError::NotInitialized)?;

        writer.write_spectrum(spectrum)?;
        Ok(())
    }

    /// Write multiple spectra to the dataset
    ///
    /// This delegates to the internal peak writer.
    pub fn write_spectra(&mut self, spectra: &[Spectrum]) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        let writer = self
            .peak_writer
            .as_mut()
            .ok_or(DatasetError::NotInitialized)?;

        writer.write_spectra(spectra)?;
        Ok(())
    }

    /// Get current statistics from the peak writer
    pub fn stats(&self) -> Option<WriterStats> {
        self.peak_writer.as_ref().map(|w| w.stats())
    }

    /// Write metadata.json for quick inspection without Parquet tools
    fn write_metadata_json(&self) -> Result<(), DatasetError> {
        let metadata_path = self.root_path.join("metadata.json");

        // Create a simplified metadata structure for human readability
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

        // Write JSON to file with pretty formatting
        let json_value = serde_json::Value::Object(json_map);
        let json_string = serde_json::to_string_pretty(&json_value)?;

        let mut file = File::create(metadata_path)?;
        file.write_all(json_string.as_bytes())?;
        file.flush()?;

        Ok(())
    }

    /// Close the dataset and finalize all writers
    ///
    /// This ensures:
    /// 1. The peak writer is properly finished and flushed
    /// 2. The chromatogram writer is properly finished (when implemented)
    /// 3. The metadata.json file is written to the root directory
    ///
    /// After calling close(), the dataset is marked as complete and valid.
    pub fn close(mut self) -> Result<DatasetStats, DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        // Finalize peak writer
        let peak_stats = if let Some(writer) = self.peak_writer.take() {
            writer.finish()?
        } else {
            return Err(DatasetError::NotInitialized);
        };

        // TODO: Finalize chromatogram writer when implemented

        // Write metadata.json to root directory
        self.write_metadata_json()?;

        // Calculate total dataset size
        let total_size = calculate_directory_size(&self.root_path)?;

        self.finalized = true;

        Ok(DatasetStats {
            peak_stats,
            chromatograms_written: 0, // Placeholder
            total_size_bytes: total_size,
        })
    }

    /// Get the root path of the dataset
    pub fn root_path(&self) -> &Path {
        &self.root_path
    }

    /// Get the peaks directory path
    pub fn peaks_dir(&self) -> PathBuf {
        self.root_path.join("peaks")
    }

    /// Get the chromatograms directory path
    pub fn chromatograms_dir(&self) -> PathBuf {
        self.root_path.join("chromatograms")
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
    use tempfile::tempdir;

    #[test]
    fn test_dataset_creation() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

        // Verify directory structure
        assert!(dataset_path.exists());
        assert!(dataset_path.is_dir());
        assert!(dataset.peaks_dir().exists());
        assert!(dataset.chromatograms_dir().exists());
    }

    #[test]
    fn test_dataset_already_exists() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("existing.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        // Create first dataset
        let _dataset1 = MzPeakDatasetWriter::new(&dataset_path, &metadata, config.clone()).unwrap();

        // Try to create again - should fail
        let result = MzPeakDatasetWriter::new(&dataset_path, &metadata, config);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_spectrum() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("write_test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

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
    fn test_write_multiple_spectra() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("multi_test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

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
    fn test_metadata_json_created() {
        use crate::metadata::{RunParameters, SdrfMetadata, SourceFileInfo};

        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("metadata_test.mzpeak");

        let mut metadata = MzPeakMetadata::new();
        metadata.sdrf = Some(SdrfMetadata::new("test_sample"));
        metadata.run_parameters = Some(RunParameters::new());
        metadata.source_file = Some(SourceFileInfo::new("test.raw"));

        let config = WriterConfig::default();

        let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

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
    fn test_peaks_file_created() {
        let dir = tempdir().unwrap();
        let dataset_path = dir.path().join("peaks_test.mzpeak");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

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
}
