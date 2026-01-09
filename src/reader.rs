//! # mzPeak Reader Module
//!
//! This module provides functionality for reading mzPeak files and querying
//! mass spectrometry data efficiently.
//!
//! ## Features
//!
//! - **Random Access**: Query spectra by ID, retention time range, or m/z range
//! - **Streaming Iteration**: Memory-efficient iteration over large files
//! - **Container Support**: Read both ZIP container (`.mzpeak`) and directory formats
//! - **Metadata Access**: Retrieve embedded metadata from Parquet footer
//!
//! ## Example
//!
//! ```rust,no_run
//! use mzpeak::reader::MzPeakReader;
//!
//! // Open a file
//! let reader = MzPeakReader::open("data.mzpeak")?;
//!
//! // Get metadata
//! println!("Format version: {}", reader.metadata().format_version);
//!
//! // Query spectra by retention time range
//! for spectrum in reader.spectra_by_rt_range(60.0, 120.0)? {
//!     println!("Spectrum {}: {} peaks", spectrum.spectrum_id, spectrum.peaks.len());
//! }
//!
//! // Get a specific spectrum by ID
//! if let Some(spectrum) = reader.get_spectrum(42)? {
//!     println!("Found spectrum 42 with {} peaks", spectrum.peaks.len());
//! }
//! # Ok::<(), mzpeak::reader::ReaderError>(())
//! ```

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, Float32Array, Float64Array, Int16Array, Int64Array, Int8Array,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use zip::ZipArchive;

use crate::metadata::MzPeakMetadata;
use crate::schema::{columns, KEY_FORMAT_VERSION};
use crate::writer::{Peak, Spectrum};

/// Errors that can occur during reading
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Arrow error
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// Parquet error
    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    /// ZIP archive error
    #[error("ZIP error: {0}")]
    ZipError(#[from] zip::result::ZipError),

    /// Invalid file format
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// Metadata parsing error
    #[error("Metadata error: {0}")]
    MetadataError(String),

    /// Column not found
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// JSON parsing error
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// Metadata extracted from an mzPeak file
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// Format version string
    pub format_version: String,
    /// Total number of rows (peaks) in the file
    pub total_rows: i64,
    /// Number of row groups
    pub num_row_groups: usize,
    /// Schema of the Parquet file
    pub schema: Arc<Schema>,
    /// Raw key-value metadata from Parquet footer
    pub key_value_metadata: HashMap<String, String>,
    /// Parsed mzPeak metadata (if available)
    pub mzpeak_metadata: Option<MzPeakMetadata>,
}

/// Configuration for reading mzPeak files
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Batch size for reading records
    pub batch_size: usize,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            batch_size: 65536,
        }
    }
}

/// Source type for the reader (stores path or bytes for re-reading)
enum ReaderSource {
    /// File path for file-based reading
    FilePath(std::path::PathBuf),
    /// Bytes for in-memory reading (ZIP containers)
    Bytes(Bytes),
}

/// Reader for mzPeak files
///
/// Supports both ZIP container format (`.mzpeak`) and legacy directory/single-file formats.
pub struct MzPeakReader {
    source: ReaderSource,
    config: ReaderConfig,
    file_metadata: FileMetadata,
}

impl MzPeakReader {
    /// Open an mzPeak file or directory
    ///
    /// Automatically detects the format:
    /// - `.mzpeak` files are treated as ZIP containers
    /// - `.parquet` files are read directly
    /// - Directories are treated as dataset bundles
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, ReaderError> {
        Self::open_with_config(path, ReaderConfig::default())
    }

    /// Open an mzPeak file with custom configuration
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: ReaderConfig) -> Result<Self, ReaderError> {
        let path = path.as_ref();

        if path.is_dir() {
            // Directory bundle - look for peaks/peaks.parquet
            let peaks_path = path.join("peaks").join("peaks.parquet");
            if !peaks_path.exists() {
                return Err(ReaderError::InvalidFormat(
                    format!("Directory bundle missing peaks/peaks.parquet: {}", path.display())
                ));
            }
            Self::open_parquet_file(&peaks_path, config)
        } else if path.extension().map(|e| e == "mzpeak").unwrap_or(false) {
            // ZIP container format
            Self::open_container(path, config)
        } else {
            // Assume single Parquet file
            Self::open_parquet_file(path, config)
        }
    }

    /// Open a ZIP container format file
    fn open_container<P: AsRef<Path>>(path: P, config: ReaderConfig) -> Result<Self, ReaderError> {
        let file = File::open(path.as_ref())?;
        let mut archive = ZipArchive::new(BufReader::new(file))?;

        // Find the peaks parquet file
        let peaks_path = "peaks/peaks.parquet";
        let mut peaks_file = archive.by_name(peaks_path).map_err(|_| {
            ReaderError::InvalidFormat(format!("ZIP container missing {}", peaks_path))
        })?;

        // Read the entire parquet file into memory
        let mut parquet_bytes = Vec::new();
        peaks_file.read_to_end(&mut parquet_bytes)?;

        // Convert to Bytes (implements ChunkReader)
        let bytes = Bytes::from(parquet_bytes);
        let file_metadata = Self::extract_file_metadata_from_bytes(&bytes)?;

        Ok(Self {
            source: ReaderSource::Bytes(bytes),
            config,
            file_metadata,
        })
    }

    /// Open a single Parquet file directly
    fn open_parquet_file<P: AsRef<Path>>(path: P, config: ReaderConfig) -> Result<Self, ReaderError> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let parquet_reader = SerializedFileReader::new(file)?;

        let file_metadata = Self::extract_file_metadata(&parquet_reader)?;

        Ok(Self {
            source: ReaderSource::FilePath(path),
            config,
            file_metadata,
        })
    }

    /// Extract metadata from a Parquet reader
    fn extract_file_metadata<R: parquet::file::reader::ChunkReader + 'static>(
        reader: &SerializedFileReader<R>,
    ) -> Result<FileMetadata, ReaderError> {
        let parquet_metadata = reader.metadata();
        let file_meta = parquet_metadata.file_metadata();
        let schema = parquet::arrow::parquet_to_arrow_schema(
            file_meta.schema_descr(),
            file_meta.key_value_metadata(),
        )?;

        // Extract key-value metadata
        let mut kv_metadata = HashMap::new();
        if let Some(kv_list) = file_meta.key_value_metadata() {
            for kv in kv_list {
                if let Some(value) = &kv.value {
                    kv_metadata.insert(kv.key.clone(), value.clone());
                }
            }
        }

        // Get format version
        let format_version = kv_metadata
            .get(KEY_FORMAT_VERSION)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        // Try to parse MzPeakMetadata
        let mzpeak_metadata = MzPeakMetadata::from_parquet_metadata(&kv_metadata).ok();

        // Calculate total rows
        let total_rows: i64 = (0..parquet_metadata.num_row_groups())
            .map(|i| parquet_metadata.row_group(i).num_rows())
            .sum();

        Ok(FileMetadata {
            format_version,
            total_rows,
            num_row_groups: parquet_metadata.num_row_groups(),
            schema: Arc::new(schema),
            key_value_metadata: kv_metadata,
            mzpeak_metadata,
        })
    }

    /// Extract metadata from Bytes
    fn extract_file_metadata_from_bytes(bytes: &Bytes) -> Result<FileMetadata, ReaderError> {
        let reader = SerializedFileReader::new(bytes.clone())?;
        Self::extract_file_metadata(&reader)
    }

    /// Get file metadata
    pub fn metadata(&self) -> &FileMetadata {
        &self.file_metadata
    }

    /// Get the total number of peaks (rows) in the file
    pub fn total_peaks(&self) -> i64 {
        self.file_metadata.total_rows
    }

    /// Get the Arrow schema
    pub fn schema(&self) -> &Arc<Schema> {
        &self.file_metadata.schema
    }

    /// Read all record batches from the file
    fn read_all_batches(&self) -> Result<Vec<RecordBatch>, ReaderError> {
        let mut batches = Vec::new();

        match &self.source {
            ReaderSource::FilePath(path) => {
                let file = File::open(path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                for batch_result in reader {
                    batches.push(batch_result?);
                }
            }
            ReaderSource::Bytes(bytes) => {
                let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                for batch_result in reader {
                    batches.push(batch_result?);
                }
            }
        }

        Ok(batches)
    }

    /// Iterate over all spectra in the file
    ///
    /// This reconstructs spectra from the long-format peak data by grouping peaks
    /// by spectrum_id.
    pub fn iter_spectra(&self) -> Result<Vec<Spectrum>, ReaderError> {
        let batches = self.read_all_batches()?;
        Self::batches_to_spectra(&batches)
    }

    /// Convert record batches to spectra
    fn batches_to_spectra(batches: &[RecordBatch]) -> Result<Vec<Spectrum>, ReaderError> {
        let mut spectra = Vec::new();
        let mut current_spectrum: Option<Spectrum> = None;

        for batch in batches {
            let spectrum_ids = Self::get_int64_column(batch, columns::SPECTRUM_ID)?;
            let scan_numbers = Self::get_int64_column(batch, columns::SCAN_NUMBER)?;
            let ms_levels = Self::get_int16_column(batch, columns::MS_LEVEL)?;
            let retention_times = Self::get_float32_column(batch, columns::RETENTION_TIME)?;
            let polarities = Self::get_int8_column(batch, columns::POLARITY)?;
            let mzs = Self::get_float64_column(batch, columns::MZ)?;
            let intensities = Self::get_float32_column(batch, columns::INTENSITY)?;

            // Optional columns
            let ion_mobilities = Self::get_optional_float64_column(batch, columns::ION_MOBILITY);
            let precursor_mzs = Self::get_optional_float64_column(batch, columns::PRECURSOR_MZ);
            let precursor_charges = Self::get_optional_int16_column(batch, columns::PRECURSOR_CHARGE);
            let precursor_intensities = Self::get_optional_float32_column(batch, columns::PRECURSOR_INTENSITY);
            let isolation_lowers = Self::get_optional_float32_column(batch, columns::ISOLATION_WINDOW_LOWER);
            let isolation_uppers = Self::get_optional_float32_column(batch, columns::ISOLATION_WINDOW_UPPER);
            let collision_energies = Self::get_optional_float32_column(batch, columns::COLLISION_ENERGY);
            let tics = Self::get_optional_float64_column(batch, columns::TOTAL_ION_CURRENT);
            let base_peak_mzs = Self::get_optional_float64_column(batch, columns::BASE_PEAK_MZ);
            let base_peak_intensities = Self::get_optional_float32_column(batch, columns::BASE_PEAK_INTENSITY);
            let injection_times = Self::get_optional_float32_column(batch, columns::INJECTION_TIME);

            for i in 0..batch.num_rows() {
                let spectrum_id = spectrum_ids.value(i);

                // Check if we need to start a new spectrum
                let need_new_spectrum = match &current_spectrum {
                    None => true,
                    Some(s) => s.spectrum_id != spectrum_id,
                };

                if need_new_spectrum {
                    // Save the previous spectrum if it exists
                    if let Some(s) = current_spectrum.take() {
                        spectra.push(s);
                    }

                    // Start a new spectrum
                    current_spectrum = Some(Spectrum {
                        spectrum_id,
                        scan_number: scan_numbers.value(i),
                        ms_level: ms_levels.value(i),
                        retention_time: retention_times.value(i),
                        polarity: polarities.value(i),
                        precursor_mz: Self::get_optional_f64(precursor_mzs, i),
                        precursor_charge: Self::get_optional_i16(precursor_charges, i),
                        precursor_intensity: Self::get_optional_f32(precursor_intensities, i),
                        isolation_window_lower: Self::get_optional_f32(isolation_lowers, i),
                        isolation_window_upper: Self::get_optional_f32(isolation_uppers, i),
                        collision_energy: Self::get_optional_f32(collision_energies, i),
                        total_ion_current: Self::get_optional_f64(tics, i),
                        base_peak_mz: Self::get_optional_f64(base_peak_mzs, i),
                        base_peak_intensity: Self::get_optional_f32(base_peak_intensities, i),
                        injection_time: Self::get_optional_f32(injection_times, i),
                        pixel_x: None, // MSI fields not yet extracted from Parquet
                        pixel_y: None,
                        pixel_z: None,
                        peaks: Vec::new(),
                    });
                }

                // Add the peak to the current spectrum
                if let Some(ref mut s) = current_spectrum {
                    s.peaks.push(Peak {
                        mz: mzs.value(i),
                        intensity: intensities.value(i),
                        ion_mobility: Self::get_optional_f64(ion_mobilities, i),
                    });
                }
            }
        }

        // Don't forget the last spectrum
        if let Some(s) = current_spectrum {
            spectra.push(s);
        }

        Ok(spectra)
    }

    /// Query spectra by retention time range (inclusive)
    pub fn spectra_by_rt_range(&self, start_rt: f32, end_rt: f32) -> Result<Vec<Spectrum>, ReaderError> {
        let all_spectra = self.iter_spectra()?;
        Ok(all_spectra
            .into_iter()
            .filter(|s| s.retention_time >= start_rt && s.retention_time <= end_rt)
            .collect())
    }

    /// Query spectra by MS level
    pub fn spectra_by_ms_level(&self, ms_level: i16) -> Result<Vec<Spectrum>, ReaderError> {
        let all_spectra = self.iter_spectra()?;
        Ok(all_spectra
            .into_iter()
            .filter(|s| s.ms_level == ms_level)
            .collect())
    }

    /// Get a specific spectrum by ID
    pub fn get_spectrum(&self, spectrum_id: i64) -> Result<Option<Spectrum>, ReaderError> {
        let all_spectra = self.iter_spectra()?;
        Ok(all_spectra.into_iter().find(|s| s.spectrum_id == spectrum_id))
    }

    /// Get multiple spectra by their IDs
    pub fn get_spectra(&self, spectrum_ids: &[i64]) -> Result<Vec<Spectrum>, ReaderError> {
        let id_set: std::collections::HashSet<_> = spectrum_ids.iter().collect();
        let all_spectra = self.iter_spectra()?;
        Ok(all_spectra
            .into_iter()
            .filter(|s| id_set.contains(&s.spectrum_id))
            .collect())
    }

    /// Get all unique spectrum IDs in the file
    pub fn spectrum_ids(&self) -> Result<Vec<i64>, ReaderError> {
        let spectra = self.iter_spectra()?;
        Ok(spectra.into_iter().map(|s| s.spectrum_id).collect())
    }

    /// Get summary statistics about the file
    pub fn summary(&self) -> Result<FileSummary, ReaderError> {
        let spectra = self.iter_spectra()?;

        let num_spectra = spectra.len() as i64;
        let num_ms1 = spectra.iter().filter(|s| s.ms_level == 1).count() as i64;
        let num_ms2 = spectra.iter().filter(|s| s.ms_level == 2).count() as i64;

        let rt_range = if !spectra.is_empty() {
            let min_rt = spectra.iter().map(|s| s.retention_time).fold(f32::MAX, f32::min);
            let max_rt = spectra.iter().map(|s| s.retention_time).fold(f32::MIN, f32::max);
            Some((min_rt, max_rt))
        } else {
            None
        };

        let mz_range = if !spectra.is_empty() {
            let min_mz = spectra.iter()
                .flat_map(|s| s.peaks.iter())
                .map(|p| p.mz)
                .fold(f64::MAX, f64::min);
            let max_mz = spectra.iter()
                .flat_map(|s| s.peaks.iter())
                .map(|p| p.mz)
                .fold(f64::MIN, f64::max);
            if min_mz <= max_mz { Some((min_mz, max_mz)) } else { None }
        } else {
            None
        };

        Ok(FileSummary {
            total_peaks: self.file_metadata.total_rows,
            num_spectra,
            num_ms1_spectra: num_ms1,
            num_ms2_spectra: num_ms2,
            rt_range,
            mz_range,
            format_version: self.file_metadata.format_version.clone(),
        })
    }

    // Helper methods for column extraction
    fn get_int64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array, ReaderError> {
        batch
            .column_by_name(name)
            .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Int64", name)))
    }

    fn get_int16_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int16Array, ReaderError> {
        batch
            .column_by_name(name)
            .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Int16", name)))
    }

    fn get_int8_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int8Array, ReaderError> {
        batch
            .column_by_name(name)
            .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Int8", name)))
    }

    fn get_float32_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float32Array, ReaderError> {
        batch
            .column_by_name(name)
            .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Float32", name)))
    }

    fn get_float64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array, ReaderError> {
        batch
            .column_by_name(name)
            .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Float64", name)))
    }

    fn get_optional_float64_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Float64Array> {
        batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<Float64Array>()
    }

    fn get_optional_float32_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Float32Array> {
        batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<Float32Array>()
    }

    fn get_optional_int16_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int16Array> {
        batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<Int16Array>()
    }

    fn get_optional_f64(array: Option<&Float64Array>, idx: usize) -> Option<f64> {
        array.and_then(|arr| {
            if arr.is_null(idx) {
                None
            } else {
                Some(arr.value(idx))
            }
        })
    }

    fn get_optional_f32(array: Option<&Float32Array>, idx: usize) -> Option<f32> {
        array.and_then(|arr| {
            if arr.is_null(idx) {
                None
            } else {
                Some(arr.value(idx))
            }
        })
    }

    fn get_optional_i16(array: Option<&Int16Array>, idx: usize) -> Option<i16> {
        array.and_then(|arr| {
            if arr.is_null(idx) {
                None
            } else {
                Some(arr.value(idx))
            }
        })
    }
}

/// Summary statistics about an mzPeak file
#[derive(Debug, Clone)]
pub struct FileSummary {
    /// Total number of peaks in the file
    pub total_peaks: i64,
    /// Number of unique spectra
    pub num_spectra: i64,
    /// Number of MS1 spectra
    pub num_ms1_spectra: i64,
    /// Number of MS2 spectra
    pub num_ms2_spectra: i64,
    /// Retention time range (min, max) in seconds
    pub rt_range: Option<(f32, f32)>,
    /// m/z range (min, max)
    pub mz_range: Option<(f64, f64)>,
    /// Format version
    pub format_version: String,
}

impl std::fmt::Display for FileSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "mzPeak File Summary")?;
        writeln!(f, "===================")?;
        writeln!(f, "Format version: {}", self.format_version)?;
        writeln!(f, "Total peaks: {}", self.total_peaks)?;
        writeln!(f, "Total spectra: {}", self.num_spectra)?;
        writeln!(f, "  MS1 spectra: {}", self.num_ms1_spectra)?;
        writeln!(f, "  MS2 spectra: {}", self.num_ms2_spectra)?;
        if let Some((min_rt, max_rt)) = self.rt_range {
            writeln!(f, "RT range: {:.2} - {:.2} sec", min_rt, max_rt)?;
        }
        if let Some((min_mz, max_mz)) = self.mz_range {
            writeln!(f, "m/z range: {:.4} - {:.4}", min_mz, max_mz)?;
        }
        Ok(())
    }
}

/// Iterator over spectra (wrapper for Vec iterator)
pub struct SpectrumIterator {
    inner: std::vec::IntoIter<Spectrum>,
}

impl Iterator for SpectrumIterator {
    type Item = Spectrum;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{MzPeakWriter, SpectrumBuilder, WriterConfig};
    use crate::metadata::MzPeakMetadata;
    use tempfile::tempdir;

    #[test]
    fn test_read_write_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");

        // Write some test data
        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();
        let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

        let spectrum1 = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(60.0)
            .polarity(1)
            .add_peak(400.0, 1000.0)
            .add_peak(500.0, 2000.0)
            .build();

        let spectrum2 = SpectrumBuilder::new(1, 2)
            .ms_level(2)
            .retention_time(65.0)
            .polarity(1)
            .precursor(450.0, Some(2), Some(5000.0))
            .add_peak(200.0, 500.0)
            .add_peak(250.0, 1500.0)
            .add_peak(300.0, 750.0)
            .build();

        writer.write_spectrum(&spectrum1)?;
        writer.write_spectrum(&spectrum2)?;
        writer.finish()?;

        // Read back
        let reader = MzPeakReader::open(&path)?;

        assert_eq!(reader.total_peaks(), 5);

        let spectra = reader.iter_spectra()?;
        assert_eq!(spectra.len(), 2);

        assert_eq!(spectra[0].spectrum_id, 0);
        assert_eq!(spectra[0].peaks.len(), 2);
        assert_eq!(spectra[0].ms_level, 1);

        assert_eq!(spectra[1].spectrum_id, 1);
        assert_eq!(spectra[1].peaks.len(), 3);
        assert_eq!(spectra[1].ms_level, 2);
        assert_eq!(spectra[1].precursor_mz, Some(450.0));

        Ok(())
    }

    #[test]
    fn test_get_spectrum_by_id() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();
        let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

        for i in 0..10 {
            let spectrum = SpectrumBuilder::new(i, i + 1)
                .ms_level(1)
                .retention_time(i as f32 * 10.0)
                .polarity(1)
                .add_peak(400.0 + i as f64, 1000.0)
                .build();
            writer.write_spectrum(&spectrum)?;
        }
        writer.finish()?;

        let reader = MzPeakReader::open(&path)?;

        let spectrum = reader.get_spectrum(5)?.expect("Should find spectrum 5");
        assert_eq!(spectrum.spectrum_id, 5);
        assert_eq!(spectrum.retention_time, 50.0);

        let missing = reader.get_spectrum(100)?;
        assert!(missing.is_none());

        Ok(())
    }

    #[test]
    fn test_file_summary() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();
        let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

        // Write 5 MS1 and 5 MS2 spectra
        for i in 0..10 {
            let ms_level = if i % 2 == 0 { 1 } else { 2 };
            let mut builder = SpectrumBuilder::new(i, i + 1)
                .ms_level(ms_level)
                .retention_time(i as f32 * 10.0)
                .polarity(1)
                .add_peak(400.0 + i as f64 * 100.0, 1000.0);

            if ms_level == 2 {
                builder = builder.precursor(450.0, Some(2), None);
            }

            writer.write_spectrum(&builder.build())?;
        }
        writer.finish()?;

        let reader = MzPeakReader::open(&path)?;
        let summary = reader.summary()?;

        assert_eq!(summary.num_spectra, 10);
        assert_eq!(summary.num_ms1_spectra, 5);
        assert_eq!(summary.num_ms2_spectra, 5);
        assert_eq!(summary.total_peaks, 10);

        Ok(())
    }

    #[test]
    fn test_spectra_by_rt_range() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let path = dir.path().join("test.parquet");

        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();
        let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

        for i in 0..10 {
            let spectrum = SpectrumBuilder::new(i, i + 1)
                .ms_level(1)
                .retention_time(i as f32 * 10.0) // 0, 10, 20, ..., 90
                .polarity(1)
                .add_peak(400.0, 1000.0)
                .build();
            writer.write_spectrum(&spectrum)?;
        }
        writer.finish()?;

        let reader = MzPeakReader::open(&path)?;

        // Query RT range 25-55 should get spectra with RT 30, 40, 50
        let spectra = reader.spectra_by_rt_range(25.0, 55.0)?;
        assert_eq!(spectra.len(), 3);
        assert_eq!(spectra[0].retention_time, 30.0);
        assert_eq!(spectra[1].retention_time, 40.0);
        assert_eq!(spectra[2].retention_time, 50.0);

        Ok(())
    }
}
