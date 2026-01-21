//! # v2 Spectra Table Reader
//!
//! This module provides functionality for reading the spectra table from mzPeak v2 containers.
//! The spectra table stores one row per spectrum with metadata and peak data pointers.
//!
//! ## v2 Format Structure
//!
//! In v2 format, spectrum metadata is stored separately from peak data:
//! - `spectra/spectra.parquet` - Spectrum metadata (one row per spectrum)
//! - `peaks/peaks.parquet` - Peak data (one row per peak)
//!
//! This normalized design enables fast metadata-only queries.

use std::fs::File;
use std::sync::Arc;

use arrow::array::{
    Array, Float32Array, Float64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::schema::spectra_columns;

use super::config::ReaderSource;
use super::utils::{get_optional_f32, get_optional_f64};
use super::{MzPeakReader, RecordBatchIterator, ReaderError};


// =============================================================================
// Spectrum Metadata View
// =============================================================================

/// View of spectrum metadata from the v2 spectra table.
///
/// This struct holds spectrum-level metadata without peak data.
/// Use `peak_offset` and `peak_count` to locate peak data in the peaks table.
#[derive(Debug, Clone)]
pub struct SpectrumMetadataView {
    /// Unique spectrum identifier (0-indexed)
    pub spectrum_id: u32,
    /// Native scan number from the instrument
    pub scan_number: Option<i32>,
    /// MS level (1 for MS1, 2 for MS2, etc.)
    pub ms_level: u8,
    /// Retention time in seconds
    pub retention_time: f32,
    /// Polarity (1 for positive, -1 for negative)
    pub polarity: i8,
    /// Byte offset in peaks.parquet (row index where this spectrum's peaks start)
    pub peak_offset: u64,
    /// Number of peaks in this spectrum
    pub peak_count: u32,
    /// Precursor m/z (for MS2+)
    pub precursor_mz: Option<f64>,
    /// Precursor charge state
    pub precursor_charge: Option<i8>,
    /// Precursor intensity
    pub precursor_intensity: Option<f32>,
    /// Isolation window lower offset
    pub isolation_window_lower: Option<f32>,
    /// Isolation window upper offset
    pub isolation_window_upper: Option<f32>,
    /// Collision energy in eV
    pub collision_energy: Option<f32>,
    /// Total ion current
    pub total_ion_current: Option<f64>,
    /// Base peak m/z
    pub base_peak_mz: Option<f64>,
    /// Base peak intensity
    pub base_peak_intensity: Option<f32>,
    /// Ion injection time in ms
    pub injection_time: Option<f32>,
    /// MSI X pixel coordinate
    pub pixel_x: Option<u16>,
    /// MSI Y pixel coordinate
    pub pixel_y: Option<u16>,
    /// MSI Z pixel coordinate
    pub pixel_z: Option<u16>,
}

// =============================================================================
// Helper Functions for Column Access
// =============================================================================

fn get_uint32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a UInt32Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not UInt32", name)))
}

fn get_uint64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a UInt64Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not UInt64", name)))
}

fn get_uint8_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a arrow::array::UInt8Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<arrow::array::UInt8Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not UInt8", name)))
}

fn get_int8_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int8Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Int8", name)))
}

fn get_float32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Float32Array, ReaderError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| ReaderError::ColumnNotFound(name.to_string()))?
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| ReaderError::InvalidFormat(format!("{} is not Float32", name)))
}

fn get_optional_int32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a arrow::array::Int32Array> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
}

fn get_optional_int8_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int8Array> {
    batch.column_by_name(name)?.as_any().downcast_ref::<Int8Array>()
}

fn get_optional_uint16_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a UInt16Array> {
    batch.column_by_name(name)?.as_any().downcast_ref::<UInt16Array>()
}

fn get_optional_float32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a Float32Array> {
    batch.column_by_name(name)?.as_any().downcast_ref::<Float32Array>()
}

fn get_optional_float64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a Float64Array> {
    batch.column_by_name(name)?.as_any().downcast_ref::<Float64Array>()
}

fn get_optional_i32(array: Option<&arrow::array::Int32Array>, idx: usize) -> Option<i32> {
    array.and_then(|arr| {
        if arr.is_null(idx) {
            None
        } else {
            Some(arr.value(idx))
        }
    })
}

fn get_optional_i8(array: Option<&Int8Array>, idx: usize) -> Option<i8> {
    array.and_then(|arr| {
        if arr.is_null(idx) {
            None
        } else {
            Some(arr.value(idx))
        }
    })
}

fn get_optional_u16(array: Option<&UInt16Array>, idx: usize) -> Option<u16> {
    array.and_then(|arr| {
        if arr.is_null(idx) {
            None
        } else {
            Some(arr.value(idx))
        }
    })
}

// =============================================================================
// Spectra Metadata Extraction from Batch
// =============================================================================

/// Extract spectrum metadata from a record batch row.
fn extract_spectrum_metadata(batch: &RecordBatch, row: usize) -> Result<SpectrumMetadataView, ReaderError> {
    let spectrum_ids = get_uint32_column(batch, spectra_columns::SPECTRUM_ID)?;
    let ms_levels = get_uint8_column(batch, spectra_columns::MS_LEVEL)?;
    let retention_times = get_float32_column(batch, spectra_columns::RETENTION_TIME)?;
    let polarities = get_int8_column(batch, spectra_columns::POLARITY)?;
    let peak_offsets = get_uint64_column(batch, spectra_columns::PEAK_OFFSET)?;
    let peak_counts = get_uint32_column(batch, spectra_columns::PEAK_COUNT)?;

    // Optional columns
    let scan_numbers = get_optional_int32_column(batch, spectra_columns::SCAN_NUMBER);
    let precursor_mzs = get_optional_float64_column(batch, spectra_columns::PRECURSOR_MZ);
    let precursor_charges = get_optional_int8_column(batch, spectra_columns::PRECURSOR_CHARGE);
    let precursor_intensities = get_optional_float32_column(batch, spectra_columns::PRECURSOR_INTENSITY);
    let isolation_lowers = get_optional_float32_column(batch, spectra_columns::ISOLATION_WINDOW_LOWER);
    let isolation_uppers = get_optional_float32_column(batch, spectra_columns::ISOLATION_WINDOW_UPPER);
    let collision_energies = get_optional_float32_column(batch, spectra_columns::COLLISION_ENERGY);
    let tics = get_optional_float64_column(batch, spectra_columns::TOTAL_ION_CURRENT);
    let base_peak_mzs = get_optional_float64_column(batch, spectra_columns::BASE_PEAK_MZ);
    let base_peak_intensities = get_optional_float32_column(batch, spectra_columns::BASE_PEAK_INTENSITY);
    let injection_times = get_optional_float32_column(batch, spectra_columns::INJECTION_TIME);
    let pixel_xs = get_optional_uint16_column(batch, spectra_columns::PIXEL_X);
    let pixel_ys = get_optional_uint16_column(batch, spectra_columns::PIXEL_Y);
    let pixel_zs = get_optional_uint16_column(batch, spectra_columns::PIXEL_Z);

    Ok(SpectrumMetadataView {
        spectrum_id: spectrum_ids.value(row),
        scan_number: get_optional_i32(scan_numbers, row),
        ms_level: ms_levels.value(row),
        retention_time: retention_times.value(row),
        polarity: polarities.value(row),
        peak_offset: peak_offsets.value(row),
        peak_count: peak_counts.value(row),
        precursor_mz: get_optional_f64(precursor_mzs, row),
        precursor_charge: get_optional_i8(precursor_charges, row),
        precursor_intensity: get_optional_f32(precursor_intensities, row),
        isolation_window_lower: get_optional_f32(isolation_lowers, row),
        isolation_window_upper: get_optional_f32(isolation_uppers, row),
        collision_energy: get_optional_f32(collision_energies, row),
        total_ion_current: get_optional_f64(tics, row),
        base_peak_mz: get_optional_f64(base_peak_mzs, row),
        base_peak_intensity: get_optional_f32(base_peak_intensities, row),
        injection_time: get_optional_f32(injection_times, row),
        pixel_x: get_optional_u16(pixel_xs, row),
        pixel_y: get_optional_u16(pixel_ys, row),
        pixel_z: get_optional_u16(pixel_zs, row),
    })
}

// =============================================================================
// Streaming Iterator for Spectrum Metadata
// =============================================================================

/// Streaming iterator over spectrum metadata from the v2 spectra table.
pub struct SpectrumMetadataIterator {
    batch_iter: RecordBatchIterator,
    current_batch: Option<RecordBatch>,
    current_row: usize,
}

impl SpectrumMetadataIterator {
    pub(super) fn new(batch_iter: RecordBatchIterator) -> Self {
        Self {
            batch_iter,
            current_batch: None,
            current_row: 0,
        }
    }
}

impl Iterator for SpectrumMetadataIterator {
    type Item = Result<SpectrumMetadataView, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have a current batch with remaining rows, extract the next spectrum
            if let Some(ref batch) = self.current_batch {
                if self.current_row < batch.num_rows() {
                    let result = extract_spectrum_metadata(batch, self.current_row);
                    self.current_row += 1;
                    return Some(result);
                }
            }

            // Load the next batch
            match self.batch_iter.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(batch);
                    self.current_row = 0;
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

// =============================================================================
// MzPeakReader v2 Methods
// =============================================================================

impl MzPeakReader {
    /// Check if this is a v2 format container with separate spectra table.
    ///
    /// Returns `true` if the container has a `spectra/spectra.parquet` table.
    pub fn has_spectra_table(&self) -> bool {
        matches!(
            &self.source,
            ReaderSource::ZipContainerV2 { .. } | ReaderSource::DirectoryV2 { .. }
        )
    }

    /// Check if this is a v2 format container.
    ///
    /// Alias for `has_spectra_table()` for clarity.
    pub fn is_v2_format(&self) -> bool {
        self.has_spectra_table()
    }

    /// Returns a streaming iterator over record batches from the spectra table.
    ///
    /// This method is only available for v2 format containers.
    /// For v1 format, returns an error.
    ///
    /// # Example
    /// ```rust,no_run
    /// use mzpeak::reader::MzPeakReader;
    ///
    /// let reader = MzPeakReader::open("data_v2.mzpeak")?;
    /// if reader.has_spectra_table() {
    ///     for batch_result in reader.iter_spectra_batches()? {
    ///         let batch = batch_result?;
    ///         println!("Processing {} spectrum rows", batch.num_rows());
    ///     }
    /// }
    /// # Ok::<(), mzpeak::reader::ReaderError>(())
    /// ```
    pub fn iter_spectra_batches(&self) -> Result<RecordBatchIterator, ReaderError> {
        match &self.source {
            ReaderSource::ZipContainerV2 {
                spectra_chunk_reader,
                ..
            } => {
                let builder =
                    ParquetRecordBatchReaderBuilder::try_new(spectra_chunk_reader.clone())?
                        .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                Ok(RecordBatchIterator::new(reader))
            }
            ReaderSource::DirectoryV2 { spectra_path, .. } => {
                let file = File::open(spectra_path)?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
                    .with_batch_size(self.config.batch_size);
                let reader = builder.build()?;
                Ok(RecordBatchIterator::new(reader))
            }
            _ => Err(ReaderError::InvalidFormat(
                "Spectra table is only available in v2 format containers".to_string(),
            )),
        }
    }

    /// Iterate over spectrum metadata from the v2 spectra table.
    ///
    /// Returns a streaming iterator that yields spectrum metadata one at a time.
    /// This is memory-efficient as it doesn't load all spectra at once.
    ///
    /// # Example
    /// ```rust,no_run
    /// use mzpeak::reader::MzPeakReader;
    ///
    /// let reader = MzPeakReader::open("data_v2.mzpeak")?;
    /// for spectrum_result in reader.iter_spectra_metadata()? {
    ///     let spectrum = spectrum_result?;
    ///     println!("Spectrum {}: {} peaks at RT {:.2}s",
    ///         spectrum.spectrum_id, spectrum.peak_count, spectrum.retention_time);
    /// }
    /// # Ok::<(), mzpeak::reader::ReaderError>(())
    /// ```
    pub fn iter_spectra_metadata(&self) -> Result<SpectrumMetadataIterator, ReaderError> {
        let batch_iter = self.iter_spectra_batches()?;
        Ok(SpectrumMetadataIterator::new(batch_iter))
    }

    /// Read all spectrum metadata from the v2 spectra table (eager).
    ///
    /// **Warning**: This loads all metadata into memory. For large files,
    /// prefer `iter_spectra_metadata()`.
    pub fn read_all_spectra_metadata(&self) -> Result<Vec<SpectrumMetadataView>, ReaderError> {
        self.iter_spectra_metadata()?.collect()
    }

    /// Read a batch of spectrum metadata from the v2 spectra table.
    ///
    /// Returns up to `batch_size` spectrum metadata records starting from `offset`.
    ///
    /// # Arguments
    /// * `offset` - The starting spectrum index (0-based)
    /// * `batch_size` - Maximum number of spectra to return
    ///
    /// # Example
    /// ```rust,no_run
    /// use mzpeak::reader::MzPeakReader;
    ///
    /// let reader = MzPeakReader::open("data_v2.mzpeak")?;
    /// // Read spectra 100-199
    /// let batch = reader.read_spectra_batch(100, 100)?;
    /// println!("Read {} spectra", batch.len());
    /// # Ok::<(), mzpeak::reader::ReaderError>(())
    /// ```
    pub fn read_spectra_batch(
        &self,
        offset: usize,
        batch_size: usize,
    ) -> Result<Vec<SpectrumMetadataView>, ReaderError> {
        let mut results = Vec::with_capacity(batch_size);
        let mut current_offset = 0;

        for spectrum_result in self.iter_spectra_metadata()? {
            if current_offset >= offset {
                results.push(spectrum_result?);
                if results.len() >= batch_size {
                    break;
                }
            }
            current_offset += 1;
        }

        Ok(results)
    }

    /// Get the schema for the spectra table (v2 format only).
    ///
    /// Returns the Arrow schema for the spectra parquet file.
    pub fn spectra_schema(&self) -> Result<Arc<Schema>, ReaderError> {
        match &self.source {
            ReaderSource::ZipContainerV2 {
                spectra_chunk_reader,
                ..
            } => {
                let reader = SerializedFileReader::new(spectra_chunk_reader.clone())?;
                let metadata = reader.metadata();
                let schema = parquet::arrow::parquet_to_arrow_schema(
                    metadata.file_metadata().schema_descr(),
                    metadata.file_metadata().key_value_metadata(),
                )?;
                Ok(Arc::new(schema))
            }
            ReaderSource::DirectoryV2 { spectra_path, .. } => {
                let file = File::open(spectra_path)?;
                let reader = SerializedFileReader::new(file)?;
                let metadata = reader.metadata();
                let schema = parquet::arrow::parquet_to_arrow_schema(
                    metadata.file_metadata().schema_descr(),
                    metadata.file_metadata().key_value_metadata(),
                )?;
                Ok(Arc::new(schema))
            }
            _ => Err(ReaderError::InvalidFormat(
                "Spectra schema is only available in v2 format containers".to_string(),
            )),
        }
    }

    /// Get the total number of spectra in a v2 container.
    ///
    /// Returns the row count from the spectra table.
    pub fn total_spectra(&self) -> Result<i64, ReaderError> {
        match &self.source {
            ReaderSource::ZipContainerV2 {
                spectra_chunk_reader,
                ..
            } => {
                let reader = SerializedFileReader::new(spectra_chunk_reader.clone())?;
                let metadata = reader.metadata();
                let total: i64 = (0..metadata.num_row_groups())
                    .map(|i| metadata.row_group(i).num_rows())
                    .sum();
                Ok(total)
            }
            ReaderSource::DirectoryV2 { spectra_path, .. } => {
                let file = File::open(spectra_path)?;
                let reader = SerializedFileReader::new(file)?;
                let metadata = reader.metadata();
                let total: i64 = (0..metadata.num_row_groups())
                    .map(|i| metadata.row_group(i).num_rows())
                    .sum();
                Ok(total)
            }
            _ => Err(ReaderError::InvalidFormat(
                "Spectra count is only available in v2 format containers".to_string(),
            )),
        }
    }

    /// Get spectrum metadata by ID (v2 format only).
    ///
    /// Returns the spectrum metadata for the given spectrum_id, or None if not found.
    pub fn get_spectrum_metadata(&self, spectrum_id: u32) -> Result<Option<SpectrumMetadataView>, ReaderError> {
        for spectrum_result in self.iter_spectra_metadata()? {
            let spectrum = spectrum_result?;
            if spectrum.spectrum_id == spectrum_id {
                return Ok(Some(spectrum));
            }
        }
        Ok(None)
    }

    /// Query spectrum metadata by retention time range (v2 format only).
    ///
    /// Returns all spectra within the given RT range (inclusive).
    pub fn spectra_metadata_by_rt_range(
        &self,
        start_rt: f32,
        end_rt: f32,
    ) -> Result<Vec<SpectrumMetadataView>, ReaderError> {
        let mut results = Vec::new();
        for spectrum_result in self.iter_spectra_metadata()? {
            let spectrum = spectrum_result?;
            if spectrum.retention_time >= start_rt && spectrum.retention_time <= end_rt {
                results.push(spectrum);
            }
        }
        Ok(results)
    }

    /// Query spectrum metadata by MS level (v2 format only).
    pub fn spectra_metadata_by_ms_level(
        &self,
        ms_level: u8,
    ) -> Result<Vec<SpectrumMetadataView>, ReaderError> {
        let mut results = Vec::new();
        for spectrum_result in self.iter_spectra_metadata()? {
            let spectrum = spectrum_result?;
            if spectrum.ms_level == ms_level {
                results.push(spectrum);
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spectrum_metadata_view_default_values() {
        let view = SpectrumMetadataView {
            spectrum_id: 0,
            scan_number: None,
            ms_level: 1,
            retention_time: 60.0,
            polarity: 1,
            peak_offset: 0,
            peak_count: 100,
            precursor_mz: None,
            precursor_charge: None,
            precursor_intensity: None,
            isolation_window_lower: None,
            isolation_window_upper: None,
            collision_energy: None,
            total_ion_current: None,
            base_peak_mz: None,
            base_peak_intensity: None,
            injection_time: None,
            pixel_x: None,
            pixel_y: None,
            pixel_z: None,
        };

        assert_eq!(view.spectrum_id, 0);
        assert_eq!(view.ms_level, 1);
        assert!(view.precursor_mz.is_none());
    }
}
