use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::buffer::Buffer;
use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::record_batch::RecordBatch;


#[cfg(feature = "rayon")]
use rayon::prelude::*;

use parquet::arrow::ArrowWriter;

use crate::metadata::MzPeakMetadata;
use crate::schema::create_mzpeak_schema_arc;

use super::config::WriterConfig;
use super::error::WriterError;
use super::stats::WriterStats;
use super::types::{
    ColumnarBatch, OptionalColumn, OptionalColumnBuf, OwnedColumnarBatch, SpectrumArrays,
};

/// Streaming writer for mzPeak Parquet files
pub struct MzPeakWriter<W: Write + Send + Sync> {
    writer: ArrowWriter<W>,
    schema: Arc<arrow::datatypes::Schema>,
    spectra_written: usize,
    peaks_written: usize,
}

impl MzPeakWriter<File> {
    /// Create a new writer to a file path
    pub fn new_file<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        let file = File::create(path)?;
        Self::new(file, metadata, config)
    }
}

impl<W: Write + Send + Sync> MzPeakWriter<W> {
    /// Create a new writer to any Write implementation
    pub fn new(
        writer: W,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        let schema = create_mzpeak_schema_arc();
        let parquet_metadata = metadata.to_parquet_metadata()?;
        let props = config.to_writer_properties(&parquet_metadata);

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            spectra_written: 0,
            peaks_written: 0,
        })
    }

    pub(super) fn peaks_written(&self) -> usize {
        self.peaks_written
    }

    // ========================================================================
    // Vectorized Array Builder Helpers
    // ========================================================================

    /// Build a Float64 array using append_slice for memcpy speed
    #[inline]
    fn build_f64_array(data: &[f64]) -> ArrayRef {
        let mut builder = Float64Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build a Float32 array using append_slice for memcpy speed
    #[inline]
    fn build_f32_array(data: &[f32]) -> ArrayRef {
        let mut builder = Float32Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build an Int64 array using append_slice for memcpy speed
    #[inline]
    fn build_i64_array(data: &[i64]) -> ArrayRef {
        let mut builder = Int64Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build an Int16 array using append_slice for memcpy speed
    #[inline]
    fn build_i16_array(data: &[i16]) -> ArrayRef {
        let mut builder = Int16Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build an Int8 array using append_slice for memcpy speed
    #[inline]
    fn build_i8_array(data: &[i8]) -> ArrayRef {
        let mut builder = Int8Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build an optional Float64 array with optimized paths for each variant
    #[inline]
    fn build_optional_f64_array(col: &OptionalColumn<f64>, len: usize) -> ArrayRef {
        match col {
            OptionalColumn::AllPresent(data) => {
                let mut builder = Float64Builder::with_capacity(data.len());
                builder.append_slice(data);
                Arc::new(builder.finish())
            }
            OptionalColumn::AllNull => {
                let mut builder = Float64Builder::with_capacity(len);
                builder.append_nulls(len);
                Arc::new(builder.finish())
            }
            OptionalColumn::WithValidity { values, validity } => {
                let mut builder = Float64Builder::with_capacity(values.len());
                builder.append_values(values, validity);
                Arc::new(builder.finish())
            }
        }
    }

    /// Build an optional Float32 array with optimized paths for each variant
    #[inline]
    fn build_optional_f32_array(col: &OptionalColumn<f32>, len: usize) -> ArrayRef {
        match col {
            OptionalColumn::AllPresent(data) => {
                let mut builder = Float32Builder::with_capacity(data.len());
                builder.append_slice(data);
                Arc::new(builder.finish())
            }
            OptionalColumn::AllNull => {
                let mut builder = Float32Builder::with_capacity(len);
                builder.append_nulls(len);
                Arc::new(builder.finish())
            }
            OptionalColumn::WithValidity { values, validity } => {
                let mut builder = Float32Builder::with_capacity(values.len());
                builder.append_values(values, validity);
                Arc::new(builder.finish())
            }
        }
    }

    /// Build an optional Int32 array with optimized paths for each variant
    #[inline]
    fn build_optional_i32_array(col: &OptionalColumn<i32>, len: usize) -> ArrayRef {
        match col {
            OptionalColumn::AllPresent(data) => {
                let mut builder = Int32Builder::with_capacity(data.len());
                builder.append_slice(data);
                Arc::new(builder.finish())
            }
            OptionalColumn::AllNull => {
                let mut builder = Int32Builder::with_capacity(len);
                builder.append_nulls(len);
                Arc::new(builder.finish())
            }
            OptionalColumn::WithValidity { values, validity } => {
                let mut builder = Int32Builder::with_capacity(values.len());
                builder.append_values(values, validity);
                Arc::new(builder.finish())
            }
        }
    }

    /// Build an optional Int16 array with optimized paths for each variant
    #[inline]
    fn build_optional_i16_array(col: &OptionalColumn<i16>, len: usize) -> ArrayRef {
        match col {
            OptionalColumn::AllPresent(data) => {
                let mut builder = Int16Builder::with_capacity(data.len());
                builder.append_slice(data);
                Arc::new(builder.finish())
            }
            OptionalColumn::AllNull => {
                let mut builder = Int16Builder::with_capacity(len);
                builder.append_nulls(len);
                Arc::new(builder.finish())
            }
            OptionalColumn::WithValidity { values, validity } => {
                let mut builder = Int16Builder::with_capacity(values.len());
                builder.append_values(values, validity);
                Arc::new(builder.finish())
            }
        }
    }

    // ========================================================================
    // Zero-Copy Owned Array Constructors
    // ========================================================================
    //
    // These functions accept owned vectors and transfer their underlying memory
    // directly to Arrow buffers without copying any data bytes. The only data
    // movement is internal compression by the Parquet engine.

    /// Convert an owned Vec<f64> to an Arrow Float64Array via zero-copy pointer transfer.
    ///
    /// The vector's heap allocation is handed directly to Arrow's Buffer without
    /// copying any data bytes.
    #[inline]
    fn vec_to_f64_array(data: Vec<f64>) -> ArrayRef {
        let buffer = ScalarBuffer::from(data);
        Arc::new(Float64Array::new(buffer, None))
    }

    /// Convert an owned Vec<f32> to an Arrow Float32Array via zero-copy pointer transfer.
    #[inline]
    fn vec_to_f32_array(data: Vec<f32>) -> ArrayRef {
        let buffer = ScalarBuffer::from(data);
        Arc::new(Float32Array::new(buffer, None))
    }

    /// Convert an owned Vec<i64> to an Arrow Int64Array via zero-copy pointer transfer.
    #[inline]
    fn vec_to_i64_array(data: Vec<i64>) -> ArrayRef {
        let buffer = ScalarBuffer::from(data);
        Arc::new(Int64Array::new(buffer, None))
    }


    /// Convert an owned Vec<i16> to an Arrow Int16Array via zero-copy pointer transfer.
    #[inline]
    fn vec_to_i16_array(data: Vec<i16>) -> ArrayRef {
        let buffer = ScalarBuffer::from(data);
        Arc::new(Int16Array::new(buffer, None))
    }

    /// Convert an owned Vec<i8> to an Arrow Int8Array via zero-copy pointer transfer.
    #[inline]
    fn vec_to_i8_array(data: Vec<i8>) -> ArrayRef {
        let buffer = ScalarBuffer::from(data);
        Arc::new(Int8Array::new(buffer, None))
    }


    /// Create a NullBuffer without checking if all values are valid.
    /// Use this when you already know the column has mixed validity.
    #[inline]
    fn create_null_buffer_unchecked(validity: Vec<bool>) -> Option<NullBuffer> {
        Some(NullBuffer::from(validity))
    }

    /// Convert an owned optional Float64 column to an Arrow Float64Array via zero-copy.
    #[inline]
    fn owned_optional_f64_to_array(
        col: OptionalColumnBuf<f64>,
        len: usize,
        zero_buffer: &Option<Buffer>,
    ) -> ArrayRef {
        match col {
            OptionalColumnBuf::AllPresent(data) => {
                let buffer = ScalarBuffer::from(data);
                Arc::new(Float64Array::new(buffer, None))
            }
            OptionalColumnBuf::AllNull { len: null_len } => {
                let count = null_len.max(len);
                let buffer = if let Some(zero_buf) = zero_buffer {
                    // Reuse shared zero buffer (safe because zero_buf size is >= count * 8)
                    ScalarBuffer::new(zero_buf.clone(), 0, count)
                } else {
                    // Fallback (shouldn't happen in optimized path)
                    ScalarBuffer::from(vec![0.0f64; count])
                };
                let null_buffer = NullBuffer::new_null(count);
                Arc::new(Float64Array::new(buffer, Some(null_buffer)))
            }
            OptionalColumnBuf::WithValidity { values, validity } => {
                let buffer = ScalarBuffer::from(values);
                let null_buffer = Self::create_null_buffer_unchecked(validity);
                Arc::new(Float64Array::new(buffer, null_buffer))
            }
        }
    }

    /// Convert an owned optional Float32 column to an Arrow Float32Array via zero-copy.
    #[inline]
    fn owned_optional_f32_to_array(
        col: OptionalColumnBuf<f32>,
        len: usize,
        zero_buffer: &Option<Buffer>,
    ) -> ArrayRef {
        match col {
            OptionalColumnBuf::AllPresent(data) => {
                let buffer = ScalarBuffer::from(data);
                Arc::new(Float32Array::new(buffer, None))
            }
            OptionalColumnBuf::AllNull { len: null_len } => {
                let count = null_len.max(len);
                let buffer = if let Some(zero_buf) = zero_buffer {
                    ScalarBuffer::new(zero_buf.clone(), 0, count)
                } else {
                    ScalarBuffer::from(vec![0.0f32; count])
                };
                let null_buffer = NullBuffer::new_null(count);
                Arc::new(Float32Array::new(buffer, Some(null_buffer)))
            }
            OptionalColumnBuf::WithValidity { values, validity } => {
                let buffer = ScalarBuffer::from(values);
                let null_buffer = Self::create_null_buffer_unchecked(validity);
                Arc::new(Float32Array::new(buffer, null_buffer))
            }
        }
    }

    /// Convert an owned optional Int32 column to an Arrow Int32Array via zero-copy.
    #[inline]
    fn owned_optional_i32_to_array(
        col: OptionalColumnBuf<i32>,
        len: usize,
        zero_buffer: &Option<Buffer>,
    ) -> ArrayRef {
        match col {
            OptionalColumnBuf::AllPresent(data) => {
                let buffer = ScalarBuffer::from(data);
                Arc::new(Int32Array::new(buffer, None))
            }
            OptionalColumnBuf::AllNull { len: null_len } => {
                let count = null_len.max(len);
                let buffer = if let Some(zero_buf) = zero_buffer {
                    ScalarBuffer::new(zero_buf.clone(), 0, count)
                } else {
                    ScalarBuffer::from(vec![0i32; count])
                };
                let null_buffer = NullBuffer::new_null(count);
                Arc::new(Int32Array::new(buffer, Some(null_buffer)))
            }
            OptionalColumnBuf::WithValidity { values, validity } => {
                let buffer = ScalarBuffer::from(values);
                let null_buffer = Self::create_null_buffer_unchecked(validity);
                Arc::new(Int32Array::new(buffer, null_buffer))
            }
        }
    }

    /// Convert an owned optional Int16 column to an Arrow Int16Array via zero-copy.
    #[inline]
    fn owned_optional_i16_to_array(
        col: OptionalColumnBuf<i16>,
        len: usize,
        zero_buffer: &Option<Buffer>,
    ) -> ArrayRef {
        match col {
            OptionalColumnBuf::AllPresent(data) => {
                let buffer = ScalarBuffer::from(data);
                Arc::new(Int16Array::new(buffer, None))
            }
            OptionalColumnBuf::AllNull { len: null_len } => {
                let count = null_len.max(len);
                let buffer = if let Some(zero_buf) = zero_buffer {
                    ScalarBuffer::new(zero_buf.clone(), 0, count)
                } else {
                    ScalarBuffer::from(vec![0i16; count])
                };
                let null_buffer = NullBuffer::new_null(count);
                Arc::new(Int16Array::new(buffer, Some(null_buffer)))
            }
            OptionalColumnBuf::WithValidity { values, validity } => {
                let buffer = ScalarBuffer::from(values);
                let null_buffer = Self::create_null_buffer_unchecked(validity);
                Arc::new(Int16Array::new(buffer, null_buffer))
            }
        }
    }

    // ========================================================================
    // High-Performance Columnar Batch Writing
    // ========================================================================

    /// Validate that all required column lengths match the expected batch length
    fn validate_batch_lengths(batch: &ColumnarBatch) -> Result<usize, WriterError> {
        let expected = batch.mz.len();
        let checks = [
            ("intensity", batch.intensity.len()),
            ("spectrum_id", batch.spectrum_id.len()),
            ("scan_number", batch.scan_number.len()),
            ("ms_level", batch.ms_level.len()),
            ("retention_time", batch.retention_time.len()),
            ("polarity", batch.polarity.len()),
        ];
        for (name, len) in checks {
            if len != expected {
                return Err(WriterError::InvalidData(format!(
                    "Column '{}' has {} elements, expected {} (matching mz length)",
                    name, len, expected
                )));
            }
        }
        Ok(expected)
    }

    /// Write a columnar batch of peaks using vectorized operations.
    ///
    /// This is the high-performance path for writing mass spectrometry data.
    /// Use this when you already have data in columnar format to avoid the
    /// overhead of per-peak iteration.
    ///
    /// # Performance
    ///
    /// - Required columns use `append_slice` (single memcpy instead of N append_value calls)
    /// - Dense optional columns (`AllPresent`) also use `append_slice`
    /// - Sparse optional columns use `append_values` with validity bitmap
    ///
    /// # Errors
    ///
    /// Returns `WriterError::InvalidData` if required column lengths don't match.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let batch = ColumnarBatch::new(
    ///     &mz_values,
    ///     &intensity_values,
    ///     &spectrum_ids,
    ///     &scan_numbers,
    ///     &ms_levels,
    ///     &retention_times,
    ///     &polarities,
    /// );
    /// writer.write_columnar_batch(&batch)?;
    /// ```
    pub fn write_columnar_batch(&mut self, batch: &ColumnarBatch) -> Result<(), WriterError> {
        let num_peaks = batch.len();
        if num_peaks == 0 {
            return Ok(());
        }

        // Validate all required column lengths match
        Self::validate_batch_lengths(batch)?;

        // Build arrays using append_slice for memcpy speed on required columns
        // and optimized optional column handling
        let arrays: Vec<ArrayRef> = vec![
            // Required columns - direct slice append (schema order)
            Self::build_i64_array(batch.spectrum_id),
            Self::build_i64_array(batch.scan_number),
            Self::build_i16_array(batch.ms_level),
            Self::build_f32_array(batch.retention_time),
            Self::build_i8_array(batch.polarity),
            Self::build_f64_array(batch.mz),
            Self::build_f32_array(batch.intensity),
            // Optional columns
            Self::build_optional_f64_array(&batch.ion_mobility, num_peaks),
            Self::build_optional_f64_array(&batch.precursor_mz, num_peaks),
            Self::build_optional_i16_array(&batch.precursor_charge, num_peaks),
            Self::build_optional_f32_array(&batch.precursor_intensity, num_peaks),
            Self::build_optional_f32_array(&batch.isolation_window_lower, num_peaks),
            Self::build_optional_f32_array(&batch.isolation_window_upper, num_peaks),
            Self::build_optional_f32_array(&batch.collision_energy, num_peaks),
            Self::build_optional_f64_array(&batch.total_ion_current, num_peaks),
            Self::build_optional_f64_array(&batch.base_peak_mz, num_peaks),
            Self::build_optional_f32_array(&batch.base_peak_intensity, num_peaks),
            Self::build_optional_f32_array(&batch.injection_time, num_peaks),
            // MSI pixel coordinates
            Self::build_optional_i32_array(&batch.pixel_x, num_peaks),
            Self::build_optional_i32_array(&batch.pixel_y, num_peaks),
            Self::build_optional_i32_array(&batch.pixel_z, num_peaks),
        ];

        let record_batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.writer.write(&record_batch)?;
        self.peaks_written += num_peaks;

        Ok(())
    }

    // ========================================================================
    // Zero-Copy Owned Batch Writing
    // ========================================================================

    /// Validate that all required column lengths match in an owned batch.
    fn validate_owned_batch_lengths(batch: &OwnedColumnarBatch) -> Result<usize, WriterError> {
        let expected = batch.mz.len();
        let checks = [
            ("intensity", batch.intensity.len()),
            ("spectrum_id", batch.spectrum_id.len()),
            ("scan_number", batch.scan_number.len()),
            ("ms_level", batch.ms_level.len()),
            ("retention_time", batch.retention_time.len()),
            ("polarity", batch.polarity.len()),
        ];
        for (name, len) in checks {
            if len != expected {
                return Err(WriterError::InvalidData(format!(
                    "Column '{}' has {} elements, expected {} (matching mz length)",
                    name, len, expected
                )));
            }
        }
        Ok(expected)
    }

    /// Write an owned columnar batch using true zero-copy ownership transfer.
    ///
    /// This is the highest-performance path for writing mass spectrometry data.
    /// Unlike [`write_columnar_batch`], this method **consumes** the input batch
    /// and transfers ownership of the underlying vector memory directly to Arrow
    /// without copying any data bytes.
    ///
    /// # Zero-Copy Guarantee
    ///
    /// The vectors in the batch are converted directly to Arrow buffers using
    /// pointer ownership transfer via `ScalarBuffer::from(Vec<T>)`. The only
    /// data movement is the internal compression performed by the Parquet engine.
    ///
    /// # Performance
    ///
    /// This method provides O(1) memory transfer for all required columns,
    /// compared to O(N) memcpy operations in [`write_columnar_batch`].
    /// For a batch with 1 million peaks:
    /// - `write_columnar_batch`: ~7 memcpy operations × N elements
    /// - `write_owned_batch`: 0 data copies, only pointer transfers
    ///
    /// # Errors
    ///
    /// Returns `WriterError::InvalidData` if required column lengths don't match.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Prepare owned data
    /// let batch = OwnedColumnarBatch::new(
    ///     mz_values,           // Vec<f64> - ownership transferred
    ///     intensity_values,    // Vec<f32> - ownership transferred
    ///     spectrum_ids,        // Vec<i64> - ownership transferred
    ///     scan_numbers,        // Vec<i64>
    ///     ms_levels,           // Vec<i16>
    ///     retention_times,     // Vec<f32>
    ///     polarities,          // Vec<i8>
    /// );
    ///
    /// // Batch is consumed; memory is transferred without copying
    /// writer.write_owned_batch(batch)?;
    /// ```
    pub fn write_owned_batch(&mut self, batch: OwnedColumnarBatch) -> Result<(), WriterError> {
        let num_peaks = batch.len();
        if num_peaks == 0 {
            return Ok(());
        }

        // Validate all required column lengths match
        Self::validate_owned_batch_lengths(&batch)?;

        // Deconstruct the batch to take ownership of all vectors
        let OwnedColumnarBatch {
            mz,
            intensity,
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            ion_mobility,
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            isolation_window_lower,
            isolation_window_upper,
            collision_energy,
            total_ion_current,
            base_peak_mz,
            base_peak_intensity,
            injection_time,
            pixel_x,
            pixel_y,
            pixel_z,
        } = batch;

        // Build arrays using zero-copy pointer transfer for required columns
        // and optimized optional column handling
        // Initialize a shared zero-buffer for AllNull columns to avoid repeated allocations.
        // We only create it if we have at least one peak.
        // We ensure it is large enough for f64 (8 bytes per element).
        let zero_buffer = if num_peaks > 0 {
             Some(Buffer::from_vec(vec![0u8; num_peaks * 8]))
        } else {
             None
        };
        let zero_buf_ref = &zero_buffer;

        // Build arrays using zero-copy pointer transfer for required columns
        // and optimized optional column handling
        let arrays: Vec<ArrayRef> = vec![
            // Required columns - zero-copy via ScalarBuffer::from(Vec<T>) (schema order)
            Self::vec_to_i64_array(spectrum_id),
            Self::vec_to_i64_array(scan_number),
            Self::vec_to_i16_array(ms_level),
            Self::vec_to_f32_array(retention_time),
            Self::vec_to_i8_array(polarity),
            Self::vec_to_f64_array(mz),
            Self::vec_to_f32_array(intensity),
            // Optional columns - zero-copy where data is present
            Self::owned_optional_f64_to_array(ion_mobility, num_peaks, zero_buf_ref),
            Self::owned_optional_f64_to_array(precursor_mz, num_peaks, zero_buf_ref),
            Self::owned_optional_i16_to_array(precursor_charge, num_peaks, zero_buf_ref),
            Self::owned_optional_f32_to_array(precursor_intensity, num_peaks, zero_buf_ref),
            Self::owned_optional_f32_to_array(isolation_window_lower, num_peaks, zero_buf_ref),
            Self::owned_optional_f32_to_array(isolation_window_upper, num_peaks, zero_buf_ref),
            Self::owned_optional_f32_to_array(collision_energy, num_peaks, zero_buf_ref),
            Self::owned_optional_f64_to_array(total_ion_current, num_peaks, zero_buf_ref),
            Self::owned_optional_f64_to_array(base_peak_mz, num_peaks, zero_buf_ref),
            Self::owned_optional_f32_to_array(base_peak_intensity, num_peaks, zero_buf_ref),
            Self::owned_optional_f32_to_array(injection_time, num_peaks, zero_buf_ref),
            // MSI pixel coordinates
            Self::owned_optional_i32_to_array(pixel_x, num_peaks, zero_buf_ref),
            Self::owned_optional_i32_to_array(pixel_y, num_peaks, zero_buf_ref),
            Self::owned_optional_i32_to_array(pixel_z, num_peaks, zero_buf_ref),
        ];

        let record_batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.writer.write(&record_batch)?;
        self.peaks_written += num_peaks;

        Ok(())
    }

    /// Write spectra by transferring peak buffers directly into owned batches.
    /// Write multiple spectra by merging them into a single OwnedColumnarBatch.
    /// This creates ONE RecordBatch for all spectra instead of one per spectrum.
    pub fn write_spectra_owned(
        &mut self,
        spectra: Vec<SpectrumArrays>,
    ) -> Result<(), WriterError> {
        if spectra.is_empty() {
            return Ok(());
        }

        let total_peaks: usize = spectra.iter().map(|s| s.peak_count()).sum();
        if total_peaks == 0 {
            return Ok(());
        }

        // Pre-allocate all buffers for the merged batch
        let mut mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut spectrum_id_buf: Vec<i64> = Vec::with_capacity(total_peaks);
        let mut scan_number_buf: Vec<i64> = Vec::with_capacity(total_peaks);
        let mut ms_level_buf: Vec<i16> = Vec::with_capacity(total_peaks);
        let mut retention_time_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut polarity_buf: Vec<i8> = Vec::with_capacity(total_peaks);

        // Ion mobility (per-peak optional) - track has_any AND all_valid to avoid O(n) scans
        let mut ion_mobility_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut ion_mobility_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_ion_mobility = false;
        let mut all_valid_ion_mobility = true;

        // Optional spectrum-level columns - track has_any (Some seen) and all_valid (no None seen)
        // This avoids O(n) validity bitmap scans on 12M+ element arrays
        let mut precursor_mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut precursor_mz_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_mz = false;
        let mut all_valid_precursor_mz = true;

        let mut precursor_charge_buf: Vec<i16> = Vec::with_capacity(total_peaks);
        let mut precursor_charge_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_charge = false;
        let mut all_valid_precursor_charge = true;

        let mut precursor_intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut precursor_intensity_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_intensity = false;
        let mut all_valid_precursor_intensity = true;

        let mut isolation_lower_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut isolation_lower_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_isolation_lower = false;
        let mut all_valid_isolation_lower = true;

        let mut isolation_upper_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut isolation_upper_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_isolation_upper = false;
        let mut all_valid_isolation_upper = true;

        let mut collision_energy_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut collision_energy_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_collision_energy = false;
        let mut all_valid_collision_energy = true;

        let mut tic_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut tic_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_tic = false;
        let mut all_valid_tic = true;

        let mut base_peak_mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut base_peak_mz_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_base_peak_mz = false;
        let mut all_valid_base_peak_mz = true;

        let mut base_peak_intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut base_peak_intensity_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_base_peak_intensity = false;
        let mut all_valid_base_peak_intensity = true;

        let mut injection_time_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut injection_time_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_injection_time = false;
        let mut all_valid_injection_time = true;

        let mut pixel_x_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_x_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_x = false;
        let mut all_valid_pixel_x = true;

        let mut pixel_y_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_y_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_y = false;
        let mut all_valid_pixel_y = true;

        let mut pixel_z_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_z_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_z = false;
        let mut all_valid_pixel_z = true;

        let spectra_len = spectra.len();

        // Merge all spectra into one batch - consuming ownership
        for spectrum in spectra {
            let num_peaks = spectrum.peak_count();
            if num_peaks == 0 {
                continue;
            }

            // Take ownership of peak arrays
            let SpectrumArrays {
                spectrum_id,
                scan_number,
                ms_level,
                retention_time,
                polarity,
                precursor_mz,
                precursor_charge,
                precursor_intensity,
                isolation_window_lower,
                isolation_window_upper,
                collision_energy,
                total_ion_current,
                base_peak_mz,
                base_peak_intensity,
                injection_time,
                pixel_x,
                pixel_y,
                pixel_z,
                peaks,
            } = spectrum;

            // Extend mz and intensity directly from owned vectors
            mz_buf.extend(peaks.mz);
            intensity_buf.extend(peaks.intensity);

            // Extend repeated metadata using resize() for Copy types (uses memset)
            let new_len = spectrum_id_buf.len() + num_peaks;
            spectrum_id_buf.resize(new_len, spectrum_id);
            scan_number_buf.resize(new_len, scan_number);
            ms_level_buf.resize(new_len, ms_level);
            retention_time_buf.resize(new_len, retention_time);
            polarity_buf.resize(new_len, polarity);

            // Ion mobility - use resize() for repeated values, track all_valid
            // OPTIMIZATION: Only allocate validity buffer when needed (mixed validity)
            let im_new_len = ion_mobility_buf.len() + num_peaks;
            match peaks.ion_mobility {
                OptionalColumnBuf::AllNull { .. } => {
                    ion_mobility_buf.resize(im_new_len, 0.0);
                    // Transition from all-valid to mixed - backfill validity
                    if all_valid_ion_mobility && has_any_ion_mobility {
                        ion_mobility_valid.resize(ion_mobility_buf.len() - num_peaks, true);
                    }
                    all_valid_ion_mobility = false;
                    ion_mobility_valid.resize(im_new_len, false);
                }
                OptionalColumnBuf::AllPresent(values) => {
                    ion_mobility_buf.extend(values);
                    has_any_ion_mobility = true;
                    // Only fill validity if we already have mixed validity
                    if !all_valid_ion_mobility {
                        ion_mobility_valid.resize(im_new_len, true);
                    }
                }
                OptionalColumnBuf::WithValidity { values, validity } => {
                    // Transition to mixed - backfill if needed
                    if all_valid_ion_mobility && has_any_ion_mobility {
                        ion_mobility_valid.resize(ion_mobility_buf.len(), true);
                    }
                    has_any_ion_mobility = true;
                    all_valid_ion_mobility = false;
                    ion_mobility_buf.extend(values);
                    ion_mobility_valid.extend(validity);
                }
            }

            // Macro for optional spectrum-level columns using resize() (memset)
            // Tracks all_valid to determine final column type (AllPresent vs WithValidity)
            // OPTIMIZATION: Only allocate validity buffer if we actually need it (mixed validity)
            macro_rules! extend_optional {
                ($opt:expr, $buf:ident, $valid:ident, $has_any:ident, $all_valid:ident, $default:expr) => {
                    let opt_new_len = $buf.len() + num_peaks;
                    match $opt {
                        Some(v) => {
                            $buf.resize(opt_new_len, v);
                            $has_any = true;
                            // Only fill validity if we already have mixed validity
                            if !$all_valid {
                                $valid.resize(opt_new_len, true);
                            }
                        }
                        None => {
                            $buf.resize(opt_new_len, $default);
                            // Transition from all-valid to mixed - need to backfill validity
                            if $all_valid && $has_any {
                                // First None after seeing Some - backfill with true
                                $valid.resize($buf.len() - num_peaks, true);
                            }
                            $all_valid = false;
                            $valid.resize(opt_new_len, false);
                        }
                    }
                };
            }

            extend_optional!(precursor_mz, precursor_mz_buf, precursor_mz_valid, has_any_precursor_mz, all_valid_precursor_mz, 0.0);
            extend_optional!(precursor_charge, precursor_charge_buf, precursor_charge_valid, has_any_precursor_charge, all_valid_precursor_charge, 0i16);
            extend_optional!(precursor_intensity, precursor_intensity_buf, precursor_intensity_valid, has_any_precursor_intensity, all_valid_precursor_intensity, 0.0f32);
            extend_optional!(isolation_window_lower, isolation_lower_buf, isolation_lower_valid, has_any_isolation_lower, all_valid_isolation_lower, 0.0f32);
            extend_optional!(isolation_window_upper, isolation_upper_buf, isolation_upper_valid, has_any_isolation_upper, all_valid_isolation_upper, 0.0f32);
            extend_optional!(collision_energy, collision_energy_buf, collision_energy_valid, has_any_collision_energy, all_valid_collision_energy, 0.0f32);
            extend_optional!(total_ion_current, tic_buf, tic_valid, has_any_tic, all_valid_tic, 0.0f64);
            extend_optional!(base_peak_mz, base_peak_mz_buf, base_peak_mz_valid, has_any_base_peak_mz, all_valid_base_peak_mz, 0.0f64);
            extend_optional!(base_peak_intensity, base_peak_intensity_buf, base_peak_intensity_valid, has_any_base_peak_intensity, all_valid_base_peak_intensity, 0.0f32);
            extend_optional!(injection_time, injection_time_buf, injection_time_valid, has_any_injection_time, all_valid_injection_time, 0.0f32);
            extend_optional!(pixel_x, pixel_x_buf, pixel_x_valid, has_any_pixel_x, all_valid_pixel_x, 0i32);
            extend_optional!(pixel_y, pixel_y_buf, pixel_y_valid, has_any_pixel_y, all_valid_pixel_y, 0i32);
            extend_optional!(pixel_z, pixel_z_buf, pixel_z_valid, has_any_pixel_z, all_valid_pixel_z, 0i32);
        }

        // Helper to create OptionalColumnBuf from owned buffers
        // CRITICAL: Uses pre-computed all_valid flag instead of O(n) .iter().all() scan
        // This eliminates ~4 billion boolean comparisons on large batches
        macro_rules! make_optional_owned {
            ($buf:ident, $valid:ident, $has_any:ident, $all_valid:ident) => {
                if !$has_any {
                    OptionalColumnBuf::AllNull { len: $buf.len() }
                } else if $all_valid {
                    OptionalColumnBuf::AllPresent($buf)
                } else {
                    OptionalColumnBuf::WithValidity {
                        values: $buf,
                        validity: $valid,
                    }
                }
            };
        }

        // Build a single merged batch
        let batch = OwnedColumnarBatch {
            mz: mz_buf,
            intensity: intensity_buf,
            spectrum_id: spectrum_id_buf,
            scan_number: scan_number_buf,
            ms_level: ms_level_buf,
            retention_time: retention_time_buf,
            polarity: polarity_buf,
            ion_mobility: make_optional_owned!(ion_mobility_buf, ion_mobility_valid, has_any_ion_mobility, all_valid_ion_mobility),
            precursor_mz: make_optional_owned!(precursor_mz_buf, precursor_mz_valid, has_any_precursor_mz, all_valid_precursor_mz),
            precursor_charge: make_optional_owned!(precursor_charge_buf, precursor_charge_valid, has_any_precursor_charge, all_valid_precursor_charge),
            precursor_intensity: make_optional_owned!(precursor_intensity_buf, precursor_intensity_valid, has_any_precursor_intensity, all_valid_precursor_intensity),
            isolation_window_lower: make_optional_owned!(isolation_lower_buf, isolation_lower_valid, has_any_isolation_lower, all_valid_isolation_lower),
            isolation_window_upper: make_optional_owned!(isolation_upper_buf, isolation_upper_valid, has_any_isolation_upper, all_valid_isolation_upper),
            collision_energy: make_optional_owned!(collision_energy_buf, collision_energy_valid, has_any_collision_energy, all_valid_collision_energy),
            total_ion_current: make_optional_owned!(tic_buf, tic_valid, has_any_tic, all_valid_tic),
            base_peak_mz: make_optional_owned!(base_peak_mz_buf, base_peak_mz_valid, has_any_base_peak_mz, all_valid_base_peak_mz),
            base_peak_intensity: make_optional_owned!(base_peak_intensity_buf, base_peak_intensity_valid, has_any_base_peak_intensity, all_valid_base_peak_intensity),
            injection_time: make_optional_owned!(injection_time_buf, injection_time_valid, has_any_injection_time, all_valid_injection_time),
            pixel_x: make_optional_owned!(pixel_x_buf, pixel_x_valid, has_any_pixel_x, all_valid_pixel_x),
            pixel_y: make_optional_owned!(pixel_y_buf, pixel_y_valid, has_any_pixel_y, all_valid_pixel_y),
            pixel_z: make_optional_owned!(pixel_z_buf, pixel_z_valid, has_any_pixel_z, all_valid_pixel_z),
        };

        // Write the single merged batch
        self.write_owned_batch(batch)?;
        self.spectra_written += spectra_len;
        Ok(())
    }

    /// Write a single spectrum by transferring ownership of its peak arrays.
    ///
    /// This implementation uses zero-copy transfer of peak data buffers.
    pub fn write_spectrum_owned(&mut self, spectrum: SpectrumArrays) -> Result<(), WriterError> {
        let batch = OwnedColumnarBatch::from_spectrum_arrays(spectrum);
        self.write_owned_batch(batch)
    }

    /// Write a batch of spectra with SoA peak layout (Sequential Implementation)
    fn write_spectra_arrays_sequential(
        &mut self,
        spectra: &[SpectrumArrays],
    ) -> Result<(), WriterError> {
        if spectra.is_empty() {
             return Ok(());
        }

        // Calculate total number of peaks for pre-allocation
        let total_peaks: usize = spectra.iter().map(|s| s.peak_count()).sum();

        if total_peaks == 0 {
            return Ok(());
        }

        // Pre-allocate all required column buffers
        let mut mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut spectrum_id_buf: Vec<i64> = Vec::with_capacity(total_peaks);
        let mut scan_number_buf: Vec<i64> = Vec::with_capacity(total_peaks);
        let mut ms_level_buf: Vec<i16> = Vec::with_capacity(total_peaks);
        let mut retention_time_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut polarity_buf: Vec<i8> = Vec::with_capacity(total_peaks);

        // Pre-allocate optional column buffers with validity tracking
        // OPTIMIZATION: Validity buffers are only allocated/filled if mixed validity is seen
        let mut ion_mobility_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut ion_mobility_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_ion_mobility = false;
        let mut all_valid_ion_mobility = true;

        let mut precursor_mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut precursor_mz_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_mz = false;
        let mut all_valid_precursor_mz = true;

        let mut precursor_charge_buf: Vec<i16> = Vec::with_capacity(total_peaks);
        let mut precursor_charge_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_charge = false;
        let mut all_valid_precursor_charge = true;

        let mut precursor_intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut precursor_intensity_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_intensity = false;
        let mut all_valid_precursor_intensity = true;

        let mut isolation_lower_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut isolation_lower_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_isolation_lower = false;
        let mut all_valid_isolation_lower = true;

        let mut isolation_upper_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut isolation_upper_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_isolation_upper = false;
        let mut all_valid_isolation_upper = true;

        let mut collision_energy_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut collision_energy_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_collision_energy = false;
        let mut all_valid_collision_energy = true;

        let mut tic_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut tic_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_tic = false;
        let mut all_valid_tic = true;

        let mut base_peak_mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut base_peak_mz_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_base_peak_mz = false;
        let mut all_valid_base_peak_mz = true;

        let mut base_peak_intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut base_peak_intensity_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_base_peak_intensity = false;
        let mut all_valid_base_peak_intensity = true;

        let mut injection_time_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut injection_time_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_injection_time = false;
        let mut all_valid_injection_time = true;

        let mut pixel_x_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_x_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_x = false;
        let mut all_valid_pixel_x = true;

        let mut pixel_y_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_y_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_y = false;
        let mut all_valid_pixel_y = true;

        let mut pixel_z_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_z_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_z = false;
        let mut all_valid_pixel_z = true;

        // Flatten spectra into columnar buffers
        for spectrum in spectra {
            spectrum
                .peaks
                .validate()
                .map_err(WriterError::InvalidData)?;
            let num_peaks = spectrum.peak_count();
            if num_peaks == 0 {
                continue;
            }

            // Required columns - peak data
            mz_buf.extend_from_slice(&spectrum.peaks.mz);
            intensity_buf.extend_from_slice(&spectrum.peaks.intensity);

            // Required columns - spectrum metadata (repeated for each peak)
            // Using resize() for Copy types (uses memset, O(1) for repeated values)
            let new_len = spectrum_id_buf.len() + num_peaks;
            spectrum_id_buf.resize(new_len, spectrum.spectrum_id);
            scan_number_buf.resize(new_len, spectrum.scan_number);
            ms_level_buf.resize(new_len, spectrum.ms_level);
            retention_time_buf.resize(new_len, spectrum.retention_time);
            polarity_buf.resize(new_len, spectrum.polarity);

            // Ion mobility (optional, per-peak)
            // Lazy allocation: only allocate if we see values
            match &spectrum.peaks.ion_mobility {
                OptionalColumnBuf::AllNull { len } => {
                    if *len != num_peaks {
                        return Err(WriterError::InvalidData(format!(
                            "ion_mobility length {} does not match peak count {}",
                            len, num_peaks
                        )));
                    }
                    
                    if has_any_ion_mobility {
                        // We have seen values before, so now we are appending zeroes (defaults)
                        // Transition from all-valid to mixed logic
                        let im_new_len = ion_mobility_buf.len() + num_peaks;
                        ion_mobility_buf.resize(im_new_len, 0.0);
                        
                        if all_valid_ion_mobility {
                            // Backfill true for previous values
                            ion_mobility_valid.resize(ion_mobility_buf.len() - num_peaks, true);
                            all_valid_ion_mobility = false;
                        }
                        ion_mobility_valid.resize(im_new_len, false);
                    }
                    // If !has_any, do nothing - buffer remains empty
                }
                OptionalColumnBuf::AllPresent(values) => {
                    if values.len() != num_peaks {
                        return Err(WriterError::InvalidData(format!(
                            "ion_mobility length {} does not match peak count {}",
                            values.len(),
                            num_peaks
                        )));
                    }
                    
                    if !has_any_ion_mobility {
                        // First values seen. Backfill defaults if we skipped previous chunks.
                        // Previous chunks count: mz_buf.len() - num_peaks
                        // Note: mz_buf is already extended for this chunk, so we subtract num_peaks
                        let prev_len = mz_buf.len() - num_peaks;
                        if prev_len > 0 {
                            ion_mobility_buf.resize(prev_len, 0.0);
                            ion_mobility_valid.resize(prev_len, false);
                            all_valid_ion_mobility = false;
                        }
                        has_any_ion_mobility = true;
                    }

                    ion_mobility_buf.extend_from_slice(values);
                    
                    // Only fill validity if we already have mixed validity
                    if !all_valid_ion_mobility {
                        let im_new_len = ion_mobility_buf.len(); // already extended
                        ion_mobility_valid.resize(im_new_len, true);
                    }
                }
                OptionalColumnBuf::WithValidity { values, validity } => {
                    if values.len() != num_peaks || validity.len() != num_peaks {
                        return Err(WriterError::InvalidData(format!(
                            "ion_mobility length {} (validity {}) does not match peak count {}",
                            values.len(),
                            validity.len(),
                            num_peaks
                        )));
                    }
                    
                    if !has_any_ion_mobility {
                        // First values seen. Backfill defaults.
                        let prev_len = mz_buf.len() - num_peaks;
                        if prev_len > 0 {
                            ion_mobility_buf.resize(prev_len, 0.0);
                            ion_mobility_valid.resize(prev_len, false);
                            all_valid_ion_mobility = false;
                        }
                        has_any_ion_mobility = true;
                    }

                    // Update validity tracking
                    if all_valid_ion_mobility && has_any_ion_mobility {
                         if validity.iter().any(|&v| !v) {
                             // This chunk introduces nulls to an all-valid sequence
                             ion_mobility_valid.resize(ion_mobility_buf.len(), true);
                             all_valid_ion_mobility = false;
                         }
                    }

                    if !all_valid_ion_mobility {
                        ion_mobility_valid.extend_from_slice(validity);
                    }

                    if validity.iter().any(|&v| !v) {
                        all_valid_ion_mobility = false;
                    }

                    ion_mobility_buf.extend_from_slice(values);
                }
            }

            // Optional spectrum-level columns (repeated for all peaks in this spectrum)
            // Use resize() for O(1) memset, but ONLY if we need to.
            macro_rules! push_optional_repeated {
                ($opt:expr, $buf:ident, $valid:ident, $has_any:ident, $all_valid:ident, $default:expr) => {
                    match $opt {
                        Some(v) => {
                            // If first value seen but have previous nulls
                            if !$has_any {
                                let prev_len = mz_buf.len() - num_peaks;
                                if prev_len > 0 {
                                    $buf.resize(prev_len, $default);
                                    $valid.resize(prev_len, false);
                                    $all_valid = false;
                                }
                                $has_any = true;
                            }
                            
                            let opt_new_len = $buf.len() + num_peaks;
                            $buf.resize(opt_new_len, v);
                            
                            // Only fill validity if we already have mixed validity
                            if !$all_valid {
                                $valid.resize(opt_new_len, true);
                            }
                        }
                        None => {
                            if $has_any {
                                // We have seen values before, so we must append defaults
                                let opt_new_len = $buf.len() + num_peaks;
                                $buf.resize(opt_new_len, $default);
                                
                                // Transition from all-valid to mixed
                                if $all_valid {
                                    $valid.resize($buf.len() - num_peaks, true);
                                    $all_valid = false;
                                }
                                $valid.resize(opt_new_len, false);
                            }
                            // If !has_any, do nothing
                        }
                    }
                };
            }

            push_optional_repeated!(
                spectrum.precursor_mz,
                precursor_mz_buf,
                precursor_mz_valid,
                has_any_precursor_mz,
                all_valid_precursor_mz,
                0.0
            );
            push_optional_repeated!(
                spectrum.precursor_charge,
                precursor_charge_buf,
                precursor_charge_valid,
                has_any_precursor_charge,
                all_valid_precursor_charge,
                0
            );
            push_optional_repeated!(
                spectrum.precursor_intensity,
                precursor_intensity_buf,
                precursor_intensity_valid,
                has_any_precursor_intensity,
                all_valid_precursor_intensity,
                0.0
            );
            push_optional_repeated!(
                spectrum.isolation_window_lower,
                isolation_lower_buf,
                isolation_lower_valid,
                has_any_isolation_lower,
                all_valid_isolation_lower,
                0.0
            );
            push_optional_repeated!(
                spectrum.isolation_window_upper,
                isolation_upper_buf,
                isolation_upper_valid,
                has_any_isolation_upper,
                all_valid_isolation_upper,
                0.0
            );
            push_optional_repeated!(
                spectrum.collision_energy,
                collision_energy_buf,
                collision_energy_valid,
                has_any_collision_energy,
                all_valid_collision_energy,
                0.0
            );
            push_optional_repeated!(
                spectrum.total_ion_current,
                tic_buf,
                tic_valid,
                has_any_tic,
                all_valid_tic,
                0.0
            );
            push_optional_repeated!(
                spectrum.base_peak_mz,
                base_peak_mz_buf,
                base_peak_mz_valid,
                has_any_base_peak_mz,
                all_valid_base_peak_mz,
                0.0
            );
            push_optional_repeated!(
                spectrum.base_peak_intensity,
                base_peak_intensity_buf,
                base_peak_intensity_valid,
                has_any_base_peak_intensity,
                all_valid_base_peak_intensity,
                0.0
            );
            push_optional_repeated!(
                spectrum.injection_time,
                injection_time_buf,
                injection_time_valid,
                has_any_injection_time,
                all_valid_injection_time,
                0.0
            );
            push_optional_repeated!(
                spectrum.pixel_x,
                pixel_x_buf,
                pixel_x_valid,
                has_any_pixel_x,
                all_valid_pixel_x,
                0
            );
            push_optional_repeated!(
                spectrum.pixel_y,
                pixel_y_buf,
                pixel_y_valid,
                has_any_pixel_y,
                all_valid_pixel_y,
                0
            );
            push_optional_repeated!(
                spectrum.pixel_z,
                pixel_z_buf,
                pixel_z_valid,
                has_any_pixel_z,
                all_valid_pixel_z,
                0
            );
        }

        // Helper to create OptionalColumnBuf from owned buffers
        macro_rules! make_optional_owned {
            ($buf:ident, $valid:ident, $has_any:ident, $all_valid:ident) => {
                if !$has_any {
                    OptionalColumnBuf::AllNull { len: total_peaks }
                } else if $all_valid {
                    OptionalColumnBuf::AllPresent($buf)
                } else {
                    OptionalColumnBuf::WithValidity {
                        values: $buf,
                        validity: $valid,
                    }
                }
            };
        }

        // Build OwnedColumnarBatch with appropriate OptionalColumnBuf variants
        // This enables zero-copy transfer to Arrow
        let batch = OwnedColumnarBatch {
            mz: mz_buf,
            intensity: intensity_buf,
            spectrum_id: spectrum_id_buf,
            scan_number: scan_number_buf,
            ms_level: ms_level_buf,
            retention_time: retention_time_buf,
            polarity: polarity_buf,
            ion_mobility: make_optional_owned!(ion_mobility_buf, ion_mobility_valid, has_any_ion_mobility, all_valid_ion_mobility),
            precursor_mz: make_optional_owned!(precursor_mz_buf, precursor_mz_valid, has_any_precursor_mz, all_valid_precursor_mz),
            precursor_charge: make_optional_owned!(
                precursor_charge_buf,
                precursor_charge_valid,
                has_any_precursor_charge,
                all_valid_precursor_charge
            ),
            precursor_intensity: make_optional_owned!(
                precursor_intensity_buf,
                precursor_intensity_valid,
                has_any_precursor_intensity,
                all_valid_precursor_intensity
            ),
            isolation_window_lower: make_optional_owned!(
                isolation_lower_buf,
                isolation_lower_valid,
                has_any_isolation_lower,
                all_valid_isolation_lower
            ),
            isolation_window_upper: make_optional_owned!(
                isolation_upper_buf,
                isolation_upper_valid,
                has_any_isolation_upper,
                all_valid_isolation_upper
            ),
            collision_energy: make_optional_owned!(
                collision_energy_buf,
                collision_energy_valid,
                has_any_collision_energy,
                all_valid_collision_energy
            ),
            total_ion_current: make_optional_owned!(tic_buf, tic_valid, has_any_tic, all_valid_tic),
            base_peak_mz: make_optional_owned!(base_peak_mz_buf, base_peak_mz_valid, has_any_base_peak_mz, all_valid_base_peak_mz),
            base_peak_intensity: make_optional_owned!(
                base_peak_intensity_buf,
                base_peak_intensity_valid,
                has_any_base_peak_intensity,
                all_valid_base_peak_intensity
            ),
            injection_time: make_optional_owned!(
                injection_time_buf,
                injection_time_valid,
                has_any_injection_time,
                all_valid_injection_time
            ),
            pixel_x: make_optional_owned!(pixel_x_buf, pixel_x_valid, has_any_pixel_x, all_valid_pixel_x),
            pixel_y: make_optional_owned!(pixel_y_buf, pixel_y_valid, has_any_pixel_y, all_valid_pixel_y),
            pixel_z: make_optional_owned!(pixel_z_buf, pixel_z_valid, has_any_pixel_z, all_valid_pixel_z),
        };

        self.spectra_written += spectra.len();
        self.write_owned_batch(batch)
    }

    /// Write a batch of spectra with SoA peak layout (Parallel Implementation)
    #[cfg(feature = "rayon")]
    fn write_spectra_arrays_parallel(
        &mut self,
        spectra: &[SpectrumArrays],
    ) -> Result<(), WriterError> {
        let total_peaks: usize = spectra.par_iter().map(|s| s.peak_count()).sum();
        if total_peaks == 0 {
            return Ok(());
        }

        // 1. Required columns (Parallel Fill)
        let mut mz_buf = Vec::with_capacity(total_peaks);
        mz_buf.par_extend(spectra.par_iter().flat_map_iter(|s| s.peaks.mz.iter().cloned()));

        let mut intensity_buf = Vec::with_capacity(total_peaks);
        intensity_buf.par_extend(spectra.par_iter().flat_map_iter(|s| s.peaks.intensity.iter().cloned()));


        // Repeated metadata columns
        // Helper to parallel fill repeated values
        macro_rules! par_extend_repeated {
            ($buf:ident, $field:ident, $type:ty) => {
                $buf.par_extend(spectra.par_iter().flat_map_iter(|s| {
                    std::iter::repeat(s.$field).take(s.peak_count())
                }));
            };
        }

        let mut spectrum_id_buf: Vec<i64> = Vec::with_capacity(total_peaks);
        par_extend_repeated!(spectrum_id_buf, spectrum_id, i64);

        let mut scan_number_buf: Vec<i64> = Vec::with_capacity(total_peaks);
        par_extend_repeated!(scan_number_buf, scan_number, i64);

        let mut ms_level_buf: Vec<i16> = Vec::with_capacity(total_peaks);
        par_extend_repeated!(ms_level_buf, ms_level, i16);

        let mut retention_time_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        par_extend_repeated!(retention_time_buf, retention_time, f32);

        let mut polarity_buf: Vec<i8> = Vec::with_capacity(total_peaks);
        par_extend_repeated!(polarity_buf, polarity, i8);

        // 2. Optional columns
        // Helper to process optional columns
        // - Checks validity globally (Map-Reduce)
        // - Fills value buffer if needed
        // - Fills validity buffer if needed (mixed validity)
        
        // Handle ion_mobility (per-peak optional column) inline to avoid lifetime issues
        let (has_any_ion_mobility, all_valid_ion_mobility) = spectra.par_iter()
            .map(|s| match &s.peaks.ion_mobility {
                OptionalColumnBuf::AllNull { .. } => (false, false),
                OptionalColumnBuf::AllPresent(_) => (true, true),
                OptionalColumnBuf::WithValidity { validity, .. } => (true, validity.iter().all(|&v| v)),
            })
            .reduce(
                || (false, true),
                |acc, x| (acc.0 || x.0, acc.1 && x.1)
            );

        let mut ion_mobility_buf: Vec<f64> = Vec::with_capacity(if has_any_ion_mobility { total_peaks } else { 0 });
        let mut ion_mobility_valid: Vec<bool> = Vec::with_capacity(if has_any_ion_mobility && !all_valid_ion_mobility { total_peaks } else { 0 });

        if has_any_ion_mobility {
            ion_mobility_buf.par_extend(spectra.par_iter().flat_map_iter(|s| {
                match &s.peaks.ion_mobility {
                    OptionalColumnBuf::AllNull { len } => {
                        rayon::iter::Either::Left(std::iter::repeat(0.0f64).take(*len))
                    }
                    OptionalColumnBuf::AllPresent(v) => {
                        rayon::iter::Either::Right(rayon::iter::Either::Left(v.iter().cloned()))
                    }
                    OptionalColumnBuf::WithValidity { values, .. } => {
                        rayon::iter::Either::Right(rayon::iter::Either::Right(values.iter().cloned()))
                    }
                }
            }));

            if !all_valid_ion_mobility {
                ion_mobility_valid.par_extend(spectra.par_iter().flat_map_iter(|s| {
                    match &s.peaks.ion_mobility {
                        OptionalColumnBuf::AllNull { len } => {
                            rayon::iter::Either::Left(std::iter::repeat(false).take(*len))
                        }
                        OptionalColumnBuf::AllPresent(v) => {
                            rayon::iter::Either::Right(rayon::iter::Either::Left(std::iter::repeat(true).take(v.len())))
                        }
                        OptionalColumnBuf::WithValidity { validity, .. } => {
                            rayon::iter::Either::Right(rayon::iter::Either::Right(validity.iter().cloned()))
                        }
                    }
                }));
            }
        }

        // Case 2: Repeated (Spectrum-level) Optional Column
        macro_rules! process_optional_col {
            ($name_buf:ident, $name_valid:ident, $has_any:ident, $all_valid:ident, $field:ident, $type:ty, $default:expr, "repeated") => {
                 let ($has_any, $all_valid) = spectra.par_iter()
                    .map(|s| match s.$field {
                        Some(_) => (true, true),
                        None => (false, false),
                    })
                    .reduce(
                        || (false, true),
                        |acc, x| (acc.0 || x.0, acc.1 && x.1)
                    );

                 let mut $name_buf: Vec<$type> = Vec::with_capacity(if $has_any { total_peaks } else { 0 });
                 let mut $name_valid: Vec<bool> = Vec::with_capacity(if $has_any && !$all_valid { total_peaks } else { 0 });

                 if $has_any {
                     $name_buf.par_extend(spectra.par_iter().flat_map_iter(|s| {
                         let val = s.$field.unwrap_or($default);
                         std::iter::repeat(val).take(s.peak_count())
                     }));

                     if !$all_valid {
                         $name_valid.par_extend(spectra.par_iter().flat_map_iter(|s| {
                             let valid = s.$field.is_some();
                             std::iter::repeat(valid).take(s.peak_count())
                         }));
                     }
                 }
            };
        }

        process_optional_col!(precursor_mz_buf, precursor_mz_valid, has_any_precursor_mz, all_valid_precursor_mz, precursor_mz, f64, 0.0, "repeated");
        process_optional_col!(precursor_charge_buf, precursor_charge_valid, has_any_precursor_charge, all_valid_precursor_charge, precursor_charge, i16, 0, "repeated");
        process_optional_col!(precursor_intensity_buf, precursor_intensity_valid, has_any_precursor_intensity, all_valid_precursor_intensity, precursor_intensity, f32, 0.0f32, "repeated");
        process_optional_col!(isolation_lower_buf, isolation_lower_valid, has_any_isolation_lower, all_valid_isolation_lower, isolation_window_lower, f32, 0.0f32, "repeated");
        process_optional_col!(isolation_upper_buf, isolation_upper_valid, has_any_isolation_upper, all_valid_isolation_upper, isolation_window_upper, f32, 0.0f32, "repeated");
        process_optional_col!(collision_energy_buf, collision_energy_valid, has_any_collision_energy, all_valid_collision_energy, collision_energy, f32, 0.0f32, "repeated");
        process_optional_col!(tic_buf, tic_valid, has_any_tic, all_valid_tic, total_ion_current, f64, 0.0f64, "repeated");
        process_optional_col!(base_peak_mz_buf, base_peak_mz_valid, has_any_base_peak_mz, all_valid_base_peak_mz, base_peak_mz, f64, 0.0f64, "repeated");
        process_optional_col!(base_peak_intensity_buf, base_peak_intensity_valid, has_any_base_peak_intensity, all_valid_base_peak_intensity, base_peak_intensity, f32, 0.0f32, "repeated");
        process_optional_col!(injection_time_buf, injection_time_valid, has_any_injection_time, all_valid_injection_time, injection_time, f32, 0.0f32, "repeated");
        process_optional_col!(pixel_x_buf, pixel_x_valid, has_any_pixel_x, all_valid_pixel_x, pixel_x, i32, 0, "repeated");
        process_optional_col!(pixel_y_buf, pixel_y_valid, has_any_pixel_y, all_valid_pixel_y, pixel_y, i32, 0, "repeated");
        process_optional_col!(pixel_z_buf, pixel_z_valid, has_any_pixel_z, all_valid_pixel_z, pixel_z, i32, 0, "repeated");

        // Helper to create OptionalColumnBuf from owned buffers
        macro_rules! make_optional_owned {
            ($buf:ident, $valid:ident, $has_any:ident, $all_valid:ident) => {
                if !$has_any {
                    OptionalColumnBuf::AllNull { len: total_peaks }
                } else if $all_valid {
                    OptionalColumnBuf::AllPresent($buf)
                } else {
                    OptionalColumnBuf::WithValidity {
                        values: $buf,
                        validity: $valid,
                    }
                }
            };
        }

        let batch = OwnedColumnarBatch {
            mz: mz_buf,
            intensity: intensity_buf,
            spectrum_id: spectrum_id_buf,
            scan_number: scan_number_buf,
            ms_level: ms_level_buf,
            retention_time: retention_time_buf,
            polarity: polarity_buf,
            ion_mobility: make_optional_owned!(ion_mobility_buf, ion_mobility_valid, has_any_ion_mobility, all_valid_ion_mobility),
            precursor_mz: make_optional_owned!(precursor_mz_buf, precursor_mz_valid, has_any_precursor_mz, all_valid_precursor_mz),
            precursor_charge: make_optional_owned!(precursor_charge_buf, precursor_charge_valid, has_any_precursor_charge, all_valid_precursor_charge),
            precursor_intensity: make_optional_owned!(precursor_intensity_buf, precursor_intensity_valid, has_any_precursor_intensity, all_valid_precursor_intensity),
            isolation_window_lower: make_optional_owned!(isolation_lower_buf, isolation_lower_valid, has_any_isolation_lower, all_valid_isolation_lower),
            isolation_window_upper: make_optional_owned!(isolation_upper_buf, isolation_upper_valid, has_any_isolation_upper, all_valid_isolation_upper),
            collision_energy: make_optional_owned!(collision_energy_buf, collision_energy_valid, has_any_collision_energy, all_valid_collision_energy),
            total_ion_current: make_optional_owned!(tic_buf, tic_valid, has_any_tic, all_valid_tic),
            base_peak_mz: make_optional_owned!(base_peak_mz_buf, base_peak_mz_valid, has_any_base_peak_mz, all_valid_base_peak_mz),
            base_peak_intensity: make_optional_owned!(base_peak_intensity_buf, base_peak_intensity_valid, has_any_base_peak_intensity, all_valid_base_peak_intensity),
            injection_time: make_optional_owned!(injection_time_buf, injection_time_valid, has_any_injection_time, all_valid_injection_time),
            pixel_x: make_optional_owned!(pixel_x_buf, pixel_x_valid, has_any_pixel_x, all_valid_pixel_x),
            pixel_y: make_optional_owned!(pixel_y_buf, pixel_y_valid, has_any_pixel_y, all_valid_pixel_y),
            pixel_z: make_optional_owned!(pixel_z_buf, pixel_z_valid, has_any_pixel_z, all_valid_pixel_z),
        };

        self.spectra_written += spectra.len();
        self.write_owned_batch(batch)
    }

    /// Write a batch of spectra with SoA peak layout
    pub fn write_spectra_arrays(
        &mut self,
        spectra: &[SpectrumArrays],
    ) -> Result<(), WriterError> {
        #[cfg(feature = "rayon")]
        {
             return self.write_spectra_arrays_parallel(spectra);
        }
        #[cfg(not(feature = "rayon"))]
        {
             return self.write_spectra_arrays_sequential(spectra);
        }
    }

    /// Write a single spectrum with SoA peak layout.
    pub fn write_spectrum_arrays(&mut self, spectrum: &SpectrumArrays) -> Result<(), WriterError> {
        self.write_spectra_arrays(std::slice::from_ref(spectrum))
    }

    /// Flush any buffered data and finalize the file
    pub fn finish(self) -> Result<WriterStats, WriterError> {
        let file_metadata = self.writer.close()?;


        Ok(WriterStats {
            spectra_written: self.spectra_written,
            peaks_written: self.peaks_written,
            row_groups_written: file_metadata.row_groups.len(),
            file_size_bytes: file_metadata
                .row_groups
                .iter()
                .map(|rg| rg.total_byte_size as u64)
                .sum(),
        })
    }

    /// Flush any buffered data, finalize the file, and return the underlying writer
    ///
    /// This is useful when the writer is backed by an in-memory buffer and you need
    /// to access the written data.
    pub fn finish_into_inner(self) -> Result<W, WriterError> {
        let inner = self.writer.into_inner()?;
        Ok(inner)
    }

    /// Get current statistics
    pub fn stats(&self) -> WriterStats {
        WriterStats {
            spectra_written: self.spectra_written,
            peaks_written: self.peaks_written,
            row_groups_written: 0, // Unknown until finish
            file_size_bytes: 0,    // Unknown until finish
        }
    }
}
