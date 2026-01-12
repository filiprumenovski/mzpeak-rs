use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use crate::metadata::MzPeakMetadata;
use crate::schema::create_mzpeak_schema_arc;

use super::config::WriterConfig;
use super::error::WriterError;
use super::stats::WriterStats;
use super::types::{
    ColumnarBatch, OptionalColumn, OptionalColumnBuf, Spectrum, SpectrumArrays,
};

/// Streaming writer for mzPeak Parquet files
pub struct MzPeakWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<arrow::datatypes::Schema>,
    config: WriterConfig,
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

impl<W: Write + Send> MzPeakWriter<W> {
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
            config,
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

    /// Build an Int32 array using append_slice for memcpy speed
    #[inline]
    fn build_i32_array(data: &[i32]) -> ArrayRef {
        let mut builder = Int32Builder::with_capacity(data.len());
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
    /// overhead of creating `Spectrum` objects and per-peak iteration.
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

    /// Write a batch of spectra to the file
    ///
    /// Spectra are expanded into the "Long" format where each peak is a row.
    /// This method flattens the spectra into columnar buffers and delegates to
    /// `write_columnar_batch` for vectorized writing.
    pub fn write_spectra(&mut self, spectra: &[Spectrum]) -> Result<(), WriterError> {
        if spectra.is_empty() {
            return Ok(());
        }

        // Calculate total number of peaks for pre-allocation
        let total_peaks: usize = spectra.iter().map(|s| s.peaks.len()).sum();

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
        let mut ion_mobility_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut ion_mobility_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_ion_mobility = false;

        let mut precursor_mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut precursor_mz_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_mz = false;

        let mut precursor_charge_buf: Vec<i16> = Vec::with_capacity(total_peaks);
        let mut precursor_charge_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_charge = false;

        let mut precursor_intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut precursor_intensity_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_intensity = false;

        let mut isolation_lower_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut isolation_lower_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_isolation_lower = false;

        let mut isolation_upper_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut isolation_upper_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_isolation_upper = false;

        let mut collision_energy_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut collision_energy_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_collision_energy = false;

        let mut tic_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut tic_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_tic = false;

        let mut base_peak_mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut base_peak_mz_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_base_peak_mz = false;

        let mut base_peak_intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut base_peak_intensity_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_base_peak_intensity = false;

        let mut injection_time_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut injection_time_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_injection_time = false;

        let mut pixel_x_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_x_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_x = false;

        let mut pixel_y_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_y_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_y = false;

        let mut pixel_z_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_z_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_z = false;

        // Flatten spectra into columnar buffers
        for spectrum in spectra {
            let num_peaks = spectrum.peaks.len();
            for peak in &spectrum.peaks {
                // Required columns - peak data
                mz_buf.push(peak.mz);
                intensity_buf.push(peak.intensity);

                // Required columns - spectrum metadata (repeated for each peak)
                spectrum_id_buf.push(spectrum.spectrum_id);
                scan_number_buf.push(spectrum.scan_number);
                ms_level_buf.push(spectrum.ms_level);
                retention_time_buf.push(spectrum.retention_time);
                polarity_buf.push(spectrum.polarity);

                // Ion mobility (optional, per-peak)
                match peak.ion_mobility {
                    Some(v) => {
                        ion_mobility_buf.push(v);
                        ion_mobility_valid.push(true);
                        has_any_ion_mobility = true;
                    }
                    None => {
                        ion_mobility_buf.push(0.0);
                        ion_mobility_valid.push(false);
                    }
                }
            }

            // Optional spectrum-level columns (repeated for all peaks in this spectrum)
            // These use a more efficient approach: push N copies at once
            macro_rules! push_optional_repeated {
                ($opt:expr, $buf:ident, $valid:ident, $has_any:ident, $default:expr) => {
                    match $opt {
                        Some(v) => {
                            for _ in 0..num_peaks {
                                $buf.push(v);
                                $valid.push(true);
                            }
                            $has_any = true;
                        }
                        None => {
                            for _ in 0..num_peaks {
                                $buf.push($default);
                                $valid.push(false);
                            }
                        }
                    }
                };
            }

            push_optional_repeated!(
                spectrum.precursor_mz,
                precursor_mz_buf,
                precursor_mz_valid,
                has_any_precursor_mz,
                0.0
            );
            push_optional_repeated!(
                spectrum.precursor_charge,
                precursor_charge_buf,
                precursor_charge_valid,
                has_any_precursor_charge,
                0
            );
            push_optional_repeated!(
                spectrum.precursor_intensity,
                precursor_intensity_buf,
                precursor_intensity_valid,
                has_any_precursor_intensity,
                0.0
            );
            push_optional_repeated!(
                spectrum.isolation_window_lower,
                isolation_lower_buf,
                isolation_lower_valid,
                has_any_isolation_lower,
                0.0
            );
            push_optional_repeated!(
                spectrum.isolation_window_upper,
                isolation_upper_buf,
                isolation_upper_valid,
                has_any_isolation_upper,
                0.0
            );
            push_optional_repeated!(
                spectrum.collision_energy,
                collision_energy_buf,
                collision_energy_valid,
                has_any_collision_energy,
                0.0
            );
            push_optional_repeated!(
                spectrum.total_ion_current,
                tic_buf,
                tic_valid,
                has_any_tic,
                0.0
            );
            push_optional_repeated!(
                spectrum.base_peak_mz,
                base_peak_mz_buf,
                base_peak_mz_valid,
                has_any_base_peak_mz,
                0.0
            );
            push_optional_repeated!(
                spectrum.base_peak_intensity,
                base_peak_intensity_buf,
                base_peak_intensity_valid,
                has_any_base_peak_intensity,
                0.0
            );
            push_optional_repeated!(
                spectrum.injection_time,
                injection_time_buf,
                injection_time_valid,
                has_any_injection_time,
                0.0
            );
            push_optional_repeated!(
                spectrum.pixel_x,
                pixel_x_buf,
                pixel_x_valid,
                has_any_pixel_x,
                0
            );
            push_optional_repeated!(
                spectrum.pixel_y,
                pixel_y_buf,
                pixel_y_valid,
                has_any_pixel_y,
                0
            );
            push_optional_repeated!(
                spectrum.pixel_z,
                pixel_z_buf,
                pixel_z_valid,
                has_any_pixel_z,
                0
            );
        }

        // Helper to create OptionalColumn from buffers
        macro_rules! make_optional {
            ($buf:ident, $valid:ident, $has_any:ident) => {
                if !$has_any {
                    OptionalColumn::AllNull
                } else if $valid.iter().all(|&v| v) {
                    OptionalColumn::AllPresent(&$buf)
                } else {
                    OptionalColumn::WithValidity {
                        values: &$buf,
                        validity: &$valid,
                    }
                }
            };
        }

        // Build ColumnarBatch with appropriate OptionalColumn variants
        let batch = ColumnarBatch {
            mz: &mz_buf,
            intensity: &intensity_buf,
            spectrum_id: &spectrum_id_buf,
            scan_number: &scan_number_buf,
            ms_level: &ms_level_buf,
            retention_time: &retention_time_buf,
            polarity: &polarity_buf,
            ion_mobility: make_optional!(ion_mobility_buf, ion_mobility_valid, has_any_ion_mobility),
            precursor_mz: make_optional!(precursor_mz_buf, precursor_mz_valid, has_any_precursor_mz),
            precursor_charge: make_optional!(
                precursor_charge_buf,
                precursor_charge_valid,
                has_any_precursor_charge
            ),
            precursor_intensity: make_optional!(
                precursor_intensity_buf,
                precursor_intensity_valid,
                has_any_precursor_intensity
            ),
            isolation_window_lower: make_optional!(
                isolation_lower_buf,
                isolation_lower_valid,
                has_any_isolation_lower
            ),
            isolation_window_upper: make_optional!(
                isolation_upper_buf,
                isolation_upper_valid,
                has_any_isolation_upper
            ),
            collision_energy: make_optional!(
                collision_energy_buf,
                collision_energy_valid,
                has_any_collision_energy
            ),
            total_ion_current: make_optional!(tic_buf, tic_valid, has_any_tic),
            base_peak_mz: make_optional!(base_peak_mz_buf, base_peak_mz_valid, has_any_base_peak_mz),
            base_peak_intensity: make_optional!(
                base_peak_intensity_buf,
                base_peak_intensity_valid,
                has_any_base_peak_intensity
            ),
            injection_time: make_optional!(
                injection_time_buf,
                injection_time_valid,
                has_any_injection_time
            ),
            pixel_x: make_optional!(pixel_x_buf, pixel_x_valid, has_any_pixel_x),
            pixel_y: make_optional!(pixel_y_buf, pixel_y_valid, has_any_pixel_y),
            pixel_z: make_optional!(pixel_z_buf, pixel_z_valid, has_any_pixel_z),
        };

        self.spectra_written += spectra.len();
        self.write_columnar_batch(&batch)
    }

    /// Write a single spectrum
    pub fn write_spectrum(&mut self, spectrum: &Spectrum) -> Result<(), WriterError> {
        self.write_spectra(std::slice::from_ref(spectrum))
    }

    /// Write a batch of spectra with SoA peak layout
    pub fn write_spectra_arrays(
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
        let mut ion_mobility_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut ion_mobility_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_ion_mobility = false;

        let mut precursor_mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut precursor_mz_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_mz = false;

        let mut precursor_charge_buf: Vec<i16> = Vec::with_capacity(total_peaks);
        let mut precursor_charge_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_charge = false;

        let mut precursor_intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut precursor_intensity_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_precursor_intensity = false;

        let mut isolation_lower_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut isolation_lower_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_isolation_lower = false;

        let mut isolation_upper_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut isolation_upper_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_isolation_upper = false;

        let mut collision_energy_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut collision_energy_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_collision_energy = false;

        let mut tic_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut tic_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_tic = false;

        let mut base_peak_mz_buf: Vec<f64> = Vec::with_capacity(total_peaks);
        let mut base_peak_mz_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_base_peak_mz = false;

        let mut base_peak_intensity_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut base_peak_intensity_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_base_peak_intensity = false;

        let mut injection_time_buf: Vec<f32> = Vec::with_capacity(total_peaks);
        let mut injection_time_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_injection_time = false;

        let mut pixel_x_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_x_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_x = false;

        let mut pixel_y_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_y_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_y = false;

        let mut pixel_z_buf: Vec<i32> = Vec::with_capacity(total_peaks);
        let mut pixel_z_valid: Vec<bool> = Vec::with_capacity(total_peaks);
        let mut has_any_pixel_z = false;

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
            for _ in 0..num_peaks {
                spectrum_id_buf.push(spectrum.spectrum_id);
                scan_number_buf.push(spectrum.scan_number);
                ms_level_buf.push(spectrum.ms_level);
                retention_time_buf.push(spectrum.retention_time);
                polarity_buf.push(spectrum.polarity);
            }

            // Ion mobility (optional, per-peak)
            match &spectrum.peaks.ion_mobility {
                OptionalColumnBuf::AllNull { len } => {
                    if *len != num_peaks {
                        return Err(WriterError::InvalidData(format!(
                            "ion_mobility length {} does not match peak count {}",
                            len, num_peaks
                        )));
                    }
                    for _ in 0..num_peaks {
                        ion_mobility_buf.push(0.0);
                        ion_mobility_valid.push(false);
                    }
                }
                OptionalColumnBuf::AllPresent(values) => {
                    if values.len() != num_peaks {
                        return Err(WriterError::InvalidData(format!(
                            "ion_mobility length {} does not match peak count {}",
                            values.len(),
                            num_peaks
                        )));
                    }
                    ion_mobility_buf.extend_from_slice(values);
                    ion_mobility_valid.extend(std::iter::repeat(true).take(num_peaks));
                    has_any_ion_mobility = true;
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
                    ion_mobility_buf.extend_from_slice(values);
                    ion_mobility_valid.extend_from_slice(validity);
                    if validity.iter().any(|&v| v) {
                        has_any_ion_mobility = true;
                    }
                }
            }

            // Optional spectrum-level columns (repeated for all peaks in this spectrum)
            // These use a more efficient approach: push N copies at once
            macro_rules! push_optional_repeated {
                ($opt:expr, $buf:ident, $valid:ident, $has_any:ident, $default:expr) => {
                    match $opt {
                        Some(v) => {
                            for _ in 0..num_peaks {
                                $buf.push(v);
                                $valid.push(true);
                            }
                            $has_any = true;
                        }
                        None => {
                            for _ in 0..num_peaks {
                                $buf.push($default);
                                $valid.push(false);
                            }
                        }
                    }
                };
            }

            push_optional_repeated!(
                spectrum.precursor_mz,
                precursor_mz_buf,
                precursor_mz_valid,
                has_any_precursor_mz,
                0.0
            );
            push_optional_repeated!(
                spectrum.precursor_charge,
                precursor_charge_buf,
                precursor_charge_valid,
                has_any_precursor_charge,
                0
            );
            push_optional_repeated!(
                spectrum.precursor_intensity,
                precursor_intensity_buf,
                precursor_intensity_valid,
                has_any_precursor_intensity,
                0.0
            );
            push_optional_repeated!(
                spectrum.isolation_window_lower,
                isolation_lower_buf,
                isolation_lower_valid,
                has_any_isolation_lower,
                0.0
            );
            push_optional_repeated!(
                spectrum.isolation_window_upper,
                isolation_upper_buf,
                isolation_upper_valid,
                has_any_isolation_upper,
                0.0
            );
            push_optional_repeated!(
                spectrum.collision_energy,
                collision_energy_buf,
                collision_energy_valid,
                has_any_collision_energy,
                0.0
            );
            push_optional_repeated!(
                spectrum.total_ion_current,
                tic_buf,
                tic_valid,
                has_any_tic,
                0.0
            );
            push_optional_repeated!(
                spectrum.base_peak_mz,
                base_peak_mz_buf,
                base_peak_mz_valid,
                has_any_base_peak_mz,
                0.0
            );
            push_optional_repeated!(
                spectrum.base_peak_intensity,
                base_peak_intensity_buf,
                base_peak_intensity_valid,
                has_any_base_peak_intensity,
                0.0
            );
            push_optional_repeated!(
                spectrum.injection_time,
                injection_time_buf,
                injection_time_valid,
                has_any_injection_time,
                0.0
            );
            push_optional_repeated!(
                spectrum.pixel_x,
                pixel_x_buf,
                pixel_x_valid,
                has_any_pixel_x,
                0
            );
            push_optional_repeated!(
                spectrum.pixel_y,
                pixel_y_buf,
                pixel_y_valid,
                has_any_pixel_y,
                0
            );
            push_optional_repeated!(
                spectrum.pixel_z,
                pixel_z_buf,
                pixel_z_valid,
                has_any_pixel_z,
                0
            );
        }

        // Helper to create OptionalColumn from buffers
        macro_rules! make_optional {
            ($buf:ident, $valid:ident, $has_any:ident) => {
                if !$has_any {
                    OptionalColumn::AllNull
                } else if $valid.iter().all(|&v| v) {
                    OptionalColumn::AllPresent(&$buf)
                } else {
                    OptionalColumn::WithValidity {
                        values: &$buf,
                        validity: &$valid,
                    }
                }
            };
        }

        // Build ColumnarBatch with appropriate OptionalColumn variants
        let batch = ColumnarBatch {
            mz: &mz_buf,
            intensity: &intensity_buf,
            spectrum_id: &spectrum_id_buf,
            scan_number: &scan_number_buf,
            ms_level: &ms_level_buf,
            retention_time: &retention_time_buf,
            polarity: &polarity_buf,
            ion_mobility: make_optional!(ion_mobility_buf, ion_mobility_valid, has_any_ion_mobility),
            precursor_mz: make_optional!(precursor_mz_buf, precursor_mz_valid, has_any_precursor_mz),
            precursor_charge: make_optional!(
                precursor_charge_buf,
                precursor_charge_valid,
                has_any_precursor_charge
            ),
            precursor_intensity: make_optional!(
                precursor_intensity_buf,
                precursor_intensity_valid,
                has_any_precursor_intensity
            ),
            isolation_window_lower: make_optional!(
                isolation_lower_buf,
                isolation_lower_valid,
                has_any_isolation_lower
            ),
            isolation_window_upper: make_optional!(
                isolation_upper_buf,
                isolation_upper_valid,
                has_any_isolation_upper
            ),
            collision_energy: make_optional!(
                collision_energy_buf,
                collision_energy_valid,
                has_any_collision_energy
            ),
            total_ion_current: make_optional!(tic_buf, tic_valid, has_any_tic),
            base_peak_mz: make_optional!(base_peak_mz_buf, base_peak_mz_valid, has_any_base_peak_mz),
            base_peak_intensity: make_optional!(
                base_peak_intensity_buf,
                base_peak_intensity_valid,
                has_any_base_peak_intensity
            ),
            injection_time: make_optional!(
                injection_time_buf,
                injection_time_valid,
                has_any_injection_time
            ),
            pixel_x: make_optional!(pixel_x_buf, pixel_x_valid, has_any_pixel_x),
            pixel_y: make_optional!(pixel_y_buf, pixel_y_valid, has_any_pixel_y),
            pixel_z: make_optional!(pixel_z_buf, pixel_z_valid, has_any_pixel_z),
        };

        self.spectra_written += spectra.len();
        self.write_columnar_batch(&batch)
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
