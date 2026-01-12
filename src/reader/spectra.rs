use std::collections::HashSet;

use arrow::record_batch::RecordBatch;

use crate::schema::columns;
use crate::writer::{OptionalColumnBuf, PeakArrays, Spectrum, SpectrumArrays};

use super::utils::{
    get_float32_column, get_float64_column, get_int16_column, get_int64_column, get_int8_column,
    get_optional_f32, get_optional_f64, get_optional_float32_column, get_optional_float64_column,
    get_optional_i16, get_optional_i32, get_optional_int16_column, get_optional_int32_column,
};
use super::{MzPeakReader, ReaderError};

impl MzPeakReader {
    /// Iterate over all spectra in the file (eager/legacy)
    ///
    /// This reconstructs spectra from the long-format peak data by grouping peaks
    /// by spectrum_id. **WARNING**: This loads all spectra into memory.
    ///
    /// For large files, prefer `iter_spectra_streaming()` which processes data lazily.
    pub fn iter_spectra(&self) -> Result<Vec<Spectrum>, ReaderError> {
        let spectra = self.iter_spectra_arrays()?;
        Ok(spectra.into_iter().map(Spectrum::from).collect())
    }

    /// Iterate over all spectra in the file as SoA arrays (eager)
    ///
    /// This reconstructs spectra from the long-format peak data by grouping peaks
    /// by spectrum_id. **WARNING**: This loads all spectra into memory.
    pub fn iter_spectra_arrays(&self) -> Result<Vec<SpectrumArrays>, ReaderError> {
        let batches = self.read_all_batches()?;
        Self::batches_to_spectra_arrays(&batches)
    }

    /// Streaming iterator over spectra (Issue 004 fix)
    ///
    /// Returns a lazy iterator that reconstructs spectra on-demand from the
    /// underlying RecordBatch stream. Memory usage is bounded by batch_size.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mzpeak::reader::MzPeakReader;
    ///
    /// let reader = MzPeakReader::open("data.mzpeak")?;
    /// for result in reader.iter_spectra_streaming()? {
    ///     let spectrum = result?;
    ///     println!("Spectrum {}: {} peaks", spectrum.spectrum_id, spectrum.peaks.len());
    /// }
    /// # Ok::<(), mzpeak::reader::ReaderError>(())
    /// ```
    pub fn iter_spectra_streaming(&self) -> Result<StreamingSpectrumIterator, ReaderError> {
        let batch_iter = self.iter_batches()?;
        Ok(StreamingSpectrumIterator::new(batch_iter))
    }

    /// Streaming iterator over spectra as SoA arrays
    ///
    /// Returns a lazy iterator that reconstructs spectra on-demand from the
    /// underlying RecordBatch stream. Memory usage is bounded by batch_size.
    pub fn iter_spectra_arrays_streaming(
        &self,
    ) -> Result<StreamingSpectrumArraysIterator, ReaderError> {
        let batch_iter = self.iter_batches()?;
        Ok(StreamingSpectrumArraysIterator::new(batch_iter))
    }

    /// Convert record batches to spectra
    fn batches_to_spectra_arrays(
        batches: &[RecordBatch],
    ) -> Result<Vec<SpectrumArrays>, ReaderError> {
        let mut spectra = Vec::new();
        let mut current_spectrum: Option<SpectrumArraysBuilder> = None;

        for batch in batches {
            let spectrum_ids = get_int64_column(batch, columns::SPECTRUM_ID)?;
            let scan_numbers = get_int64_column(batch, columns::SCAN_NUMBER)?;
            let ms_levels = get_int16_column(batch, columns::MS_LEVEL)?;
            let retention_times = get_float32_column(batch, columns::RETENTION_TIME)?;
            let polarities = get_int8_column(batch, columns::POLARITY)?;
            let mzs = get_float64_column(batch, columns::MZ)?;
            let intensities = get_float32_column(batch, columns::INTENSITY)?;

            // Optional columns
            let ion_mobilities = get_optional_float64_column(batch, columns::ION_MOBILITY);
            let precursor_mzs = get_optional_float64_column(batch, columns::PRECURSOR_MZ);
            let precursor_charges = get_optional_int16_column(batch, columns::PRECURSOR_CHARGE);
            let precursor_intensities =
                get_optional_float32_column(batch, columns::PRECURSOR_INTENSITY);
            let isolation_lowers =
                get_optional_float32_column(batch, columns::ISOLATION_WINDOW_LOWER);
            let isolation_uppers =
                get_optional_float32_column(batch, columns::ISOLATION_WINDOW_UPPER);
            let collision_energies = get_optional_float32_column(batch, columns::COLLISION_ENERGY);
            let tics = get_optional_float64_column(batch, columns::TOTAL_ION_CURRENT);
            let base_peak_mzs = get_optional_float64_column(batch, columns::BASE_PEAK_MZ);
            let base_peak_intensities =
                get_optional_float32_column(batch, columns::BASE_PEAK_INTENSITY);
            let injection_times = get_optional_float32_column(batch, columns::INJECTION_TIME);
            let pixel_xs = get_optional_int32_column(batch, columns::PIXEL_X);
            let pixel_ys = get_optional_int32_column(batch, columns::PIXEL_Y);
            let pixel_zs = get_optional_int32_column(batch, columns::PIXEL_Z);

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
                        spectra.push(s.finish());
                    }

                    // Start a new spectrum
                    current_spectrum = Some(SpectrumArraysBuilder::new(
                        spectrum_id,
                        scan_numbers.value(i),
                        ms_levels.value(i),
                        retention_times.value(i),
                        polarities.value(i),
                        get_optional_f64(precursor_mzs, i),
                        get_optional_i16(precursor_charges, i),
                        get_optional_f32(precursor_intensities, i),
                        get_optional_f32(isolation_lowers, i),
                        get_optional_f32(isolation_uppers, i),
                        get_optional_f32(collision_energies, i),
                        get_optional_f64(tics, i),
                        get_optional_f64(base_peak_mzs, i),
                        get_optional_f32(base_peak_intensities, i),
                        get_optional_f32(injection_times, i),
                        get_optional_i32(pixel_xs, i),
                        get_optional_i32(pixel_ys, i),
                        get_optional_i32(pixel_zs, i),
                        ion_mobilities.is_some(),
                    ));
                }

                // Add the peak to the current spectrum
                if let Some(ref mut s) = current_spectrum {
                    s.push_peak(
                        mzs.value(i),
                        intensities.value(i),
                        get_optional_f64(ion_mobilities, i),
                    );
                }
            }
        }

        // Don't forget the last spectrum
        if let Some(s) = current_spectrum {
            spectra.push(s.finish());
        }

        Ok(spectra)
    }

    /// Query spectra by retention time range (inclusive)
    pub fn spectra_by_rt_range(
        &self,
        start_rt: f32,
        end_rt: f32,
    ) -> Result<Vec<Spectrum>, ReaderError> {
        let all_spectra = self.iter_spectra()?;
        Ok(all_spectra
            .into_iter()
            .filter(|s| s.retention_time >= start_rt && s.retention_time <= end_rt)
            .collect())
    }

    /// Query spectra by retention time range (inclusive), SoA layout
    pub fn spectra_by_rt_range_arrays(
        &self,
        start_rt: f32,
        end_rt: f32,
    ) -> Result<Vec<SpectrumArrays>, ReaderError> {
        let all_spectra = self.iter_spectra_arrays()?;
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

    /// Query spectra by MS level, SoA layout
    pub fn spectra_by_ms_level_arrays(
        &self,
        ms_level: i16,
    ) -> Result<Vec<SpectrumArrays>, ReaderError> {
        let all_spectra = self.iter_spectra_arrays()?;
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

    /// Get a specific spectrum by ID, SoA layout
    pub fn get_spectrum_arrays(
        &self,
        spectrum_id: i64,
    ) -> Result<Option<SpectrumArrays>, ReaderError> {
        let all_spectra = self.iter_spectra_arrays()?;
        Ok(all_spectra
            .into_iter()
            .find(|s| s.spectrum_id == spectrum_id))
    }

    /// Get multiple spectra by their IDs
    pub fn get_spectra(&self, spectrum_ids: &[i64]) -> Result<Vec<Spectrum>, ReaderError> {
        let id_set: HashSet<_> = spectrum_ids.iter().collect();
        let all_spectra = self.iter_spectra()?;
        Ok(all_spectra
            .into_iter()
            .filter(|s| id_set.contains(&s.spectrum_id))
            .collect())
    }

    /// Get multiple spectra by their IDs, SoA layout
    pub fn get_spectra_arrays(
        &self,
        spectrum_ids: &[i64],
    ) -> Result<Vec<SpectrumArrays>, ReaderError> {
        let id_set: HashSet<_> = spectrum_ids.iter().collect();
        let all_spectra = self.iter_spectra_arrays()?;
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
}

/// Legacy iterator over spectra (wrapper for Vec iterator)
///
/// This is the eager iterator that loads all spectra into memory.
/// Prefer `StreamingSpectrumIterator` for large files.
pub struct SpectrumIterator {
    inner: std::vec::IntoIter<Spectrum>,
}

impl Iterator for SpectrumIterator {
    type Item = Spectrum;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Legacy iterator over SoA spectra (wrapper for Vec iterator)
pub struct SpectrumArraysIterator {
    inner: std::vec::IntoIter<SpectrumArrays>,
}

impl Iterator for SpectrumArraysIterator {
    type Item = SpectrumArrays;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Streaming iterator over spectra (AoS wrapper over SoA).
///
/// This iterator builds SoA spectra first and converts to AoS on demand.
pub struct StreamingSpectrumIterator {
    inner: StreamingSpectrumArraysIterator,
}

impl StreamingSpectrumIterator {
    /// Create a new streaming spectrum iterator
    pub(super) fn new(batch_iter: super::batches::RecordBatchIterator) -> Self {
        Self {
            inner: StreamingSpectrumArraysIterator::new(batch_iter),
        }
    }
}

impl Iterator for StreamingSpectrumIterator {
    type Item = Result<Spectrum, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|result| result.map(Spectrum::from))
    }
}

/// Streaming iterator over spectra (SoA layout)
///
/// Reconstructs spectra on-demand from RecordBatch stream, yielding one
/// spectrum at a time with bounded memory proportional to batch_size.
pub struct StreamingSpectrumArraysIterator {
    /// The underlying batch iterator
    batch_iter: super::batches::RecordBatchIterator,
    /// Current batch being processed
    current_batch: Option<RecordBatch>,
    /// Current row index within the batch
    current_row: usize,
    /// Spectrum being assembled (may span batch boundaries)
    pending_spectrum: Option<SpectrumArraysBuilder>,
    /// Whether we've finished all batches
    exhausted: bool,
}

impl StreamingSpectrumArraysIterator {
    /// Create a new streaming spectrum iterator
    pub(super) fn new(batch_iter: super::batches::RecordBatchIterator) -> Self {
        Self {
            batch_iter,
            current_batch: None,
            current_row: 0,
            pending_spectrum: None,
            exhausted: false,
        }
    }

    /// Load the next batch from the iterator
    fn load_next_batch(&mut self) -> Option<RecordBatch> {
        match self.batch_iter.next() {
            Some(Ok(batch)) => {
                self.current_row = 0;
                Some(batch)
            }
            Some(Err(e)) => {
                log::error!("Error reading batch: {}", e);
                self.exhausted = true;
                None
            }
            None => {
                self.exhausted = true;
                None
            }
        }
    }
}

impl Iterator for StreamingSpectrumArraysIterator {
    type Item = Result<SpectrumArrays, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Load batch if needed
            if self.current_batch.is_none() {
                if self.exhausted {
                    // Return any pending spectrum before finishing
                    return self.pending_spectrum.take().map(|s| Ok(s.finish()));
                }
                self.current_batch = self.load_next_batch();
                if self.current_batch.is_none() {
                    // No more batches, return pending spectrum if any
                    return self.pending_spectrum.take().map(|s| Ok(s.finish()));
                }
            }

            let batch = self.current_batch.as_ref()?;

            // If we've processed all rows in this batch, load next
            if self.current_row >= batch.num_rows() {
                self.current_batch = None;
                continue;
            }

            // Extract columns (fail fast on schema issues)
            let spectrum_ids = match get_int64_column(batch, columns::SPECTRUM_ID) {
                Ok(col) => col,
                Err(e) => return Some(Err(e)),
            };
            let scan_numbers = match get_int64_column(batch, columns::SCAN_NUMBER) {
                Ok(col) => col,
                Err(e) => return Some(Err(e)),
            };
            let ms_levels = match get_int16_column(batch, columns::MS_LEVEL) {
                Ok(col) => col,
                Err(e) => return Some(Err(e)),
            };
            let retention_times = match get_float32_column(batch, columns::RETENTION_TIME) {
                Ok(col) => col,
                Err(e) => return Some(Err(e)),
            };
            let polarities = match get_int8_column(batch, columns::POLARITY) {
                Ok(col) => col,
                Err(e) => return Some(Err(e)),
            };
            let mzs = match get_float64_column(batch, columns::MZ) {
                Ok(col) => col,
                Err(e) => return Some(Err(e)),
            };
            let intensities = match get_float32_column(batch, columns::INTENSITY) {
                Ok(col) => col,
                Err(e) => return Some(Err(e)),
            };

            // Optional columns
            let ion_mobilities = get_optional_float64_column(batch, columns::ION_MOBILITY);
            let precursor_mzs = get_optional_float64_column(batch, columns::PRECURSOR_MZ);
            let precursor_charges = get_optional_int16_column(batch, columns::PRECURSOR_CHARGE);
            let precursor_intensities =
                get_optional_float32_column(batch, columns::PRECURSOR_INTENSITY);
            let isolation_lowers =
                get_optional_float32_column(batch, columns::ISOLATION_WINDOW_LOWER);
            let isolation_uppers =
                get_optional_float32_column(batch, columns::ISOLATION_WINDOW_UPPER);
            let collision_energies = get_optional_float32_column(batch, columns::COLLISION_ENERGY);
            let tics = get_optional_float64_column(batch, columns::TOTAL_ION_CURRENT);
            let base_peak_mzs = get_optional_float64_column(batch, columns::BASE_PEAK_MZ);
            let base_peak_intensities =
                get_optional_float32_column(batch, columns::BASE_PEAK_INTENSITY);
            let injection_times = get_optional_float32_column(batch, columns::INJECTION_TIME);
            let pixel_xs = get_optional_int32_column(batch, columns::PIXEL_X);
            let pixel_ys = get_optional_int32_column(batch, columns::PIXEL_Y);
            let pixel_zs = get_optional_int32_column(batch, columns::PIXEL_Z);

            // Process rows in this batch until we complete a spectrum
            while self.current_row < batch.num_rows() {
                let i = self.current_row;
                let spectrum_id = spectrum_ids.value(i);

                // Check if this row belongs to a new spectrum
                let is_new_spectrum = match &self.pending_spectrum {
                    None => true,
                    Some(s) => s.spectrum_id != spectrum_id,
                };

                if is_new_spectrum {
                    // If we have a pending spectrum, return it now
                    if let Some(completed) = self.pending_spectrum.take() {
                        // Don't advance current_row - we'll process this row next time
                        return Some(Ok(completed.finish()));
                    }

                    // Start a new spectrum
                    self.pending_spectrum = Some(SpectrumArraysBuilder::new(
                        spectrum_id,
                        scan_numbers.value(i),
                        ms_levels.value(i),
                        retention_times.value(i),
                        polarities.value(i),
                        get_optional_f64(precursor_mzs, i),
                        get_optional_i16(precursor_charges, i),
                        get_optional_f32(precursor_intensities, i),
                        get_optional_f32(isolation_lowers, i),
                        get_optional_f32(isolation_uppers, i),
                        get_optional_f32(collision_energies, i),
                        get_optional_f64(tics, i),
                        get_optional_f64(base_peak_mzs, i),
                        get_optional_f32(base_peak_intensities, i),
                        get_optional_f32(injection_times, i),
                        get_optional_i32(pixel_xs, i),
                        get_optional_i32(pixel_ys, i),
                        get_optional_i32(pixel_zs, i),
                        ion_mobilities.is_some(),
                    ));
                }

                // Add peak to the current spectrum
                if let Some(ref mut s) = self.pending_spectrum {
                    s.push_peak(
                        mzs.value(i),
                        intensities.value(i),
                        get_optional_f64(ion_mobilities, i),
                    );
                }

                self.current_row += 1;
            }

            // End of batch, but spectrum may continue in next batch
            self.current_batch = None;
        }
    }
}

struct SpectrumArraysBuilder {
    spectrum_id: i64,
    scan_number: i64,
    ms_level: i16,
    retention_time: f32,
    polarity: i8,
    precursor_mz: Option<f64>,
    precursor_charge: Option<i16>,
    precursor_intensity: Option<f32>,
    isolation_window_lower: Option<f32>,
    isolation_window_upper: Option<f32>,
    collision_energy: Option<f32>,
    total_ion_current: Option<f64>,
    base_peak_mz: Option<f64>,
    base_peak_intensity: Option<f32>,
    injection_time: Option<f32>,
    pixel_x: Option<i32>,
    pixel_y: Option<i32>,
    pixel_z: Option<i32>,
    mz: Vec<f64>,
    intensity: Vec<f32>,
    ion_mobility: Option<IonMobilityBuffer>,
}

impl SpectrumArraysBuilder {
    #[allow(clippy::too_many_arguments)]
    fn new(
        spectrum_id: i64,
        scan_number: i64,
        ms_level: i16,
        retention_time: f32,
        polarity: i8,
        precursor_mz: Option<f64>,
        precursor_charge: Option<i16>,
        precursor_intensity: Option<f32>,
        isolation_window_lower: Option<f32>,
        isolation_window_upper: Option<f32>,
        collision_energy: Option<f32>,
        total_ion_current: Option<f64>,
        base_peak_mz: Option<f64>,
        base_peak_intensity: Option<f32>,
        injection_time: Option<f32>,
        pixel_x: Option<i32>,
        pixel_y: Option<i32>,
        pixel_z: Option<i32>,
        has_ion_mobility: bool,
    ) -> Self {
        Self {
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
            mz: Vec::new(),
            intensity: Vec::new(),
            ion_mobility: if has_ion_mobility {
                Some(IonMobilityBuffer::default())
            } else {
                None
            },
        }
    }

    fn push_peak(&mut self, mz: f64, intensity: f32, ion_mobility: Option<f64>) {
        self.mz.push(mz);
        self.intensity.push(intensity);
        if let Some(ref mut buffer) = self.ion_mobility {
            buffer.push(ion_mobility);
        }
    }

    fn finish(self) -> SpectrumArrays {
        let len = self.mz.len();
        let ion_mobility = match self.ion_mobility {
            None => OptionalColumnBuf::all_null(len),
            Some(buffer) => buffer.finish(len),
        };

        SpectrumArrays {
            spectrum_id: self.spectrum_id,
            scan_number: self.scan_number,
            ms_level: self.ms_level,
            retention_time: self.retention_time,
            polarity: self.polarity,
            precursor_mz: self.precursor_mz,
            precursor_charge: self.precursor_charge,
            precursor_intensity: self.precursor_intensity,
            isolation_window_lower: self.isolation_window_lower,
            isolation_window_upper: self.isolation_window_upper,
            collision_energy: self.collision_energy,
            total_ion_current: self.total_ion_current,
            base_peak_mz: self.base_peak_mz,
            base_peak_intensity: self.base_peak_intensity,
            injection_time: self.injection_time,
            pixel_x: self.pixel_x,
            pixel_y: self.pixel_y,
            pixel_z: self.pixel_z,
            peaks: PeakArrays {
                mz: self.mz,
                intensity: self.intensity,
                ion_mobility,
            },
        }
    }
}

#[derive(Default)]
struct IonMobilityBuffer {
    values: Vec<f64>,
    validity: Vec<bool>,
    has_any: bool,
    all_present: bool,
}

impl IonMobilityBuffer {
    fn push(&mut self, value: Option<f64>) {
        if self.values.is_empty() {
            self.all_present = true;
        }
        match value {
            Some(v) => {
                self.values.push(v);
                self.validity.push(true);
                self.has_any = true;
            }
            None => {
                self.values.push(0.0);
                self.validity.push(false);
                self.all_present = false;
            }
        }
    }

    fn finish(self, len: usize) -> OptionalColumnBuf<f64> {
        if !self.has_any {
            OptionalColumnBuf::all_null(len)
        } else if self.all_present {
            OptionalColumnBuf::AllPresent(self.values)
        } else {
            OptionalColumnBuf::WithValidity {
                values: self.values,
                validity: self.validity,
            }
        }
    }
}
