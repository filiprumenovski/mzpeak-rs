use std::collections::HashSet;

use arrow::record_batch::RecordBatch;

use crate::schema::columns;
use crate::writer::{Peak, Spectrum};

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
        let batches = self.read_all_batches()?;
        Self::batches_to_spectra(&batches)
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

    /// Convert record batches to spectra
    fn batches_to_spectra(batches: &[RecordBatch]) -> Result<Vec<Spectrum>, ReaderError> {
        let mut spectra = Vec::new();
        let mut current_spectrum: Option<Spectrum> = None;

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
                        spectra.push(s);
                    }

                    // Start a new spectrum
                    current_spectrum = Some(Spectrum {
                        spectrum_id,
                        scan_number: scan_numbers.value(i),
                        ms_level: ms_levels.value(i),
                        retention_time: retention_times.value(i),
                        polarity: polarities.value(i),
                        precursor_mz: get_optional_f64(precursor_mzs, i),
                        precursor_charge: get_optional_i16(precursor_charges, i),
                        precursor_intensity: get_optional_f32(precursor_intensities, i),
                        isolation_window_lower: get_optional_f32(isolation_lowers, i),
                        isolation_window_upper: get_optional_f32(isolation_uppers, i),
                        collision_energy: get_optional_f32(collision_energies, i),
                        total_ion_current: get_optional_f64(tics, i),
                        base_peak_mz: get_optional_f64(base_peak_mzs, i),
                        base_peak_intensity: get_optional_f32(base_peak_intensities, i),
                        injection_time: get_optional_f32(injection_times, i),
                        pixel_x: get_optional_i32(pixel_xs, i),
                        pixel_y: get_optional_i32(pixel_ys, i),
                        pixel_z: get_optional_i32(pixel_zs, i),
                        peaks: Vec::new(),
                    });
                }

                // Add the peak to the current spectrum
                if let Some(ref mut s) = current_spectrum {
                    s.peaks.push(Peak {
                        mz: mzs.value(i),
                        intensity: intensities.value(i),
                        ion_mobility: get_optional_f64(ion_mobilities, i),
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
        let id_set: HashSet<_> = spectrum_ids.iter().collect();
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

/// Streaming iterator over spectra (Issue 004 fix)
///
/// Reconstructs spectra on-demand from RecordBatch stream, yielding one
/// spectrum at a time with bounded memory proportional to batch_size.
///
/// # Memory Bounds
///
/// Memory usage is O(batch_size + current_spectrum_peaks), not O(file_size).
/// Spectra spanning batch boundaries are handled correctly.
pub struct StreamingSpectrumIterator {
    /// The underlying batch iterator
    batch_iter: super::batches::RecordBatchIterator,
    /// Current batch being processed
    current_batch: Option<RecordBatch>,
    /// Current row index within the batch
    current_row: usize,
    /// Spectrum being assembled (may span batch boundaries)
    pending_spectrum: Option<Spectrum>,
    /// Whether we've finished all batches
    exhausted: bool,
}

impl StreamingSpectrumIterator {
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

impl Iterator for StreamingSpectrumIterator {
    type Item = Result<Spectrum, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Load batch if needed
            if self.current_batch.is_none() {
                if self.exhausted {
                    // Return any pending spectrum before finishing
                    return self.pending_spectrum.take().map(Ok);
                }
                self.current_batch = self.load_next_batch();
                if self.current_batch.is_none() {
                    // No more batches, return pending spectrum if any
                    return self.pending_spectrum.take().map(Ok);
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
                        return Some(Ok(completed));
                    }

                    // Start a new spectrum
                    self.pending_spectrum = Some(Spectrum {
                        spectrum_id,
                        scan_number: scan_numbers.value(i),
                        ms_level: ms_levels.value(i),
                        retention_time: retention_times.value(i),
                        polarity: polarities.value(i),
                        precursor_mz: get_optional_f64(precursor_mzs, i),
                        precursor_charge: get_optional_i16(precursor_charges, i),
                        precursor_intensity: get_optional_f32(precursor_intensities, i),
                        isolation_window_lower: get_optional_f32(isolation_lowers, i),
                        isolation_window_upper: get_optional_f32(isolation_uppers, i),
                        collision_energy: get_optional_f32(collision_energies, i),
                        total_ion_current: get_optional_f64(tics, i),
                        base_peak_mz: get_optional_f64(base_peak_mzs, i),
                        base_peak_intensity: get_optional_f32(base_peak_intensities, i),
                        injection_time: get_optional_f32(injection_times, i),
                        pixel_x: get_optional_i32(pixel_xs, i),
                        pixel_y: get_optional_i32(pixel_ys, i),
                        pixel_z: get_optional_i32(pixel_zs, i),
                        peaks: Vec::new(),
                    });
                }

                // Add peak to the current spectrum
                if let Some(ref mut s) = self.pending_spectrum {
                    s.peaks.push(Peak {
                        mz: mzs.value(i),
                        intensity: intensities.value(i),
                        ion_mobility: get_optional_f64(ion_mobilities, i),
                    });
                }

                self.current_row += 1;
            }

            // End of batch, but spectrum may continue in next batch
            self.current_batch = None;
        }
    }
}
