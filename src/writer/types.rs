// ============================================================================
// Columnar Batch API - High-Performance Vectorized Writing
// ============================================================================

/// Represents optional column data in columnar format.
///
/// This enum allows efficient handling of nullable columns with three distinct cases:
/// - `AllPresent`: All values are present, enabling `append_slice` (memcpy speed)
/// - `AllNull`: No values are present, enabling `append_nulls` (very fast)
/// - `WithValidity`: Mixed presence, using `append_values` with a validity bitmap
#[derive(Debug, Clone, Copy)]
pub enum OptionalColumn<'a, T> {
    /// All values are present - uses `append_slice` for memcpy speed
    AllPresent(&'a [T]),
    /// No values are present - all nulls
    AllNull,
    /// Mixed presence - values with validity bitmap (true = present, false = null)
    WithValidity {
        /// The values array (must be same length as validity)
        values: &'a [T],
        /// Validity bitmap (true = value present, false = null)
        validity: &'a [bool],
    },
}

impl<'a, T> OptionalColumn<'a, T> {
    /// Returns the number of elements this column represents
    pub fn len(&self, batch_len: usize) -> usize {
        match self {
            OptionalColumn::AllPresent(data) => data.len(),
            OptionalColumn::AllNull => batch_len,
            OptionalColumn::WithValidity { values, .. } => values.len(),
        }
    }
}

/// Owned optional column data for SoA-style peak storage.
#[derive(Debug, Clone)]
pub enum OptionalColumnBuf<T> {
    /// All values are present.
    AllPresent(Vec<T>),
    /// No values are present; length tracked explicitly.
    AllNull {
        /// Number of null values.
        len: usize,
    },
    /// Mixed presence with explicit validity bitmap.
    WithValidity {
        /// The values (only valid where validity is true).
        values: Vec<T>,
        /// Boolean bitmap indicating which values are present.
        validity: Vec<bool>,
    },
}

impl<T> OptionalColumnBuf<T> {
    /// Create an all-null column with the given length.
    pub fn all_null(len: usize) -> Self {
        Self::AllNull { len }
    }

    /// Returns the number of elements represented by this column.
    pub fn len(&self) -> usize {
        match self {
            OptionalColumnBuf::AllPresent(values) => values.len(),
            OptionalColumnBuf::AllNull { len } => *len,
            OptionalColumnBuf::WithValidity { values, .. } => values.len(),
        }
    }

    /// Returns true if this column represents no values.
    pub fn is_all_null(&self) -> bool {
        matches!(self, OptionalColumnBuf::AllNull { .. })
    }

    /// Borrow as a column view.
    pub fn as_column(&self) -> OptionalColumn<'_, T> {
        match self {
            OptionalColumnBuf::AllPresent(values) => OptionalColumn::AllPresent(values),
            OptionalColumnBuf::AllNull { .. } => OptionalColumn::AllNull,
            OptionalColumnBuf::WithValidity { values, validity } => OptionalColumn::WithValidity {
                values,
                validity,
            },
        }
    }
}

/// Columnar batch for high-throughput vectorized writing.
///
/// Use this API when you already have data in columnar format to avoid
/// the overhead of creating `Spectrum` objects and per-peak iteration.
///
/// # Performance
///
/// This API is significantly faster than `write_spectra` for large datasets:
/// - Required columns use `append_slice` (single memcpy instead of N append_value calls)
/// - Dense optional columns (`AllPresent`) also use `append_slice`
/// - Sparse optional columns use `append_values` with validity bitmap
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
#[derive(Debug, Clone)]
pub struct ColumnarBatch<'a> {
    // === Required columns (must all have same length) ===
    /// Mass-to-charge ratios (Float64)
    pub mz: &'a [f64],
    /// Peak intensities (Float32)
    pub intensity: &'a [f32],
    /// Spectrum IDs (Int64) - same value repeated for all peaks in a spectrum
    pub spectrum_id: &'a [i64],
    /// Scan numbers (Int64)
    pub scan_number: &'a [i64],
    /// MS levels (Int16) - typically 1 or 2
    pub ms_level: &'a [i16],
    /// Retention times in seconds (Float32)
    pub retention_time: &'a [f32],
    /// Polarity: 1 (positive) or -1 (negative) (Int8)
    pub polarity: &'a [i8],

    // === Optional columns ===
    /// Ion mobility values (Float64), optional per-peak
    pub ion_mobility: OptionalColumn<'a, f64>,
    /// Precursor m/z (Float64), optional (MS2+ only)
    pub precursor_mz: OptionalColumn<'a, f64>,
    /// Precursor charge (Int16), optional
    pub precursor_charge: OptionalColumn<'a, i16>,
    /// Precursor intensity (Float32), optional
    pub precursor_intensity: OptionalColumn<'a, f32>,
    /// Isolation window lower offset (Float32), optional
    pub isolation_window_lower: OptionalColumn<'a, f32>,
    /// Isolation window upper offset (Float32), optional
    pub isolation_window_upper: OptionalColumn<'a, f32>,
    /// Collision energy in eV (Float32), optional
    pub collision_energy: OptionalColumn<'a, f32>,
    /// Total ion current (Float64), optional
    pub total_ion_current: OptionalColumn<'a, f64>,
    /// Base peak m/z (Float64), optional
    pub base_peak_mz: OptionalColumn<'a, f64>,
    /// Base peak intensity (Float32), optional
    pub base_peak_intensity: OptionalColumn<'a, f32>,
    /// Ion injection time in ms (Float32), optional
    pub injection_time: OptionalColumn<'a, f32>,
    /// MSI X pixel coordinate (Int32), optional
    pub pixel_x: OptionalColumn<'a, i32>,
    /// MSI Y pixel coordinate (Int32), optional
    pub pixel_y: OptionalColumn<'a, i32>,
    /// MSI Z pixel coordinate (Int32), optional
    pub pixel_z: OptionalColumn<'a, i32>,
}

impl<'a> ColumnarBatch<'a> {
    /// Create a batch with only required columns (all optional columns set to AllNull)
    pub fn new(
        mz: &'a [f64],
        intensity: &'a [f32],
        spectrum_id: &'a [i64],
        scan_number: &'a [i64],
        ms_level: &'a [i16],
        retention_time: &'a [f32],
        polarity: &'a [i8],
    ) -> Self {
        Self {
            mz,
            intensity,
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            ion_mobility: OptionalColumn::AllNull,
            precursor_mz: OptionalColumn::AllNull,
            precursor_charge: OptionalColumn::AllNull,
            precursor_intensity: OptionalColumn::AllNull,
            isolation_window_lower: OptionalColumn::AllNull,
            isolation_window_upper: OptionalColumn::AllNull,
            collision_energy: OptionalColumn::AllNull,
            total_ion_current: OptionalColumn::AllNull,
            base_peak_mz: OptionalColumn::AllNull,
            base_peak_intensity: OptionalColumn::AllNull,
            injection_time: OptionalColumn::AllNull,
            pixel_x: OptionalColumn::AllNull,
            pixel_y: OptionalColumn::AllNull,
            pixel_z: OptionalColumn::AllNull,
        }
    }

    /// Returns the batch length (number of peaks)
    #[inline]
    pub fn len(&self) -> usize {
        self.mz.len()
    }

    /// Returns true if the batch is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.mz.is_empty()
    }
}

/// SoA peak storage for a single spectrum.
#[derive(Debug, Clone)]
pub struct PeakArrays {
    /// Mass-to-charge ratios (Float64).
    pub mz: Vec<f64>,
    /// Peak intensities (Float32).
    pub intensity: Vec<f32>,
    /// Ion mobility values (Float64), optional per-peak.
    pub ion_mobility: OptionalColumnBuf<f64>,
}

impl PeakArrays {
    /// Create a new peak array set with required columns.
    pub fn new(mz: Vec<f64>, intensity: Vec<f32>) -> Self {
        let len = mz.len();
        Self {
            mz,
            intensity,
            ion_mobility: OptionalColumnBuf::all_null(len),
        }
    }

    /// Returns the number of peaks.
    pub fn len(&self) -> usize {
        self.mz.len()
    }

    /// Returns true if there are no peaks.
    pub fn is_empty(&self) -> bool {
        self.mz.is_empty()
    }

    /// Validate that all arrays have matching lengths.
    pub fn validate(&self) -> Result<(), String> {
        let len = self.mz.len();
        if self.intensity.len() != len {
            return Err(format!(
                "intensity length {} does not match mz length {}",
                self.intensity.len(),
                len
            ));
        }
        if self.ion_mobility.len() != len {
            return Err(format!(
                "ion_mobility length {} does not match mz length {}",
                self.ion_mobility.len(),
                len
            ));
        }
        Ok(())
    }
}

/// Spectrum with SoA peak layout.
#[derive(Debug, Clone)]
pub struct SpectrumArrays {
    /// Unique spectrum identifier (typically 0-indexed)
    pub spectrum_id: i64,
    /// Native scan number from the instrument
    pub scan_number: i64,
    /// MS level (1, 2, 3, ...)
    pub ms_level: i16,
    /// Retention time in seconds
    pub retention_time: f32,
    /// Polarity: 1 for positive, -1 for negative
    pub polarity: i8,
    /// Precursor m/z (for MS2+)
    pub precursor_mz: Option<f64>,
    /// Precursor charge state
    pub precursor_charge: Option<i16>,
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
    /// X coordinate for imaging data (pixels)
    pub pixel_x: Option<i32>,
    /// Y coordinate for imaging data (pixels)
    pub pixel_y: Option<i32>,
    /// Z coordinate for 3D imaging data (pixels)
    pub pixel_z: Option<i32>,
    /// Peak arrays (SoA)
    pub peaks: PeakArrays,
}

impl SpectrumArrays {
    /// Create a new MS1 spectrum.
    pub fn new_ms1(
        spectrum_id: i64,
        scan_number: i64,
        retention_time: f32,
        polarity: i8,
        peaks: PeakArrays,
    ) -> Self {
        Self {
            spectrum_id,
            scan_number,
            ms_level: 1,
            retention_time,
            polarity,
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
            peaks,
        }
    }

    /// Create a new MS2 spectrum with precursor information.
    pub fn new_ms2(
        spectrum_id: i64,
        scan_number: i64,
        retention_time: f32,
        polarity: i8,
        precursor_mz: f64,
        peaks: PeakArrays,
    ) -> Self {
        Self {
            spectrum_id,
            scan_number,
            ms_level: 2,
            retention_time,
            polarity,
            precursor_mz: Some(precursor_mz),
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
            peaks,
        }
    }

    /// Calculate and set spectrum statistics (TIC, base peak).
    pub fn compute_statistics(&mut self) {
        if self.peaks.is_empty() {
            return;
        }

        let mut tic: f64 = 0.0;
        let mut max_intensity: f32 = 0.0;
        let mut max_mz: f64 = 0.0;

        for (mz, intensity) in self.peaks.mz.iter().zip(self.peaks.intensity.iter()) {
            tic += *intensity as f64;
            if *intensity > max_intensity {
                max_intensity = *intensity;
                max_mz = *mz;
            }
        }

        self.total_ion_current = Some(tic);
        self.base_peak_mz = Some(max_mz);
        self.base_peak_intensity = Some(max_intensity);
    }

    /// Get the number of peaks in this spectrum.
    pub fn peak_count(&self) -> usize {
        self.peaks.len()
    }
}
