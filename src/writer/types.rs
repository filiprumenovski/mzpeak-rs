#[derive(Debug, Clone)]
pub enum OptionalColumnBuf<T> {
    AllNull { len: usize },
    AllPresent(Vec<T>),
    WithValidity { values: Vec<T>, validity: Vec<bool> },
}
impl<T> OptionalColumnBuf<T> {
    pub fn all_null(len: usize) -> Self {
        Self::AllNull { len }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::AllNull { len } => *len,
            Self::AllPresent(v) => v.len(),
            Self::WithValidity { values, .. } => values.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn is_all_null(&self) -> bool {
        matches!(self, Self::AllNull { .. })
    }
}

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

// ============================================================================
// V2.0 Schema Types - Separated Spectrum Metadata and Peak Data
// ============================================================================

/// Spectrum-level metadata (one per spectrum) - for spectra.parquet
///
/// This type separates spectrum metadata from peak data for the v2.0 schema,
/// enabling more efficient storage and querying patterns.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpectrumMetadata {
    /// Unique spectrum identifier (0-indexed)
    pub spectrum_id: u32,
    /// Native scan number from the instrument
    pub scan_number: Option<i32>,
    /// MS level (1, 2, 3, ...)
    pub ms_level: u8,
    /// Retention time in seconds
    pub retention_time: f32,
    /// Polarity: 1 for positive, -1 for negative
    pub polarity: i8,
    /// Number of peaks in this spectrum
    pub peak_count: u32,

    // === Precursor info (MS2+) ===
    /// Precursor m/z
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

    // === Summary stats ===
    /// Total ion current
    pub total_ion_current: Option<f64>,
    /// Base peak m/z
    pub base_peak_mz: Option<f64>,
    /// Base peak intensity
    pub base_peak_intensity: Option<f32>,
    /// Ion injection time in ms
    pub injection_time: Option<f32>,

    // === Imaging coordinates ===
    /// X coordinate for imaging data (pixels)
    pub pixel_x: Option<u16>,
    /// Y coordinate for imaging data (pixels)
    pub pixel_y: Option<u16>,
    /// Z coordinate for 3D imaging data (pixels)
    pub pixel_z: Option<u16>,
}

impl SpectrumMetadata {
    /// Create new MS1 spectrum metadata.
    pub fn new_ms1(
        spectrum_id: u32,
        scan_number: Option<i32>,
        retention_time: f32,
        polarity: i8,
        peak_count: u32,
    ) -> Self {
        Self {
            spectrum_id,
            scan_number,
            ms_level: 1,
            retention_time,
            polarity,
            peak_count,
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
        }
    }

    /// Create new MS2 spectrum metadata with precursor information.
    pub fn new_ms2(
        spectrum_id: u32,
        scan_number: Option<i32>,
        retention_time: f32,
        polarity: i8,
        peak_count: u32,
        precursor_mz: f64,
    ) -> Self {
        Self {
            spectrum_id,
            scan_number,
            ms_level: 2,
            retention_time,
            polarity,
            peak_count,
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
        }
    }
}

/// Peak-level data (one per peak) - for peaks.parquet
///
/// This is a simplified peak array type for the v2.0 schema that uses
/// `Option<Vec<f64>>` for ion mobility instead of `OptionalColumnBuf`.
#[derive(Debug, Clone)]
pub struct PeakArraysV2 {
    /// Mass-to-charge ratios (Float64)
    pub mz: Vec<f64>,
    /// Peak intensities (Float32)
    pub intensity: Vec<f32>,
    /// Ion mobility values (Float64), None for 3D data, Some for 4D data
    pub ion_mobility: Option<Vec<f64>>,
}

impl PeakArraysV2 {
    /// Create a new peak array set without ion mobility (3D data).
    pub fn new(mz: Vec<f64>, intensity: Vec<f32>) -> Self {
        Self {
            mz,
            intensity,
            ion_mobility: None,
        }
    }

    /// Create a new peak array set with ion mobility (4D data).
    pub fn with_ion_mobility(mz: Vec<f64>, intensity: Vec<f32>, ion_mobility: Vec<f64>) -> Self {
        Self {
            mz,
            intensity,
            ion_mobility: Some(ion_mobility),
        }
    }

    /// Returns the number of peaks.
    #[inline]
    pub fn len(&self) -> usize {
        self.mz.len()
    }

    /// Returns true if there are no peaks.
    #[inline]
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
        if let Some(ref im) = self.ion_mobility {
            if im.len() != len {
                return Err(format!(
                    "ion_mobility length {} does not match mz length {}",
                    im.len(),
                    len
                ));
            }
        }
        if self.mz.iter().any(|v| !v.is_finite() || *v <= 0.0)
            || self.intensity.iter().any(|v| !v.is_finite() || *v < 0.0)
            || self
                .ion_mobility
                .as_ref()
                .is_some_and(|values| values.iter().any(|v| !v.is_finite() || *v < 0.0))
        {
            return Err("non-finite or invalid peak values".into());
        }
        Ok(())
    }
}

/// Combined spectrum for writing (used by converters) - v2.0 schema
///
/// This type combines spectrum metadata with peak data for convenient
/// spectrum-at-a-time processing while maintaining the v2.0 schema separation.
#[derive(Debug, Clone)]
pub struct SpectrumV2 {
    /// Spectrum-level metadata
    pub metadata: SpectrumMetadata,
    /// Peak-level data arrays
    pub peaks: PeakArraysV2,
}

impl SpectrumV2 {
    /// Create a new v2 spectrum from metadata and peaks.
    pub fn new(metadata: SpectrumMetadata, peaks: PeakArraysV2) -> Self {
        Self { metadata, peaks }
    }

    /// Get the number of peaks in this spectrum.
    #[inline]
    pub fn peak_count(&self) -> u32 {
        self.peaks.len() as u32
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

        self.metadata.total_ion_current = Some(tic);
        self.metadata.base_peak_mz = Some(max_mz);
        self.metadata.base_peak_intensity = Some(max_intensity);
    }
}

impl SpectrumV2 {
    /// Fallible conversion that validates narrowing conversions to preserve fidelity.
    pub fn try_from_spectrum_arrays(v1: SpectrumArrays) -> Result<Self, super::error::WriterError> {
        use super::error::WriterError;

        if v1.spectrum_id < 0 || v1.spectrum_id > u32::MAX as i64 {
            return Err(WriterError::InvalidData(format!(
                "spectrum_id out of range for v2: {}",
                v1.spectrum_id
            )));
        }

        if v1.scan_number < i32::MIN as i64 || v1.scan_number > i32::MAX as i64 {
            return Err(WriterError::InvalidData(format!(
                "scan_number out of range for v2: {}",
                v1.scan_number
            )));
        }

        if v1.ms_level < 1 || v1.ms_level > u8::MAX as i16 {
            return Err(WriterError::InvalidData(format!(
                "ms_level out of range for v2: {}",
                v1.ms_level
            )));
        }

        let peak_count = v1.peaks.len();
        if peak_count > u32::MAX as usize {
            return Err(WriterError::InvalidData(format!(
                "peak_count out of range for v2: {}",
                peak_count
            )));
        }

        if let Some(charge) = v1.precursor_charge {
            if charge < i8::MIN as i16 || charge > i8::MAX as i16 {
                return Err(WriterError::InvalidData(format!(
                    "precursor_charge out of range for v2: {}",
                    charge
                )));
            }
        }

        for (label, value) in [
            ("pixel_x", v1.pixel_x),
            ("pixel_y", v1.pixel_y),
            ("pixel_z", v1.pixel_z),
        ] {
            if let Some(coord) = value {
                if coord < 0 || coord > u16::MAX as i32 {
                    return Err(WriterError::InvalidData(format!(
                        "{label} out of range for v2: {}",
                        coord
                    )));
                }
            }
        }

        let ion_mobility = match v1.peaks.ion_mobility {
            OptionalColumnBuf::AllPresent(values) => Some(values),
            OptionalColumnBuf::AllNull { .. } => None,
            OptionalColumnBuf::WithValidity { values, validity } => {
                let all_present = validity.iter().all(|&v| v);
                let all_null = validity.iter().all(|&v| !v);
                if all_present {
                    Some(values)
                } else if all_null {
                    None
                } else {
                    return Err(WriterError::InvalidData(
                        "mixed ion_mobility validity cannot be represented in v2".to_string(),
                    ));
                }
            }
        };

        let peaks = PeakArraysV2 {
            mz: v1.peaks.mz,
            intensity: v1.peaks.intensity,
            ion_mobility,
        };
        peaks.validate().map_err(|e| WriterError::InvalidData(e))?;

        let metadata = SpectrumMetadata {
            spectrum_id: v1.spectrum_id as u32,
            scan_number: Some(v1.scan_number as i32),
            ms_level: v1.ms_level as u8,
            retention_time: v1.retention_time,
            polarity: v1.polarity,
            peak_count: peak_count as u32,
            precursor_mz: v1.precursor_mz,
            precursor_charge: v1.precursor_charge.map(|c| c as i8),
            precursor_intensity: v1.precursor_intensity,
            isolation_window_lower: v1.isolation_window_lower,
            isolation_window_upper: v1.isolation_window_upper,
            collision_energy: v1.collision_energy,
            total_ion_current: v1.total_ion_current,
            base_peak_mz: v1.base_peak_mz,
            base_peak_intensity: v1.base_peak_intensity,
            injection_time: v1.injection_time,
            pixel_x: v1.pixel_x.map(|x| x as u16),
            pixel_y: v1.pixel_y.map(|y| y as u16),
            pixel_z: v1.pixel_z.map(|z| z as u16),
        };

        Ok(Self { metadata, peaks })
    }
}

impl Default for SpectrumMetadata {
    fn default() -> Self {
        Self::new_ms1(0, None, 0.0, 1, 0)
    }
}
