//! Thin-waist ingestion contract types and validation.

use crate::writer::{OptionalColumnBuf, PeakArrays, SpectrumArrays, WriterError};

/// Errors returned when the ingestion contract is violated.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// Contract violation with a human-readable message.
    #[error("ingest contract violation: {0}")]
    ContractViolation(String),
}

impl IngestError {
    fn violation(message: impl Into<String>) -> Self {
        Self::ContractViolation(message.into())
    }
}

impl From<IngestError> for WriterError {
    fn from(error: IngestError) -> Self {
        WriterError::InvalidData(error.to_string())
    }
}

/// Thin-waist ingestion contract for a single spectrum.
///
/// Invariants:
/// - Peak arrays have identical lengths.
/// - Spectrum IDs are contiguous in stream order (checked by `IngestSpectrumConverter`).
/// - Units match the contract (RT seconds, m/z in Th, ion mobility in ms when provided).
#[derive(Debug, Clone)]
pub struct IngestSpectrum {
    /// Unique spectrum identifier (typically 0-indexed).
    pub spectrum_id: i64,
    /// Native scan number from the instrument.
    pub scan_number: i64,
    /// MS level (1, 2, 3, ...).
    pub ms_level: i16,
    /// Retention time in seconds.
    pub retention_time: f32,
    /// Polarity: 1 for positive, -1 for negative, 0 for unknown.
    pub polarity: i8,
    /// Precursor m/z (for MS2+).
    pub precursor_mz: Option<f64>,
    /// Precursor charge state.
    pub precursor_charge: Option<i16>,
    /// Precursor intensity.
    pub precursor_intensity: Option<f32>,
    /// Isolation window lower offset.
    pub isolation_window_lower: Option<f32>,
    /// Isolation window upper offset.
    pub isolation_window_upper: Option<f32>,
    /// Collision energy in eV.
    pub collision_energy: Option<f32>,
    /// Total ion current.
    pub total_ion_current: Option<f64>,
    /// Base peak m/z.
    pub base_peak_mz: Option<f64>,
    /// Base peak intensity.
    pub base_peak_intensity: Option<f32>,
    /// Ion injection time in ms.
    pub injection_time: Option<f32>,
    /// X coordinate for imaging data (pixels).
    pub pixel_x: Option<i32>,
    /// Y coordinate for imaging data (pixels).
    pub pixel_y: Option<i32>,
    /// Z coordinate for 3D imaging data (pixels).
    pub pixel_z: Option<i32>,
    /// Peak arrays (SoA).
    pub peaks: PeakArrays,
}

impl IngestSpectrum {
    /// Validate the thin-waist contract invariants for a single spectrum.
    pub fn validate_contract(&self) -> Result<(), IngestError> {
        if self.ms_level < 1 {
            return Err(IngestError::violation(format!(
                "ms_level must be >= 1, got {}",
                self.ms_level
            )));
        }

        if !matches!(self.polarity, -1 | 0 | 1) {
            return Err(IngestError::violation(format!(
                "polarity must be -1, 0, or 1, got {}",
                self.polarity
            )));
        }

        if !self.retention_time.is_finite() {
            return Err(IngestError::violation(format!(
                "retention_time must be finite, got {}",
                self.retention_time
            )));
        }

        self.peaks.validate().map_err(IngestError::violation)?;
        Self::validate_optional_column_len("ion_mobility", &self.peaks.ion_mobility, self.peaks.mz.len())?;

        Ok(())
    }

    fn validate_optional_column_len<T>(
        name: &str,
        column: &OptionalColumnBuf<T>,
        expected: usize,
    ) -> Result<(), IngestError> {
        match column {
            OptionalColumnBuf::AllPresent(values) => {
                if values.len() != expected {
                    return Err(IngestError::violation(format!(
                        "{name} length {} does not match expected {expected}",
                        values.len()
                    )));
                }
            }
            OptionalColumnBuf::AllNull { len } => {
                if *len != expected {
                    return Err(IngestError::violation(format!(
                        "{name} length {} does not match expected {expected}",
                        len
                    )));
                }
            }
            OptionalColumnBuf::WithValidity { values, validity } => {
                if values.len() != expected {
                    return Err(IngestError::violation(format!(
                        "{name} length {} does not match expected {expected}",
                        values.len()
                    )));
                }
                if validity.len() != values.len() {
                    return Err(IngestError::violation(format!(
                        "{name} validity length {} does not match values length {}",
                        validity.len(),
                        values.len()
                    )));
                }
            }
        }

        Ok(())
    }
}

/// Stateful converter from `IngestSpectrum` to `SpectrumArrays` with contract enforcement.
#[derive(Debug, Default)]
pub struct IngestSpectrumConverter {
    next_spectrum_id: Option<i64>,
}

impl IngestSpectrumConverter {
    /// Create a new contract-enforcing converter.
    pub fn new() -> Self {
        Self { next_spectrum_id: None }
    }

    /// Convert an ingestion spectrum into `SpectrumArrays`, enforcing contract invariants.
    pub fn convert(&mut self, ingest: IngestSpectrum) -> Result<SpectrumArrays, IngestError> {
        ingest.validate_contract()?;
        self.validate_ordering(ingest.spectrum_id)?;

        let IngestSpectrum {
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
        } = ingest;

        let mut spectrum = SpectrumArrays {
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
        };

        // Preserve existing behavior: compute TIC/BPC from peak arrays.
        spectrum.compute_statistics();

        Ok(spectrum)
    }

    fn validate_ordering(&mut self, spectrum_id: i64) -> Result<(), IngestError> {
        if let Some(expected) = self.next_spectrum_id {
            if spectrum_id != expected {
                return Err(IngestError::violation(format!(
                    "spectrum_id out of order: expected {expected}, got {spectrum_id}"
                )));
            }
        }

        self.next_spectrum_id = Some(spectrum_id + 1);
        Ok(())
    }
}
