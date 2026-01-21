//! Converter from Thermo RAW spectra to thin-waist IngestSpectrum.

use crate::ingest::IngestSpectrum;
use crate::thermo::ThermoError;
use crate::writer::{OptionalColumnBuf, PeakArrays};

use thermorawfilereader::schema::Polarity;
use thermorawfilereader::RawSpectrum;

/// Configuration for Thermo RAW spectrum conversion.
#[derive(Debug, Clone)]
pub struct ThermoConversionConfig {
    /// Whether to centroid profile spectra during conversion.
    /// If true, the thermorawfilereader will centroid profile data.
    pub centroid_spectra: bool,
}

impl Default for ThermoConversionConfig {
    fn default() -> Self {
        Self {
            centroid_spectra: true,
        }
    }
}

/// Converter from Thermo RAW spectra to thin-waist `IngestSpectrum`.
#[derive(Debug, Clone, Default)]
pub struct ThermoConverter {
    config: ThermoConversionConfig,
}

impl ThermoConverter {
    /// Create a new converter with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new converter with custom configuration.
    pub fn with_config(config: ThermoConversionConfig) -> Self {
        Self { config }
    }

    /// Whether centroiding is enabled.
    pub fn centroid_spectra(&self) -> bool {
        self.config.centroid_spectra
    }

    /// Convert a Thermo RawSpectrum to IngestSpectrum.
    ///
    /// # Arguments
    /// * `raw` - The raw spectrum from thermorawfilereader
    /// * `spectrum_id` - The 0-based contiguous spectrum ID for thin-waist contract
    ///
    /// # Returns
    /// An `IngestSpectrum` ready for contract validation and writing.
    pub fn convert_spectrum(
        &self,
        raw: RawSpectrum,
        spectrum_id: i64,
    ) -> Result<IngestSpectrum, ThermoError> {
        // Extract scan number (1-based in Thermo, convert to native)
        let scan_number = (raw.index() + 1) as i64;

        // MS level (1, 2, 3, ...)
        let ms_level = raw.ms_level() as i16;

        // Retention time: Thermo reports in MINUTES, thin-waist needs SECONDS
        let retention_time = (raw.time() * 60.0) as f32;

        // Polarity: 1 for positive, -1 for negative, 0 for unknown
        let polarity = match raw.polarity() {
            Polarity::Positive => 1i8,
            Polarity::Negative => -1i8,
            _ => 0i8,
        };

        // Extract peak data
        let (mz, intensity) = if let Some(data) = raw.data() {
            let mz_slice = data.mz();
            let int_slice = data.intensity();
            (mz_slice.to_vec(), int_slice.iter().map(|&x| x as f32).collect())
        } else {
            (Vec::new(), Vec::new())
        };

        let peak_count = mz.len();

        // Build PeakArrays (no ion mobility for Thermo data)
        let peaks = PeakArrays {
            mz,
            intensity,
            ion_mobility: OptionalColumnBuf::AllNull { len: peak_count },
        };

        // Extract precursor information for MS2+ spectra
        let (
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            isolation_window_lower,
            isolation_window_upper,
            collision_energy,
        ) = if let Some(precursor) = raw.precursor() {
            let prec_mz = Some(precursor.mz());
            let prec_charge = if precursor.charge() != 0 {
                Some(precursor.charge() as i16)
            } else {
                None
            };
            let prec_intensity = if precursor.intensity() > 0.0 {
                Some(precursor.intensity() as f32)
            } else {
                None
            };

            // Isolation window from precursor's isolation_window struct
            let iso_window = precursor.isolation_window();
            let iso_lower = {
                let lower = iso_window.lower();
                if lower > 0.0 { Some(lower as f32) } else { None }
            };
            let iso_upper = {
                let upper = iso_window.upper();
                if upper > 0.0 { Some(upper as f32) } else { None }
            };

            // Collision energy from activation struct
            let ce = {
                let activation = precursor.activation();
                let energy = activation.collision_energy();
                if energy > 0.0 { Some(energy as f32) } else { None }
            };

            (prec_mz, prec_charge, prec_intensity, iso_lower, iso_upper, ce)
        } else {
            (None, None, None, None, None, None)
        };

        // Extract injection time from acquisition if available
        let injection_time = raw
            .acquisition()
            .and_then(|acq| {
                let it = acq.injection_time();
                if it > 0.0 { Some(it as f32) } else { None }
            });

        Ok(IngestSpectrum {
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
            total_ion_current: None, // Will be computed by IngestSpectrumConverter
            base_peak_mz: None,      // Will be computed by IngestSpectrumConverter
            base_peak_intensity: None, // Will be computed by IngestSpectrumConverter
            injection_time,
            pixel_x: None, // Not applicable for Thermo LC-MS data
            pixel_y: None,
            pixel_z: None,
            peaks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let converter = ThermoConverter::new();
        assert!(converter.centroid_spectra());
    }

    #[test]
    fn test_custom_config() {
        let config = ThermoConversionConfig {
            centroid_spectra: false,
        };
        let converter = ThermoConverter::with_config(config);
        assert!(!converter.centroid_spectra());
    }
}
