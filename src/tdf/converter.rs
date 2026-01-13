//! Conversion from Bruker TDF format to mzpeak thin-waist contract.

use std::path::Path;

use timsrust::readers::FrameReader;

use crate::ingest::{IngestSpectrum, IngestSpectrumConverter, PeakArrays};
use crate::writer::types::{IonMobilityArrays, OptionalColumnBuf, SpectrumArrays};

use super::error::TdfError;

/// Configuration for TDF to SpectrumArrays conversion.
pub struct TdfConversionConfig {
    /// Whether to include extended metadata
    pub include_extended_metadata: bool,
}

impl Default for TdfConversionConfig {
    fn default() -> Self {
        Self {
            include_extended_metadata: true,
        }
    }
}

/// Statistics from TDF conversion.
pub struct TdfConversionStats {
    pub spectra_read: usize,
    pub peaks_total: usize,
    pub ms1_count: usize,
    pub ms2_count: usize,
    pub imaging_frames: usize,
}

/// Converter from Bruker TDF format to mzpeak SpectrumArrays.
pub struct TdfConverter {
    config: TdfConversionConfig,
}

impl TdfConverter {
    /// Create a new TDF converter with default configuration.
    pub fn new() -> Self {
        Self::with_config(TdfConversionConfig::default())
    }

    /// Create a new TDF converter with custom configuration.
    pub fn with_config(config: TdfConversionConfig) -> Self {
        Self { config }
    }

    /// Convert a Bruker TDF dataset to SpectrumArrays.
    pub fn try_convert<P: AsRef<Path>>(path: P) -> Result<SpectrumArrays, TdfError> {
        let converter = Self::new();
        converter.convert(path)
    }

    /// Convert a Bruker TDF dataset to SpectrumArrays.
    pub fn convert<P: AsRef<Path>>(&self, path: P) -> Result<SpectrumArrays, TdfError> {
        let path = path.as_ref();

        // Validate path
        if !path.exists() {
            return Err(TdfError::InvalidPath(format!(
                "Path does not exist: {}",
                path.display()
            )));
        }

        if !path.is_dir() {
            return Err(TdfError::InvalidPath(format!(
                "Not a directory: {}",
                path.display()
            )));
        }

        // Open frame reader
        let reader = FrameReader::new(path).map_err(|e| {
            TdfError::ReadError(format!("Failed to open TDF: {}", e))
        })?;

        // Create converter for thin-waist contract
        let mut spectrum_converter = IngestSpectrumConverter::new();

        // Process each frame
        let mut imaging_frame_count = 0;

        for frame_idx in 0..reader.len() {
            let frame = reader.get(frame_idx).map_err(|e| {
                TdfError::FrameParsingError(format!("Failed to read frame {}: {}", frame_idx, e))
            })?;

            // Check if this is imaging data
            if frame.maldi_info.is_some() {
                imaging_frame_count += 1;
            }

            // Build IngestSpectrum from frame
            let ingest_spectrum = self.build_ingest_spectrum(&frame, frame_idx)?;

            // Convert through thin-waist contract
            spectrum_converter
                .convert(ingest_spectrum)
                .map_err(|e| TdfError::PeakConversionError(format!("{}", e)))?;
        }

        // Get final SpectrumArrays
        let spectra = spectrum_converter
            .finalize()
            .map_err(|e| TdfError::PeakConversionError(format!("{}", e)))?;

        Ok(spectra)
    }

    /// Build an IngestSpectrum from a timsrust Frame.
    fn build_ingest_spectrum(
        &self,
        frame: &timsrust::ms_data::Frame,
        frame_idx: usize,
    ) -> Result<IngestSpectrum, TdfError> {
        // Get peak count
        let peak_count = frame.intensities.len();

        if peak_count == 0 {
            return Err(TdfError::PeakConversionError(
                "Frame has no peaks".to_string(),
            ));
        }

        // Reconstruct m/z values from TOF indices using domain converters
        // For now, use a placeholder - in a real implementation this would use
        // timsrust's domain converters to map TOF indices to m/z values
        let mz_values: Vec<f64> = frame
            .tof_indices
            .iter()
            .map(|&tof| {
                // TODO: Use proper TOF to m/z conversion from timsrust converters
                // This is a placeholder mapping
                100.0 + (tof as f64 * 0.0001)
            })
            .collect();

        // Convert intensities to f32
        let intensities: Vec<f32> = frame
            .intensities
            .iter()
            .map(|&i| (i as f32) * frame.intensity_correction_factor as f32)
            .collect();

        // Prepare ion mobility data
        // TODO: Properly reconstruct scan-to-mobility mapping from scan_offsets
        let ion_mobilities = if peak_count > 0 {
            let mobilities: Vec<f64> = vec![0.5; peak_count]; // Placeholder
            IonMobilityArrays::AllPresent(mobilities)
        } else {
            IonMobilityArrays::NoData { len: 0 }
        };

        // Get extended metadata
        let (tic, max_intensity, injection_time_ms) = if self.config.include_extended_metadata {
            let tic = Some(frame.intensities.iter().map(|&v| v as f64).sum());
            let max_intensity = frame
                .intensities
                .iter()
                .max()
                .map(|&v| f32::from_bits(v.to_bits() as u32));
            let injection_time_ms = Some(
                frame.intensity_correction_factor as f32 * 1000.0, // Convert to milliseconds
            );
            (tic, max_intensity, injection_time_ms)
        } else {
            (None, None, None)
        };

        // Get MALDI imaging coordinates if present
        let (pixel_x, pixel_y) = if let Some(maldi) = &frame.maldi_info {
            (
                Some(maldi.pixel_x as u32),
                Some(maldi.pixel_y as u32),
            )
        } else {
            (None, None)
        };

        // Determine MS level
        let ms_level = match frame.ms_level {
            timsrust::ms_data::MSLevel::MS1 => 1u8,
            timsrust::ms_data::MSLevel::MS2 => 2u8,
            timsrust::ms_data::MSLevel::Unknown => 0u8,
        };

        // Build IngestSpectrum
        let ingest = IngestSpectrum {
            spectrum_id: 0, // Will be set by converter
            scan_number: frame.index as i64,
            ms_level,
            retention_time: frame.rt_in_seconds as f32,
            polarity: 0, // TODO: Get from frame metadata
            precursor_mz: None, // TODO: Get from PASEF precursor table for MS2
            precursor_charge: None,
            isolation_mz: None,
            isolation_width: None,
            collision_energy_ev: None,
            tic: tic,
            base_peak_mz: None, // TODO: Calculate from peaks
            base_peak_intensity: max_intensity,
            injection_time_ms: injection_time_ms,
            pixel_x,
            pixel_y,
            peaks: PeakArrays {
                mz: mz_values,
                intensity: intensities,
                ion_mobilities,
            },
        };

        Ok(ingest)
    }
}

impl Default for TdfConverter {
    fn default() -> Self {
        Self::new()
    }
}
