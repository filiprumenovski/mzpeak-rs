use super::MzMLConverter;
use super::super::models::MzMLSpectrum;
use crate::writer::{OptionalColumnBuf, PeakArrays, SpectrumArrays};

impl MzMLConverter {
    /// Convert a single mzML spectrum to mzPeak format
    pub(crate) fn convert_spectrum(&self, mzml: &MzMLSpectrum) -> SpectrumArrays {
        let scan_number = mzml.scan_number().unwrap_or(mzml.index + 1);

        let mz = mzml.mz_array.clone();
        let intensity: Vec<f32> = mzml
            .intensity_array
            .iter()
            .map(|&value| value as f32)
            .collect();
        let ion_mobility = if !mzml.ion_mobility_array.is_empty()
            && mzml.ion_mobility_array.len() == mz.len()
        {
            OptionalColumnBuf::AllPresent(mzml.ion_mobility_array.clone())
        } else {
            OptionalColumnBuf::all_null(mz.len())
        };

        let peaks = PeakArrays {
            mz,
            intensity,
            ion_mobility,
        };

        let mut spectrum = SpectrumArrays {
            spectrum_id: mzml.index,
            scan_number,
            ms_level: mzml.ms_level,
            retention_time: mzml.retention_time.unwrap_or(0.0) as f32,
            polarity: mzml.polarity,
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
        };

        // Add injection time if available
        if let Some(it) = mzml.ion_injection_time {
            spectrum.injection_time = Some(it as f32);
        }

        // Add MSI pixel coordinates if present
        if let (Some(x), Some(y)) = (mzml.pixel_x, mzml.pixel_y) {
            spectrum.pixel_x = Some(x);
            spectrum.pixel_y = Some(y);
            spectrum.pixel_z = mzml.pixel_z;
        }

        // Add precursor information for MS2+
        if mzml.ms_level >= 2 {
            if let Some(precursor) = mzml.precursors.first() {
                let precursor_mz = precursor
                    .selected_ion_mz
                    .or(precursor.isolation_window_target)
                    .unwrap_or(0.0);

                spectrum.precursor_mz = Some(precursor_mz);
                spectrum.precursor_charge = precursor.selected_ion_charge;
                spectrum.precursor_intensity =
                    precursor.selected_ion_intensity.map(|i| i as f32);

                // Isolation window
                if let (Some(lower), Some(upper)) =
                    (precursor.isolation_window_lower, precursor.isolation_window_upper)
                {
                    spectrum.isolation_window_lower = Some(lower as f32);
                    spectrum.isolation_window_upper = Some(upper as f32);
                }

                // Collision energy
                if let Some(ce) = precursor.collision_energy {
                    spectrum.collision_energy = Some(ce as f32);
                }
            }
        }

        spectrum.compute_statistics();
        spectrum
    }
}
