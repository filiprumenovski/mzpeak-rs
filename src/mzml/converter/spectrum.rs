use super::MzMLConverter;
use super::super::models::MzMLSpectrum;
use crate::writer::{Peak, Spectrum, SpectrumBuilder};

impl MzMLConverter {
    /// Convert a single mzML spectrum to mzPeak format
    pub(crate) fn convert_spectrum(&self, mzml: &MzMLSpectrum) -> Spectrum {
        let scan_number = mzml.scan_number().unwrap_or(mzml.index + 1);

        let mut builder = SpectrumBuilder::new(mzml.index, scan_number)
            .ms_level(mzml.ms_level)
            .retention_time(mzml.retention_time.unwrap_or(0.0) as f32)
            .polarity(mzml.polarity);

        // Add injection time if available
        if let Some(it) = mzml.ion_injection_time {
            builder = builder.injection_time(it as f32);
        }

        // Add MSI pixel coordinates if present
        if let (Some(x), Some(y)) = (mzml.pixel_x, mzml.pixel_y) {
            builder = if let Some(z) = mzml.pixel_z {
                builder.pixel_3d(x, y, z)
            } else {
                builder.pixel(x, y)
            };
        }

        // Add precursor information for MS2+
        if mzml.ms_level >= 2 {
            if let Some(precursor) = mzml.precursors.first() {
                let precursor_mz = precursor
                    .selected_ion_mz
                    .or(precursor.isolation_window_target)
                    .unwrap_or(0.0);

                builder = builder.precursor(
                    precursor_mz,
                    precursor.selected_ion_charge,
                    precursor.selected_ion_intensity.map(|i| i as f32),
                );

                // Isolation window
                if let (Some(lower), Some(upper)) =
                    (precursor.isolation_window_lower, precursor.isolation_window_upper)
                {
                    builder = builder.isolation_window(lower as f32, upper as f32);
                }

                // Collision energy
                if let Some(ce) = precursor.collision_energy {
                    builder = builder.collision_energy(ce as f32);
                }
            }
        }

        // Convert peaks with ion mobility if available
        let peaks: Vec<Peak> = if !mzml.ion_mobility_array.is_empty()
            && mzml.ion_mobility_array.len() == mzml.mz_array.len()
        {
            mzml.mz_array
                .iter()
                .zip(mzml.intensity_array.iter())
                .zip(mzml.ion_mobility_array.iter())
                .map(|((&mz, &intensity), &ion_mobility)| Peak {
                    mz,
                    intensity: intensity as f32,
                    ion_mobility: Some(ion_mobility),
                })
                .collect()
        } else {
            mzml.mz_array
                .iter()
                .zip(mzml.intensity_array.iter())
                .map(|(&mz, &intensity)| Peak {
                    mz,
                    intensity: intensity as f32,
                    ion_mobility: None,
                })
                .collect()
        };

        builder = builder.peaks(peaks);

        builder.build()
    }
}
