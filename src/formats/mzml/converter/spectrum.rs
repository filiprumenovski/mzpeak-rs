use super::super::models::{RawBinaryData, RawMzMLSpectrum};
use super::ConversionError;
use super::MzMLConverter;
use crate::ingest::IngestSpectrum;
use crate::mzml::binary::BinaryDecodeError;
use crate::mzml::binary::BinaryDecoder;
use crate::writer::{OptionalColumnBuf, PeakArrays};

pub(crate) struct DecodedRawSpectrum {
    pub ingest: IngestSpectrum,
    pub total_ion_current: Option<f64>,
    pub base_peak_intensity: Option<f64>,
}

impl MzMLConverter {
    pub(crate) fn build_ingest_spectrum_raw(
        &self,
        raw: RawMzMLSpectrum,
    ) -> Result<DecodedRawSpectrum, ConversionError> {
        let scan_number = raw.scan_number().unwrap_or(raw.index + 1);
        let RawMzMLSpectrum {
            index,
            id,
            default_array_length,
            ms_level,
            retention_time,
            total_ion_current,
            base_peak_mz,
            base_peak_intensity,
            polarity,
            ion_injection_time,
            pixel_x,
            pixel_y,
            pixel_z,
            precursors,
            mz_data,
            intensity_data,
            ion_mobility_data,
            ..
        } = raw;

        let mz = decode_f64(&mz_data, default_array_length).map_err(|err| {
            ConversionError::BinaryDecodeError {
                index,
                id: id.clone(),
                source: err,
            }
        })?;
        let intensity = decode_f32(&intensity_data, default_array_length).map_err(|err| {
            ConversionError::BinaryDecodeError {
                index,
                id: id.clone(),
                source: err,
            }
        })?;

        let ion_mobility = if let Some(im_data) = ion_mobility_data {
            let values = decode_f64(&im_data, default_array_length).map_err(|err| {
                ConversionError::BinaryDecodeError {
                    index,
                    id,
                    source: err,
                }
            })?;
            if values.len() == mz.len() {
                OptionalColumnBuf::AllPresent(values)
            } else {
                OptionalColumnBuf::all_null(mz.len())
            }
        } else {
            OptionalColumnBuf::all_null(mz.len())
        };

        let peaks = PeakArrays {
            mz,
            intensity,
            ion_mobility,
        };

        let mut spectrum = IngestSpectrum {
            spectrum_id: index,
            scan_number,
            ms_level,
            retention_time: retention_time.unwrap_or(0.0) as f32,
            polarity,
            precursor_mz: None,
            precursor_charge: None,
            precursor_intensity: None,
            isolation_window_lower: None,
            isolation_window_upper: None,
            collision_energy: None,
            total_ion_current,
            base_peak_mz,
            base_peak_intensity: base_peak_intensity.map(|value| value as f32),
            injection_time: None,
            pixel_x: None,
            pixel_y: None,
            pixel_z: None,
            peaks,
        };

        if let Some(it) = ion_injection_time {
            spectrum.injection_time = Some(it as f32);
        }

        if let (Some(x), Some(y)) = (pixel_x, pixel_y) {
            spectrum.pixel_x = Some(x);
            spectrum.pixel_y = Some(y);
            spectrum.pixel_z = pixel_z;
        }

        if ms_level >= 2 {
            if let Some(precursor) = precursors.first() {
                let precursor_mz = precursor
                    .selected_ion_mz
                    .or(precursor.isolation_window_target)
                    .unwrap_or(0.0);

                spectrum.precursor_mz = Some(precursor_mz);
                spectrum.precursor_charge = precursor.selected_ion_charge;
                spectrum.precursor_intensity = precursor.selected_ion_intensity.map(|i| i as f32);

                if let (Some(lower), Some(upper)) = (
                    precursor.isolation_window_lower,
                    precursor.isolation_window_upper,
                ) {
                    spectrum.isolation_window_lower = Some(lower as f32);
                    spectrum.isolation_window_upper = Some(upper as f32);
                }

                if let Some(ce) = precursor.collision_energy {
                    spectrum.collision_energy = Some(ce as f32);
                }
            }
        }

        Ok(DecodedRawSpectrum {
            ingest: spectrum,
            total_ion_current,
            base_peak_intensity,
        })
    }
}

fn decode_f64(data: &RawBinaryData, expected_len: usize) -> Result<Vec<f64>, BinaryDecodeError> {
    BinaryDecoder::decode(
        &data.base64,
        data.encoding,
        data.compression,
        Some(expected_len),
    )
}
fn decode_f32(data: &RawBinaryData, expected_len: usize) -> Result<Vec<f32>, BinaryDecodeError> {
    BinaryDecoder::decode_f32(
        &data.base64,
        data.encoding,
        data.compression,
        Some(expected_len),
    )
}
