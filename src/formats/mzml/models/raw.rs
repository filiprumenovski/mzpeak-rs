use std::collections::HashMap;

use super::spectrum::{MzMLSpectrum, Precursor};
use crate::mzml::binary::{BinaryEncoding, CompressionType};
use crate::mzml::cv_params::CvParam;

/// Raw binary data container for deferred decoding
///
/// This struct holds the raw Base64-encoded data along with its encoding
/// and compression metadata, allowing decoding to be deferred until
/// parallel processing.
#[derive(Debug, Clone, Default)]
pub struct RawBinaryData {
    /// Raw Base64-encoded string from the mzML file
    pub base64: String,
    /// Binary encoding precision (Float32 or Float64)
    pub encoding: BinaryEncoding,
    /// Compression type (None, Zlib, etc.)
    pub compression: CompressionType,
}

impl RawBinaryData {
    /// Check if this container has data
    pub fn is_empty(&self) -> bool {
        self.base64.is_empty()
    }
}

/// Raw spectrum data with deferred binary decoding
///
/// This struct mirrors `MzMLSpectrum` but holds binary arrays as raw
/// Base64 strings instead of decoded `Vec<f64>`. This enables the
/// parallel decoding architecture where:
///
/// 1. **Phase 1 (Sequential)**: XML parsing populates `RawMzMLSpectrum`
/// 2. **Phase 2 (Parallel)**: Multiple spectra are decoded concurrently via Rayon
///
/// # Example
/// ```ignore
/// // Parallel decoding with Rayon
/// let decoded_spectra: Vec<MzMLSpectrum> = raw_spectra
///     .into_par_iter()
///     .map(|raw| raw.decode())
///     .collect::<Result<_, _>>()?;
/// ```
#[derive(Debug, Clone, Default)]
pub struct RawMzMLSpectrum {
    // ========================================================================
    // Metadata fields (same as MzMLSpectrum)
    // ========================================================================

    /// Spectrum index (0-based)
    pub index: i64,

    /// Native spectrum ID from the file
    pub id: String,

    /// Default array length (number of peaks)
    pub default_array_length: usize,

    /// MS level (1 for MS1, 2 for MS2, etc.)
    pub ms_level: i16,

    /// Whether this is a centroid (true) or profile (false) spectrum
    pub centroided: bool,

    /// Polarity: 1 for positive, -1 for negative, 0 for unknown
    pub polarity: i8,

    /// Retention time in seconds
    pub retention_time: Option<f64>,

    /// Total ion current
    pub total_ion_current: Option<f64>,

    /// Base peak m/z
    pub base_peak_mz: Option<f64>,

    /// Base peak intensity
    pub base_peak_intensity: Option<f64>,

    /// Lowest observed m/z
    pub lowest_mz: Option<f64>,

    /// Highest observed m/z
    pub highest_mz: Option<f64>,

    /// Scan window lower limit
    pub scan_window_lower: Option<f64>,

    /// Scan window upper limit
    pub scan_window_upper: Option<f64>,

    /// MSI X coordinate (pixel)
    pub pixel_x: Option<i32>,

    /// MSI Y coordinate (pixel)
    pub pixel_y: Option<i32>,

    /// MSI Z coordinate (pixel, optional for 3D)
    pub pixel_z: Option<i32>,

    /// Ion injection time in milliseconds
    pub ion_injection_time: Option<f64>,

    /// Filter string (vendor-specific)
    pub filter_string: Option<String>,

    /// Preset scan configuration
    pub preset_scan_configuration: Option<i32>,

    /// Precursor information (for MS2+ spectra)
    pub precursors: Vec<Precursor>,

    // ========================================================================
    // Raw Binary Data Containers (NOT decoded yet)
    // ========================================================================

    /// Raw m/z array data (Base64 + encoding info)
    pub mz_data: RawBinaryData,

    /// Raw intensity array data (Base64 + encoding info)
    pub intensity_data: RawBinaryData,

    /// Raw ion mobility array data (optional)
    pub ion_mobility_data: Option<RawBinaryData>,

    // ========================================================================
    // CV and User Parameters
    // ========================================================================

    /// All CV parameters for this spectrum
    pub cv_params: Vec<CvParam>,

    /// User parameters
    pub user_params: HashMap<String, String>,
}

impl RawMzMLSpectrum {
    /// Get the scan number from the native ID.
    pub fn scan_number(&self) -> Option<i64> {
        if let Some(pos) = self.id.find("scan=") {
            let start = pos + 5;
            let end = self.id[start..]
                .find(|c: char| !c.is_ascii_digit())
                .map(|i| start + i)
                .unwrap_or(self.id.len());
            self.id[start..end].parse().ok()
        } else if self.id.starts_with('S') {
            self.id[1..].parse().ok()
        } else {
            Some(self.index + 1)
        }
    }

    /// Decode this raw spectrum into a fully decoded MzMLSpectrum
    ///
    /// This method performs the CPU-intensive Base64 decoding and
    /// decompression. When using parallel processing, this should be
    /// called from within a Rayon parallel iterator.
    ///
    /// # SIMD Acceleration
    /// When the `parallel-decode` feature is enabled, this uses SIMD-accelerated
    /// decoding for significantly improved performance (2-4x faster).
    ///
    /// # Errors
    /// Returns an error if decoding fails (invalid Base64, decompression error, etc.)
    #[cfg(feature = "parallel-decode")]
    pub fn decode(self) -> Result<MzMLSpectrum, crate::mzml::binary::BinaryDecodeError> {
        use crate::mzml::simd::decode_binary_array_simd;

        // Decode binary arrays using SIMD-accelerated decoder
        let mz_array = if !self.mz_data.is_empty() {
            decode_binary_array_simd(
                &self.mz_data.base64,
                self.mz_data.encoding,
                self.mz_data.compression,
                Some(self.default_array_length),
            )?
        } else {
            Vec::new()
        };

        let intensity_array = if !self.intensity_data.is_empty() {
            decode_binary_array_simd(
                &self.intensity_data.base64,
                self.intensity_data.encoding,
                self.intensity_data.compression,
                Some(self.default_array_length),
            )?
        } else {
            Vec::new()
        };

        let ion_mobility_array = if let Some(ref im_data) = self.ion_mobility_data {
            decode_binary_array_simd(
                &im_data.base64,
                im_data.encoding,
                im_data.compression,
                Some(self.default_array_length),
            )?
        } else {
            Vec::new()
        };

        Ok(MzMLSpectrum {
            index: self.index,
            id: self.id,
            default_array_length: self.default_array_length,
            ms_level: self.ms_level,
            centroided: self.centroided,
            polarity: self.polarity,
            retention_time: self.retention_time,
            total_ion_current: self.total_ion_current,
            base_peak_mz: self.base_peak_mz,
            base_peak_intensity: self.base_peak_intensity,
            lowest_mz: self.lowest_mz,
            highest_mz: self.highest_mz,
            scan_window_lower: self.scan_window_lower,
            scan_window_upper: self.scan_window_upper,
            pixel_x: self.pixel_x,
            pixel_y: self.pixel_y,
            pixel_z: self.pixel_z,
            ion_injection_time: self.ion_injection_time,
            filter_string: self.filter_string,
            preset_scan_configuration: self.preset_scan_configuration,
            precursors: self.precursors,
            mz_array,
            intensity_array,
            ion_mobility_array,
            mz_precision_64bit: self.mz_data.encoding == BinaryEncoding::Float64,
            intensity_precision_64bit: self.intensity_data.encoding == BinaryEncoding::Float64,
            cv_params: self.cv_params,
            user_params: self.user_params,
        })
    }

    /// Decode this raw spectrum into a fully decoded MzMLSpectrum (non-SIMD fallback)
    ///
    /// This uses the standard BinaryDecoder when the `parallel-decode` feature
    /// is not enabled.
    #[cfg(not(feature = "parallel-decode"))]
    pub fn decode(self) -> Result<MzMLSpectrum, crate::mzml::binary::BinaryDecodeError> {
        use crate::mzml::binary::BinaryDecoder;

        // Decode binary arrays using standard decoder
        let mz_array = if !self.mz_data.is_empty() {
            BinaryDecoder::decode(
                &self.mz_data.base64,
                self.mz_data.encoding,
                self.mz_data.compression,
                Some(self.default_array_length),
            )?
        } else {
            Vec::new()
        };

        let intensity_array = if !self.intensity_data.is_empty() {
            BinaryDecoder::decode(
                &self.intensity_data.base64,
                self.intensity_data.encoding,
                self.intensity_data.compression,
                Some(self.default_array_length),
            )?
        } else {
            Vec::new()
        };

        let ion_mobility_array = if let Some(ref im_data) = self.ion_mobility_data {
            BinaryDecoder::decode(
                &im_data.base64,
                im_data.encoding,
                im_data.compression,
                Some(self.default_array_length),
            )?
        } else {
            Vec::new()
        };

        Ok(MzMLSpectrum {
            index: self.index,
            id: self.id,
            default_array_length: self.default_array_length,
            ms_level: self.ms_level,
            centroided: self.centroided,
            polarity: self.polarity,
            retention_time: self.retention_time,
            total_ion_current: self.total_ion_current,
            base_peak_mz: self.base_peak_mz,
            base_peak_intensity: self.base_peak_intensity,
            lowest_mz: self.lowest_mz,
            highest_mz: self.highest_mz,
            scan_window_lower: self.scan_window_lower,
            scan_window_upper: self.scan_window_upper,
            pixel_x: self.pixel_x,
            pixel_y: self.pixel_y,
            pixel_z: self.pixel_z,
            ion_injection_time: self.ion_injection_time,
            filter_string: self.filter_string,
            preset_scan_configuration: self.preset_scan_configuration,
            precursors: self.precursors,
            mz_array,
            intensity_array,
            ion_mobility_array,
            mz_precision_64bit: self.mz_data.encoding == BinaryEncoding::Float64,
            intensity_precision_64bit: self.intensity_data.encoding == BinaryEncoding::Float64,
            cv_params: self.cv_params,
            user_params: self.user_params,
        })
    }
}
