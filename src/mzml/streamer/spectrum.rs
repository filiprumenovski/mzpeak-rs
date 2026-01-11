use std::io::BufRead;

use quick_xml::events::{BytesStart, Event};

use super::helpers::{get_attribute, parse_cv_param};
use super::{MzMLError, MzMLStreamer};
use crate::mzml::binary::{BinaryDecoder, BinaryEncoding, CompressionType};
use crate::mzml::cv_params::{normalize_retention_time, CvParam, MS_CV_ACCESSIONS};
use crate::mzml::models::{MzMLSpectrum, Precursor, RawBinaryData, RawMzMLSpectrum};

impl<R: BufRead> MzMLStreamer<R> {
    /// Read the next spectrum from the stream
    pub fn next_spectrum(&mut self) -> Result<Option<MzMLSpectrum>, MzMLError> {
        if !self.in_spectrum_list {
            // Try to find spectrumList if we haven't read metadata
            self.read_metadata()?;
            if !self.in_spectrum_list {
                return Ok(None);
            }
        }

        let mut buf = Vec::new();
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    if e.name().as_ref() == b"spectrum" {
                        let spectrum = self.parse_spectrum(&e)?;
                        self.current_spectrum_index += 1;
                        return Ok(Some(spectrum));
                    }
                }
                Ok(Event::End(ref e)) => {
                    if e.name().as_ref() == b"spectrumList" {
                        self.in_spectrum_list = false;
                        return Ok(None);
                    }
                }
                Ok(Event::Eof) => return Ok(None),
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }
    }

    /// Read the next spectrum from the stream WITHOUT decoding binary data
    ///
    /// This method is the foundation of the parallel decoding architecture.
    /// It parses the XML and extracts all metadata, but defers the expensive
    /// Base64 decoding and decompression to a later parallel phase.
    ///
    /// # Performance
    /// This method uses `std::mem::take` to move the Base64 string buffer
    /// instead of cloning, minimizing allocation overhead.
    ///
    /// # Example
    /// ```ignore
    /// use rayon::prelude::*;
    ///
    /// let mut raw_batch = Vec::new();
    /// while let Some(raw) = streamer.next_raw_spectrum()? {
    ///     raw_batch.push(raw);
    ///     if raw_batch.len() >= batch_size {
    ///         let decoded: Vec<_> = raw_batch
    ///             .par_drain(..)
    ///             .map(|r| r.decode())
    ///             .collect::<Result<_, _>>()?;
    ///         // process decoded batch...
    ///     }
    /// }
    /// ```
    pub fn next_raw_spectrum(&mut self) -> Result<Option<RawMzMLSpectrum>, MzMLError> {
        if !self.in_spectrum_list {
            // Try to find spectrumList if we haven't read metadata
            self.read_metadata()?;
            if !self.in_spectrum_list {
                return Ok(None);
            }
        }

        let mut buf = Vec::new();
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    if e.name().as_ref() == b"spectrum" {
                        let spectrum = self.parse_raw_spectrum(&e)?;
                        self.current_spectrum_index += 1;
                        return Ok(Some(spectrum));
                    }
                }
                Ok(Event::End(ref e)) => {
                    if e.name().as_ref() == b"spectrumList" {
                        self.in_spectrum_list = false;
                        return Ok(None);
                    }
                }
                Ok(Event::Eof) => return Ok(None),
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }
    }

    /// Parse a single spectrum element
    fn parse_spectrum(&mut self, start_event: &BytesStart) -> Result<MzMLSpectrum, MzMLError> {
        let mut spectrum = MzMLSpectrum::default();

        // Get attributes from spectrum element
        spectrum.index = get_attribute(start_event, "index")?
            .and_then(|s| s.parse().ok())
            .unwrap_or(self.current_spectrum_index);
        spectrum.id = get_attribute(start_event, "id")?.unwrap_or_default();
        spectrum.default_array_length = get_attribute(start_event, "defaultArrayLength")?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let mut depth = 1;
        let mut in_scan_list = false;
        let mut in_precursor_list = false;
        let mut in_binary_data_array_list = false;
        let mut current_precursor: Option<Precursor> = None;
        let mut current_binary_array: Option<BinaryArrayContext> = None;
        let mut buf = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    depth += 1;
                    match e.name().as_ref() {
                        b"cvParam" => {
                            let cv_param = parse_cv_param(e)?;
                            if in_binary_data_array_list {
                                if let Some(ref mut ctx) = current_binary_array {
                                    ctx.cv_params.push(cv_param);
                                }
                            } else if in_precursor_list {
                                if let Some(ref mut prec) = current_precursor {
                                    prec.cv_params.push(cv_param);
                                }
                            } else {
                                spectrum.cv_params.push(cv_param);
                            }
                        }
                        b"scanList" => {
                            in_scan_list = true;
                        }
                        b"precursorList" => {
                            in_precursor_list = true;
                        }
                        b"precursor" => {
                            let mut prec = Precursor::default();
                            prec.spectrum_ref = get_attribute(e, "spectrumRef")?;
                            current_precursor = Some(prec);
                        }
                        b"isolationWindow" => {}
                        b"selectedIon" => {}
                        b"activation" => {}
                        b"binaryDataArrayList" => {
                            in_binary_data_array_list = true;
                        }
                        b"binaryDataArray" => {
                            current_binary_array = Some(BinaryArrayContext::default());
                        }
                        b"binary" => {}
                        _ => {}
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv_param = parse_cv_param(e)?;

                        if in_binary_data_array_list {
                            if let Some(ref mut ctx) = current_binary_array {
                                ctx.cv_params.push(cv_param);
                            }
                        } else if in_precursor_list {
                            if let Some(ref mut prec) = current_precursor {
                                Self::apply_precursor_cv_param(prec, &cv_param);
                                prec.cv_params.push(cv_param);
                            }
                        } else if in_scan_list {
                            Self::apply_scan_cv_param(&mut spectrum, &cv_param);
                            spectrum.cv_params.push(cv_param);
                        } else {
                            Self::apply_spectrum_cv_param(&mut spectrum, &cv_param);
                            spectrum.cv_params.push(cv_param);
                        }
                    } else if e.name().as_ref() == b"userParam" {
                        let name = get_attribute(e, "name")?.unwrap_or_default();
                        let value = get_attribute(e, "value")?.unwrap_or_default();
                        spectrum.user_params.insert(name, value);
                    }
                }
                Ok(Event::Text(ref t)) => {
                    if let Some(ref mut ctx) = current_binary_array {
                        ctx.base64_data = t.unescape()?.into_owned();
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    match e.name().as_ref() {
                        b"spectrum" => {
                            if depth == 0 {
                                break;
                            }
                        }
                        b"scanList" => {
                            in_scan_list = false;
                        }
                        b"precursorList" => {
                            in_precursor_list = false;
                        }
                        b"precursor" => {
                            if let Some(prec) = current_precursor.take() {
                                spectrum.precursors.push(prec);
                            }
                        }
                        b"binaryDataArrayList" => {
                            in_binary_data_array_list = false;
                        }
                        b"binaryDataArray" => {
                            if let Some(ctx) = current_binary_array.take() {
                                self.decode_binary_array(&mut spectrum, ctx)?;
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::Eof) => {
                    return Err(MzMLError::InvalidStructure(
                        "Unexpected EOF in spectrum".to_string(),
                    ));
                }
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(spectrum)
    }

    /// Parse a single spectrum element WITHOUT decoding binary data
    ///
    /// This is the core of the deferred decoding architecture. It captures
    /// all metadata and raw Base64 strings, allowing parallel decoding later.
    fn parse_raw_spectrum(
        &mut self,
        start_event: &BytesStart,
    ) -> Result<RawMzMLSpectrum, MzMLError> {
        let mut spectrum = RawMzMLSpectrum::default();

        // Get attributes from spectrum element
        spectrum.index = get_attribute(start_event, "index")?
            .and_then(|s| s.parse().ok())
            .unwrap_or(self.current_spectrum_index);
        spectrum.id = get_attribute(start_event, "id")?.unwrap_or_default();
        spectrum.default_array_length = get_attribute(start_event, "defaultArrayLength")?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let mut depth = 1;
        let mut in_scan_list = false;
        let mut in_precursor_list = false;
        let mut in_binary_data_array_list = false;
        let mut current_precursor: Option<Precursor> = None;

        // Track current binary array context for raw capture
        let mut current_binary_cv_params: Vec<CvParam> = Vec::new();
        let mut current_binary_data = String::with_capacity(1024 * 64);
        let mut buf = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    depth += 1;
                    match e.name().as_ref() {
                        b"cvParam" => {
                            let cv_param = parse_cv_param(e)?;
                            if in_binary_data_array_list {
                                current_binary_cv_params.push(cv_param);
                            } else if in_precursor_list {
                                if let Some(ref mut prec) = current_precursor {
                                    prec.cv_params.push(cv_param);
                                }
                            } else {
                                spectrum.cv_params.push(cv_param);
                            }
                        }
                        b"scanList" => {
                            in_scan_list = true;
                        }
                        b"precursorList" => {
                            in_precursor_list = true;
                        }
                        b"precursor" => {
                            let mut prec = Precursor::default();
                            prec.spectrum_ref = get_attribute(e, "spectrumRef")?;
                            current_precursor = Some(prec);
                        }
                        b"isolationWindow" => {}
                        b"selectedIon" => {}
                        b"activation" => {}
                        b"binaryDataArrayList" => {
                            in_binary_data_array_list = true;
                        }
                        b"binaryDataArray" => {
                            current_binary_cv_params.clear();
                            current_binary_data.clear();
                        }
                        b"binary" => {}
                        _ => {}
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv_param = parse_cv_param(e)?;

                        if in_binary_data_array_list {
                            current_binary_cv_params.push(cv_param);
                        } else if in_precursor_list {
                            if let Some(ref mut prec) = current_precursor {
                                Self::apply_precursor_cv_param(prec, &cv_param);
                                prec.cv_params.push(cv_param);
                            }
                        } else if in_scan_list {
                            Self::apply_raw_scan_cv_param(&mut spectrum, &cv_param);
                            spectrum.cv_params.push(cv_param);
                        } else {
                            Self::apply_raw_spectrum_cv_param(&mut spectrum, &cv_param);
                            spectrum.cv_params.push(cv_param);
                        }
                    } else if e.name().as_ref() == b"userParam" {
                        let name = get_attribute(e, "name")?.unwrap_or_default();
                        let value = get_attribute(e, "value")?.unwrap_or_default();
                        spectrum.user_params.insert(name, value);
                    }
                }
                Ok(Event::Text(ref t)) => {
                    if in_binary_data_array_list {
                        // Use push_str to append, supporting multiline Base64
                        if let Ok(text) = t.unescape() {
                            current_binary_data.push_str(&text);
                        }
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    match e.name().as_ref() {
                        b"spectrum" => {
                            if depth == 0 {
                                break;
                            }
                        }
                        b"scanList" => {
                            in_scan_list = false;
                        }
                        b"precursorList" => {
                            in_precursor_list = false;
                        }
                        b"precursor" => {
                            if let Some(prec) = current_precursor.take() {
                                spectrum.precursors.push(prec);
                            }
                        }
                        b"binaryDataArrayList" => {
                            in_binary_data_array_list = false;
                        }
                        b"binaryDataArray" => {
                            // Store raw binary data WITHOUT decoding
                            self.store_raw_binary_array(
                                &mut spectrum,
                                &current_binary_cv_params,
                                &mut current_binary_data,
                            );
                            current_binary_cv_params.clear();
                        }
                        _ => {}
                    }
                }
                Ok(Event::Eof) => {
                    return Err(MzMLError::InvalidStructure(
                        "Unexpected EOF in spectrum".to_string(),
                    ));
                }
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(spectrum)
    }

    /// Store raw binary array data in the RawMzMLSpectrum without decoding
    fn store_raw_binary_array(
        &self,
        spectrum: &mut RawMzMLSpectrum,
        cv_params: &[CvParam],
        base64_data: &mut String,
    ) {
        let mut encoding = BinaryEncoding::Float64;
        let mut compression = CompressionType::None;
        let mut is_mz = false;
        let mut is_intensity = false;
        let mut is_ion_mobility = false;

        for cv in cv_params {
            match cv.accession.as_str() {
                MS_CV_ACCESSIONS::FLOAT_32_BIT => encoding = BinaryEncoding::Float32,
                MS_CV_ACCESSIONS::FLOAT_64_BIT => encoding = BinaryEncoding::Float64,
                MS_CV_ACCESSIONS::ZLIB_COMPRESSION => compression = CompressionType::Zlib,
                MS_CV_ACCESSIONS::NO_COMPRESSION => compression = CompressionType::None,
                MS_CV_ACCESSIONS::MZ_ARRAY => is_mz = true,
                MS_CV_ACCESSIONS::INTENSITY_ARRAY => is_intensity = true,
                MS_CV_ACCESSIONS::ION_MOBILITY_ARRAY => is_ion_mobility = true,
                _ => {}
            }
        }

        // Use std::mem::take to move the string without cloning
        let raw_data = RawBinaryData {
            base64: std::mem::take(base64_data),
            encoding,
            compression,
        };

        if is_mz {
            spectrum.mz_data = raw_data;
        } else if is_intensity {
            spectrum.intensity_data = raw_data;
        } else if is_ion_mobility {
            spectrum.ion_mobility_data = Some(raw_data);
        }
    }

    /// Apply CV param to raw spectrum properties
    fn apply_raw_spectrum_cv_param(spectrum: &mut RawMzMLSpectrum, cv: &CvParam) {
        match cv.accession.as_str() {
            MS_CV_ACCESSIONS::MS_LEVEL => {
                spectrum.ms_level = cv.value_as_i64().unwrap_or(1) as i16;
            }
            MS_CV_ACCESSIONS::CENTROID_SPECTRUM => {
                spectrum.centroided = true;
            }
            MS_CV_ACCESSIONS::PROFILE_SPECTRUM => {
                spectrum.centroided = false;
            }
            MS_CV_ACCESSIONS::POSITIVE_SCAN => {
                spectrum.polarity = 1;
            }
            MS_CV_ACCESSIONS::NEGATIVE_SCAN => {
                spectrum.polarity = -1;
            }
            MS_CV_ACCESSIONS::TOTAL_ION_CURRENT => {
                spectrum.total_ion_current = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::BASE_PEAK_MZ => {
                spectrum.base_peak_mz = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::BASE_PEAK_INTENSITY => {
                spectrum.base_peak_intensity = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::LOWEST_OBSERVED_MZ => {
                spectrum.lowest_mz = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::HIGHEST_OBSERVED_MZ => {
                spectrum.highest_mz = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::FILTER_STRING => {
                spectrum.filter_string = cv.value.clone();
            }
            MS_CV_ACCESSIONS::PRESET_SCAN_CONFIGURATION => {
                spectrum.preset_scan_configuration = cv.value_as_i32();
            }
            _ => {}
        }
    }

    /// Apply CV param to raw scan properties
    fn apply_raw_scan_cv_param(spectrum: &mut RawMzMLSpectrum, cv: &CvParam) {
        match cv.accession.as_str() {
            MS_CV_ACCESSIONS::SCAN_START_TIME => {
                if let Some(val) = cv.value_as_f64() {
                    spectrum.retention_time =
                        Some(normalize_retention_time(val, cv.unit_accession.as_deref()));
                }
            }
            MS_CV_ACCESSIONS::ION_INJECTION_TIME => {
                spectrum.ion_injection_time = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::SCAN_WINDOW_LOWER_LIMIT => {
                spectrum.scan_window_lower = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::SCAN_WINDOW_UPPER_LIMIT => {
                spectrum.scan_window_upper = cv.value_as_f64();
            }
            _ => {
                Self::apply_raw_spectrum_cv_param(spectrum, cv);
            }
        }
    }

    /// Apply CV param to spectrum properties
    fn apply_spectrum_cv_param(spectrum: &mut MzMLSpectrum, cv: &CvParam) {
        match cv.accession.as_str() {
            MS_CV_ACCESSIONS::MS_LEVEL => {
                spectrum.ms_level = cv.value_as_i64().unwrap_or(1) as i16;
            }
            MS_CV_ACCESSIONS::CENTROID_SPECTRUM => {
                spectrum.centroided = true;
            }
            MS_CV_ACCESSIONS::PROFILE_SPECTRUM => {
                spectrum.centroided = false;
            }
            MS_CV_ACCESSIONS::POSITIVE_SCAN => {
                spectrum.polarity = 1;
            }
            MS_CV_ACCESSIONS::NEGATIVE_SCAN => {
                spectrum.polarity = -1;
            }
            MS_CV_ACCESSIONS::TOTAL_ION_CURRENT => {
                spectrum.total_ion_current = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::BASE_PEAK_MZ => {
                spectrum.base_peak_mz = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::BASE_PEAK_INTENSITY => {
                spectrum.base_peak_intensity = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::LOWEST_OBSERVED_MZ => {
                spectrum.lowest_mz = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::HIGHEST_OBSERVED_MZ => {
                spectrum.highest_mz = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::FILTER_STRING => {
                spectrum.filter_string = cv.value.clone();
            }
            MS_CV_ACCESSIONS::PRESET_SCAN_CONFIGURATION => {
                spectrum.preset_scan_configuration = cv.value_as_i32();
            }
            _ => {}
        }
    }

    /// Apply CV param to scan properties
    fn apply_scan_cv_param(spectrum: &mut MzMLSpectrum, cv: &CvParam) {
        match cv.accession.as_str() {
            MS_CV_ACCESSIONS::SCAN_START_TIME => {
                if let Some(val) = cv.value_as_f64() {
                    spectrum.retention_time =
                        Some(normalize_retention_time(val, cv.unit_accession.as_deref()));
                }
            }
            MS_CV_ACCESSIONS::ION_INJECTION_TIME => {
                spectrum.ion_injection_time = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::SCAN_WINDOW_LOWER_LIMIT => {
                spectrum.scan_window_lower = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::SCAN_WINDOW_UPPER_LIMIT => {
                spectrum.scan_window_upper = cv.value_as_f64();
            }
            _ => {
                Self::apply_spectrum_cv_param(spectrum, cv);
            }
        }
    }

    /// Apply CV param to precursor properties
    fn apply_precursor_cv_param(precursor: &mut Precursor, cv: &CvParam) {
        match cv.accession.as_str() {
            MS_CV_ACCESSIONS::ISOLATION_WINDOW_TARGET_MZ => {
                precursor.isolation_window_target = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::ISOLATION_WINDOW_LOWER_OFFSET => {
                precursor.isolation_window_lower = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::ISOLATION_WINDOW_UPPER_OFFSET => {
                precursor.isolation_window_upper = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::SELECTED_ION_MZ => {
                precursor.selected_ion_mz = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::PEAK_INTENSITY => {
                precursor.selected_ion_intensity = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::CHARGE_STATE => {
                precursor.selected_ion_charge = cv.value_as_i64().map(|v| v as i16);
            }
            MS_CV_ACCESSIONS::COLLISION_ENERGY => {
                precursor.collision_energy = cv.value_as_f64();
            }
            MS_CV_ACCESSIONS::CID
            | MS_CV_ACCESSIONS::HCD
            | MS_CV_ACCESSIONS::ETD
            | MS_CV_ACCESSIONS::ECD => {
                precursor.activation_method = Some(cv.name.clone());
            }
            _ => {}
        }
    }

    /// Decode binary array and add to spectrum
    fn decode_binary_array(
        &self,
        spectrum: &mut MzMLSpectrum,
        ctx: BinaryArrayContext,
    ) -> Result<(), MzMLError> {
        let mut encoding = BinaryEncoding::Float64;
        let mut compression = CompressionType::None;
        let mut is_mz = false;
        let mut is_intensity = false;
        let mut is_ion_mobility = false;

        for cv in &ctx.cv_params {
            match cv.accession.as_str() {
                MS_CV_ACCESSIONS::FLOAT_32_BIT => encoding = BinaryEncoding::Float32,
                MS_CV_ACCESSIONS::FLOAT_64_BIT => encoding = BinaryEncoding::Float64,
                MS_CV_ACCESSIONS::ZLIB_COMPRESSION => compression = CompressionType::Zlib,
                MS_CV_ACCESSIONS::NO_COMPRESSION => compression = CompressionType::None,
                MS_CV_ACCESSIONS::MZ_ARRAY => is_mz = true,
                MS_CV_ACCESSIONS::INTENSITY_ARRAY => is_intensity = true,
                MS_CV_ACCESSIONS::ION_MOBILITY_ARRAY => is_ion_mobility = true,
                _ => {}
            }
        }

        if ctx.base64_data.is_empty() {
            return Ok(());
        }

        let values = BinaryDecoder::decode(
            &ctx.base64_data,
            encoding,
            compression,
            Some(spectrum.default_array_length),
        )?;

        if is_mz {
            spectrum.mz_array = values;
            spectrum.mz_precision_64bit = encoding == BinaryEncoding::Float64;
        } else if is_intensity {
            spectrum.intensity_array = values;
            spectrum.intensity_precision_64bit = encoding == BinaryEncoding::Float64;
        } else if is_ion_mobility {
            spectrum.ion_mobility_array = values;
        }

        Ok(())
    }
}

/// Context for parsing binary data arrays
#[derive(Debug, Default)]
pub(super) struct BinaryArrayContext {
    pub(super) cv_params: Vec<CvParam>,
    pub(super) base64_data: String,
}
