//! Streaming mzML parser using quick-xml
//!
//! This module provides a pull-based streaming parser for mzML files,
//! designed to handle arbitrarily large files with minimal memory usage.

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

use super::binary::{BinaryDecoder, BinaryEncoding, CompressionType};
use super::cv_params::{normalize_retention_time, CvParam, MS_CV_ACCESSIONS};
use super::models::*;

/// Errors that can occur during mzML parsing
#[derive(Debug, thiserror::Error)]
pub enum MzMLError {
    #[error("XML parsing error: {0}")]
    XmlError(#[from] quick_xml::Error),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Binary decode error: {0}")]
    BinaryError(#[from] super::binary::BinaryDecodeError),

    #[error("Invalid mzML structure: {0}")]
    InvalidStructure(String),

    #[error("Missing required attribute: {0}")]
    MissingAttribute(String),

    #[error("Invalid attribute value: {0}")]
    InvalidAttributeValue(String),

    #[error("UTF-8 encoding error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
}

/// Streaming parser for mzML files
pub struct MzMLStreamer<R: BufRead> {
    reader: Reader<R>,
    metadata: MzMLFileMetadata,
    #[allow(dead_code)]
    index: MzMLIndex,
    in_spectrum_list: bool,
    #[allow(dead_code)]
    in_chromatogram_list: bool,
    spectrum_count: Option<usize>,
    #[allow(dead_code)]
    chromatogram_count: Option<usize>,
    current_spectrum_index: i64,
    #[allow(dead_code)]
    current_chromatogram_index: i64,
}

impl MzMLStreamer<BufReader<File>> {
    /// Open an mzML file for streaming
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MzMLError> {
        let file = File::open(path.as_ref())?;
        let reader = BufReader::with_capacity(64 * 1024, file);
        Self::new(reader)
    }

    /// Open an indexed mzML file and read the index first
    pub fn open_indexed<P: AsRef<Path>>(path: P) -> Result<Self, MzMLError> {
        let mut file = File::open(path.as_ref())?;

        // Try to read the index from the end of the file
        let index = Self::read_index_from_file(&mut file)?;

        // Reset to beginning
        file.seek(SeekFrom::Start(0))?;

        let reader = BufReader::with_capacity(64 * 1024, file);
        let mut streamer = Self::new(reader)?;
        streamer.index = index;

        Ok(streamer)
    }

    /// Read the index from the end of an indexed mzML file
    fn read_index_from_file(file: &mut File) -> Result<MzMLIndex, MzMLError> {
        let file_size = file.seek(SeekFrom::End(0))?;

        // Read last 1KB to find indexListOffset
        let read_size = std::cmp::min(1024, file_size) as usize;
        file.seek(SeekFrom::End(-(read_size as i64)))?;

        let mut tail = vec![0u8; read_size];
        file.read_exact(&mut tail)?;

        let tail_str = String::from_utf8_lossy(&tail);

        // Look for indexListOffset
        let mut index = MzMLIndex::default();
        if let Some(pos) = tail_str.find("<indexListOffset>") {
            let start = pos + 17;
            if let Some(end) = tail_str[start..].find("</indexListOffset>") {
                if let Ok(offset) = tail_str[start..start + end].trim().parse::<u64>() {
                    index.index_list_offset = Some(offset);

                    // Seek to index and parse it
                    file.seek(SeekFrom::Start(offset))?;
                    let mut index_data = Vec::new();
                    file.read_to_end(&mut index_data)?;

                    index = Self::parse_index_data(&index_data, offset)?;
                }
            }
        }

        Ok(index)
    }

    /// Parse the indexList from raw XML data
    fn parse_index_data(data: &[u8], offset: u64) -> Result<MzMLIndex, MzMLError> {
        let mut reader = Reader::from_reader(data);
        reader.config_mut().trim_text(true);

        let mut buf = Vec::new();
        let mut index = MzMLIndex {
            index_list_offset: Some(offset),
            ..Default::default()
        };

        let mut current_index_name: Option<String> = None;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                    match e.name().as_ref() {
                        b"index" => {
                            current_index_name = get_attribute(e, "name")?;
                        }
                        b"offset" => {
                            let id = get_attribute(e, "idRef")?;
                            // Read the offset value
                            let mut offset_buf = Vec::new();
                            let offset_val = match reader.read_event_into(&mut offset_buf) {
                                Ok(Event::Text(t)) => {
                                    t.unescape()?.trim().parse::<u64>().unwrap_or(0)
                                }
                                _ => 0,
                            };

                            if let Some(ref name) = current_index_name {
                                let entry = IndexEntry {
                                    id: id.unwrap_or_default(),
                                    offset: offset_val,
                                };
                                match name.as_str() {
                                    "spectrum" => index.spectrum_index.push(entry),
                                    "chromatogram" => index.chromatogram_index.push(entry),
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::End(ref e)) => {
                    if e.name().as_ref() == b"index" {
                        current_index_name = None;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(index)
    }
}

impl<R: BufRead> MzMLStreamer<R> {
    /// Create a new streamer from a BufRead source
    pub fn new(reader: R) -> Result<Self, MzMLError> {
        let mut xml_reader = Reader::from_reader(reader);
        xml_reader.config_mut().trim_text(true);

        Ok(Self {
            reader: xml_reader,
            metadata: MzMLFileMetadata::default(),
            index: MzMLIndex::default(),
            in_spectrum_list: false,
            in_chromatogram_list: false,
            spectrum_count: None,
            chromatogram_count: None,
            current_spectrum_index: 0,
            current_chromatogram_index: 0,
        })
    }

    /// Read file-level metadata (everything before spectrumList)
    pub fn read_metadata(&mut self) -> Result<&MzMLFileMetadata, MzMLError> {
        let mut buf = Vec::new();
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    match e.name().as_ref() {
                        b"mzML" => {
                            self.metadata.version = get_attribute(e, "version")?;
                        }
                        b"fileDescription" => {
                            self.parse_file_description()?;
                        }
                        b"softwareList" => {
                            self.parse_software_list()?;
                        }
                        b"instrumentConfigurationList" => {
                            self.parse_instrument_configuration_list()?;
                        }
                        b"dataProcessingList" => {
                            self.parse_data_processing_list()?;
                        }
                        b"sampleList" => {
                            self.parse_sample_list()?;
                        }
                        b"run" => {
                            self.metadata.run_id = get_attribute(e, "id")?;
                            self.metadata.run_start_time = get_attribute(e, "startTimeStamp")?;
                            self.metadata.default_instrument_configuration_ref =
                                get_attribute(e, "defaultInstrumentConfigurationRef")?;
                            self.metadata.default_source_file_ref =
                                get_attribute(e, "defaultSourceFileRef")?;
                        }
                        b"spectrumList" => {
                            self.in_spectrum_list = true;
                            self.spectrum_count = get_attribute(e, "count")?
                                .and_then(|s| s.parse().ok());
                            break;
                        }
                        b"chromatogramList" => {
                            self.in_chromatogram_list = true;
                            self.chromatogram_count = get_attribute(e, "count")?
                                .and_then(|s| s.parse().ok());
                            break;
                        }
                        _ => {}
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(&self.metadata)
    }

    /// Get the file metadata
    pub fn metadata(&self) -> &MzMLFileMetadata {
        &self.metadata
    }

    /// Get the index if available
    pub fn index(&self) -> &MzMLIndex {
        &self.index
    }

    /// Get expected spectrum count
    pub fn spectrum_count(&self) -> Option<usize> {
        if self.index.is_indexed() {
            Some(self.index.spectrum_count())
        } else {
            self.spectrum_count
        }
    }

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

    /// Iterate over all spectra
    pub fn spectra(self) -> SpectrumIterator<R> {
        SpectrumIterator { streamer: self }
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
            MS_CV_ACCESSIONS::CID | MS_CV_ACCESSIONS::HCD | MS_CV_ACCESSIONS::ETD
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

    /// Parse fileDescription element
    fn parse_file_description(&mut self) -> Result<(), MzMLError> {
        let mut depth = 1;
        let mut buf = Vec::new();
        let mut pending_source_files = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    depth += 1;
                    if e.name().as_ref() == b"sourceFile" {
                        // Collect attributes and parse inline
                        let id = get_attribute(&e, "id")?.unwrap_or_default();
                        let name = get_attribute(&e, "name")?.unwrap_or_default();
                        let location = get_attribute(&e, "location")?;
                        let source = self.parse_source_file_content(id, name, location)?;
                        pending_source_files.push(source);
                        depth -= 1; // parse_source_file_content consumes the end tag
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv = parse_cv_param(e)?;
                        self.metadata.file_content.push(cv);
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"fileDescription" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        self.metadata.source_files.extend(pending_source_files);
        Ok(())
    }

    /// Parse sourceFile element content (after start tag has been consumed)
    fn parse_source_file_content(
        &mut self,
        id: String,
        name: String,
        location: Option<String>,
    ) -> Result<SourceFile, MzMLError> {
        let mut source = SourceFile {
            id,
            name,
            location,
            ..Default::default()
        };

        let mut depth = 1;
        let mut buf = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(_)) => {
                    depth += 1;
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv = parse_cv_param(e)?;
                        match cv.accession.as_str() {
                            MS_CV_ACCESSIONS::SHA1_CHECKSUM => {
                                source.checksum = cv.value.clone();
                                source.checksum_type = Some("SHA-1".to_string());
                            }
                            MS_CV_ACCESSIONS::MD5_CHECKSUM => {
                                source.checksum = cv.value.clone();
                                source.checksum_type = Some("MD5".to_string());
                            }
                            _ => {
                                if cv.name.contains("format") || cv.name.contains("Format") {
                                    source.file_format = Some(cv.name.clone());
                                }
                            }
                        }
                        source.cv_params.push(cv);
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"sourceFile" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(source)
    }

    /// Parse softwareList element
    fn parse_software_list(&mut self) -> Result<(), MzMLError> {
        let mut depth = 1;
        let mut buf = Vec::new();
        let mut pending_software = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    depth += 1;
                    if e.name().as_ref() == b"software" {
                        let id = get_attribute(&e, "id")?.unwrap_or_default();
                        let version = get_attribute(&e, "version")?;
                        let sw = self.parse_software_content(id, version)?;
                        pending_software.push(sw);
                        depth -= 1;
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"softwareList" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        self.metadata.software_list.extend(pending_software);
        Ok(())
    }

    /// Parse software element content
    fn parse_software_content(
        &mut self,
        id: String,
        version: Option<String>,
    ) -> Result<Software, MzMLError> {
        let mut sw = Software {
            id,
            version,
            ..Default::default()
        };

        let mut depth = 1;
        let mut buf = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(_)) => {
                    depth += 1;
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv = parse_cv_param(e)?;
                        if sw.name.is_none() {
                            sw.name = Some(cv.name.clone());
                        }
                        sw.cv_params.push(cv);
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"software" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(sw)
    }

    /// Parse instrumentConfigurationList element
    fn parse_instrument_configuration_list(&mut self) -> Result<(), MzMLError> {
        let mut depth = 1;
        let mut buf = Vec::new();
        let mut pending_configs = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    depth += 1;
                    if e.name().as_ref() == b"instrumentConfiguration" {
                        let id = get_attribute(&e, "id")?.unwrap_or_default();
                        let ic = self.parse_instrument_configuration_content(id)?;
                        pending_configs.push(ic);
                        depth -= 1;
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"instrumentConfigurationList" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        self.metadata.instrument_configurations.extend(pending_configs);
        Ok(())
    }

    /// Parse instrumentConfiguration element content
    fn parse_instrument_configuration_content(
        &mut self,
        id: String,
    ) -> Result<InstrumentConfiguration, MzMLError> {
        let mut ic = InstrumentConfiguration {
            id,
            ..Default::default()
        };

        let mut depth = 1;
        let mut buf = Vec::new();
        let mut current_component_type = ComponentType::Unknown;

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    depth += 1;
                    match e.name().as_ref() {
                        b"source" => {
                            current_component_type = ComponentType::Source;
                        }
                        b"analyzer" => {
                            current_component_type = ComponentType::Analyzer;
                        }
                        b"detector" => {
                            current_component_type = ComponentType::Detector;
                        }
                        b"softwareRef" => {
                            ic.software_ref = get_attribute(e, "ref")?;
                        }
                        _ => {}
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv = parse_cv_param(e)?;
                        if current_component_type == ComponentType::Unknown {
                            ic.cv_params.push(cv);
                        }
                    } else if e.name().as_ref() == b"softwareRef" {
                        ic.software_ref = get_attribute(e, "ref")?;
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    match e.name().as_ref() {
                        b"instrumentConfiguration" if depth == 0 => break,
                        b"source" | b"analyzer" | b"detector" => {
                            current_component_type = ComponentType::Unknown;
                        }
                        _ => {}
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(ic)
    }

    /// Parse dataProcessingList element
    fn parse_data_processing_list(&mut self) -> Result<(), MzMLError> {
        let mut depth = 1;
        let mut buf = Vec::new();
        let mut pending_processing = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    depth += 1;
                    if e.name().as_ref() == b"dataProcessing" {
                        let id = get_attribute(&e, "id")?.unwrap_or_default();
                        let dp = self.parse_data_processing_content(id)?;
                        pending_processing.push(dp);
                        depth -= 1;
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"dataProcessingList" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        self.metadata.data_processing.extend(pending_processing);
        Ok(())
    }

    /// Parse dataProcessing element content
    fn parse_data_processing_content(&mut self, id: String) -> Result<DataProcessing, MzMLError> {
        let mut dp = DataProcessing {
            id,
            ..Default::default()
        };

        let mut depth = 1;
        let mut buf = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    depth += 1;
                    if e.name().as_ref() == b"processingMethod" {
                        let order = get_attribute(&e, "order")?
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        let software_ref = get_attribute(&e, "softwareRef")?;
                        let pm = self.parse_processing_method_content(order, software_ref)?;
                        dp.processing_methods.push(pm);
                        depth -= 1;
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"dataProcessing" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(dp)
    }

    /// Parse processingMethod element content
    fn parse_processing_method_content(
        &mut self,
        order: i32,
        software_ref: Option<String>,
    ) -> Result<ProcessingMethod, MzMLError> {
        let mut pm = ProcessingMethod {
            order,
            software_ref,
            ..Default::default()
        };

        let mut depth = 1;
        let mut buf = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(_)) => {
                    depth += 1;
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv = parse_cv_param(e)?;
                        pm.cv_params.push(cv);
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"processingMethod" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(pm)
    }

    /// Parse sampleList element
    fn parse_sample_list(&mut self) -> Result<(), MzMLError> {
        let mut depth = 1;
        let mut buf = Vec::new();
        let mut pending_samples = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    depth += 1;
                    if e.name().as_ref() == b"sample" {
                        let id = get_attribute(&e, "id")?.unwrap_or_default();
                        let name = get_attribute(&e, "name")?;
                        let sample = self.parse_sample_content(id, name)?;
                        pending_samples.push(sample);
                        depth -= 1;
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"sampleList" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        self.metadata.samples.extend(pending_samples);
        Ok(())
    }

    /// Parse sample element content
    fn parse_sample_content(
        &mut self,
        id: String,
        name: Option<String>,
    ) -> Result<Sample, MzMLError> {
        let mut sample = Sample {
            id,
            name,
            ..Default::default()
        };

        let mut depth = 1;
        let mut buf = Vec::new();

        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(_)) => {
                    depth += 1;
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv = parse_cv_param(e)?;
                        sample.cv_params.push(cv);
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    if e.name().as_ref() == b"sample" && depth == 0 {
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(sample)
    }
}

/// Context for parsing binary data arrays
#[derive(Debug, Default)]
struct BinaryArrayContext {
    cv_params: Vec<CvParam>,
    base64_data: String,
}

/// Iterator over spectra in an mzML file
pub struct SpectrumIterator<R: BufRead> {
    streamer: MzMLStreamer<R>,
}

impl<R: BufRead> Iterator for SpectrumIterator<R> {
    type Item = Result<MzMLSpectrum, MzMLError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.streamer.next_spectrum() {
            Ok(Some(spectrum)) => Some(Ok(spectrum)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Helper function to get an attribute value from a BytesStart
fn get_attribute(e: &BytesStart, name: &str) -> Result<Option<String>, MzMLError> {
    for attr in e.attributes() {
        let attr = attr.map_err(|e| MzMLError::XmlError(quick_xml::Error::from(e)))?;
        if attr.key.as_ref() == name.as_bytes() {
            let value = std::str::from_utf8(&attr.value)?.to_string();
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// Parse a cvParam element
fn parse_cv_param(e: &BytesStart) -> Result<CvParam, MzMLError> {
    Ok(CvParam {
        cv_ref: get_attribute(e, "cvRef")?.unwrap_or_default(),
        accession: get_attribute(e, "accession")?.unwrap_or_default(),
        name: get_attribute(e, "name")?.unwrap_or_default(),
        value: get_attribute(e, "value")?,
        unit_cv_ref: get_attribute(e, "unitCvRef")?,
        unit_accession: get_attribute(e, "unitAccession")?,
        unit_name: get_attribute(e, "unitName")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_MZML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <run id="test_run">
    <spectrumList count="1">
      <spectrum index="0" id="scan=1" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan"/>
        <scanList count="1">
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="60.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <binary>AAAAAAAAWUAAAAAAAABpQA==</binary>
          </binaryDataArray>
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <binary>AADIQgAASEM=</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
  </run>
</mzML>"#;

    #[test]
    fn test_parse_minimal_mzml() {
        let reader = std::io::Cursor::new(MINIMAL_MZML);
        let mut streamer = MzMLStreamer::new(BufReader::new(reader)).unwrap();

        let spectrum = streamer.next_spectrum().unwrap().unwrap();

        assert_eq!(spectrum.index, 0);
        assert_eq!(spectrum.id, "scan=1");
        assert_eq!(spectrum.ms_level, 1);
        assert_eq!(spectrum.polarity, 1);
        assert!((spectrum.retention_time.unwrap() - 60.0).abs() < 0.001);
        assert_eq!(spectrum.mz_array.len(), 2);
        assert_eq!(spectrum.intensity_array.len(), 2);
        assert!((spectrum.mz_array[0] - 100.0).abs() < 0.001);
        assert!((spectrum.mz_array[1] - 200.0).abs() < 0.001);
    }

    #[test]
    fn test_scan_number_extraction() {
        let spectrum = MzMLSpectrum {
            id: "controllerType=0 controllerNumber=1 scan=12345".to_string(),
            ..Default::default()
        };
        assert_eq!(spectrum.scan_number(), Some(12345));

        let spectrum2 = MzMLSpectrum {
            id: "scan=999".to_string(),
            ..Default::default()
        };
        assert_eq!(spectrum2.scan_number(), Some(999));
    }
}
