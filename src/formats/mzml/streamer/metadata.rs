use std::io::BufRead;

use quick_xml::events::Event;

use super::helpers::{get_attribute, parse_cv_param};
use super::{MzMLError, MzMLStreamer};
use crate::mzml::cv_params::MS_CV_ACCESSIONS;
use crate::mzml::models::*;

impl<R: BufRead> MzMLStreamer<R> {
    /// Read file-level metadata (everything before spectrumList)
    pub fn read_metadata(&mut self) -> Result<&MzMLFileMetadata, MzMLError> {
        let mut buf = Vec::new();
        loop {
            match self.reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => match e.name().as_ref() {
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
                        self.spectrum_count =
                            get_attribute(e, "count")?.and_then(|s| s.parse().ok());
                        break;
                    }
                    b"chromatogramList" => {
                        self.in_chromatogram_list = true;
                        self.chromatogram_count =
                            get_attribute(e, "count")?.and_then(|s| s.parse().ok());
                        break;
                    }
                    _ => {}
                },
                Ok(Event::Eof) => break,
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            buf.clear();
        }

        Ok(&self.metadata)
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
    fn parse_sample_content(&mut self, id: String, name: Option<String>) -> Result<Sample, MzMLError> {
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
