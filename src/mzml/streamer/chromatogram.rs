use std::io::BufRead;

use quick_xml::events::{BytesStart, Event};

use super::helpers::{get_attribute, parse_cv_param};
use super::spectrum::BinaryArrayContext;
use super::{MzMLError, MzMLStreamer};
use crate::mzml::binary::{BinaryDecoder, BinaryEncoding, CompressionType};
use crate::mzml::cv_params::CvParam;
use crate::mzml::models::{ChromatogramType, MzMLChromatogram};

impl<R: BufRead> MzMLStreamer<R> {
    /// Read the next chromatogram from the stream
    pub fn next_chromatogram(&mut self) -> Result<Option<MzMLChromatogram>, MzMLError> {
        self.event_buf.clear();

        loop {
            let event = self.reader.read_event_into(&mut self.event_buf);
            match event {
                Ok(Event::Start(ref e)) => match e.name().as_ref() {
                    b"chromatogram" => {
                        let e_owned = e.to_owned();
                        let chromatogram = self.parse_chromatogram(&e_owned)?;
                        self.current_chromatogram_index += 1;
                        return Ok(Some(chromatogram));
                    }
                    b"chromatogramList" => {
                        self.in_chromatogram_list = true;
                        // Read count attribute
                        if let Some(count_str) = get_attribute(e, "count")? {
                            if let Ok(count) = count_str.parse::<usize>() {
                                self.chromatogram_count = Some(count);
                            }
                        }
                    }
                    _ => {}
                },
                Ok(Event::End(ref e)) => {
                    if e.name().as_ref() == b"chromatogramList" {
                        self.in_chromatogram_list = false;
                        return Ok(None);
                    }
                }
                Ok(Event::Eof) => return Ok(None),
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            self.event_buf.clear();
        }
    }

    /// Parse a single chromatogram element
    fn parse_chromatogram(
        &mut self,
        start_event: &BytesStart,
    ) -> Result<MzMLChromatogram, MzMLError> {
        let mut chromatogram = MzMLChromatogram::default();

        // Get attributes from chromatogram element
        chromatogram.index = get_attribute(start_event, "index")?
            .and_then(|s| s.parse().ok())
            .unwrap_or(self.current_chromatogram_index);
        chromatogram.id = get_attribute(start_event, "id")?.unwrap_or_default();
        chromatogram.default_array_length = get_attribute(start_event, "defaultArrayLength")?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let mut depth = 1;
        let mut in_binary_data_array_list = false;
        let mut in_binary_array = false;

        self.binary_array_ctx.cv_params.clear();
        self.binary_array_ctx.base64_data.clear();
        self.element_buf.clear();

        loop {
            match self.reader.read_event_into(&mut self.element_buf) {
                Ok(Event::Start(ref e)) => {
                    depth += 1;
                    match e.name().as_ref() {
                        b"cvParam" => {
                            let cv_param = parse_cv_param(e)?;
                            if in_binary_data_array_list && in_binary_array {
                                self.binary_array_ctx.cv_params.push(cv_param);
                            } else {
                                chromatogram.cv_params.push(cv_param);
                            }
                        }
                        b"binaryDataArrayList" => {
                            in_binary_data_array_list = true;
                        }
                        b"binaryDataArray" => {
                            in_binary_array = true;
                            self.binary_array_ctx.cv_params.clear();
                            self.binary_array_ctx.base64_data.clear();
                        }
                        b"binary" => {}
                        _ => {}
                    }
                }
                Ok(Event::Empty(ref e)) => {
                    if e.name().as_ref() == b"cvParam" {
                        let cv_param = parse_cv_param(e)?;

                        if in_binary_data_array_list && in_binary_array {
                            self.binary_array_ctx.cv_params.push(cv_param);
                        } else {
                            Self::apply_chromatogram_cv_param(&mut chromatogram, &cv_param);
                            chromatogram.cv_params.push(cv_param);
                        }
                    }
                }
                Ok(Event::Text(ref t)) => {
                    if in_binary_array {
                        self.binary_array_ctx.base64_data = t.unescape()?.into_owned();
                    }
                }
                Ok(Event::End(ref e)) => {
                    depth -= 1;
                    match e.name().as_ref() {
                        b"chromatogram" => {
                            if depth == 0 {
                                break;
                            }
                        }
                        b"binaryDataArrayList" => {
                            in_binary_data_array_list = false;
                        }
                        b"binaryDataArray" => {
                            if in_binary_array {
                                let ctx = std::mem::take(&mut self.binary_array_ctx);
                                let ctx =
                                    self.decode_chromatogram_binary_array(&mut chromatogram, ctx)?;
                                self.binary_array_ctx = ctx;
                                in_binary_array = false;
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::Eof) => {
                    return Err(MzMLError::InvalidStructure(
                        "Unexpected EOF in chromatogram".to_string(),
                    ));
                }
                Err(e) => return Err(MzMLError::XmlError(e)),
                _ => {}
            }
            self.element_buf.clear();
        }

        Ok(chromatogram)
    }

    /// Apply chromatogram-specific CV parameters
    fn apply_chromatogram_cv_param(chromatogram: &mut MzMLChromatogram, cv: &CvParam) {
        chromatogram.chromatogram_type = ChromatogramType::from_cv_accession(&cv.accession);
    }

    /// Decode a binary array for chromatograms (time or intensity)
    fn decode_chromatogram_binary_array(
        &self,
        chromatogram: &mut MzMLChromatogram,
        mut ctx: BinaryArrayContext,
    ) -> Result<BinaryArrayContext, MzMLError> {
        let mut encoding = BinaryEncoding::Float64;
        let mut compression = CompressionType::None;
        let mut is_time = false;
        let mut is_intensity = false;

        for cv in &ctx.cv_params {
            match cv.accession.as_str() {
                "MS:1000523" => encoding = BinaryEncoding::Float64,
                "MS:1000521" => encoding = BinaryEncoding::Float32,
                "MS:1000574" => compression = CompressionType::Zlib,
                "MS:1000576" => compression = CompressionType::None,
                "MS:1000595" => is_time = true,      // time array
                "MS:1000515" => is_intensity = true, // intensity array
                _ => {}
            }
        }

        if ctx.base64_data.is_empty() {
            ctx.cv_params.clear();
            ctx.base64_data.clear();
            return Ok(ctx);
        }

        let values = BinaryDecoder::decode(
            &ctx.base64_data,
            encoding,
            compression,
            Some(chromatogram.default_array_length),
        )?;

        if is_time {
            chromatogram.time_array = values;
        } else if is_intensity {
            chromatogram.intensity_array = values;
        }

        ctx.cv_params.clear();
        ctx.base64_data.clear();
        Ok(ctx)
    }
}
