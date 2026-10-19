//! Streaming mzML parser using quick-xml
//!
//! This module provides a pull-based streaming parser for mzML files,
//! designed to handle arbitrarily large files with minimal memory usage.

use std::io::BufRead;

use quick_xml::Reader;

use super::models::{MzMLFileMetadata, MzMLIndex};
use crate::mzml::cv_params::CvParam;
use crate::mzml::streamer::spectrum::BinaryArrayContext;
use crate::mzml::ExternalBinaryReader;

pub use error::MzMLError;
pub use index::DEFAULT_INPUT_BUFFER_SIZE;
pub use iterators::{RawSpectrumIterator, SpectrumIterator};

mod chromatogram;
mod error;
mod helpers;
mod index;
mod iterators;
mod metadata;
mod spectrum;

#[cfg(test)]
mod tests;

/// Streaming parser for mzML files
pub struct MzMLStreamer<R: BufRead> {
    reader: Reader<R>,
    document_root: Option<Vec<u8>>,
    document_closed: bool,
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
    external_binary: Option<ExternalBinaryReader>,
    event_buf: Vec<u8>,
    element_buf: Vec<u8>,
    binary_array_ctx: BinaryArrayContext,
    raw_binary_cv_params: Vec<CvParam>,
}

impl<R: BufRead> MzMLStreamer<R> {
    /// Create a new streamer from a BufRead source
    pub fn new(reader: R) -> Result<Self, MzMLError> {
        let mut xml_reader = Reader::from_reader(reader);
        xml_reader.config_mut().trim_text(true);

        Ok(Self {
            reader: xml_reader,
            document_root: None,
            document_closed: false,
            metadata: MzMLFileMetadata::default(),
            index: MzMLIndex::default(),
            in_spectrum_list: false,
            in_chromatogram_list: false,
            spectrum_count: None,
            chromatogram_count: None,
            current_spectrum_index: 0,
            current_chromatogram_index: 0,
            external_binary: None,
            event_buf: Vec::new(),
            element_buf: Vec::new(),
            binary_array_ctx: BinaryArrayContext::default(),
            raw_binary_cv_params: Vec::new(),
        })
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

    /// Iterate over all spectra
    pub fn spectra(self) -> SpectrumIterator<R> {
        SpectrumIterator { streamer: self }
    }

    /// Iterate over all spectra as raw (undecoded) data
    ///
    /// This is useful for collecting spectra before parallel decoding.
    pub fn raw_spectra(self) -> RawSpectrumIterator<R> {
        RawSpectrumIterator { streamer: self }
    }
}

impl<R: BufRead> MzMLStreamer<R> {
    /// Consume the trailer and require a complete, single XML document.
    pub fn finish_document(&mut self) -> Result<(), MzMLError> {
        use quick_xml::events::Event;
        loop {
            self.event_buf.clear();
            match self.reader.read_event_into(&mut self.event_buf)? {
                Event::End(ref e) if self.document_root.as_deref() == Some(e.name().as_ref()) => {
                    self.document_closed = true
                }
                Event::Start(_) | Event::Empty(_) if self.document_closed => {
                    return Err(MzMLError::InvalidStructure(
                        "trailing XML element after document root".into(),
                    ))
                }
                Event::Text(ref t)
                    if self.document_closed && !t.as_ref().iter().all(u8::is_ascii_whitespace) =>
                {
                    return Err(MzMLError::InvalidStructure(
                        "trailing text after document root".into(),
                    ))
                }
                Event::Eof => break,
                _ => {}
            }
        }
        if !self.document_closed {
            return Err(MzMLError::InvalidStructure("truncated XML document".into()));
        }
        Ok(())
    }
}
