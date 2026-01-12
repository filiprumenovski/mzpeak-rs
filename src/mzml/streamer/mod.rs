//! Streaming mzML parser using quick-xml
//!
//! This module provides a pull-based streaming parser for mzML files,
//! designed to handle arbitrarily large files with minimal memory usage.

use std::io::BufRead;

use quick_xml::Reader;

use super::models::{MzMLFileMetadata, MzMLIndex};
use crate::mzml::ExternalBinaryReader;

pub use error::MzMLError;
pub use index::DEFAULT_INPUT_BUFFER_SIZE;
pub use iterators::{RawSpectrumIterator, SpectrumIterator};

mod error;
mod helpers;
mod index;
mod iterators;
mod metadata;
mod spectrum;
mod chromatogram;

#[cfg(test)]
mod tests;

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
    external_binary: Option<ExternalBinaryReader>,
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
            external_binary: None,
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
