use std::io::BufRead;

use super::{MzMLError, MzMLStreamer};
use crate::mzml::models::{MzMLSpectrum, RawMzMLSpectrum};

/// Iterator over spectra in an mzML file
pub struct SpectrumIterator<R: BufRead> {
    pub(super) streamer: MzMLStreamer<R>,
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

/// Iterator over raw (undecoded) spectra in an mzML file
///
/// This iterator is designed for collecting spectra before parallel decoding.
/// Each item is a `RawMzMLSpectrum` that can later be decoded using `.decode()`.
pub struct RawSpectrumIterator<R: BufRead> {
    pub(super) streamer: MzMLStreamer<R>,
}

impl<R: BufRead> Iterator for RawSpectrumIterator<R> {
    type Item = Result<RawMzMLSpectrum, MzMLError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.streamer.next_raw_spectrum() {
            Ok(Some(spectrum)) => Some(Ok(spectrum)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
