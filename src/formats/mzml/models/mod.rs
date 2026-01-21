//! Data models for mzML structures
//!
//! These models represent the parsed mzML data in a Rust-native format,
//! ready for conversion to mzPeak Parquet format.

mod chromatogram;
mod index;
mod metadata;
mod raw;
mod spectrum;

pub use chromatogram::{ChromatogramType, MzMLChromatogram};
pub use index::{IndexEntry, MzMLIndex};
pub use metadata::{
    ComponentType, DataProcessing, InstrumentComponent, InstrumentConfiguration, MzMLFileMetadata,
    ProcessingMethod, Sample, Software, SourceFile,
};
pub use raw::{RawBinaryData, RawMzMLSpectrum};
pub use spectrum::{MzMLSpectrum, Precursor};
