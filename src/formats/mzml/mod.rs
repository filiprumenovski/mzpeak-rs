//! Streaming mzML and external-array imzML ingestion.
mod binary;
pub mod converter;
mod cv_params;
mod external;
mod models;
mod streamer;

pub use binary::{BinaryDecoder, BinaryEncoding, CompressionType as BinaryCompression};
pub use converter::{ConversionConfig, ConversionStats, MzMLConverter};
pub use cv_params::{extract_cv_value, CvParam, IMS_CV_ACCESSIONS, MS_CV_ACCESSIONS};
pub(crate) use external::ExternalBinaryReader;
pub use models::*;
pub use streamer::{
    MzMLError, MzMLStreamer, RawSpectrumIterator, SpectrumIterator, DEFAULT_INPUT_BUFFER_SIZE,
};
