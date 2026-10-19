//! Error types for Thermo RAW file processing.

use thiserror::Error;

/// Errors that can occur during Thermo RAW file reading and conversion.
#[derive(Error, Debug)]
pub enum ThermoError {
    /// Error opening the RAW file (file not found, invalid format, etc.)
    #[error("Failed to open RAW file: {0}")]
    OpenError(String),

    /// Error reading spectrum data
    #[error("Spectrum read error: {0}")]
    ReadError(String),

    /// Error during peak data conversion
    #[error("Peak conversion error: {0}")]
    PeakConversionError(String),

    /// Path does not exist or is not a valid .raw file
    #[error("Invalid RAW path: {0}")]
    InvalidPath(String),

    /// Missing required metadata or data
    #[error("Missing required data: {0}")]
    MissingData(String),

    /// The rawr process failed to produce a complete run.
    #[error("rawr-ms: {0}")]
    Process(String),

    /// Parsing the mzML stream emitted by rawr failed.
    #[error(transparent)]
    MzML(#[from] crate::mzml::MzMLError),

    /// Decoding the mzML stream failed.
    #[error(transparent)]
    Conversion(#[from] crate::mzml::converter::ConversionError),

    /// The current mzPeak ingestion schema cannot represent this data.
    #[error("Unsupported RAW representation: {0}")]
    UnsupportedRepresentation(String),

    /// Parquet or ingestion writer failure.
    #[error(transparent)]
    Writer(#[from] crate::writer::WriterError),

    /// Dataset container failure.
    #[error(transparent)]
    Dataset(#[from] crate::dataset::DatasetError),

    /// Generic I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

impl From<ThermoError> for crate::writer::WriterError {
    fn from(error: ThermoError) -> Self {
        crate::writer::WriterError::InvalidData(error.to_string())
    }
}
