//! Error types for TDF format processing.

use thiserror::Error;

/// Errors that can occur during TDF reading and conversion.
#[derive(Error, Debug)]
pub enum TdfError {
    /// Error reading from TDF file (SQL or binary)
    #[error("TDF read error: {0}")]
    ReadError(String),

    /// Error during frame data decompression or parsing
    #[error("Frame parsing error: {0}")]
    FrameParsingError(String),

    /// Error converting peak data to required format
    #[error("Peak conversion error: {0}")]
    PeakConversionError(String),

    /// Path does not exist or is not a valid .d directory
    #[error("Invalid TDF path: {0}")]
    InvalidPath(String),

    /// Missing required metadata or data
    #[error("Missing required data: {0}")]
    MissingData(String),

    /// Ion mobility conversion failed
    #[error("Ion mobility conversion error: {0}")]
    MobilityConversionError(String),

    /// Generic I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

