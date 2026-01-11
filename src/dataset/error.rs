use crate::writer::WriterError;

/// Errors that can occur during dataset operations
#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    /// I/O error during file operations
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error from the underlying peak writer
    #[error("Writer error: {0}")]
    WriterError(#[from] WriterError),

    /// Error processing metadata
    #[error("Metadata error: {0}")]
    MetadataError(#[from] crate::metadata::MetadataError),

    /// Error serializing/deserializing JSON
    #[error("JSON serialization error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    /// Error from the ZIP container library
    #[error("ZIP error: {0}")]
    ZipError(#[from] zip::result::ZipError),

    /// Error from the chromatogram writer
    #[error("Chromatogram writer error: {0}")]
    ChromatogramWriterError(String),

    /// Error from the mobilogram writer
    #[error("Mobilogram writer error: {0}")]
    MobilogramWriterError(String),

    /// Invalid or malformed dataset path
    #[error("Invalid dataset path: {0}")]
    InvalidPath(String),

    /// Dataset already exists at the specified location
    #[error("Dataset already exists: {0}")]
    AlreadyExists(String),

    /// Dataset was not properly initialized before use
    #[error("Dataset not properly initialized")]
    NotInitialized,
}
