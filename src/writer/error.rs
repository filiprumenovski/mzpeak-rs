/// Errors that can occur during writing
#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    /// I/O error during file operations
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error from the Arrow library during array operations
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// Error from the Parquet library during file writing
    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    /// Error processing metadata
    #[error("Metadata error: {0}")]
    MetadataError(#[from] crate::metadata::MetadataError),

    /// Invalid data provided to the writer
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Writer was not properly initialized
    #[error("Writer not initialized")]
    NotInitialized,

    /// Error from background writer thread
    #[error("Background writer error: {0}")]
    BackgroundWriterError(String),

    /// Background writer thread panicked
    #[error("Background writer thread panicked")]
    ThreadPanicked,
}
