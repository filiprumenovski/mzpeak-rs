/// Errors that can occur during reading
#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Arrow error
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// Parquet error
    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    /// ZIP archive error
    #[error("ZIP error: {0}")]
    ZipError(#[from] zip::result::ZipError),

    /// Invalid file format
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// Metadata parsing error
    #[error("Metadata error: {0}")]
    MetadataError(String),

    /// Column not found
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// JSON parsing error
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}
