/// Errors that can occur during metadata processing
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    /// I/O error reading metadata file
    #[error("Failed to read file: {0}")]
    IoError(#[from] std::io::Error),

    /// CSV/TSV parsing error
    #[error("CSV parsing error: {0}")]
    CsvError(#[from] csv::Error),

    /// Missing required column in SDRF file
    #[error("Missing required SDRF column: {0}")]
    MissingColumn(String),

    /// Invalid SDRF file format
    #[error("Invalid SDRF format: {0}")]
    InvalidFormat(String),

    /// JSON serialization/deserialization error
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
}
