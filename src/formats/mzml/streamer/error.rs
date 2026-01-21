/// Errors that can occur during mzML parsing
#[derive(Debug, thiserror::Error)]
pub enum MzMLError {
    /// Error parsing XML
    #[error("XML parsing error: {0}")]
    XmlError(#[from] quick_xml::Error),

    /// I/O error during file operations
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error decoding binary data arrays
    #[error("Binary decode error: {0}")]
    BinaryError(#[from] crate::mzml::binary::BinaryDecodeError),

    /// Invalid mzML document structure
    #[error("Invalid mzML structure: {0}")]
    InvalidStructure(String),

    /// Required XML attribute is missing
    #[error("Missing required attribute: {0}")]
    MissingAttribute(String),

    /// Invalid value for an XML attribute
    #[error("Invalid attribute value: {0}")]
    InvalidAttributeValue(String),

    /// UTF-8 encoding error in text content
    #[error("UTF-8 encoding error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
}
