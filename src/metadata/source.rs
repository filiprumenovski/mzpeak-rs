use serde::{Deserialize, Serialize};

use super::MetadataError;

/// Source file information for provenance tracking
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceFileInfo {
    /// Original file name
    pub name: String,

    /// Original file path
    pub path: Option<String>,

    /// File format (e.g., "Thermo RAW", "Bruker .d")
    pub format: Option<String>,

    /// File size in bytes
    pub size_bytes: Option<u64>,

    /// SHA-256 checksum of the original file
    pub sha256: Option<String>,

    /// MD5 checksum (for legacy compatibility)
    pub md5: Option<String>,

    /// Vendor file version/format version
    pub format_version: Option<String>,
}

impl SourceFileInfo {
    /// Create new source file info with the given filename
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    /// Serialize to JSON for Parquet footer storage
    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}
