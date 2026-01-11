//! # mzPeak Validation Module
//!
//! Deep integrity validation for .mzpeak files (both Directory bundles and ZIP containers).
//! This module ensures compliance with the mzPeak format specification and helps avoid
//! the "fragmented implementation" issues seen in mzML.
//!
//! ## Validation Checklist
//!
//! 1. **Structure Check**: Validates file/directory structure, checks for required files
//! 2. **Metadata Integrity**: Deserializes and validates metadata.json against schema
//! 3. **Schema Contract**: Verifies Parquet schema matches the mzPeak specification
//! 4. **Data Sanity**: Performs semantic checks on data values
//!
//! ## Usage
//!
//! ```rust,no_run
//! use mzpeak::validator::validate_mzpeak_file;
//! use std::path::Path;
//!
//! let result = validate_mzpeak_file(Path::new("data.mzpeak"));
//! match result {
//!     Ok(report) => {
//!         println!("{}", report);
//!     }
//!     Err(e) => {
//!         eprintln!("Validation failed: {}", e);
//!     }
//! }
//! ```

use std::path::Path;

use anyhow::Result;
use bytes::Bytes;

pub use report::{CheckStatus, ValidationCheck, ValidationReport};

mod data;
mod metadata;
mod report;
mod schema;
mod structure;

/// Validation error types
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    /// Error in file or directory structure
    #[error("Structure error: {0}")]
    StructureError(String),

    /// Error in metadata format or content
    #[error("Metadata error: {0}")]
    MetadataError(String),

    /// Error in Parquet schema
    #[error("Schema error: {0}")]
    SchemaError(String),

    /// Data sanity check failure
    #[error("Data sanity error: {0}")]
    DataSanityError(String),

    /// I/O error during file operations
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Error from the Parquet library
    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),
}

/// Enum to represent the validation target (file path or in-memory ZIP data)
#[derive(Debug)]
enum ValidationTarget {
    /// Direct Parquet file path
    FilePath(std::path::PathBuf),
    /// In-memory Parquet data from ZIP container
    InMemory(Bytes),
}

/// Main validation entry point
pub fn validate_mzpeak_file(path: &Path) -> Result<ValidationReport> {
    let mut report = ValidationReport::new(path.display().to_string());

    // 1. Structure Check
    let validation_target = structure::check_structure(path, &mut report)?;

    // 2. Metadata Integrity Check
    metadata::check_metadata_integrity(path, &validation_target, &mut report)?;

    // 3. Schema Contract Check
    schema::check_schema_contract(&validation_target, &mut report)?;

    // 4. Data Sanity Check
    data::check_data_sanity(&validation_target, &mut report)?;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_report_display() {
        let mut report = ValidationReport::new("test.mzpeak");
        report.add_check(ValidationCheck::ok("Test check 1"));
        report.add_check(ValidationCheck::warning("Test check 2", "This is a warning"));
        report.add_check(ValidationCheck::failed("Test check 3", "This failed"));

        let output = format!("{}", report);
        assert!(output.contains("✓"));
        assert!(output.contains("⚠"));
        assert!(output.contains("✗"));
        assert!(output.contains("1 passed, 1 warnings, 1 failed"));
    }
}
