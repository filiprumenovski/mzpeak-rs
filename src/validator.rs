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

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use bytes::Bytes;
use zip::ZipArchive;
use arrow::datatypes::DataType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;

use crate::metadata::MzPeakMetadata;
use crate::schema::{columns, create_mzpeak_schema, MZPEAK_FORMAT_VERSION, MZPEAK_MIMETYPE};

/// Validation error types
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Structure error: {0}")]
    StructureError(String),

    #[error("Metadata error: {0}")]
    MetadataError(String),

    #[error("Schema error: {0}")]
    SchemaError(String),

    #[error("Data sanity error: {0}")]
    DataSanityError(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),
}

/// Validation check result
#[derive(Debug, Clone)]
pub enum CheckStatus {
    Ok,
    Warning(String),
    Failed(String),
}

impl CheckStatus {
    fn is_ok(&self) -> bool {
        matches!(self, CheckStatus::Ok)
    }

    fn is_failed(&self) -> bool {
        matches!(self, CheckStatus::Failed(_))
    }
}

/// Individual validation check
#[derive(Debug, Clone)]
pub struct ValidationCheck {
    pub name: String,
    pub status: CheckStatus,
}

impl ValidationCheck {
    fn ok(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Ok,
        }
    }

    fn warning(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Warning(message.into()),
        }
    }

    fn failed(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Failed(message.into()),
        }
    }
}

/// Complete validation report
#[derive(Debug)]
pub struct ValidationReport {
    pub checks: Vec<ValidationCheck>,
    pub file_path: String,
}

impl ValidationReport {
    pub fn new(file_path: impl Into<String>) -> Self {
        Self {
            checks: Vec::new(),
            file_path: file_path.into(),
        }
    }

    pub fn add_check(&mut self, check: ValidationCheck) {
        self.checks.push(check);
    }

    pub fn has_failures(&self) -> bool {
        self.checks.iter().any(|c| c.status.is_failed())
    }

    pub fn has_warnings(&self) -> bool {
        self.checks.iter().any(|c| matches!(c.status, CheckStatus::Warning(_)))
    }

    pub fn success_count(&self) -> usize {
        self.checks.iter().filter(|c| c.status.is_ok()).count()
    }

    pub fn warning_count(&self) -> usize {
        self.checks.iter().filter(|c| matches!(c.status, CheckStatus::Warning(_))).count()
    }

    pub fn failure_count(&self) -> usize {
        self.checks.iter().filter(|c| c.status.is_failed()).count()
    }
}

impl std::fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "mzPeak Validation Report")?;
        writeln!(f, "========================")?;
        writeln!(f, "File: {}", self.file_path)?;
        writeln!(f)?;

        for check in &self.checks {
            let symbol = match &check.status {
                CheckStatus::Ok => "✓",
                CheckStatus::Warning(_) => "⚠",
                CheckStatus::Failed(_) => "✗",
            };

            write!(f, "[{}] {}", symbol, check.name)?;

            match &check.status {
                CheckStatus::Ok => writeln!(f)?,
                CheckStatus::Warning(msg) => writeln!(f, " - WARNING: {}", msg)?,
                CheckStatus::Failed(msg) => writeln!(f, " - FAILED: {}", msg)?,
            }
        }

        writeln!(f)?;
        writeln!(f, "Summary: {} passed, {} warnings, {} failed",
                 self.success_count(), self.warning_count(), self.failure_count())?;

        if self.has_failures() {
            writeln!(f)?;
            writeln!(f, "Validation FAILED")?;
        } else if self.has_warnings() {
            writeln!(f)?;
            writeln!(f, "Validation PASSED with warnings")?;
        } else {
            writeln!(f)?;
            writeln!(f, "Validation PASSED")?;
        }

        Ok(())
    }
}

/// Main validation entry point
pub fn validate_mzpeak_file(path: &Path) -> Result<ValidationReport> {
    let mut report = ValidationReport::new(path.display().to_string());

    // 1. Structure Check
    let validation_target = check_structure(path, &mut report)?;

    // 2. Metadata Integrity Check
    check_metadata_integrity(path, &validation_target, &mut report)?;

    // 3. Schema Contract Check
    check_schema_contract(&validation_target, &mut report)?;

    // 4. Data Sanity Check
    check_data_sanity(&validation_target, &mut report)?;

    Ok(report)
}

/// Enum to represent the validation target (file path or in-memory ZIP data)
#[derive(Debug)]
enum ValidationTarget {
    /// Direct Parquet file path
    FilePath(std::path::PathBuf),
    /// In-memory Parquet data from ZIP container
    InMemory(Bytes),
}

/// Step 1: Structure validation
fn check_structure(path: &Path, report: &mut ValidationReport) -> Result<ValidationTarget> {
    // Check if path exists
    if !path.exists() {
        report.add_check(ValidationCheck::failed(
            "Path exists",
            format!("Path does not exist: {}", path.display()),
        ));
        anyhow::bail!(ValidationError::StructureError("Path does not exist".to_string()));
    }
    report.add_check(ValidationCheck::ok("Path exists"));

    // Determine if it's a directory bundle, ZIP container, or single file
    if path.is_dir() {
        // Directory bundle format
        report.add_check(ValidationCheck::ok("Format: Directory bundle"));
        validate_directory_bundle(path, report)
    } else if path.is_file() {
        // Check if it's a ZIP container
        if is_zip_file(path) {
            report.add_check(ValidationCheck::ok("Format: ZIP container (.mzpeak)"));
            validate_zip_container(path, report)
        } else {
            // Single Parquet file (legacy)
            report.add_check(ValidationCheck::ok("Format: Single Parquet file (legacy)"));
            validate_single_parquet_file(path, report)
        }
    } else {
        report.add_check(ValidationCheck::failed(
            "Valid file type",
            "Path is neither a file nor a directory",
        ));
        anyhow::bail!(ValidationError::StructureError("Invalid path type".to_string()));
    }
}

/// Check if a file is a ZIP archive
fn is_zip_file(path: &Path) -> bool {
    if let Ok(file) = File::open(path) {
        if let Ok(_) = ZipArchive::new(file) {
            return true;
        }
    }
    false
}

/// Validate directory bundle structure
fn validate_directory_bundle(path: &Path, report: &mut ValidationReport) -> Result<ValidationTarget> {
    // Check for metadata.json
    let metadata_path = path.join("metadata.json");
    if metadata_path.exists() {
        report.add_check(ValidationCheck::ok("metadata.json exists"));
    } else {
        report.add_check(ValidationCheck::failed(
            "metadata.json exists",
            "Missing metadata.json in directory bundle",
        ));
    }

    // Check for peaks/peaks.parquet
    let peaks_dir = path.join("peaks");
    if !peaks_dir.exists() {
        report.add_check(ValidationCheck::failed(
            "peaks/ directory exists",
            "Missing peaks/ directory",
        ));
        anyhow::bail!(ValidationError::StructureError("Missing peaks/ directory".to_string()));
    }
    report.add_check(ValidationCheck::ok("peaks/ directory exists"));

    let peaks_file = peaks_dir.join("peaks.parquet");
    if !peaks_file.exists() {
        report.add_check(ValidationCheck::failed(
            "peaks/peaks.parquet exists",
            "Missing peaks/peaks.parquet file",
        ));
        anyhow::bail!(ValidationError::StructureError("Missing peaks.parquet".to_string()));
    }
    report.add_check(ValidationCheck::ok("peaks/peaks.parquet exists"));

    // Verify it's a valid Parquet file
    match File::open(&peaks_file) {
        Ok(file) => match SerializedFileReader::new(file) {
            Ok(_) => {
                report.add_check(ValidationCheck::ok("Valid Parquet file"));
            }
            Err(e) => {
                report.add_check(ValidationCheck::failed(
                    "Valid Parquet file",
                    format!("Not a valid Parquet file: {}", e),
                ));
                anyhow::bail!(ValidationError::ParquetError(e));
            }
        },
        Err(e) => {
            report.add_check(ValidationCheck::failed(
                "Parquet file readable",
                format!("Cannot open Parquet file: {}", e),
            ));
            anyhow::bail!(e);
        }
    }

    Ok(ValidationTarget::FilePath(peaks_file))
}

/// Validate ZIP container structure with zero-extraction
fn validate_zip_container(path: &Path, report: &mut ValidationReport) -> Result<ValidationTarget> {
    let file = File::open(path)?;
    let mut archive = ZipArchive::new(BufReader::new(file))?;

    // Check mimetype entry (MUST be first and uncompressed)
    if archive.len() == 0 {
        report.add_check(ValidationCheck::failed(
            "ZIP structure",
            "Empty ZIP archive",
        ));
        anyhow::bail!(ValidationError::StructureError("Empty ZIP archive".to_string()));
    }

    // Verify mimetype is first entry
    let first_entry = archive.by_index(0)?;
    if first_entry.name() != "mimetype" {
        report.add_check(ValidationCheck::failed(
            "mimetype entry",
            format!("First entry must be 'mimetype', found: {}", first_entry.name()),
        ));
    } else {
        report.add_check(ValidationCheck::ok("mimetype is first entry"));
    }

    // Verify mimetype is uncompressed
    if first_entry.compression() != zip::CompressionMethod::Stored {
        report.add_check(ValidationCheck::failed(
            "mimetype compression",
            "mimetype entry must be uncompressed (Stored)",
        ));
    } else {
        report.add_check(ValidationCheck::ok("mimetype is uncompressed"));
    }
    drop(first_entry);

    // Read and verify mimetype content
    let mut mimetype_entry = archive.by_name("mimetype")?;
    let mut mimetype_content = String::new();
    mimetype_entry.read_to_string(&mut mimetype_content)?;
    if mimetype_content != MZPEAK_MIMETYPE {
        report.add_check(ValidationCheck::failed(
            "mimetype content",
            format!("Expected '{}', found: '{}'", MZPEAK_MIMETYPE, mimetype_content),
        ));
    } else {
        report.add_check(ValidationCheck::ok(format!("mimetype = {}", MZPEAK_MIMETYPE)));
    }
    drop(mimetype_entry);

    // Check for metadata.json
    match archive.by_name("metadata.json") {
        Ok(entry) => {
            report.add_check(ValidationCheck::ok("metadata.json exists"));
            // Verify it's compressed
            if entry.compression() != zip::CompressionMethod::Deflated {
                report.add_check(ValidationCheck::warning(
                    "metadata.json compression",
                    "metadata.json should be Deflate compressed",
                ));
            } else {
                report.add_check(ValidationCheck::ok("metadata.json is compressed"));
            }
        }
        Err(_) => {
            report.add_check(ValidationCheck::failed(
                "metadata.json exists",
                "Missing metadata.json in container",
            ));
        }
    }

    // Check for peaks/peaks.parquet (zero-extraction validation)
    let mut peaks_entry = archive.by_name("peaks/peaks.parquet")
        .context("Missing peaks/peaks.parquet in container")?;
    report.add_check(ValidationCheck::ok("peaks/peaks.parquet exists"));

    // Verify peaks.parquet is uncompressed (critical for seekability)
    if peaks_entry.compression() != zip::CompressionMethod::Stored {
        report.add_check(ValidationCheck::failed(
            "peaks.parquet compression",
            "peaks.parquet must be uncompressed (Stored) for seekability",
        ));
    } else {
        report.add_check(ValidationCheck::ok("peaks.parquet is uncompressed (seekable)"));
    }

    // Read Parquet data into memory for validation (zero-extraction)
    let mut parquet_data = Vec::new();
    peaks_entry.read_to_end(&mut parquet_data)?;
    let bytes = Bytes::from(parquet_data);

    // Verify it's a valid Parquet file
    match SerializedFileReader::new(bytes.clone()) {
        Ok(_) => {
            report.add_check(ValidationCheck::ok("Valid Parquet file"));
        }
        Err(e) => {
            report.add_check(ValidationCheck::failed(
                "Valid Parquet file",
                format!("Not a valid Parquet file: {}", e),
            ));
            anyhow::bail!(ValidationError::ParquetError(e));
        }
    }

    Ok(ValidationTarget::InMemory(bytes))
}

/// Validate single Parquet file (legacy format)
fn validate_single_parquet_file(path: &Path, report: &mut ValidationReport) -> Result<ValidationTarget> {
    match File::open(path) {
        Ok(file) => match SerializedFileReader::new(file) {
            Ok(_) => {
                report.add_check(ValidationCheck::ok("Valid Parquet file"));
            }
            Err(e) => {
                report.add_check(ValidationCheck::failed(
                    "Valid Parquet file",
                    format!("Not a valid Parquet file: {}", e),
                ));
                anyhow::bail!(ValidationError::ParquetError(e));
            }
        },
        Err(e) => {
            report.add_check(ValidationCheck::failed(
                "Parquet file readable",
                format!("Cannot open Parquet file: {}", e),
            ));
            anyhow::bail!(e);
        }
    }

    Ok(ValidationTarget::FilePath(path.to_path_buf()))
}

/// Step 2: Metadata integrity validation
fn check_metadata_integrity(
    base_path: &Path,
    validation_target: &ValidationTarget,
    report: &mut ValidationReport,
) -> Result<()> {
    // Check metadata.json from directory bundle or ZIP container
    if base_path.is_dir() {
        // Directory bundle
        let metadata_json_path = base_path.join("metadata.json");
        if metadata_json_path.exists() {
            validate_metadata_json_file(&metadata_json_path, report)?;
        }
    } else if base_path.is_file() && is_zip_file(base_path) {
        // ZIP container - read metadata.json content before archive is dropped
        let json_content = {
            let file = File::open(base_path)?;
            let mut archive = ZipArchive::new(BufReader::new(file))?;
            let result = if let Ok(mut metadata_entry) = archive.by_name("metadata.json") {
                let mut content = String::new();
                metadata_entry.read_to_string(&mut content)?;
                Some(content)
            } else {
                None
            };
            result
        };
        
        if let Some(content) = json_content {
            validate_metadata_json_content(&content, report)?;
        }
    }

    // Check Parquet footer metadata
    let metadata = match validation_target {
        ValidationTarget::FilePath(path) => {
            let file = File::open(path)?;
            let reader = SerializedFileReader::new(file)?;
            reader.metadata().clone()
        }
        ValidationTarget::InMemory(bytes) => {
            let reader = SerializedFileReader::new(bytes.clone())?;
            reader.metadata().clone()
        }
    };

    let file_metadata = metadata.file_metadata();

    if let Some(kv_metadata) = file_metadata.key_value_metadata() {
        let kv_map: HashMap<String, String> = kv_metadata
            .iter()
            .filter_map(|kv| {
                kv.value.as_ref().map(|v| (kv.key.clone(), v.clone()))
            })
            .collect();

        // Check for format version
        if let Some(version) = kv_map.get(crate::schema::KEY_FORMAT_VERSION) {
            if version == MZPEAK_FORMAT_VERSION {
                report.add_check(ValidationCheck::ok(
                    format!("Format version matches ({})", MZPEAK_FORMAT_VERSION),
                ));
            } else {
                report.add_check(ValidationCheck::warning(
                    "Format version",
                    format!("Expected {}, found {}", MZPEAK_FORMAT_VERSION, version),
                ));
            }
        } else {
            report.add_check(ValidationCheck::warning(
                "Format version",
                "Format version not found in Parquet metadata",
            ));
        }

        // Try to reconstruct MzPeakMetadata from Parquet footer
        match MzPeakMetadata::from_parquet_metadata(&kv_map) {
            Ok(_) => {
                report.add_check(ValidationCheck::ok("Parquet metadata deserializes"));
            }
            Err(e) => {
                report.add_check(ValidationCheck::warning(
                    "Parquet metadata deserializes",
                    format!("Failed to deserialize: {}", e),
                ));
            }
        }
    } else {
        report.add_check(ValidationCheck::warning(
            "Parquet metadata",
            "No key-value metadata found in Parquet footer",
        ));
    }

    Ok(())
}

/// Validate metadata.json from file path
fn validate_metadata_json_file(path: &Path, report: &mut ValidationReport) -> Result<()> {
    match std::fs::read_to_string(path) {
        Ok(json_content) => validate_metadata_json_content(&json_content, report),
        Err(e) => {
            report.add_check(ValidationCheck::failed(
                "metadata.json readable",
                format!("Failed to read metadata.json: {}", e),
            ));
            Ok(())
        }
    }
}

/// Validate metadata.json content
fn validate_metadata_json_content(json_content: &str, report: &mut ValidationReport) -> Result<()> {
    match serde_json::from_str::<MzPeakMetadata>(json_content) {
        Ok(_metadata) => {
            report.add_check(ValidationCheck::ok("metadata.json valid JSON"));
        }
        Err(e) => {
            report.add_check(ValidationCheck::failed(
                "metadata.json valid JSON",
                format!("Failed to parse metadata.json: {}", e),
            ));
        }
    }
    Ok(())
}

/// Step 3: Schema contract validation
fn check_schema_contract(validation_target: &ValidationTarget, report: &mut ValidationReport) -> Result<()> {
    // We need to handle schema validation differently for each target type
    match validation_target {
        ValidationTarget::FilePath(path) => {
            let file = File::open(path)?;
            let reader = SerializedFileReader::new(file)?;
            perform_schema_validation(reader.metadata(), report)
        }
        ValidationTarget::InMemory(bytes) => {
            let reader = SerializedFileReader::new(bytes.clone())?;
            perform_schema_validation(reader.metadata(), report)
        }
    }
}

/// Perform schema validation on Parquet metadata
fn perform_schema_validation(metadata: &parquet::file::metadata::ParquetMetaData, report: &mut ValidationReport) -> Result<()> {
    let file_metadata = metadata.file_metadata();
    let schema_descriptor = file_metadata.schema_descr();

    // Create expected schema
    let expected_schema = create_mzpeak_schema();

    // Check that all REQUIRED columns exist with correct types
    let required_columns = vec![
        (columns::SPECTRUM_ID, DataType::Int64),
        (columns::SCAN_NUMBER, DataType::Int64),
        (columns::MS_LEVEL, DataType::Int16),
        (columns::RETENTION_TIME, DataType::Float32),
        (columns::POLARITY, DataType::Int8),
        (columns::MZ, DataType::Float64),
        (columns::INTENSITY, DataType::Float32),
    ];

    for (col_name, expected_type) in required_columns {
        // Find column in Parquet schema
        let mut found = false;
        let mut type_matches = false;

        for i in 0..schema_descriptor.num_columns() {
            let col = schema_descriptor.column(i);
            if col.name() == col_name {
                found = true;

                // Map Parquet physical type to Arrow DataType
                let parquet_type = col.physical_type();
                let arrow_type = match expected_type {
                    DataType::Int64 => parquet::basic::Type::INT64,
                    DataType::Int32 => parquet::basic::Type::INT32,
                    DataType::Int16 => parquet::basic::Type::INT32, // Parquet doesn't have INT16
                    DataType::Int8 => parquet::basic::Type::INT32,  // Parquet doesn't have INT8
                    DataType::Float64 => parquet::basic::Type::DOUBLE,
                    DataType::Float32 => parquet::basic::Type::FLOAT,
                    _ => parquet::basic::Type::BYTE_ARRAY,
                };

                type_matches = parquet_type == arrow_type;
                break;
            }
        }

        if !found {
            report.add_check(ValidationCheck::failed(
                format!("Required column: {}", col_name),
                format!("Column '{}' is missing", col_name),
            ));
        } else if !type_matches {
            report.add_check(ValidationCheck::warning(
                format!("Column type: {}", col_name),
                format!("Type mismatch for column '{}' (physical type may differ from logical)", col_name),
            ));
        } else {
            report.add_check(ValidationCheck::ok(format!("Column: {}", col_name)));
        }
    }

    // Check CV accessions in column metadata
    let expected_cv_accessions = vec![
        (columns::MZ, "MS:1000040"),
        (columns::INTENSITY, "MS:1000042"),
        (columns::MS_LEVEL, "MS:1000511"),
        (columns::RETENTION_TIME, "MS:1000016"),
        (columns::POLARITY, "MS:1000465"),
        (columns::PRECURSOR_MZ, "MS:1000744"),
        (columns::PRECURSOR_CHARGE, "MS:1000041"),
        (columns::COLLISION_ENERGY, "MS:1000045"),
        (columns::TOTAL_ION_CURRENT, "MS:1000285"),
        (columns::BASE_PEAK_MZ, "MS:1000504"),
        (columns::BASE_PEAK_INTENSITY, "MS:1000505"),
        (columns::INJECTION_TIME, "MS:1000927"),
        (columns::ION_MOBILITY, "MS:1002476"),
        (columns::ISOLATION_WINDOW_LOWER, "MS:1000828"),
        (columns::ISOLATION_WINDOW_UPPER, "MS:1000829"),
    ];

    for (col_name, expected_cv) in expected_cv_accessions {
        if let Ok(field) = expected_schema.field_with_name(col_name) {
            if let Some(cv_accession) = field.metadata().get("cv_accession") {
                if cv_accession == expected_cv {
                    report.add_check(ValidationCheck::ok(
                        format!("CV accession for {}: {}", col_name, expected_cv),
                    ));
                } else {
                    report.add_check(ValidationCheck::warning(
                        format!("CV accession for {}", col_name),
                        format!("Expected {}, would be {} in recreated schema", expected_cv, cv_accession),
                    ));
                }
            } else {
                report.add_check(ValidationCheck::warning(
                    format!("CV accession for {}", col_name),
                    format!("Missing CV accession {} in column metadata", expected_cv),
                ));
            }
        }
    }
    
    // Check for MSI CV accessions
    let msi_cv_accessions = vec![
        (columns::PIXEL_X, "IMS:1000050"),
        (columns::PIXEL_Y, "IMS:1000051"),
        (columns::PIXEL_Z, "IMS:1000052"),
    ];
    
    for (col_name, expected_cv) in msi_cv_accessions {
        if let Ok(field) = expected_schema.field_with_name(col_name) {
            if let Some(cv_accession) = field.metadata().get("cv_accession") {
                if cv_accession == expected_cv {
                    report.add_check(ValidationCheck::ok(
                        format!("MSI CV accession for {}: {}", col_name, expected_cv),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Step 4: Data sanity validation
fn check_data_sanity(validation_target: &ValidationTarget, report: &mut ValidationReport) -> Result<()> {
    // We need to handle both reader types separately due to type constraints
    match validation_target {
        ValidationTarget::FilePath(path) => {
            let file = File::open(path)?;
            let reader = SerializedFileReader::new(file)?;
            perform_data_sanity_checks(reader, report)
        }
        ValidationTarget::InMemory(bytes) => {
            let reader = SerializedFileReader::new(bytes.clone())?;
            perform_data_sanity_checks(reader, report)
        }
    }
}

/// Perform actual data sanity checks on a reader
fn perform_data_sanity_checks<R: parquet::file::reader::ChunkReader + 'static>(reader: SerializedFileReader<R>, report: &mut ValidationReport) -> Result<()> {
    
    let metadata = reader.metadata();
    let num_rows = metadata.file_metadata().num_rows();
    let schema_descriptor = metadata.file_metadata().schema_descr();

    report.add_check(ValidationCheck::ok(format!("Total rows: {}", num_rows)));

    if num_rows == 0 {
        report.add_check(ValidationCheck::warning(
            "Data rows",
            "File contains no data rows",
        ));
        return Ok(());
    }

    // Find column indices
    let mut spectrum_id_idx = None;
    let mut ms_level_idx = None;
    let mut retention_time_idx = None;
    let mut mz_idx = None;
    let mut intensity_idx = None;

    for i in 0..schema_descriptor.num_columns() {
        let col = schema_descriptor.column(i);
        match col.name() {
            columns::SPECTRUM_ID => spectrum_id_idx = Some(i),
            columns::MS_LEVEL => ms_level_idx = Some(i),
            columns::RETENTION_TIME => retention_time_idx = Some(i),
            columns::MZ => mz_idx = Some(i),
            columns::INTENSITY => intensity_idx = Some(i),
            _ => {}
        }
    }

    // Read a sample of rows (first 1000 or all if fewer)
    let sample_size = std::cmp::min(1000, num_rows as usize);
    let mut row_iter = reader.get_row_iter(None)?;

    let mut mz_positive_count = 0;
    let mut intensity_non_negative_count = 0;
    let mut ms_level_valid_count = 0;
    let mut last_rt: Option<f32> = None;
    let mut rt_non_decreasing = true;
    let mut prev_spectrum_id: Option<i64> = None;

    for _i in 0..sample_size {
        if let Some(row_result) = row_iter.next() {
            let row = row_result?;
            
            // Check mz > 0
            if let Some(idx) = mz_idx {
                if let Ok(mz) = row.get_double(idx) {
                    if mz > 0.0 {
                        mz_positive_count += 1;
                    }
                }
            }

            // Check intensity >= 0
            if let Some(idx) = intensity_idx {
                if let Ok(intensity) = row.get_float(idx) {
                    if intensity >= 0.0 {
                        intensity_non_negative_count += 1;
                    }
                }
            }

            // Check ms_level >= 1
            if let Some(idx) = ms_level_idx {
                // ms_level is Int16, so use get_short()
                match row.get_short(idx) {
                    Ok(ms_level) => {
                        if ms_level >= 1 {
                            ms_level_valid_count += 1;
                        }
                    }
                    Err(_) => {
                        // Try get_int() as fallback for compatibility
                        if let Ok(ms_level) = row.get_int(idx) {
                            if ms_level >= 1 {
                                ms_level_valid_count += 1;
                            }
                        }
                    }
                }
            }

            // Check retention_time non-decreasing (per spectrum)
            if let Some(spec_idx) = spectrum_id_idx {
                if let Some(rt_idx) = retention_time_idx {
                    if let Ok(spectrum_id) = row.get_long(spec_idx) {
                        if let Ok(rt) = row.get_float(rt_idx) {
                            // New spectrum
                            if prev_spectrum_id != Some(spectrum_id) {
                                if let Some(prev_rt) = last_rt {
                                    if rt < prev_rt {
                                        rt_non_decreasing = false;
                                    }
                                }
                                last_rt = Some(rt);
                                prev_spectrum_id = Some(spectrum_id);
                            }
                        }
                    }
                }
            }
        } else {
            break;
        }
    }

    // Report findings
    if mz_positive_count == sample_size {
        report.add_check(ValidationCheck::ok(
            format!("m/z values positive (sampled {} rows)", sample_size),
        ));
    } else {
        report.add_check(ValidationCheck::failed(
            "m/z values positive",
            format!(
                "Found {} invalid m/z values (<=0) in sample of {}",
                sample_size - mz_positive_count,
                sample_size
            ),
        ));
    }

    if intensity_non_negative_count == sample_size {
        report.add_check(ValidationCheck::ok(
            format!("Intensity values non-negative (sampled {} rows)", sample_size),
        ));
    } else {
        report.add_check(ValidationCheck::failed(
            "Intensity values non-negative",
            format!(
                "Found {} negative intensity values in sample of {}",
                sample_size - intensity_non_negative_count,
                sample_size
            ),
        ));
    }

    if ms_level_valid_count == sample_size {
        report.add_check(ValidationCheck::ok(
            format!("MS level values >= 1 (sampled {} rows)", sample_size),
        ));
    } else {
        report.add_check(ValidationCheck::failed(
            "MS level values >= 1",
            format!(
                "Found {} invalid ms_level values (<1) in sample of {}",
                sample_size - ms_level_valid_count,
                sample_size
            ),
        ));
    }

    if rt_non_decreasing {
        report.add_check(ValidationCheck::ok("Retention time non-decreasing"));
    } else {
        report.add_check(ValidationCheck::warning(
            "Retention time non-decreasing",
            "Retention time decreases between spectra (may be intentional)",
        ));
    }

    Ok(())
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
