use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bytes::Bytes;
use parquet::file::reader::SerializedFileReader;
use zip::ZipArchive;

use crate::schema::MZPEAK_MIMETYPE;

use super::{ValidationCheck, ValidationError, ValidationReport, ValidationTarget};

/// Step 1: Structure validation
pub(crate) fn check_structure(path: &Path, report: &mut ValidationReport) -> Result<ValidationTarget> {
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
pub(crate) fn is_zip_file(path: &Path) -> bool {
    if let Ok(file) = File::open(path) {
        if ZipArchive::new(file).is_ok() {
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
    let mut peaks_entry = archive
        .by_name("peaks/peaks.parquet")
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

    Ok(ValidationTarget::FilePath(PathBuf::from(path)))
}
