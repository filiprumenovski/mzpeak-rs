use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use parquet::file::reader::SerializedFileReader;
use zip::ZipArchive;

use crate::dataset::MZPEAK_V2_MIMETYPE;
use crate::reader::ZipEntryChunkReader;
use crate::schema::MZPEAK_MIMETYPE;

use super::{ParquetSource, SchemaVersion, ValidationCheck, ValidationError, ValidationReport, ValidationTarget};

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
    // Detect schema version by checking for manifest.json (v2.0) or metadata.json only (v1.0)
    let manifest_path = path.join("manifest.json");
    let metadata_path = path.join("metadata.json");
    let spectra_dir = path.join("spectra");

    let (schema_version, manifest_content) = if manifest_path.exists() {
        report.add_check(ValidationCheck::ok("manifest.json exists (v2.0 format)"));
        let content = std::fs::read_to_string(&manifest_path).ok();
        (SchemaVersion::V2, content)
    } else {
        report.add_check(ValidationCheck::ok("No manifest.json (v1.0 format)"));
        (SchemaVersion::V1, None)
    };

    // Check for metadata.json (required for both versions)
    if metadata_path.exists() {
        report.add_check(ValidationCheck::ok("metadata.json exists"));
    } else {
        report.add_check(ValidationCheck::failed(
            "metadata.json exists",
            "Missing metadata.json in directory bundle",
        ));
    }

    // V2.0 specific: check for spectra/ directory
    let mut spectra_file = None;
    if schema_version == SchemaVersion::V2 {
        if spectra_dir.exists() {
            report.add_check(ValidationCheck::ok("spectra/ directory exists"));

            let spectra_path = spectra_dir.join("spectra.parquet");
            if spectra_path.exists() {
                report.add_check(ValidationCheck::ok("spectra/spectra.parquet exists"));
                spectra_file = Some(spectra_path.clone());

                // Verify it's a valid Parquet file
                match File::open(&spectra_path) {
                    Ok(file) => match SerializedFileReader::new(file) {
                        Ok(_) => {
                            report.add_check(ValidationCheck::ok("spectra.parquet is valid Parquet"));
                        }
                        Err(e) => {
                            report.add_check(ValidationCheck::failed(
                                "Valid spectra.parquet",
                                format!("Not a valid Parquet file: {}", e),
                            ));
                        }
                    },
                    Err(e) => {
                        report.add_check(ValidationCheck::failed(
                            "spectra.parquet readable",
                            format!("Cannot open spectra.parquet: {}", e),
                        ));
                    }
                }
            } else {
                report.add_check(ValidationCheck::failed(
                    "spectra/spectra.parquet exists",
                    "Missing spectra/spectra.parquet file",
                ));
            }
        } else {
            report.add_check(ValidationCheck::failed(
                "spectra/ directory exists",
                "Missing spectra/ directory for v2.0 format",
            ));
        }
    }

    // Check for peaks/peaks.parquet (required for both versions)
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
                report.add_check(ValidationCheck::ok("peaks.parquet is valid Parquet"));
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

    Ok(ValidationTarget {
        schema_version,
        peaks: ParquetSource::FilePath(peaks_file),
        spectra: spectra_file.map(ParquetSource::FilePath),
        manifest: manifest_content,
    })
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
    let schema_version = if mimetype_content == MZPEAK_MIMETYPE {
        report.add_check(ValidationCheck::ok(format!("mimetype = {}", MZPEAK_MIMETYPE)));
        SchemaVersion::V1
    } else if mimetype_content == MZPEAK_V2_MIMETYPE {
        report.add_check(ValidationCheck::ok(format!("mimetype = {}", MZPEAK_V2_MIMETYPE)));
        SchemaVersion::V2
    } else {
        report.add_check(ValidationCheck::failed(
            "mimetype content",
            format!(
                "Expected '{}' or '{}', found: '{}'",
                MZPEAK_MIMETYPE, MZPEAK_V2_MIMETYPE, mimetype_content
            ),
        ));
        SchemaVersion::V1
    };
    drop(mimetype_entry);

    // Detect schema version by checking for manifest.json
    let (manifest_content, manifest_present) = match archive.by_name("manifest.json") {
        Ok(mut entry) => {
            report.add_check(ValidationCheck::ok("manifest.json exists (v2.0 format)"));
            // Verify it's compressed
            if entry.compression() != zip::CompressionMethod::Deflated {
                report.add_check(ValidationCheck::warning(
                    "manifest.json compression",
                    "manifest.json should be Deflate compressed",
                ));
            } else {
                report.add_check(ValidationCheck::ok("manifest.json is compressed"));
            }
            let mut content = String::new();
            entry.read_to_string(&mut content)?;
            (Some(content), true)
        }
        Err(_) => {
            report.add_check(ValidationCheck::ok("No manifest.json (v1.0 format)"));
            (None, false)
        }
    };

    if manifest_present && schema_version != SchemaVersion::V2 {
        report.add_check(ValidationCheck::failed(
            "manifest/mimetype mismatch",
            "manifest.json present but mimetype is not v2",
        ));
    }
    if !manifest_present && schema_version == SchemaVersion::V2 {
        report.add_check(ValidationCheck::failed(
            "manifest.json exists",
            "mimetype indicates v2 but manifest.json is missing",
        ));
    }

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

    // V2.0 specific: check for spectra/spectra.parquet
    let mut spectra_source = None;
    if schema_version == SchemaVersion::V2 {
        match archive.by_name("spectra/spectra.parquet") {
            Ok(entry) => {
                report.add_check(ValidationCheck::ok("spectra/spectra.parquet exists"));
                // Verify it's uncompressed for seekability
                if entry.compression() != zip::CompressionMethod::Stored {
                    report.add_check(ValidationCheck::failed(
                        "spectra.parquet compression",
                        "spectra.parquet must be uncompressed (Stored) for seekability",
                    ));
                } else {
                    report.add_check(ValidationCheck::ok("spectra.parquet is uncompressed (seekable)"));
                }
                spectra_source = Some(ParquetSource::ZipEntry {
                    zip_path: path.to_path_buf(),
                    entry_name: "spectra/spectra.parquet".to_string(),
                });

                match ZipEntryChunkReader::new(path, "spectra/spectra.parquet") {
                    Ok(reader) => match SerializedFileReader::new(reader) {
                        Ok(_) => {
                            report.add_check(ValidationCheck::ok("spectra.parquet is valid Parquet"));
                        }
                        Err(e) => {
                            report.add_check(ValidationCheck::failed(
                                "Valid spectra.parquet",
                                format!("Not a valid Parquet file: {}", e),
                            ));
                        }
                    },
                    Err(e) => {
                        report.add_check(ValidationCheck::failed(
                            "spectra.parquet readable",
                            format!("Cannot open spectra.parquet: {}", e),
                        ));
                    }
                }
            }
            Err(_) => {
                report.add_check(ValidationCheck::failed(
                    "spectra/spectra.parquet exists",
                    "Missing spectra/spectra.parquet in v2.0 container",
                ));
            }
        }
    }

    // Check for peaks/peaks.parquet (zero-extraction validation)
    let peaks_entry = archive
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

    // Verify it's a valid Parquet file
    match ZipEntryChunkReader::new(path, "peaks/peaks.parquet") {
        Ok(reader) => match SerializedFileReader::new(reader) {
            Ok(_) => {
                report.add_check(ValidationCheck::ok("peaks.parquet is valid Parquet"));
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
            anyhow::bail!(ValidationError::StructureError(e.to_string()));
        }
    }

    Ok(ValidationTarget {
        schema_version,
        peaks: ParquetSource::ZipEntry {
            zip_path: path.to_path_buf(),
            entry_name: "peaks/peaks.parquet".to_string(),
        },
        spectra: spectra_source,
        manifest: manifest_content,
    })
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

    Ok(ValidationTarget {
        schema_version: SchemaVersion::V1,
        peaks: ParquetSource::FilePath(PathBuf::from(path)),
        spectra: None,
        manifest: None,
    })
}
