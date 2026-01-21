use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};
use zip::ZipArchive;

use crate::metadata::MzPeakMetadata;
use crate::reader::ZipEntryChunkReader;
use crate::schema::{KEY_FORMAT_VERSION, MZPEAK_FORMAT_VERSION};
use crate::schema::manifest::Manifest;

use super::structure::is_zip_file;
use super::{ParquetSource, SchemaVersion, ValidationCheck, ValidationReport, ValidationTarget};

/// Step 2: Metadata integrity validation
pub(crate) fn check_metadata_integrity(
    base_path: &Path,
    validation_target: &ValidationTarget,
    report: &mut ValidationReport,
) -> Result<()> {
    // Check metadata.json from directory bundle or ZIP container
    if base_path.is_dir() {
        let metadata_json_path = base_path.join("metadata.json");
        if metadata_json_path.exists() {
            validate_metadata_json_file(&metadata_json_path, report)?;
        }
    } else if base_path.is_file() && is_zip_file(base_path) {
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
    if validation_target.schema_version == SchemaVersion::V2 {
        match validation_target.manifest.as_deref() {
            Some(content) => match serde_json::from_str::<Manifest>(content) {
                Ok(manifest) => {
                    if manifest.format_version == "2.0" {
                        report.add_check(ValidationCheck::ok("Manifest format version = 2.0"));
                    } else {
                        report.add_check(ValidationCheck::warning(
                            "Manifest format version",
                            format!("Expected 2.0, found {}", manifest.format_version),
                        ));
                    }
                    if manifest.schema_version == "2.0" {
                        report.add_check(ValidationCheck::ok("Manifest schema version = 2.0"));
                    } else {
                        report.add_check(ValidationCheck::warning(
                            "Manifest schema version",
                            format!("Expected 2.0, found {}", manifest.schema_version),
                        ));
                    }
                }
                Err(e) => {
                    report.add_check(ValidationCheck::failed(
                        "manifest.json valid JSON",
                        format!("Failed to parse manifest.json: {}", e),
                    ));
                }
            },
            None => {
                report.add_check(ValidationCheck::failed(
                    "manifest.json exists",
                    "Missing manifest.json content for v2 container",
                ));
            }
        }
    }

    let kv_map = match read_parquet_kv_metadata(&validation_target.peaks) {
        Ok(Some(map)) => Some(map),
        Ok(None) => None,
        Err(_) => None,
    };

    let kv_map = if kv_map.is_none() && validation_target.schema_version == SchemaVersion::V2 {
        if let Some(spectra_source) = &validation_target.spectra {
            read_parquet_kv_metadata(spectra_source).ok().flatten()
        } else {
            None
        }
    } else {
        kv_map
    };

    if let Some(kv_map) = kv_map {
        if let Some(version) = kv_map.get(KEY_FORMAT_VERSION) {
            let expected = if validation_target.schema_version == SchemaVersion::V2 {
                "2.0"
            } else {
                MZPEAK_FORMAT_VERSION
            };
            if version == expected {
                report.add_check(ValidationCheck::ok(format!(
                    "Format version matches ({})",
                    expected
                )));
            } else {
                report.add_check(ValidationCheck::warning(
                    "Format version",
                    format!("Expected {}, found {}", expected, version),
                ));
            }
        } else {
            report.add_check(ValidationCheck::warning(
                "Format version",
                "Format version not found in Parquet metadata",
            ));
        }

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

fn read_parquet_kv_metadata(
    source: &ParquetSource,
) -> Result<Option<HashMap<String, String>>> {
    let metadata = match source {
        ParquetSource::FilePath(path) => {
            let file = File::open(path)?;
            let reader = SerializedFileReader::new(file)?;
            reader.metadata().clone()
        }
        ParquetSource::ZipEntry { zip_path, entry_name } => {
            let reader = ZipEntryChunkReader::new(zip_path, entry_name)?;
            let reader = SerializedFileReader::new(reader)?;
            reader.metadata().clone()
        }
        ParquetSource::InMemory(bytes) => {
            let reader = SerializedFileReader::new(bytes.clone())?;
            reader.metadata().clone()
        }
    };

    let file_metadata = metadata.file_metadata();
    let Some(kv_metadata) = file_metadata.key_value_metadata() else {
        return Ok(None);
    };

    let kv_map = kv_metadata
        .iter()
        .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.clone(), v.clone())))
        .collect();
    Ok(Some(kv_map))
}
