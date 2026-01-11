use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::Result;
use parquet::file::reader::{FileReader, SerializedFileReader};
use zip::ZipArchive;

use crate::metadata::MzPeakMetadata;
use crate::schema::{KEY_FORMAT_VERSION, MZPEAK_FORMAT_VERSION};

use super::structure::is_zip_file;
use super::{ValidationCheck, ValidationReport, ValidationTarget};

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
            .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.clone(), v.clone())))
            .collect();

        // Check for format version
        if let Some(version) = kv_map.get(KEY_FORMAT_VERSION) {
            if version == MZPEAK_FORMAT_VERSION {
                report.add_check(ValidationCheck::ok(format!(
                    "Format version matches ({})",
                    MZPEAK_FORMAT_VERSION
                )));
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
