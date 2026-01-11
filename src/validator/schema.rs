use std::fs::File;

use anyhow::Result;
use arrow::datatypes::DataType;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::schema::{columns, create_mzpeak_schema};

use super::{ValidationCheck, ValidationReport, ValidationTarget};

/// Step 3: Schema contract validation
pub(crate) fn check_schema_contract(
    validation_target: &ValidationTarget,
    report: &mut ValidationReport,
) -> Result<()> {
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
fn perform_schema_validation(
    metadata: &parquet::file::metadata::ParquetMetaData,
    report: &mut ValidationReport,
) -> Result<()> {
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
                    DataType::Int16 => parquet::basic::Type::INT32,
                    DataType::Int8 => parquet::basic::Type::INT32,
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
                format!(
                    "Type mismatch for column '{}' (physical type may differ from logical)",
                    col_name
                ),
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
                    report.add_check(ValidationCheck::ok(format!(
                        "CV accession for {}: {}",
                        col_name, expected_cv
                    )));
                } else {
                    report.add_check(ValidationCheck::warning(
                        format!("CV accession for {}", col_name),
                        format!(
                            "Expected {}, would be {} in recreated schema",
                            expected_cv, cv_accession
                        ),
                    ));
                }
            } else {
                report.add_check(ValidationCheck::warning(
                    format!("CV accession for {}", col_name),
                    format!(
                        "Missing CV accession {} in column metadata",
                        expected_cv
                    ),
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
                    report.add_check(ValidationCheck::ok(format!(
                        "MSI CV accession for {}: {}",
                        col_name, expected_cv
                    )));
                }
            }
        }
    }

    Ok(())
}
