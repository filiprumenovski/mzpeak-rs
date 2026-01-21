use std::fs::File;

use anyhow::Result;
use arrow::datatypes::DataType;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::reader::ZipEntryChunkReader;
use crate::schema::{columns, create_mzpeak_schema, create_peaks_schema_v2, spectra_columns};

use super::{ParquetSource, SchemaVersion, ValidationCheck, ValidationReport, ValidationTarget};

/// Step 3: Schema contract validation
pub(crate) fn check_schema_contract(
    validation_target: &ValidationTarget,
    report: &mut ValidationReport,
) -> Result<()> {
    match validation_target.schema_version {
        SchemaVersion::V1 => {
            let metadata = read_parquet_metadata(&validation_target.peaks)?;
            perform_schema_validation(&metadata, report)
        }
        SchemaVersion::V2 => {
            let metadata = read_parquet_metadata(&validation_target.peaks)?;
            perform_peaks_v2_schema_validation(&metadata, report)?;

            if let Some(spectra_source) = &validation_target.spectra {
                let spectra_metadata = read_parquet_metadata(spectra_source)?;
                perform_spectra_v2_schema_validation(&spectra_metadata, report)?;
            } else {
                report.add_check(ValidationCheck::failed(
                    "spectra.parquet available",
                    "Missing spectra.parquet for v2 validation",
                ));
            }
            Ok(())
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

// =============================================================================
// V2.0 Schema Validation
// =============================================================================

/// Check if the schema matches v2.0 peaks table format.
///
/// V2.0 peaks table has 3-4 columns:
/// - spectrum_id (UInt32)
/// - mz (Float64)
/// - intensity (Float32)
/// - ion_mobility (Float64, optional)
pub(crate) fn check_peaks_v2_schema_contract(
    validation_target: &ValidationTarget,
    report: &mut ValidationReport,
) -> Result<()> {
    let metadata = read_parquet_metadata(&validation_target.peaks)?;
    perform_peaks_v2_schema_validation(&metadata, report)
}

/// Perform v2.0 peaks schema validation
fn perform_peaks_v2_schema_validation(
    metadata: &parquet::file::metadata::ParquetMetaData,
    report: &mut ValidationReport,
) -> Result<()> {
    let schema_descriptor = metadata.file_metadata().schema_descr();

    // V2.0 peaks table required columns with new types
    let required_columns = vec![
        ("spectrum_id", parquet::basic::Type::INT32), // UInt32 stored as INT32
        ("mz", parquet::basic::Type::DOUBLE),
        ("intensity", parquet::basic::Type::FLOAT),
    ];

    for (col_name, expected_type) in required_columns {
        let mut found = false;
        let mut type_matches = false;

        for i in 0..schema_descriptor.num_columns() {
            let col = schema_descriptor.column(i);
            if col.name() == col_name {
                found = true;
                type_matches = col.physical_type() == expected_type;
                break;
            }
        }

        if !found {
            report.add_check(ValidationCheck::failed(
                format!("V2 Peaks column: {}", col_name),
                format!("Required column '{}' is missing", col_name),
            ));
        } else if !type_matches {
            report.add_check(ValidationCheck::warning(
                format!("V2 Peaks column type: {}", col_name),
                format!("Type mismatch for column '{}'", col_name),
            ));
        } else {
            report.add_check(ValidationCheck::ok(format!("V2 Peaks column: {}", col_name)));
        }
    }

    // Check for optional ion_mobility column
    let mut has_ion_mobility = false;
    for i in 0..schema_descriptor.num_columns() {
        let col = schema_descriptor.column(i);
        if col.name() == "ion_mobility" {
            has_ion_mobility = true;
            if col.physical_type() == parquet::basic::Type::DOUBLE {
                report.add_check(ValidationCheck::ok("V2 Peaks column: ion_mobility (4D data)"));
            } else {
                report.add_check(ValidationCheck::warning(
                    "V2 Peaks column type: ion_mobility",
                    "Expected DOUBLE type for ion_mobility",
                ));
            }
            break;
        }
    }

    if !has_ion_mobility {
        report.add_check(ValidationCheck::ok("V2 Peaks: 3D data (no ion_mobility column)"));
    }

    let expected_schema = create_peaks_schema_v2(has_ion_mobility);

    for col_name in ["mz", "intensity", "ion_mobility", "spectrum_id"] {
        if let Ok(field) = expected_schema.field_with_name(col_name) {
            if let Some(cv_accession) = field.metadata().get("cv_accession") {
                report.add_check(ValidationCheck::ok(format!(
                    "V2 CV accession for {}: {}",
                    col_name, cv_accession
                )));
            }
        }
    }

    Ok(())
}

fn perform_spectra_v2_schema_validation(
    metadata: &parquet::file::metadata::ParquetMetaData,
    report: &mut ValidationReport,
) -> Result<()> {
    let schema_descriptor = metadata.file_metadata().schema_descr();
    let expected_schema = spectra_columns::create_spectra_schema();

    let required_columns = vec![
        (spectra_columns::SPECTRUM_ID, DataType::UInt32),
        (spectra_columns::MS_LEVEL, DataType::UInt8),
        (spectra_columns::RETENTION_TIME, DataType::Float32),
        (spectra_columns::POLARITY, DataType::Int8),
        (spectra_columns::PEAK_OFFSET, DataType::UInt64),
        (spectra_columns::PEAK_COUNT, DataType::UInt32),
    ];

    for (col_name, expected_type) in required_columns {
        let mut found = false;
        let mut type_matches = false;

        for i in 0..schema_descriptor.num_columns() {
            let col = schema_descriptor.column(i);
            if col.name() == col_name {
                found = true;
                let parquet_type = col.physical_type();
                let arrow_type = match expected_type {
                    DataType::UInt32 => parquet::basic::Type::INT32,
                    DataType::UInt8 => parquet::basic::Type::INT32,
                    DataType::Int8 => parquet::basic::Type::INT32,
                    DataType::UInt64 => parquet::basic::Type::INT64,
                    DataType::Float32 => parquet::basic::Type::FLOAT,
                    _ => parquet::basic::Type::BYTE_ARRAY,
                };
                type_matches = parquet_type == arrow_type;
                break;
            }
        }

        if !found {
            report.add_check(ValidationCheck::failed(
                format!("V2 Spectra column: {}", col_name),
                format!("Required column '{}' is missing", col_name),
            ));
        } else if !type_matches {
            report.add_check(ValidationCheck::warning(
                format!("V2 Spectra column type: {}", col_name),
                format!("Type mismatch for column '{}'", col_name),
            ));
        } else {
            report.add_check(ValidationCheck::ok(format!("V2 Spectra column: {}", col_name)));
        }
    }

    for col_name in [
        spectra_columns::SPECTRUM_ID,
        spectra_columns::SCAN_NUMBER,
        spectra_columns::MS_LEVEL,
        spectra_columns::RETENTION_TIME,
        spectra_columns::POLARITY,
        spectra_columns::PRECURSOR_MZ,
        spectra_columns::PRECURSOR_CHARGE,
        spectra_columns::COLLISION_ENERGY,
        spectra_columns::TOTAL_ION_CURRENT,
        spectra_columns::BASE_PEAK_MZ,
        spectra_columns::BASE_PEAK_INTENSITY,
        spectra_columns::INJECTION_TIME,
        spectra_columns::PIXEL_X,
        spectra_columns::PIXEL_Y,
        spectra_columns::PIXEL_Z,
    ] {
        if let Ok(field) = expected_schema.field_with_name(col_name) {
            if let Some(cv_accession) = field.metadata().get("cv_accession") {
                report.add_check(ValidationCheck::ok(format!(
                    "V2 spectra CV accession for {}: {}",
                    col_name, cv_accession
                )));
            }
        }
    }

    Ok(())
}

fn read_parquet_metadata(
    source: &ParquetSource,
) -> Result<parquet::file::metadata::ParquetMetaData> {
    match source {
        ParquetSource::FilePath(path) => {
            let file = File::open(path)?;
            let reader = SerializedFileReader::new(file)?;
            Ok(reader.metadata().clone())
        }
        ParquetSource::ZipEntry { zip_path, entry_name } => {
            let reader = ZipEntryChunkReader::new(zip_path, entry_name)?;
            let reader = SerializedFileReader::new(reader)?;
            Ok(reader.metadata().clone())
        }
        ParquetSource::InMemory(bytes) => {
            let reader = SerializedFileReader::new(bytes.clone())?;
            Ok(reader.metadata().clone())
        }
    }
}
