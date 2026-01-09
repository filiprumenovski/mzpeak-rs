//! # mzPeak Schema Definition
//!
//! This module defines the Apache Arrow schema for the mzPeak "Long" table format.
//!
//! ## Design Rationale
//!
//! The "Long" format stores every individual peak as its own row, enabling Parquet's
//! Run-Length Encoding (RLE) to efficiently compress metadata that repeats across peaks
//! within the same spectrum. This is in contrast to a "Wide" format where arrays would
//! be stored as nested lists.
//!
//! ## Schema Columns
//!
//! | Column | Type | Description | CV Term |
//! |--------|------|-------------|---------|
//! | spectrum_id | Int64 | Unique spectrum identifier | MS:1000796 |
//! | scan_number | Int64 | Native scan number from instrument | MS:1000797 |
//! | ms_level | Int16 | MS level (1 for MS1, 2 for MS2, etc.) | MS:1000511 |
//! | retention_time | Float32 | RT in seconds | MS:1000016 |
//! | polarity | Int8 | 1 for positive, -1 for negative | MS:1000465/MS:1000129 |
//! | mz | Float64 | Mass-to-charge ratio | MS:1000040 |
//! | intensity | Float32 | Signal intensity | MS:1000042 |
//! | ion_mobility | Float64 (nullable) | Ion mobility drift time | MS:1002476 |
//! | precursor_mz | Float64 (nullable) | Precursor m/z for MS2+ | MS:1000744 |
//! | precursor_charge | Int16 (nullable) | Precursor charge state | MS:1000041 |
//! | precursor_intensity | Float32 (nullable) | Precursor intensity | MS:1000042 |
//! | isolation_window_lower | Float32 (nullable) | Lower isolation window offset | MS:1000828 |
//! | isolation_window_upper | Float32 (nullable) | Upper isolation window offset | MS:1000829 |
//! | collision_energy | Float32 (nullable) | Collision energy in eV | MS:1000045 |
//! | total_ion_current | Float64 (nullable) | TIC for the spectrum | MS:1000285 |
//! | base_peak_mz | Float64 (nullable) | Base peak m/z | MS:1000504 |
//! | base_peak_intensity | Float32 (nullable) | Base peak intensity | MS:1000505 |
//! | injection_time | Float32 (nullable) | Ion injection time in ms | MS:1000927 |
//! | pixel_x | Int32 (nullable) | X coordinate for MSI data | IMS:1000050 |
//! | pixel_y | Int32 (nullable) | Y coordinate for MSI data | IMS:1000051 |
//! | pixel_z | Int32 (nullable) | Z coordinate for 3D MSI data | IMS:1000052 |
//!
//! ## Compression Strategy
//!
//! By sorting data by spectrum_id, all peaks from the same spectrum are grouped together.
//! Metadata columns (ms_level, retention_time, precursor_mz, etc.) will have identical
//! values within each spectrum group, allowing RLE to achieve excellent compression ratios.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};

/// HUPO-PSI MS CV namespace prefix
pub const MS_CV_PREFIX: &str = "MS";

/// mzPeak format version - follows semantic versioning
pub const MZPEAK_FORMAT_VERSION: &str = "1.0.0";

/// File extension for mzPeak files (legacy single-file format)
pub const MZPEAK_EXTENSION: &str = ".mzpeak.parquet";

/// MIME type for mzPeak container files (public for use in validator and dataset modules)
pub const MZPEAK_MIMETYPE: &str = "application/vnd.mzpeak";

/// Metadata key for format version in Parquet footer
pub const KEY_FORMAT_VERSION: &str = "mzpeak:format_version";

/// Metadata key for SDRF metadata in Parquet footer
pub const KEY_SDRF_METADATA: &str = "mzpeak:sdrf_metadata";

/// Metadata key for instrument configuration in Parquet footer
pub const KEY_INSTRUMENT_CONFIG: &str = "mzpeak:instrument_config";

/// Metadata key for LC configuration in Parquet footer
pub const KEY_LC_CONFIG: &str = "mzpeak:lc_config";

/// Metadata key for run-level technical parameters in Parquet footer
pub const KEY_RUN_PARAMETERS: &str = "mzpeak:run_parameters";

/// Metadata key for source file information
pub const KEY_SOURCE_FILE: &str = "mzpeak:source_file";

/// Metadata key for conversion timestamp
pub const KEY_CONVERSION_TIMESTAMP: &str = "mzpeak:conversion_timestamp";

/// Metadata key for converter software info
pub const KEY_CONVERTER_INFO: &str = "mzpeak:converter_info";

/// Metadata key for data processing history
pub const KEY_PROCESSING_HISTORY: &str = "mzpeak:processing_history";

/// Metadata key for checksum of original raw file
pub const KEY_RAW_FILE_CHECKSUM: &str = "mzpeak:raw_file_checksum";

/// Column names as constants for type safety
pub mod columns {
    pub const SPECTRUM_ID: &str = "spectrum_id";
    pub const SCAN_NUMBER: &str = "scan_number";
    pub const MS_LEVEL: &str = "ms_level";
    pub const RETENTION_TIME: &str = "retention_time";
    pub const POLARITY: &str = "polarity";
    pub const MZ: &str = "mz";
    pub const INTENSITY: &str = "intensity";
    pub const ION_MOBILITY: &str = "ion_mobility";
    pub const PRECURSOR_MZ: &str = "precursor_mz";
    pub const PRECURSOR_CHARGE: &str = "precursor_charge";
    pub const PRECURSOR_INTENSITY: &str = "precursor_intensity";
    pub const ISOLATION_WINDOW_LOWER: &str = "isolation_window_lower";
    pub const ISOLATION_WINDOW_UPPER: &str = "isolation_window_upper";
    pub const COLLISION_ENERGY: &str = "collision_energy";
    pub const TOTAL_ION_CURRENT: &str = "total_ion_current";
    pub const BASE_PEAK_MZ: &str = "base_peak_mz";
    pub const BASE_PEAK_INTENSITY: &str = "base_peak_intensity";
    pub const INJECTION_TIME: &str = "injection_time";

    // MSI (Mass Spectrometry Imaging) spatial columns
    /// X coordinate position for imaging data (pixels)
    pub const PIXEL_X: &str = "pixel_x";
    /// Y coordinate position for imaging data (pixels)
    pub const PIXEL_Y: &str = "pixel_y";
    /// Z coordinate position for 3D imaging data (pixels, optional)
    pub const PIXEL_Z: &str = "pixel_z";
}

/// Column names for chromatogram schema
pub mod chromatogram_columns {
    pub const CHROMATOGRAM_ID: &str = "chromatogram_id";
    pub const CHROMATOGRAM_TYPE: &str = "chromatogram_type";
    pub const TIME_ARRAY: &str = "time_array";
    pub const INTENSITY_ARRAY: &str = "intensity_array";
}

/// Creates a Field with CV term metadata annotation
fn field_with_cv(name: &str, data_type: DataType, nullable: bool, cv_accession: &str) -> Field {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("cv_accession".to_string(), cv_accession.to_string());
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

/// Creates the core mzPeak Arrow schema for LC-MS data.
///
/// This schema uses the "Long" table format where each peak is a separate row.
/// Spectrum-level metadata is repeated for each peak within a spectrum, allowing
/// Parquet's RLE compression to efficiently store the data.
///
/// # Example
///
/// ```
/// use mzpeak::schema::create_mzpeak_schema;
///
/// let schema = create_mzpeak_schema();
/// assert_eq!(schema.fields().len(), 21); // includes MSI spatial columns
/// ```
pub fn create_mzpeak_schema() -> Schema {
    let mut builder = SchemaBuilder::new();

    // Core identification columns (required)
    builder.push(field_with_cv(
        columns::SPECTRUM_ID,
        DataType::Int64,
        false,
        "MS:1000796", // spectrum identifier nativeID format
    ));

    builder.push(field_with_cv(
        columns::SCAN_NUMBER,
        DataType::Int64,
        false,
        "MS:1000797", // peak list scans
    ));

    // MS level (required) - Int16 is sufficient for MS1-MS10
    builder.push(field_with_cv(
        columns::MS_LEVEL,
        DataType::Int16,
        false,
        "MS:1000511", // ms level
    ));

    // Retention time in seconds (required for LC-MS)
    builder.push(field_with_cv(
        columns::RETENTION_TIME,
        DataType::Float32,
        false,
        "MS:1000016", // scan start time
    ));

    // Polarity: 1 for positive, -1 for negative (required)
    builder.push(field_with_cv(
        columns::POLARITY,
        DataType::Int8,
        false,
        "MS:1000465", // scan polarity
    ));

    // Peak data columns (required)
    builder.push(field_with_cv(
        columns::MZ,
        DataType::Float64,
        false,
        "MS:1000040", // m/z
    ));

    builder.push(field_with_cv(
        columns::INTENSITY,
        DataType::Float32,
        false,
        "MS:1000042", // peak intensity
    ));

    // Ion Mobility (nullable)
    builder.push(field_with_cv(
        columns::ION_MOBILITY,
        DataType::Float64,
        true,
        "MS:1002476", // ion mobility drift time
    ));

    // Precursor information (nullable - only for MS2+)
    builder.push(field_with_cv(
        columns::PRECURSOR_MZ,
        DataType::Float64,
        true,
        "MS:1000744", // selected ion m/z
    ));

    builder.push(field_with_cv(
        columns::PRECURSOR_CHARGE,
        DataType::Int16,
        true,
        "MS:1000041", // charge state
    ));

    builder.push(field_with_cv(
        columns::PRECURSOR_INTENSITY,
        DataType::Float32,
        true,
        "MS:1000042", // peak intensity (for precursor)
    ));

    // Isolation window parameters (nullable)
    builder.push(field_with_cv(
        columns::ISOLATION_WINDOW_LOWER,
        DataType::Float32,
        true,
        "MS:1000828", // isolation window lower offset
    ));

    builder.push(field_with_cv(
        columns::ISOLATION_WINDOW_UPPER,
        DataType::Float32,
        true,
        "MS:1000829", // isolation window upper offset
    ));

    // Fragmentation parameters (nullable)
    builder.push(field_with_cv(
        columns::COLLISION_ENERGY,
        DataType::Float32,
        true,
        "MS:1000045", // collision energy
    ));

    // Spectrum-level summary statistics (nullable but recommended)
    builder.push(field_with_cv(
        columns::TOTAL_ION_CURRENT,
        DataType::Float64,
        true,
        "MS:1000285", // total ion current
    ));

    builder.push(field_with_cv(
        columns::BASE_PEAK_MZ,
        DataType::Float64,
        true,
        "MS:1000504", // base peak m/z
    ));

    builder.push(field_with_cv(
        columns::BASE_PEAK_INTENSITY,
        DataType::Float32,
        true,
        "MS:1000505", // base peak intensity
    ));

    // Injection time (nullable)
    builder.push(field_with_cv(
        columns::INJECTION_TIME,
        DataType::Float32,
        true,
        "MS:1000927", // ion injection time
    ));

    // MSI (Mass Spectrometry Imaging) spatial columns (nullable)
    // These columns enable ion image extraction and spatial analysis
    builder.push(field_with_cv(
        columns::PIXEL_X,
        DataType::Int32,
        true,
        "IMS:1000050", // position x (from imzML imaging MS CV)
    ));

    builder.push(field_with_cv(
        columns::PIXEL_Y,
        DataType::Int32,
        true,
        "IMS:1000051", // position y (from imzML imaging MS CV)
    ));

    builder.push(field_with_cv(
        columns::PIXEL_Z,
        DataType::Int32,
        true,
        "IMS:1000052", // position z (from imzML imaging MS CV)
    ));

    let mut schema = builder.finish();

    // Add schema-level metadata
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(KEY_FORMAT_VERSION.to_string(), MZPEAK_FORMAT_VERSION.to_string());
    metadata.insert(
        "mzpeak:schema_description".to_string(),
        "Long-format LC-MS peak data with RLE-optimized spectrum metadata".to_string(),
    );
    metadata.insert(
        "mzpeak:cv_namespace".to_string(),
        "https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo".to_string(),
    );

    schema = schema.with_metadata(metadata);
    schema
}

/// Returns an Arc-wrapped schema for shared ownership
pub fn create_mzpeak_schema_arc() -> Arc<Schema> {
    Arc::new(create_mzpeak_schema())
}

/// Creates the chromatogram Arrow schema for the "Wide" format.
///
/// Unlike the "Long" format used for peaks, chromatograms are stored as rows of arrays
/// (Time and Intensity vectors) to enable instant trace visualization without scanning
/// the entire peak table.
///
/// # Schema Columns
///
/// | Column | Type | Description | CV Term |
/// |--------|------|-------------|---------|
/// | chromatogram_id | Utf8 | Unique chromatogram identifier | MS:1000235 |
/// | chromatogram_type | Utf8 | Type of chromatogram (TIC, BPC, etc.) | MS:1000235 |
/// | time_array | List<Float64> | Time values in seconds | MS:1000595 |
/// | intensity_array | List<Float32> | Intensity values | MS:1000515 |
///
/// # Example
///
/// ```
/// use mzpeak::schema::create_chromatogram_schema;
///
/// let schema = create_chromatogram_schema();
/// assert_eq!(schema.fields().len(), 4);
/// ```
pub fn create_chromatogram_schema() -> Schema {
    let mut builder = SchemaBuilder::new();

    // Chromatogram ID - string identifier
    builder.push(field_with_cv(
        chromatogram_columns::CHROMATOGRAM_ID,
        DataType::Utf8,
        false,
        "MS:1000235", // total ion current chromatogram (generic CV for chromatogram)
    ));

    // Chromatogram type - string describing the type (TIC, BPC, SRM, etc.)
    builder.push(field_with_cv(
        chromatogram_columns::CHROMATOGRAM_TYPE,
        DataType::Utf8,
        false,
        "MS:1000235", // chromatogram type
    ));

    // Time array - List of Float64 values
    builder.push(field_with_cv(
        chromatogram_columns::TIME_ARRAY,
        DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
        false,
        "MS:1000595", // time array
    ));

    // Intensity array - List of Float32 values
    builder.push(field_with_cv(
        chromatogram_columns::INTENSITY_ARRAY,
        DataType::List(Arc::new(Field::new("item", DataType::Float32, false))),
        false,
        "MS:1000515", // intensity array
    ));

    let mut schema = builder.finish();

    // Add schema-level metadata
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(KEY_FORMAT_VERSION.to_string(), MZPEAK_FORMAT_VERSION.to_string());
    metadata.insert(
        "mzpeak:schema_description".to_string(),
        "Wide-format chromatogram data with array storage for instant trace visualization".to_string(),
    );
    metadata.insert(
        "mzpeak:cv_namespace".to_string(),
        "https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo".to_string(),
    );

    schema = schema.with_metadata(metadata);
    schema
}

/// Returns an Arc-wrapped chromatogram schema for shared ownership
pub fn create_chromatogram_schema_arc() -> Arc<Schema> {
    Arc::new(create_chromatogram_schema())
}

/// Validates that a schema is compatible with the mzPeak format.
///
/// Returns `Ok(())` if the schema contains all required columns with correct types,
/// or an error describing the incompatibility.
pub fn validate_schema(schema: &Schema) -> Result<(), SchemaValidationError> {
    let required_columns = [
        (columns::SPECTRUM_ID, DataType::Int64),
        (columns::SCAN_NUMBER, DataType::Int64),
        (columns::MS_LEVEL, DataType::Int16),
        (columns::RETENTION_TIME, DataType::Float32),
        (columns::POLARITY, DataType::Int8),
        (columns::MZ, DataType::Float64),
        (columns::INTENSITY, DataType::Float32),
    ];

    for (name, expected_type) in required_columns {
        match schema.field_with_name(name) {
            Ok(field) => {
                if field.data_type() != &expected_type {
                    return Err(SchemaValidationError::TypeMismatch {
                        column: name.to_string(),
                        expected: format!("{:?}", expected_type),
                        found: format!("{:?}", field.data_type()),
                    });
                }
            }
            Err(_) => {
                return Err(SchemaValidationError::MissingColumn(name.to_string()));
            }
        }
    }

    Ok(())
}

/// Errors that can occur during schema validation
#[derive(Debug, thiserror::Error)]
pub enum SchemaValidationError {
    #[error("Missing required column: {0}")]
    MissingColumn(String),

    #[error("Type mismatch for column '{column}': expected {expected}, found {found}")]
    TypeMismatch {
        column: String,
        expected: String,
        found: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = create_mzpeak_schema();
        assert_eq!(schema.fields().len(), 21); // 18 original + 3 MSI columns

        // Check required columns exist
        assert!(schema.field_with_name(columns::SPECTRUM_ID).is_ok());
        assert!(schema.field_with_name(columns::MZ).is_ok());
        assert!(schema.field_with_name(columns::INTENSITY).is_ok());
        assert!(schema.field_with_name(columns::ION_MOBILITY).is_ok());

        // Check MSI columns exist
        assert!(schema.field_with_name(columns::PIXEL_X).is_ok());
        assert!(schema.field_with_name(columns::PIXEL_Y).is_ok());
        assert!(schema.field_with_name(columns::PIXEL_Z).is_ok());
    }

    #[test]
    fn test_schema_validation() {
        let schema = create_mzpeak_schema();
        assert!(validate_schema(&schema).is_ok());
    }

    #[test]
    fn test_cv_metadata() {
        let schema = create_mzpeak_schema();
        let mz_field = schema.field_with_name(columns::MZ).unwrap();
        let cv = mz_field.metadata().get("cv_accession").unwrap();
        assert_eq!(cv, "MS:1000040");
    }

    #[test]
    fn test_chromatogram_schema_creation() {
        let schema = create_chromatogram_schema();
        assert_eq!(schema.fields().len(), 4);

        // Check required columns exist
        assert!(schema.field_with_name(chromatogram_columns::CHROMATOGRAM_ID).is_ok());
        assert!(schema.field_with_name(chromatogram_columns::CHROMATOGRAM_TYPE).is_ok());
        assert!(schema.field_with_name(chromatogram_columns::TIME_ARRAY).is_ok());
        assert!(schema.field_with_name(chromatogram_columns::INTENSITY_ARRAY).is_ok());

        // Verify list types
        let time_field = schema.field_with_name(chromatogram_columns::TIME_ARRAY).unwrap();
        assert!(matches!(time_field.data_type(), DataType::List(_)));

        let intensity_field = schema.field_with_name(chromatogram_columns::INTENSITY_ARRAY).unwrap();
        assert!(matches!(intensity_field.data_type(), DataType::List(_)));
    }
}
