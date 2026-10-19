use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};

use super::chromatogram_columns;
use super::columns;
use super::constants::KEY_FORMAT_VERSION;
use super::constants::MZPEAK_FORMAT_VERSION;

/// Schema for stored chromatogram arrays.
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
    let mut metadata = HashMap::new();
    metadata.insert(
        KEY_FORMAT_VERSION.to_string(),
        MZPEAK_FORMAT_VERSION.to_string(),
    );
    metadata.insert(
        "mzpeak:schema_description".to_string(),
        "Wide-format chromatogram data with array storage for instant trace visualization"
            .to_string(),
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

// =============================================================================
// v2.0 Schema Builders
// =============================================================================

/// Creates the v2.0 peaks Arrow schema with minimal columns.
///
/// The v2.0 schema is a simplified format with only 4 columns (or 3 for 3D datasets
/// without ion mobility):
///
/// | Column | Type | Nullable | CV Term | Encoding |
/// |--------|------|----------|---------|----------|
/// | spectrum_id | UInt32 | No | MS:1000796 | DELTA_BINARY_PACKED |
/// | mz | Float64 | No | MS:1000040 | BYTE_STREAM_SPLIT |
/// | intensity | Float32 | No | MS:1000042 | BYTE_STREAM_SPLIT |
/// | ion_mobility | Float64 | Yes* | MS:1002476 | BYTE_STREAM_SPLIT |
///
/// *ion_mobility is omitted entirely for 3D datasets (when `has_ion_mobility` is false)
///
/// # Arguments
///
/// * `has_ion_mobility` - If true, includes the ion_mobility column; if false, omits it entirely
///
/// # Example
///
/// ```
/// use mzpeak::schema::create_peaks_schema_v2;
///
/// // 4D dataset with ion mobility
/// let schema_4d = create_peaks_schema_v2(true);
/// assert_eq!(schema_4d.fields().len(), 4);
///
/// // 3D dataset without ion mobility
/// let schema_3d = create_peaks_schema_v2(false);
/// assert_eq!(schema_3d.fields().len(), 3);
/// ```
pub fn create_peaks_schema_v2(has_ion_mobility: bool) -> Schema {
    let mut builder = SchemaBuilder::new();

    // spectrum_id (UInt32, required) - uses DELTA_BINARY_PACKED encoding
    builder.push(field_with_cv(
        columns::SPECTRUM_ID_V2,
        DataType::UInt32,
        false,
        "MS:1000796", // spectrum identifier nativeID format
    ));

    // mz (Float64, required) - uses BYTE_STREAM_SPLIT encoding
    builder.push(field_with_cv(
        columns::MZ,
        DataType::Float64,
        false,
        "MS:1000040", // m/z
    ));

    // intensity (Float32, required) - uses BYTE_STREAM_SPLIT encoding
    builder.push(field_with_cv(
        columns::INTENSITY,
        DataType::Float32,
        false,
        "MS:1000042", // peak intensity
    ));

    // ion_mobility (Float64, optional) - uses BYTE_STREAM_SPLIT encoding
    // Only included for 4D datasets
    if has_ion_mobility {
        builder.push(field_with_cv(
            columns::ION_MOBILITY,
            DataType::Float64,
            true,
            "MS:1002476", // ion mobility drift time
        ));
    }

    let mut schema = builder.finish();

    // Add schema-level metadata
    let mut metadata = HashMap::new();
    metadata.insert(KEY_FORMAT_VERSION.to_string(), "2.0".to_string());
    metadata.insert(
        "mzpeak:schema_description".to_string(),
        "v2.0 minimal peaks schema with optimized encodings".to_string(),
    );
    metadata.insert(
        "mzpeak:cv_namespace".to_string(),
        "https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo".to_string(),
    );

    schema = schema.with_metadata(metadata);
    schema
}

/// Returns an Arc-wrapped v2.0 peaks schema for shared ownership
pub fn create_peaks_schema_v2_arc(has_ion_mobility: bool) -> Arc<Schema> {
    Arc::new(create_peaks_schema_v2(has_ion_mobility))
}

fn field_with_cv(name: &str, data_type: DataType, nullable: bool, cv_accession: &str) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert("cv_accession".to_string(), cv_accession.to_string());
    Field::new(name, data_type, nullable).with_metadata(metadata)
}
