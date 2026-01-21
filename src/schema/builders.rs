use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};

use super::chromatogram_columns;
use super::columns;
use super::constants::KEY_FORMAT_VERSION;
use super::constants::MZPEAK_FORMAT_VERSION;

/// Creates a Field with CV term metadata annotation
fn field_with_cv(name: &str, data_type: DataType, nullable: bool, cv_accession: &str) -> Field {
    let mut metadata = HashMap::new();
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
    let mut metadata = HashMap::new();
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
/// | time_array | `List<Float64>` | Time values in seconds | MS:1000595 |
/// | intensity_array | `List<Float32>` | Intensity values | MS:1000515 |
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
    let mut metadata = HashMap::new();
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
