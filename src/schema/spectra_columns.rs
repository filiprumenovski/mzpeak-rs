//! # Spectra Table Schema for mzPeak v2.0
//!
//! This module defines the Arrow schema for the spectra table, which stores one row
//! per spectrum with spectrum-level metadata and pointers to peak data.
//!
//! ## Design Rationale
//!
//! The spectra table separates spectrum-level metadata from peak data, enabling:
//! - Fast spectrum-level queries without scanning peak data
//! - Efficient random access via peak_offset/peak_count pointers
//! - Compact storage of spectrum metadata
//!
//! ## Schema Columns
//!
//! | Column | Type | Nullable | CV Term | Notes |
//! |--------|------|----------|---------|-------|
//! | spectrum_id | UInt32 | No | MS:1000796 | Primary key, 0-indexed |
//! | scan_number | Int32 | Yes | MS:1000797 | Vendor native ID |
//! | ms_level | UInt8 | No | MS:1000511 | 1-10 range |
//! | retention_time | Float32 | No | MS:1000016 | Seconds |
//! | polarity | Int8 | No | MS:1000465 | 1/-1 |
//! | peak_offset | UInt64 | No | - | Byte offset in peaks.parquet |
//! | peak_count | UInt32 | No | - | Number of peaks |
//! | precursor_mz | Float64 | Yes | MS:1000744 | MS2+ only |
//! | precursor_charge | Int8 | Yes | MS:1000041 | +/-7 range |
//! | precursor_intensity | Float32 | Yes | MS:1000042 | |
//! | isolation_window_lower | Float32 | Yes | MS:1000828 | |
//! | isolation_window_upper | Float32 | Yes | MS:1000829 | |
//! | collision_energy | Float32 | Yes | MS:1000045 | eV |
//! | total_ion_current | Float64 | Yes | MS:1000285 | |
//! | base_peak_mz | Float64 | Yes | MS:1000504 | |
//! | base_peak_intensity | Float32 | Yes | MS:1000505 | |
//! | injection_time | Float32 | Yes | MS:1000927 | ms |
//! | pixel_x | UInt16 | Yes | IMS:1000050 | Imaging only |
//! | pixel_y | UInt16 | Yes | IMS:1000051 | Imaging only |
//! | pixel_z | UInt16 | Yes | IMS:1000052 | 3D imaging |

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder};

use super::constants::{KEY_FORMAT_VERSION, MZPEAK_FORMAT_VERSION};

// =============================================================================
// Column Name Constants
// =============================================================================

/// Unique spectrum identifier (primary key, 0-indexed)
/// CV: MS:1000796 - spectrum identifier nativeID format
pub const SPECTRUM_ID: &str = "spectrum_id";

/// Native scan number from the instrument vendor
/// CV: MS:1000797 - peak list scans
pub const SCAN_NUMBER: &str = "scan_number";

/// MS level (1 for MS1, 2 for MS/MS, etc., range 1-10)
/// CV: MS:1000511 - ms level
pub const MS_LEVEL: &str = "ms_level";

/// Retention time in seconds
/// CV: MS:1000016 - scan start time
pub const RETENTION_TIME: &str = "retention_time";

/// Polarity (1 for positive, -1 for negative)
/// CV: MS:1000465 - scan polarity
pub const POLARITY: &str = "polarity";

/// Byte offset in peaks.parquet file
pub const PEAK_OFFSET: &str = "peak_offset";

/// Number of peaks in this spectrum
pub const PEAK_COUNT: &str = "peak_count";

/// Precursor m/z for MS2+ spectra
/// CV: MS:1000744 - selected ion m/z
pub const PRECURSOR_MZ: &str = "precursor_mz";

/// Precursor charge state (+/-7 range)
/// CV: MS:1000041 - charge state
pub const PRECURSOR_CHARGE: &str = "precursor_charge";

/// Precursor intensity
/// CV: MS:1000042 - peak intensity
pub const PRECURSOR_INTENSITY: &str = "precursor_intensity";

/// Lower isolation window offset
/// CV: MS:1000828 - isolation window lower offset
pub const ISOLATION_WINDOW_LOWER: &str = "isolation_window_lower";

/// Upper isolation window offset
/// CV: MS:1000829 - isolation window upper offset
pub const ISOLATION_WINDOW_UPPER: &str = "isolation_window_upper";

/// Collision energy in eV
/// CV: MS:1000045 - collision energy
pub const COLLISION_ENERGY: &str = "collision_energy";

/// Total ion current
/// CV: MS:1000285 - total ion current
pub const TOTAL_ION_CURRENT: &str = "total_ion_current";

/// Base peak m/z
/// CV: MS:1000504 - base peak m/z
pub const BASE_PEAK_MZ: &str = "base_peak_mz";

/// Base peak intensity
/// CV: MS:1000505 - base peak intensity
pub const BASE_PEAK_INTENSITY: &str = "base_peak_intensity";

/// Ion injection time in milliseconds
/// CV: MS:1000927 - ion injection time
pub const INJECTION_TIME: &str = "injection_time";

/// X coordinate position for imaging data (pixels)
/// CV: IMS:1000050 - position x
pub const PIXEL_X: &str = "pixel_x";

/// Y coordinate position for imaging data (pixels)
/// CV: IMS:1000051 - position y
pub const PIXEL_Y: &str = "pixel_y";

/// Z coordinate position for 3D imaging data (pixels)
/// CV: IMS:1000052 - position z
pub const PIXEL_Z: &str = "pixel_z";

// =============================================================================
// Schema Builder Functions
// =============================================================================

/// Creates a Field with CV term metadata annotation
fn field_with_cv(name: &str, data_type: DataType, nullable: bool, cv_accession: &str) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert("cv_accession".to_string(), cv_accession.to_string());
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

/// Creates a Field without CV term metadata (for internal columns)
fn field_without_cv(name: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable)
}

/// Creates the spectra table Arrow schema for mzPeak v2.0.
///
/// This schema stores one row per spectrum with spectrum-level metadata
/// and pointers (peak_offset, peak_count) to the corresponding peak data
/// in a separate peaks.parquet file.
///
/// # Schema Overview
///
/// - **Required columns**: spectrum_id, ms_level, retention_time, polarity, peak_offset, peak_count
/// - **Optional columns**: scan_number, precursor info, isolation window, summary stats, imaging coords
///
/// # Example
///
/// ```
/// use mzpeak::schema::spectra_columns::create_spectra_schema;
///
/// let schema = create_spectra_schema();
/// assert_eq!(schema.fields().len(), 20);
/// ```
pub fn create_spectra_schema() -> Schema {
    let mut builder = SchemaBuilder::new();

    // ==========================================================================
    // Core identification columns (required)
    // ==========================================================================

    // spectrum_id - Primary key, 0-indexed
    builder.push(field_with_cv(
        SPECTRUM_ID,
        DataType::UInt32,
        false,
        "MS:1000796", // spectrum identifier nativeID format
    ));

    // scan_number - Vendor native ID (nullable)
    builder.push(field_with_cv(
        SCAN_NUMBER,
        DataType::Int32,
        true,
        "MS:1000797", // peak list scans
    ));

    // ms_level - MS level (1-10 range)
    builder.push(field_with_cv(
        MS_LEVEL,
        DataType::UInt8,
        false,
        "MS:1000511", // ms level
    ));

    // retention_time - RT in seconds
    builder.push(field_with_cv(
        RETENTION_TIME,
        DataType::Float32,
        false,
        "MS:1000016", // scan start time
    ));

    // polarity - 1 for positive, -1 for negative
    builder.push(field_with_cv(
        POLARITY,
        DataType::Int8,
        false,
        "MS:1000465", // scan polarity
    ));

    // ==========================================================================
    // Peak data pointers (required)
    // ==========================================================================

    // peak_offset - Byte offset in peaks.parquet
    builder.push(field_without_cv(PEAK_OFFSET, DataType::UInt64, false));

    // peak_count - Number of peaks in this spectrum
    builder.push(field_without_cv(PEAK_COUNT, DataType::UInt32, false));

    // ==========================================================================
    // Precursor information (nullable - only for MS2+)
    // ==========================================================================

    // precursor_mz - Selected ion m/z
    builder.push(field_with_cv(
        PRECURSOR_MZ,
        DataType::Float64,
        true,
        "MS:1000744", // selected ion m/z
    ));

    // precursor_charge - Charge state (+/-7 range)
    builder.push(field_with_cv(
        PRECURSOR_CHARGE,
        DataType::Int8,
        true,
        "MS:1000041", // charge state
    ));

    // precursor_intensity - Precursor intensity
    builder.push(field_with_cv(
        PRECURSOR_INTENSITY,
        DataType::Float32,
        true,
        "MS:1000042", // peak intensity
    ));

    // ==========================================================================
    // Isolation window parameters (nullable)
    // ==========================================================================

    // isolation_window_lower - Lower offset
    builder.push(field_with_cv(
        ISOLATION_WINDOW_LOWER,
        DataType::Float32,
        true,
        "MS:1000828", // isolation window lower offset
    ));

    // isolation_window_upper - Upper offset
    builder.push(field_with_cv(
        ISOLATION_WINDOW_UPPER,
        DataType::Float32,
        true,
        "MS:1000829", // isolation window upper offset
    ));

    // ==========================================================================
    // Fragmentation parameters (nullable)
    // ==========================================================================

    // collision_energy - Collision energy in eV
    builder.push(field_with_cv(
        COLLISION_ENERGY,
        DataType::Float32,
        true,
        "MS:1000045", // collision energy
    ));

    // ==========================================================================
    // Spectrum-level summary statistics (nullable)
    // ==========================================================================

    // total_ion_current - TIC
    builder.push(field_with_cv(
        TOTAL_ION_CURRENT,
        DataType::Float64,
        true,
        "MS:1000285", // total ion current
    ));

    // base_peak_mz - Base peak m/z
    builder.push(field_with_cv(
        BASE_PEAK_MZ,
        DataType::Float64,
        true,
        "MS:1000504", // base peak m/z
    ));

    // base_peak_intensity - Base peak intensity
    builder.push(field_with_cv(
        BASE_PEAK_INTENSITY,
        DataType::Float32,
        true,
        "MS:1000505", // base peak intensity
    ));

    // injection_time - Ion injection time in milliseconds
    builder.push(field_with_cv(
        INJECTION_TIME,
        DataType::Float32,
        true,
        "MS:1000927", // ion injection time
    ));

    // ==========================================================================
    // MSI (Mass Spectrometry Imaging) spatial columns (nullable)
    // ==========================================================================

    // pixel_x - X coordinate for imaging data
    builder.push(field_with_cv(
        PIXEL_X,
        DataType::UInt16,
        true,
        "IMS:1000050", // position x
    ));

    // pixel_y - Y coordinate for imaging data
    builder.push(field_with_cv(
        PIXEL_Y,
        DataType::UInt16,
        true,
        "IMS:1000051", // position y
    ));

    // pixel_z - Z coordinate for 3D imaging data
    builder.push(field_with_cv(
        PIXEL_Z,
        DataType::UInt16,
        true,
        "IMS:1000052", // position z
    ));

    let mut schema = builder.finish();

    // ==========================================================================
    // Schema-level metadata
    // ==========================================================================

    let mut metadata = HashMap::new();
    metadata.insert(
        KEY_FORMAT_VERSION.to_string(),
        MZPEAK_FORMAT_VERSION.to_string(),
    );
    metadata.insert(
        "mzpeak:schema_description".to_string(),
        "Spectra table storing one row per spectrum with peak data pointers".to_string(),
    );
    metadata.insert(
        "mzpeak:cv_namespace".to_string(),
        "https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo".to_string(),
    );

    schema = schema.with_metadata(metadata);
    schema
}

/// Returns an Arc-wrapped spectra schema for shared ownership
pub fn create_spectra_schema_arc() -> Arc<Schema> {
    Arc::new(create_spectra_schema())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spectra_schema_field_count() {
        let schema = create_spectra_schema();
        assert_eq!(schema.fields().len(), 20);
    }

    #[test]
    fn test_spectra_schema_required_fields() {
        let schema = create_spectra_schema();

        // Check required fields are not nullable
        let spectrum_id = schema.field_with_name(SPECTRUM_ID).unwrap();
        assert!(!spectrum_id.is_nullable());
        assert_eq!(spectrum_id.data_type(), &DataType::UInt32);

        let ms_level = schema.field_with_name(MS_LEVEL).unwrap();
        assert!(!ms_level.is_nullable());
        assert_eq!(ms_level.data_type(), &DataType::UInt8);

        let retention_time = schema.field_with_name(RETENTION_TIME).unwrap();
        assert!(!retention_time.is_nullable());
        assert_eq!(retention_time.data_type(), &DataType::Float32);

        let polarity = schema.field_with_name(POLARITY).unwrap();
        assert!(!polarity.is_nullable());
        assert_eq!(polarity.data_type(), &DataType::Int8);

        let peak_offset = schema.field_with_name(PEAK_OFFSET).unwrap();
        assert!(!peak_offset.is_nullable());
        assert_eq!(peak_offset.data_type(), &DataType::UInt64);

        let peak_count = schema.field_with_name(PEAK_COUNT).unwrap();
        assert!(!peak_count.is_nullable());
        assert_eq!(peak_count.data_type(), &DataType::UInt32);
    }

    #[test]
    fn test_spectra_schema_nullable_fields() {
        let schema = create_spectra_schema();

        // Check nullable fields
        let scan_number = schema.field_with_name(SCAN_NUMBER).unwrap();
        assert!(scan_number.is_nullable());
        assert_eq!(scan_number.data_type(), &DataType::Int32);

        let precursor_mz = schema.field_with_name(PRECURSOR_MZ).unwrap();
        assert!(precursor_mz.is_nullable());
        assert_eq!(precursor_mz.data_type(), &DataType::Float64);

        let precursor_charge = schema.field_with_name(PRECURSOR_CHARGE).unwrap();
        assert!(precursor_charge.is_nullable());
        assert_eq!(precursor_charge.data_type(), &DataType::Int8);

        let pixel_x = schema.field_with_name(PIXEL_X).unwrap();
        assert!(pixel_x.is_nullable());
        assert_eq!(pixel_x.data_type(), &DataType::UInt16);
    }

    #[test]
    fn test_spectra_schema_cv_metadata() {
        let schema = create_spectra_schema();

        let spectrum_id = schema.field_with_name(SPECTRUM_ID).unwrap();
        let cv = spectrum_id.metadata().get("cv_accession").unwrap();
        assert_eq!(cv, "MS:1000796");

        let ms_level = schema.field_with_name(MS_LEVEL).unwrap();
        let cv = ms_level.metadata().get("cv_accession").unwrap();
        assert_eq!(cv, "MS:1000511");

        let precursor_mz = schema.field_with_name(PRECURSOR_MZ).unwrap();
        let cv = precursor_mz.metadata().get("cv_accession").unwrap();
        assert_eq!(cv, "MS:1000744");
    }

    #[test]
    fn test_spectra_schema_metadata() {
        let schema = create_spectra_schema();
        let metadata = schema.metadata();

        assert!(metadata.contains_key(KEY_FORMAT_VERSION));
        assert!(metadata.contains_key("mzpeak:schema_description"));
        assert!(metadata.contains_key("mzpeak:cv_namespace"));
    }

    #[test]
    fn test_spectra_schema_arc() {
        let schema_arc = create_spectra_schema_arc();
        assert_eq!(schema_arc.fields().len(), 20);
    }
}
