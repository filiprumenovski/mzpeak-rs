use super::*;
use arrow::datatypes::DataType;

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
    assert!(schema
        .field_with_name(chromatogram_columns::CHROMATOGRAM_ID)
        .is_ok());
    assert!(schema
        .field_with_name(chromatogram_columns::CHROMATOGRAM_TYPE)
        .is_ok());
    assert!(schema
        .field_with_name(chromatogram_columns::TIME_ARRAY)
        .is_ok());
    assert!(schema
        .field_with_name(chromatogram_columns::INTENSITY_ARRAY)
        .is_ok());

    // Verify list types
    let time_field = schema
        .field_with_name(chromatogram_columns::TIME_ARRAY)
        .unwrap();
    assert!(matches!(time_field.data_type(), DataType::List(_)));

    let intensity_field = schema
        .field_with_name(chromatogram_columns::INTENSITY_ARRAY)
        .unwrap();
    assert!(matches!(intensity_field.data_type(), DataType::List(_)));
}
