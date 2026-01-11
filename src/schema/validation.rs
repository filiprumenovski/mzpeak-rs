use arrow::datatypes::{DataType, Schema};

use super::columns;

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
    /// A required column is missing from the schema
    #[error("Missing required column: {0}")]
    MissingColumn(String),

    /// A column has an incorrect data type
    #[error("Type mismatch for column '{column}': expected {expected}, found {found}")]
    TypeMismatch {
        /// Name of the column with the type mismatch
        column: String,
        /// Expected data type
        expected: String,
        /// Actual data type found
        found: String,
    },
}
