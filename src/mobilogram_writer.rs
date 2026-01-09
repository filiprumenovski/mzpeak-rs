//! # Mobilogram Writer Module
//!
//! This module provides functionality for writing mobilogram data
//! to the mzPeak Parquet format using the "Wide" schema.
//!
//! Mobilograms are ion mobility vs intensity traces, analogous to chromatograms
//! but for the ion mobility dimension. They enable visualization of ion mobility
//! distributions and extracted mobilograms (XIM).
//!
//! ## Schema Columns
//!
//! | Column | Type | Description | CV Term |
//! |--------|------|-------------|---------|
//! | mobilogram_id | Utf8 | Unique mobilogram identifier | MS:1003006 |
//! | mobilogram_type | Utf8 | Type (TIM, XIM, etc.) | MS:1003006 |
//! | mobility_array | List<Float64> | Ion mobility values | MS:1002476 |
//! | intensity_array | List<Float32> | Intensity values | MS:1000515 |

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Builder, Float64Builder, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::KeyValue;

use crate::metadata::MzPeakMetadata;
use crate::schema::{KEY_FORMAT_VERSION, MZPEAK_FORMAT_VERSION};

/// Column names for mobilogram schema
pub mod mobilogram_columns {
    /// Unique mobilogram identifier
    pub const MOBILOGRAM_ID: &str = "mobilogram_id";
    /// Type of mobilogram (TIM, XIM, etc.)
    pub const MOBILOGRAM_TYPE: &str = "mobilogram_type";
    /// Ion mobility array
    pub const MOBILITY_ARRAY: &str = "mobility_array";
    /// Intensity array
    pub const INTENSITY_ARRAY: &str = "intensity_array";
}

/// Creates a Field with CV term metadata annotation
fn field_with_cv(name: &str, data_type: DataType, nullable: bool, cv_accession: &str) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert("cv_accession".to_string(), cv_accession.to_string());
    Field::new(name, data_type, nullable).with_metadata(metadata)
}

/// Creates the mobilogram Arrow schema for the "Wide" format.
///
/// # Example
///
/// ```
/// use mzpeak::mobilogram_writer::create_mobilogram_schema;
///
/// let schema = create_mobilogram_schema();
/// assert_eq!(schema.fields().len(), 4);
/// ```
pub fn create_mobilogram_schema() -> Schema {
    let mut fields = Vec::new();

    // Mobilogram ID - string identifier
    fields.push(field_with_cv(
        mobilogram_columns::MOBILOGRAM_ID,
        DataType::Utf8,
        false,
        "MS:1003006", // mobilogram
    ));

    // Mobilogram type - string describing the type (TIM, XIM, etc.)
    fields.push(field_with_cv(
        mobilogram_columns::MOBILOGRAM_TYPE,
        DataType::Utf8,
        false,
        "MS:1003006", // mobilogram type
    ));

    // Mobility array - List of Float64 values
    fields.push(field_with_cv(
        mobilogram_columns::MOBILITY_ARRAY,
        DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
        false,
        "MS:1002476", // ion mobility drift time
    ));

    // Intensity array - List of Float32 values
    fields.push(field_with_cv(
        mobilogram_columns::INTENSITY_ARRAY,
        DataType::List(Arc::new(Field::new("item", DataType::Float32, false))),
        false,
        "MS:1000515", // intensity array
    ));

    let mut schema = Schema::new(fields);

    // Add schema-level metadata
    let mut metadata = HashMap::new();
    metadata.insert(KEY_FORMAT_VERSION.to_string(), MZPEAK_FORMAT_VERSION.to_string());
    metadata.insert(
        "mzpeak:schema_description".to_string(),
        "Wide-format mobilogram data with array storage for instant trace visualization".to_string(),
    );
    metadata.insert(
        "mzpeak:cv_namespace".to_string(),
        "https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo".to_string(),
    );

    schema = schema.with_metadata(metadata);
    schema
}

/// Returns an Arc-wrapped mobilogram schema for shared ownership
pub fn create_mobilogram_schema_arc() -> Arc<Schema> {
    Arc::new(create_mobilogram_schema())
}

/// Errors that can occur during mobilogram writing
#[derive(Debug, thiserror::Error)]
pub enum MobilogramWriterError {
    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Arrow error
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// Parquet error
    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    /// Metadata error
    #[error("Metadata error: {0}")]
    MetadataError(#[from] crate::metadata::MetadataError),

    /// Invalid data
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Array length mismatch
    #[error("Array length mismatch: mobility array has {mobility_len} elements, intensity array has {intensity_len} elements")]
    ArrayLengthMismatch {
        /// Length of mobility array
        mobility_len: usize,
        /// Length of intensity array
        intensity_len: usize,
    },
}

/// Configuration for the mobilogram writer
#[derive(Debug, Clone)]
pub struct MobilogramWriterConfig {
    /// Compression level (ZSTD, 1-22, default 3)
    pub compression_level: i32,

    /// Target row group size
    pub row_group_size: usize,

    /// Data page size in bytes
    pub data_page_size: usize,

    /// Whether to write statistics for columns
    pub write_statistics: bool,
}

impl Default for MobilogramWriterConfig {
    fn default() -> Self {
        Self {
            compression_level: 3,
            row_group_size: 100, // Smaller than peaks since arrays are large
            data_page_size: 1024 * 1024,
            write_statistics: true,
        }
    }
}

impl MobilogramWriterConfig {
    /// Create writer properties from this configuration
    fn to_writer_properties(&self, metadata: &HashMap<String, String>) -> WriterProperties {
        let compression = Compression::ZSTD(
            ZstdLevel::try_new(self.compression_level).unwrap_or(ZstdLevel::default())
        );

        let statistics = if self.write_statistics {
            EnabledStatistics::Chunk
        } else {
            EnabledStatistics::None
        };

        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_data_page_size_limit(self.data_page_size)
            .set_statistics_enabled(statistics)
            .set_max_row_group_size(self.row_group_size);

        // Disable dictionary encoding for array columns (high-cardinality data)
        builder = builder.set_column_dictionary_enabled(
            parquet::schema::types::ColumnPath::new(vec![mobilogram_columns::MOBILITY_ARRAY.to_string()]),
            false,
        );
        builder = builder.set_column_dictionary_enabled(
            parquet::schema::types::ColumnPath::new(vec![mobilogram_columns::INTENSITY_ARRAY.to_string()]),
            false,
        );

        // Add key-value metadata
        let kv_metadata: Vec<KeyValue> = metadata
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.clone(),
                value: Some(v.clone()),
            })
            .collect();

        builder = builder.set_key_value_metadata(Some(kv_metadata));

        builder.build()
    }
}

/// Represents a single mobilogram in the "Wide" format
#[derive(Debug, Clone)]
pub struct Mobilogram {
    /// Unique mobilogram identifier
    pub mobilogram_id: String,

    /// Type of mobilogram (TIM = total ion mobilogram, XIM = extracted ion mobilogram)
    pub mobilogram_type: String,

    /// Ion mobility values (in appropriate units, typically 1/K0 or drift time)
    pub mobility_array: Vec<f64>,

    /// Intensity values
    pub intensity_array: Vec<f32>,
}

impl Mobilogram {
    /// Create a new mobilogram
    pub fn new(
        mobilogram_id: String,
        mobilogram_type: String,
        mobility_array: Vec<f64>,
        intensity_array: Vec<f32>,
    ) -> Result<Self, MobilogramWriterError> {
        // Validate array lengths match
        if mobility_array.len() != intensity_array.len() {
            return Err(MobilogramWriterError::ArrayLengthMismatch {
                mobility_len: mobility_array.len(),
                intensity_len: intensity_array.len(),
            });
        }

        Ok(Self {
            mobilogram_id,
            mobilogram_type,
            mobility_array,
            intensity_array,
        })
    }

    /// Create a Total Ion Mobilogram (TIM)
    pub fn new_tim(mobilogram_id: String, mobility_array: Vec<f64>, intensity_array: Vec<f32>) -> Result<Self, MobilogramWriterError> {
        Self::new(mobilogram_id, "TIM".to_string(), mobility_array, intensity_array)
    }

    /// Create an Extracted Ion Mobilogram (XIM)
    pub fn new_xim(mobilogram_id: String, mobility_array: Vec<f64>, intensity_array: Vec<f32>) -> Result<Self, MobilogramWriterError> {
        Self::new(mobilogram_id, "XIM".to_string(), mobility_array, intensity_array)
    }

    /// Get the number of data points
    pub fn len(&self) -> usize {
        self.mobility_array.len()
    }

    /// Check if the mobilogram is empty
    pub fn is_empty(&self) -> bool {
        self.mobility_array.is_empty()
    }
}

/// Streaming writer for mobilogram Parquet files
pub struct MobilogramWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    mobilograms_written: usize,
    data_points_written: usize,
}

impl MobilogramWriter<File> {
    /// Create a new writer to a file path
    pub fn new_file<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: MobilogramWriterConfig,
    ) -> Result<Self, MobilogramWriterError> {
        let file = File::create(path)?;
        Self::new(file, metadata, config)
    }
}

impl<W: Write + Send> MobilogramWriter<W> {
    /// Create a new writer to any Write implementation
    pub fn new(
        writer: W,
        metadata: &MzPeakMetadata,
        config: MobilogramWriterConfig,
    ) -> Result<Self, MobilogramWriterError> {
        let schema = create_mobilogram_schema_arc();
        let parquet_metadata = metadata.to_parquet_metadata()?;
        let props = config.to_writer_properties(&parquet_metadata);

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            mobilograms_written: 0,
            data_points_written: 0,
        })
    }

    /// Write a batch of mobilograms
    pub fn write_mobilograms(&mut self, mobilograms: &[Mobilogram]) -> Result<(), MobilogramWriterError> {
        if mobilograms.is_empty() {
            return Ok(());
        }

        // Build arrays for each column
        let mut id_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        // Use Field with nullable=false to match schema
        let mobility_field = Field::new("item", DataType::Float64, false);
        let intensity_field = Field::new("item", DataType::Float32, false);
        let mut mobility_builder = ListBuilder::new(Float64Builder::new()).with_field(mobility_field);
        let mut intensity_builder = ListBuilder::new(Float32Builder::new()).with_field(intensity_field);

        for mobilogram in mobilograms {
            id_builder.append_value(&mobilogram.mobilogram_id);
            type_builder.append_value(&mobilogram.mobilogram_type);

            // Append mobility array
            let mobility_values = mobility_builder.values();
            for &val in &mobilogram.mobility_array {
                mobility_values.append_value(val);
            }
            mobility_builder.append(true);

            // Append intensity array
            let intensity_values = intensity_builder.values();
            for &val in &mobilogram.intensity_array {
                intensity_values.append_value(val);
            }
            intensity_builder.append(true);

            self.data_points_written += mobilogram.len();
        }

        // Build the arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(id_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(mobility_builder.finish()),
            Arc::new(intensity_builder.finish()),
        ];

        // Create record batch
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        // Write the batch
        self.writer.write(&batch)?;

        self.mobilograms_written += mobilograms.len();

        Ok(())
    }

    /// Write a single mobilogram
    pub fn write_mobilogram(&mut self, mobilogram: &Mobilogram) -> Result<(), MobilogramWriterError> {
        self.write_mobilograms(&[mobilogram.clone()])
    }

    /// Flush any buffered data and finalize the file
    pub fn finish(self) -> Result<MobilogramWriterStats, MobilogramWriterError> {
        let file_metadata = self.writer.close()?;

        Ok(MobilogramWriterStats {
            mobilograms_written: self.mobilograms_written,
            data_points_written: self.data_points_written,
            row_groups_written: file_metadata.row_groups.len(),
            file_size_bytes: file_metadata
                .row_groups
                .iter()
                .map(|rg| rg.total_byte_size as u64)
                .sum(),
        })
    }

    /// Flush any buffered data, finalize the file, and return the underlying writer
    pub fn finish_into_inner(self) -> Result<W, MobilogramWriterError> {
        let inner = self.writer.into_inner()?;
        Ok(inner)
    }

    /// Get current statistics
    pub fn stats(&self) -> MobilogramWriterStats {
        MobilogramWriterStats {
            mobilograms_written: self.mobilograms_written,
            data_points_written: self.data_points_written,
            row_groups_written: 0,
            file_size_bytes: 0,
        }
    }
}

/// Statistics from a completed mobilogram write operation
#[derive(Debug, Clone)]
pub struct MobilogramWriterStats {
    /// Number of mobilograms written
    pub mobilograms_written: usize,
    /// Total number of data points written
    pub data_points_written: usize,
    /// Number of row groups written
    pub row_groups_written: usize,
    /// Total file size in bytes
    pub file_size_bytes: u64,
}

impl std::fmt::Display for MobilogramWriterStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Wrote {} mobilograms ({} data points) in {} row groups",
            self.mobilograms_written, self.data_points_written, self.row_groups_written
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_mobilogram_schema() {
        let schema = create_mobilogram_schema();
        assert_eq!(schema.fields().len(), 4);

        assert!(schema.field_with_name(mobilogram_columns::MOBILOGRAM_ID).is_ok());
        assert!(schema.field_with_name(mobilogram_columns::MOBILOGRAM_TYPE).is_ok());
        assert!(schema.field_with_name(mobilogram_columns::MOBILITY_ARRAY).is_ok());
        assert!(schema.field_with_name(mobilogram_columns::INTENSITY_ARRAY).is_ok());
    }

    #[test]
    fn test_mobilogram_creation() {
        let mobilogram = Mobilogram::new(
            "mob1".to_string(),
            "TIM".to_string(),
            vec![0.5, 0.6, 0.7, 0.8, 0.9],
            vec![100.0, 200.0, 500.0, 300.0, 150.0],
        ).unwrap();

        assert_eq!(mobilogram.len(), 5);
        assert!(!mobilogram.is_empty());
    }

    #[test]
    fn test_mobilogram_length_mismatch() {
        let result = Mobilogram::new(
            "mob1".to_string(),
            "TIM".to_string(),
            vec![0.5, 0.6, 0.7],
            vec![100.0, 200.0], // Mismatch!
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_write_mobilograms() -> Result<(), MobilogramWriterError> {
        let metadata = MzPeakMetadata::new();
        let config = MobilogramWriterConfig::default();

        let buffer = Cursor::new(Vec::new());
        let mut writer = MobilogramWriter::new(buffer, &metadata, config)?;

        let tim = Mobilogram::new_tim(
            "tim1".to_string(),
            vec![0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
            vec![100.0, 200.0, 500.0, 800.0, 400.0, 100.0],
        )?;

        let xim = Mobilogram::new_xim(
            "xim_500.25".to_string(),
            vec![0.55, 0.60, 0.65, 0.70],
            vec![50.0, 150.0, 300.0, 100.0],
        )?;

        writer.write_mobilogram(&tim)?;
        writer.write_mobilogram(&xim)?;

        let stats = writer.finish()?;
        assert_eq!(stats.mobilograms_written, 2);
        assert_eq!(stats.data_points_written, 10);

        Ok(())
    }
}
