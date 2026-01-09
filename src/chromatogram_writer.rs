//! # Chromatogram Writer Module
//!
//! This module provides functionality for writing chromatogram data
//! to the mzPeak Parquet format using the "Wide" schema.
//!
//! Unlike the "Long" format used for peaks, chromatograms are stored as rows of arrays
//! (Time and Intensity vectors) to enable instant trace visualization without scanning
//! the entire peak table.

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
use crate::schema::{chromatogram_columns, create_chromatogram_schema_arc};

/// Errors that can occur during chromatogram writing
#[derive(Debug, thiserror::Error)]
pub enum ChromatogramWriterError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("Metadata error: {0}")]
    MetadataError(#[from] crate::metadata::MetadataError),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Array length mismatch: time array has {time_len} elements, intensity array has {intensity_len} elements")]
    ArrayLengthMismatch {
        time_len: usize,
        intensity_len: usize,
    },
}

/// Configuration for the chromatogram writer
#[derive(Debug, Clone)]
pub struct ChromatogramWriterConfig {
    /// Compression type to use (ZSTD level 3 recommended)
    pub compression_level: i32,

    /// Target row group size
    pub row_group_size: usize,

    /// Data page size in bytes
    pub data_page_size: usize,

    /// Whether to write statistics for columns
    pub write_statistics: bool,
}

impl Default for ChromatogramWriterConfig {
    fn default() -> Self {
        Self {
            compression_level: 3,
            row_group_size: 100, // Smaller than peaks since arrays are large
            data_page_size: 1024 * 1024,
            write_statistics: true,
        }
    }
}

impl ChromatogramWriterConfig {
    /// Create writer properties from this configuration
    fn to_writer_properties(&self, metadata: &std::collections::HashMap<String, String>) -> WriterProperties {
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
        // Arrays contain unique values, so dictionary encoding is inefficient
        builder = builder.set_column_dictionary_enabled(
            parquet::schema::types::ColumnPath::new(vec![chromatogram_columns::TIME_ARRAY.to_string()]),
            false,
        );
        builder = builder.set_column_dictionary_enabled(
            parquet::schema::types::ColumnPath::new(vec![chromatogram_columns::INTENSITY_ARRAY.to_string()]),
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

/// Represents a single chromatogram in the "Wide" format
#[derive(Debug, Clone)]
pub struct Chromatogram {
    /// Unique chromatogram identifier
    pub chromatogram_id: String,

    /// Type of chromatogram (TIC, BPC, SRM, etc.)
    pub chromatogram_type: String,

    /// Time values in seconds
    pub time_array: Vec<f64>,

    /// Intensity values
    pub intensity_array: Vec<f32>,
}

impl Chromatogram {
    /// Create a new chromatogram
    pub fn new(
        chromatogram_id: String,
        chromatogram_type: String,
        time_array: Vec<f64>,
        intensity_array: Vec<f32>,
    ) -> Result<Self, ChromatogramWriterError> {
        // Validate array lengths match
        if time_array.len() != intensity_array.len() {
            return Err(ChromatogramWriterError::ArrayLengthMismatch {
                time_len: time_array.len(),
                intensity_len: intensity_array.len(),
            });
        }

        Ok(Self {
            chromatogram_id,
            chromatogram_type,
            time_array,
            intensity_array,
        })
    }

    /// Get the number of data points in this chromatogram
    pub fn data_point_count(&self) -> usize {
        self.time_array.len()
    }
}

/// Streaming writer for chromatogram Parquet files
pub struct ChromatogramWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    #[allow(dead_code)]
    config: ChromatogramWriterConfig,
    chromatograms_written: usize,
    data_points_written: usize,
}

impl ChromatogramWriter<File> {
    /// Create a new writer to a file path
    pub fn new_file<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: ChromatogramWriterConfig,
    ) -> Result<Self, ChromatogramWriterError> {
        let file = File::create(path)?;
        Self::new(file, metadata, config)
    }
}

impl<W: Write + Send> ChromatogramWriter<W> {
    /// Create a new writer to any Write implementation
    pub fn new(
        writer: W,
        metadata: &MzPeakMetadata,
        config: ChromatogramWriterConfig,
    ) -> Result<Self, ChromatogramWriterError> {
        let schema = create_chromatogram_schema_arc();
        let parquet_metadata = metadata.to_parquet_metadata()?;
        let props = config.to_writer_properties(&parquet_metadata);

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            config,
            chromatograms_written: 0,
            data_points_written: 0,
        })
    }

    /// Write a batch of chromatograms to the file
    pub fn write_chromatograms(&mut self, chromatograms: &[Chromatogram]) -> Result<(), ChromatogramWriterError> {
        if chromatograms.is_empty() {
            return Ok(());
        }

        // Build arrays for each column
        let mut id_builder = StringBuilder::with_capacity(chromatograms.len(), 1024);
        let mut type_builder = StringBuilder::with_capacity(chromatograms.len(), 1024);
        
        // Create list builders with proper field definitions matching the schema
        let time_field = Arc::new(Field::new("item", DataType::Float64, false));
        let intensity_field = Arc::new(Field::new("item", DataType::Float32, false));
        let mut time_array_builder = ListBuilder::new(Float64Builder::new()).with_field(time_field);
        let mut intensity_array_builder = ListBuilder::new(Float32Builder::new()).with_field(intensity_field);

        // Process each chromatogram
        for chromatogram in chromatograms {
            // Validate array lengths
            if chromatogram.time_array.len() != chromatogram.intensity_array.len() {
                return Err(ChromatogramWriterError::ArrayLengthMismatch {
                    time_len: chromatogram.time_array.len(),
                    intensity_len: chromatogram.intensity_array.len(),
                });
            }

            // Append ID and type
            id_builder.append_value(&chromatogram.chromatogram_id);
            type_builder.append_value(&chromatogram.chromatogram_type);

            // Append time array
            for &time in &chromatogram.time_array {
                time_array_builder.values().append_value(time);
            }
            time_array_builder.append(true);

            // Append intensity array
            for &intensity in &chromatogram.intensity_array {
                intensity_array_builder.values().append_value(intensity);
            }
            intensity_array_builder.append(true);

            self.data_points_written += chromatogram.data_point_count();
        }

        // Build the arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(id_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(time_array_builder.finish()),
            Arc::new(intensity_array_builder.finish()),
        ];

        // Create record batch
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        // Write the batch
        self.writer.write(&batch)?;

        self.chromatograms_written += chromatograms.len();

        Ok(())
    }

    /// Write a single chromatogram
    pub fn write_chromatogram(&mut self, chromatogram: &Chromatogram) -> Result<(), ChromatogramWriterError> {
        self.write_chromatograms(&[chromatogram.clone()])
    }

    /// Flush any buffered data and finalize the file
    pub fn finish(self) -> Result<ChromatogramWriterStats, ChromatogramWriterError> {
        let file_metadata = self.writer.close()?;

        Ok(ChromatogramWriterStats {
            chromatograms_written: self.chromatograms_written,
            data_points_written: self.data_points_written,
            row_groups_written: file_metadata.row_groups.len(),
            file_size_bytes: file_metadata
                .row_groups
                .iter()
                .map(|rg| rg.total_byte_size as u64)
                .sum(),
        })
    }

    /// Finalize and return the inner writer (for buffer extraction)
    pub fn finish_into_inner(self) -> Result<W, ChromatogramWriterError> {
        // Close the writer and get the inner writer back
        let writer = self.writer.into_inner()?;
        Ok(writer)
    }

    /// Get current statistics
    pub fn stats(&self) -> ChromatogramWriterStats {
        ChromatogramWriterStats {
            chromatograms_written: self.chromatograms_written,
            data_points_written: self.data_points_written,
            row_groups_written: 0, // Unknown until finish
            file_size_bytes: 0,    // Unknown until finish
        }
    }
}

/// Statistics from a completed chromatogram write operation
#[derive(Debug, Clone)]
pub struct ChromatogramWriterStats {
    pub chromatograms_written: usize,
    pub data_points_written: usize,
    pub row_groups_written: usize,
    pub file_size_bytes: u64,
}

impl std::fmt::Display for ChromatogramWriterStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Wrote {} chromatograms ({} data points) in {} row groups",
            self.chromatograms_written, self.data_points_written, self.row_groups_written
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_chromatogram_creation() {
        let time = vec![0.0, 1.0, 2.0, 3.0];
        let intensity = vec![100.0, 200.0, 150.0, 50.0];
        
        let chromatogram = Chromatogram::new(
            "TIC".to_string(),
            "TIC".to_string(),
            time.clone(),
            intensity.clone(),
        );
        
        assert!(chromatogram.is_ok());
        let chromatogram = chromatogram.unwrap();
        assert_eq!(chromatogram.data_point_count(), 4);
    }

    #[test]
    fn test_chromatogram_array_mismatch() {
        let time = vec![0.0, 1.0, 2.0];
        let intensity = vec![100.0, 200.0]; // One less element
        
        let result = Chromatogram::new(
            "TIC".to_string(),
            "TIC".to_string(),
            time,
            intensity,
        );
        
        assert!(result.is_err());
        assert!(matches!(result, Err(ChromatogramWriterError::ArrayLengthMismatch { .. })));
    }

    #[test]
    fn test_write_chromatogram() -> Result<(), ChromatogramWriterError> {
        let metadata = MzPeakMetadata::new();
        let config = ChromatogramWriterConfig::default();

        let buffer = Cursor::new(Vec::new());
        let mut writer = ChromatogramWriter::new(buffer, &metadata, config)?;

        let chromatogram = Chromatogram::new(
            "TIC".to_string(),
            "TIC".to_string(),
            vec![0.0, 1.0, 2.0, 3.0],
            vec![100.0, 200.0, 150.0, 50.0],
        )?;

        writer.write_chromatogram(&chromatogram)?;

        let stats = writer.finish()?;
        assert_eq!(stats.chromatograms_written, 1);
        assert_eq!(stats.data_points_written, 4);

        Ok(())
    }

    #[test]
    fn test_write_multiple_chromatograms() -> Result<(), ChromatogramWriterError> {
        let metadata = MzPeakMetadata::new();
        let config = ChromatogramWriterConfig::default();

        let buffer = Cursor::new(Vec::new());
        let mut writer = ChromatogramWriter::new(buffer, &metadata, config)?;

        let tic = Chromatogram::new(
            "TIC".to_string(),
            "TIC".to_string(),
            vec![0.0, 1.0, 2.0],
            vec![100.0, 200.0, 150.0],
        )?;

        let bpc = Chromatogram::new(
            "BPC".to_string(),
            "BPC".to_string(),
            vec![0.0, 1.0, 2.0],
            vec![50.0, 150.0, 100.0],
        )?;

        writer.write_chromatograms(&[tic, bpc])?;

        let stats = writer.finish()?;
        assert_eq!(stats.chromatograms_written, 2);
        assert_eq!(stats.data_points_written, 6);

        Ok(())
    }
}
