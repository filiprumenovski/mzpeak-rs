use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::metadata::MzPeakMetadata;
use crate::schema::KEY_FORMAT_VERSION;

use super::{MzPeakReader, ReaderError};

/// Metadata extracted from an mzPeak file
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// Format version string
    pub format_version: String,
    /// Total number of rows (peaks) in the file
    pub total_rows: i64,
    /// Number of row groups
    pub num_row_groups: usize,
    /// Schema of the Parquet file
    pub schema: Arc<Schema>,
    /// Raw key-value metadata from Parquet footer
    pub key_value_metadata: HashMap<String, String>,
    /// Parsed mzPeak metadata (if available)
    pub mzpeak_metadata: Option<MzPeakMetadata>,
}

impl MzPeakReader {
    /// Extract metadata from a Parquet reader
    pub(super) fn extract_file_metadata<R: parquet::file::reader::ChunkReader + 'static>(
        reader: &SerializedFileReader<R>,
    ) -> Result<FileMetadata, ReaderError> {
        let parquet_metadata = reader.metadata();
        let file_meta = parquet_metadata.file_metadata();
        let schema = parquet::arrow::parquet_to_arrow_schema(
            file_meta.schema_descr(),
            file_meta.key_value_metadata(),
        )?;

        // Extract key-value metadata
        let mut kv_metadata = HashMap::new();
        if let Some(kv_list) = file_meta.key_value_metadata() {
            for kv in kv_list {
                if let Some(value) = &kv.value {
                    kv_metadata.insert(kv.key.clone(), value.clone());
                }
            }
        }

        // Get format version
        let format_version = kv_metadata
            .get(KEY_FORMAT_VERSION)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        // Try to parse MzPeakMetadata
        let mzpeak_metadata = MzPeakMetadata::from_parquet_metadata(&kv_metadata).ok();

        // Calculate total rows
        let total_rows: i64 = (0..parquet_metadata.num_row_groups())
            .map(|i| parquet_metadata.row_group(i).num_rows())
            .sum();

        Ok(FileMetadata {
            format_version,
            total_rows,
            num_row_groups: parquet_metadata.num_row_groups(),
            schema: Arc::new(schema),
            key_value_metadata: kv_metadata,
            mzpeak_metadata,
        })
    }

    /// Extract metadata from Bytes
    pub(super) fn extract_file_metadata_from_bytes(
        bytes: &Bytes,
    ) -> Result<FileMetadata, ReaderError> {
        let reader = SerializedFileReader::new(bytes.clone())?;
        Self::extract_file_metadata(&reader)
    }

    /// Get file metadata
    pub fn metadata(&self) -> &FileMetadata {
        &self.file_metadata
    }

    /// Get the total number of peaks (rows) in the file
    pub fn total_peaks(&self) -> i64 {
        self.file_metadata.total_rows
    }

    /// Get the Arrow schema
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.file_metadata.schema)
    }
}
