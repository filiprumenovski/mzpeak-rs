//! # Spectra Writer for mzPeak v2.0
//!
//! This module provides the `SpectraWriter` for writing spectrum-level metadata
//! to the `spectra.parquet` file in the mzPeak v2.0 container format.
//!
//! ## Design
//!
//! The spectra table stores one row per spectrum with:
//! - Core identification: spectrum_id, scan_number, ms_level, retention_time, polarity
//! - Peak pointers: peak_offset, peak_count (linking to peaks.parquet)
//! - Precursor info: precursor_mz, precursor_charge, precursor_intensity (MS2+)
//! - Isolation window: isolation_window_lower, isolation_window_upper
//! - Fragmentation: collision_energy
//! - Summary stats: total_ion_current, base_peak_mz, base_peak_intensity, injection_time
//! - Imaging coords: pixel_x, pixel_y, pixel_z (MSI data only)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use mzpeak::writer::{SpectraWriter, SpectraWriterConfig};
//! use mzpeak::writer::types::SpectrumMetadata;
//!
//! let file = std::fs::File::create("spectra.parquet")?;
//! let config = SpectraWriterConfig::default();
//! let mut writer = SpectraWriter::new(file, &config)?;
//!
//! // Write spectrum metadata
//! let metadata = SpectrumMetadata::new_ms1(0, Some(1), 60.0, 1, 1000);
//! writer.write_spectrum_metadata(&metadata)?;
//!
//! // Finish and get stats
//! let stats = writer.finish()?;
//! ```

use std::collections::HashMap;
use std::io::{Seek, Write};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Builder, Float64Builder, Int32Builder, Int8Builder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::KeyValue;
use parquet::schema::types::ColumnPath;

use crate::schema::spectra_columns::{
    create_spectra_schema_arc, BASE_PEAK_INTENSITY, BASE_PEAK_MZ, COLLISION_ENERGY,
    INJECTION_TIME, ISOLATION_WINDOW_LOWER, ISOLATION_WINDOW_UPPER, MS_LEVEL, PEAK_OFFSET,
    POLARITY, PRECURSOR_CHARGE, PRECURSOR_INTENSITY, PRECURSOR_MZ, RETENTION_TIME,
    SPECTRUM_ID, TOTAL_ION_CURRENT,
};

use super::config::CompressionType;
use super::error::WriterError;
use super::types::SpectrumMetadata;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for the SpectraWriter
#[derive(Debug, Clone)]
pub struct SpectraWriterConfig {
    /// Compression type to use
    pub compression: CompressionType,

    /// Target row group size (number of spectra per group)
    /// Smaller = better random access, larger = better compression
    pub row_group_size: usize,

    /// Data page size in bytes
    pub data_page_size: usize,

    /// Whether to write statistics for columns
    pub write_statistics: bool,

    /// Dictionary encoding page size limit
    pub dictionary_page_size_limit: usize,

    /// Optional key-value metadata to include in the file
    pub metadata: HashMap<String, String>,
}

impl Default for SpectraWriterConfig {
    fn default() -> Self {
        Self {
            // ZSTD level 9 for good compression
            compression: CompressionType::Zstd(9),
            // 10k spectra per row group is a good balance for spectrum-level data
            row_group_size: 10_000,
            // 1MB data pages
            data_page_size: 1024 * 1024,
            write_statistics: true,
            // 1MB dictionary page limit
            dictionary_page_size_limit: 1024 * 1024,
            metadata: HashMap::new(),
        }
    }
}

impl SpectraWriterConfig {
    /// Create writer properties from this configuration
    fn to_writer_properties(&self) -> WriterProperties {
        let compression = match self.compression {
            CompressionType::Zstd(level) => {
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default()))
            }
            CompressionType::Snappy => Compression::SNAPPY,
            CompressionType::Uncompressed => Compression::UNCOMPRESSED,
        };

        let statistics = if self.write_statistics {
            EnabledStatistics::Chunk
        } else {
            EnabledStatistics::None
        };

        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_data_page_size_limit(self.data_page_size)
            .set_dictionary_page_size_limit(self.dictionary_page_size_limit)
            .set_statistics_enabled(statistics)
            .set_max_row_group_size(self.row_group_size);

        // Enable dictionary encoding for columns that benefit from it
        // These columns often have repeated values across many spectra
        let dict_columns = [
            MS_LEVEL,
            POLARITY,
            PRECURSOR_CHARGE,
        ];

        for col in dict_columns {
            builder = builder.set_column_dictionary_enabled(
                ColumnPath::new(vec![col.to_string()]),
                true,
            );
        }

        // Disable dictionary for high-cardinality columns
        let no_dict_columns = [
            SPECTRUM_ID,
            RETENTION_TIME,
            PEAK_OFFSET,
            PRECURSOR_MZ,
            TOTAL_ION_CURRENT,
            BASE_PEAK_MZ,
        ];

        for col in no_dict_columns {
            builder = builder.set_column_dictionary_enabled(
                ColumnPath::new(vec![col.to_string()]),
                false,
            );
        }

        // Use BYTE_STREAM_SPLIT for floating-point columns
        let float_columns = [
            RETENTION_TIME,
            PRECURSOR_MZ,
            PRECURSOR_INTENSITY,
            ISOLATION_WINDOW_LOWER,
            ISOLATION_WINDOW_UPPER,
            COLLISION_ENERGY,
            TOTAL_ION_CURRENT,
            BASE_PEAK_MZ,
            BASE_PEAK_INTENSITY,
            INJECTION_TIME,
        ];

        for col in float_columns {
            builder = builder.set_column_encoding(
                ColumnPath::new(vec![col.to_string()]),
                Encoding::BYTE_STREAM_SPLIT,
            );
        }

        // Add key-value metadata
        if !self.metadata.is_empty() {
            let kv_metadata: Vec<KeyValue> = self
                .metadata
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.clone(),
                    value: Some(v.clone()),
                })
                .collect();

            builder = builder.set_key_value_metadata(Some(kv_metadata));
        }

        builder.build()
    }
}

// =============================================================================
// Writer Statistics
// =============================================================================

/// Statistics from a completed spectra write operation
#[derive(Debug, Clone)]
pub struct SpectraWriterStats {
    /// Number of spectra written
    pub spectra_written: u64,
    /// Number of Parquet row groups written
    pub row_groups_written: usize,
    /// Total file size in bytes (approximate)
    pub file_size_bytes: u64,
}

impl std::fmt::Display for SpectraWriterStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Wrote {} spectra in {} row groups ({} bytes)",
            self.spectra_written, self.row_groups_written, self.file_size_bytes
        )
    }
}

// =============================================================================
// Column Buffers
// =============================================================================

/// Buffered column data for efficient batch writing
#[derive(Debug)]
struct ColumnBuffers {
    // Required columns
    spectrum_id: Vec<u32>,
    scan_number: Vec<Option<i32>>,
    ms_level: Vec<u8>,
    retention_time: Vec<f32>,
    polarity: Vec<i8>,
    peak_offset: Vec<u64>,
    peak_count: Vec<u32>,

    // Precursor info (nullable)
    precursor_mz: Vec<Option<f64>>,
    precursor_charge: Vec<Option<i8>>,
    precursor_intensity: Vec<Option<f32>>,

    // Isolation window (nullable)
    isolation_window_lower: Vec<Option<f32>>,
    isolation_window_upper: Vec<Option<f32>>,

    // Fragmentation (nullable)
    collision_energy: Vec<Option<f32>>,

    // Summary stats (nullable)
    total_ion_current: Vec<Option<f64>>,
    base_peak_mz: Vec<Option<f64>>,
    base_peak_intensity: Vec<Option<f32>>,
    injection_time: Vec<Option<f32>>,

    // Imaging coords (nullable)
    pixel_x: Vec<Option<u16>>,
    pixel_y: Vec<Option<u16>>,
    pixel_z: Vec<Option<u16>>,
}

impl ColumnBuffers {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            spectrum_id: Vec::with_capacity(capacity),
            scan_number: Vec::with_capacity(capacity),
            ms_level: Vec::with_capacity(capacity),
            retention_time: Vec::with_capacity(capacity),
            polarity: Vec::with_capacity(capacity),
            peak_offset: Vec::with_capacity(capacity),
            peak_count: Vec::with_capacity(capacity),
            precursor_mz: Vec::with_capacity(capacity),
            precursor_charge: Vec::with_capacity(capacity),
            precursor_intensity: Vec::with_capacity(capacity),
            isolation_window_lower: Vec::with_capacity(capacity),
            isolation_window_upper: Vec::with_capacity(capacity),
            collision_energy: Vec::with_capacity(capacity),
            total_ion_current: Vec::with_capacity(capacity),
            base_peak_mz: Vec::with_capacity(capacity),
            base_peak_intensity: Vec::with_capacity(capacity),
            injection_time: Vec::with_capacity(capacity),
            pixel_x: Vec::with_capacity(capacity),
            pixel_y: Vec::with_capacity(capacity),
            pixel_z: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.spectrum_id.len()
    }

    fn is_empty(&self) -> bool {
        self.spectrum_id.is_empty()
    }

    fn clear(&mut self) {
        self.spectrum_id.clear();
        self.scan_number.clear();
        self.ms_level.clear();
        self.retention_time.clear();
        self.polarity.clear();
        self.peak_offset.clear();
        self.peak_count.clear();
        self.precursor_mz.clear();
        self.precursor_charge.clear();
        self.precursor_intensity.clear();
        self.isolation_window_lower.clear();
        self.isolation_window_upper.clear();
        self.collision_energy.clear();
        self.total_ion_current.clear();
        self.base_peak_mz.clear();
        self.base_peak_intensity.clear();
        self.injection_time.clear();
        self.pixel_x.clear();
        self.pixel_y.clear();
        self.pixel_z.clear();
    }

    /// Push a spectrum's metadata into the buffers
    fn push(&mut self, metadata: &SpectrumMetadata, peak_offset: u64) {
        self.spectrum_id.push(metadata.spectrum_id);
        self.scan_number.push(metadata.scan_number);
        self.ms_level.push(metadata.ms_level);
        self.retention_time.push(metadata.retention_time);
        self.polarity.push(metadata.polarity);
        self.peak_offset.push(peak_offset);
        self.peak_count.push(metadata.peak_count);
        self.precursor_mz.push(metadata.precursor_mz);
        self.precursor_charge.push(metadata.precursor_charge);
        self.precursor_intensity.push(metadata.precursor_intensity);
        self.isolation_window_lower.push(metadata.isolation_window_lower);
        self.isolation_window_upper.push(metadata.isolation_window_upper);
        self.collision_energy.push(metadata.collision_energy);
        self.total_ion_current.push(metadata.total_ion_current);
        self.base_peak_mz.push(metadata.base_peak_mz);
        self.base_peak_intensity.push(metadata.base_peak_intensity);
        self.injection_time.push(metadata.injection_time);
        self.pixel_x.push(metadata.pixel_x);
        self.pixel_y.push(metadata.pixel_y);
        self.pixel_z.push(metadata.pixel_z);
    }
}

// =============================================================================
// SpectraWriter Implementation
// =============================================================================

/// Writer for spectra.parquet files in mzPeak v2.0 format.
///
/// This writer handles one row per spectrum with spectrum-level metadata.
/// It buffers rows and flushes them to row groups for efficient Parquet writing.
///
/// # Example
///
/// ```rust,ignore
/// use mzpeak::writer::{SpectraWriter, SpectraWriterConfig};
/// use mzpeak::writer::types::SpectrumMetadata;
/// use std::fs::File;
///
/// let file = File::create("spectra.parquet")?;
/// let config = SpectraWriterConfig::default();
/// let mut writer = SpectraWriter::new(file, &config)?;
///
/// // Write multiple spectra
/// for i in 0..100 {
///     let metadata = SpectrumMetadata::new_ms1(i, Some(i as i32 + 1), i as f32 * 0.1, 1, 500);
///     writer.write_spectrum_metadata(&metadata)?;
/// }
///
/// let stats = writer.finish()?;
/// println!("Written: {}", stats);
/// ```
pub struct SpectraWriter<W: Write + Seek> {
    writer: ArrowWriter<W>,
    schema: Arc<arrow::datatypes::Schema>,
    row_group_size: usize,
    spectra_written: u64,
    buffers: ColumnBuffers,
    /// Current byte offset in the peaks.parquet file (tracked externally)
    current_peak_offset: u64,
}

impl<W: Write + Seek + Send> SpectraWriter<W> {
    /// Create a new SpectraWriter with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `writer` - The underlying writer (file, buffer, etc.)
    /// * `config` - Writer configuration
    ///
    /// # Returns
    ///
    /// A new SpectraWriter ready to write spectrum metadata.
    pub fn new(writer: W, config: &SpectraWriterConfig) -> Result<Self, WriterError> {
        let schema = create_spectra_schema_arc();
        let props = config.to_writer_properties();

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            row_group_size: config.row_group_size,
            spectra_written: 0,
            buffers: ColumnBuffers::with_capacity(config.row_group_size),
            current_peak_offset: 0,
        })
    }

    /// Set the current peak offset for tracking byte positions in peaks.parquet.
    ///
    /// This should be called before writing spectrum metadata to track where
    /// each spectrum's peaks are located in the peaks file.
    pub fn set_peak_offset(&mut self, offset: u64) {
        self.current_peak_offset = offset;
    }

    /// Get the current peak offset.
    pub fn peak_offset(&self) -> u64 {
        self.current_peak_offset
    }

    /// Write a single spectrum's metadata.
    ///
    /// The peak_offset is taken from the current internal offset. Call
    /// `set_peak_offset` before this method if you need to track peak positions.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The spectrum metadata to write
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if writing fails.
    pub fn write_spectrum_metadata(&mut self, metadata: &SpectrumMetadata) -> Result<(), WriterError> {
        self.buffers.push(metadata, self.current_peak_offset);
        self.spectra_written += 1;

        // Flush if buffer is full
        if self.buffers.len() >= self.row_group_size {
            self.flush_buffers()?;
        }

        Ok(())
    }

    /// Write a single spectrum's metadata with an explicit peak offset.
    ///
    /// # Arguments
    ///
    /// * `metadata` - The spectrum metadata to write
    /// * `peak_offset` - The byte offset of this spectrum's peaks in peaks.parquet
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if writing fails.
    pub fn write_spectrum_metadata_with_offset(
        &mut self,
        metadata: &SpectrumMetadata,
        peak_offset: u64,
    ) -> Result<(), WriterError> {
        self.buffers.push(metadata, peak_offset);
        self.spectra_written += 1;

        // Flush if buffer is full
        if self.buffers.len() >= self.row_group_size {
            self.flush_buffers()?;
        }

        Ok(())
    }

    /// Write a batch of spectrum metadata.
    ///
    /// This is more efficient than writing spectra one at a time when you have
    /// multiple spectra ready.
    ///
    /// # Arguments
    ///
    /// * `metadata_batch` - Iterator of (SpectrumMetadata, peak_offset) tuples
    pub fn write_spectrum_metadata_batch<'a, I>(
        &mut self,
        metadata_batch: I,
    ) -> Result<(), WriterError>
    where
        I: IntoIterator<Item = (&'a SpectrumMetadata, u64)>,
    {
        for (metadata, peak_offset) in metadata_batch {
            self.buffers.push(metadata, peak_offset);
            self.spectra_written += 1;

            // Flush if buffer is full
            if self.buffers.len() >= self.row_group_size {
                self.flush_buffers()?;
            }
        }

        Ok(())
    }

    /// Flush buffered data to the underlying writer.
    fn flush_buffers(&mut self) -> Result<(), WriterError> {
        if self.buffers.is_empty() {
            return Ok(());
        }

        let arrays = self.build_arrays()?;
        let record_batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.writer.write(&record_batch)?;
        self.buffers.clear();

        Ok(())
    }

    /// Build Arrow arrays from the buffered data.
    fn build_arrays(&self) -> Result<Vec<ArrayRef>, WriterError> {
        let len = self.buffers.len();

        // Build arrays in schema order (20 columns)
        let arrays: Vec<ArrayRef> = vec![
            // 1. spectrum_id (UInt32, required)
            Self::build_u32_array(&self.buffers.spectrum_id),
            // 2. scan_number (Int32, nullable)
            Self::build_optional_i32_array(&self.buffers.scan_number, len),
            // 3. ms_level (UInt8, required)
            Self::build_u8_array(&self.buffers.ms_level),
            // 4. retention_time (Float32, required)
            Self::build_f32_array(&self.buffers.retention_time),
            // 5. polarity (Int8, required)
            Self::build_i8_array(&self.buffers.polarity),
            // 6. peak_offset (UInt64, required)
            Self::build_u64_array(&self.buffers.peak_offset),
            // 7. peak_count (UInt32, required)
            Self::build_u32_array(&self.buffers.peak_count),
            // 8. precursor_mz (Float64, nullable)
            Self::build_optional_f64_array(&self.buffers.precursor_mz, len),
            // 9. precursor_charge (Int8, nullable)
            Self::build_optional_i8_array(&self.buffers.precursor_charge, len),
            // 10. precursor_intensity (Float32, nullable)
            Self::build_optional_f32_array(&self.buffers.precursor_intensity, len),
            // 11. isolation_window_lower (Float32, nullable)
            Self::build_optional_f32_array(&self.buffers.isolation_window_lower, len),
            // 12. isolation_window_upper (Float32, nullable)
            Self::build_optional_f32_array(&self.buffers.isolation_window_upper, len),
            // 13. collision_energy (Float32, nullable)
            Self::build_optional_f32_array(&self.buffers.collision_energy, len),
            // 14. total_ion_current (Float64, nullable)
            Self::build_optional_f64_array(&self.buffers.total_ion_current, len),
            // 15. base_peak_mz (Float64, nullable)
            Self::build_optional_f64_array(&self.buffers.base_peak_mz, len),
            // 16. base_peak_intensity (Float32, nullable)
            Self::build_optional_f32_array(&self.buffers.base_peak_intensity, len),
            // 17. injection_time (Float32, nullable)
            Self::build_optional_f32_array(&self.buffers.injection_time, len),
            // 18. pixel_x (UInt16, nullable)
            Self::build_optional_u16_array(&self.buffers.pixel_x, len),
            // 19. pixel_y (UInt16, nullable)
            Self::build_optional_u16_array(&self.buffers.pixel_y, len),
            // 20. pixel_z (UInt16, nullable)
            Self::build_optional_u16_array(&self.buffers.pixel_z, len),
        ];

        Ok(arrays)
    }

    // =========================================================================
    // Array Builder Helpers
    // =========================================================================

    /// Build a UInt32 array from a slice
    #[inline]
    fn build_u32_array(data: &[u32]) -> ArrayRef {
        let mut builder = UInt32Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build a UInt64 array from a slice
    #[inline]
    fn build_u64_array(data: &[u64]) -> ArrayRef {
        let mut builder = UInt64Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build a UInt8 array from a slice
    #[inline]
    fn build_u8_array(data: &[u8]) -> ArrayRef {
        let mut builder = UInt8Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build a Float32 array from a slice
    #[inline]
    fn build_f32_array(data: &[f32]) -> ArrayRef {
        let mut builder = Float32Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build an Int8 array from a slice
    #[inline]
    fn build_i8_array(data: &[i8]) -> ArrayRef {
        let mut builder = Int8Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    /// Build an optional Int32 array
    #[inline]
    fn build_optional_i32_array(data: &[Option<i32>], len: usize) -> ArrayRef {
        let mut builder = Int32Builder::with_capacity(len);
        for val in data {
            builder.append_option(*val);
        }
        Arc::new(builder.finish())
    }

    /// Build an optional Int8 array
    #[inline]
    fn build_optional_i8_array(data: &[Option<i8>], len: usize) -> ArrayRef {
        let mut builder = Int8Builder::with_capacity(len);
        for val in data {
            builder.append_option(*val);
        }
        Arc::new(builder.finish())
    }

    /// Build an optional Float32 array
    #[inline]
    fn build_optional_f32_array(data: &[Option<f32>], len: usize) -> ArrayRef {
        let mut builder = Float32Builder::with_capacity(len);
        for val in data {
            builder.append_option(*val);
        }
        Arc::new(builder.finish())
    }

    /// Build an optional Float64 array
    #[inline]
    fn build_optional_f64_array(data: &[Option<f64>], len: usize) -> ArrayRef {
        let mut builder = Float64Builder::with_capacity(len);
        for val in data {
            builder.append_option(*val);
        }
        Arc::new(builder.finish())
    }

    /// Build an optional UInt16 array
    #[inline]
    fn build_optional_u16_array(data: &[Option<u16>], len: usize) -> ArrayRef {
        let mut builder = UInt16Builder::with_capacity(len);
        for val in data {
            builder.append_option(*val);
        }
        Arc::new(builder.finish())
    }

    /// Finish writing and close the file.
    ///
    /// This method:
    /// 1. Flushes any remaining buffered data
    /// 2. Writes the Parquet footer
    /// 3. Returns statistics about the written data
    ///
    /// # Returns
    ///
    /// Statistics about the completed write operation.
    pub fn finish(mut self) -> Result<SpectraWriterStats, WriterError> {
        // Flush any remaining data
        self.flush_buffers()?;

        // Close the writer
        let file_metadata = self.writer.close()?;

        Ok(SpectraWriterStats {
            spectra_written: self.spectra_written,
            row_groups_written: file_metadata.row_groups.len(),
            file_size_bytes: file_metadata
                .row_groups
                .iter()
                .map(|rg| rg.total_byte_size as u64)
                .sum(),
        })
    }

    /// Finish writing and return the underlying writer.
    ///
    /// This is useful when writing to an in-memory buffer.
    pub fn finish_into_inner(mut self) -> Result<W, WriterError> {
        // Flush any remaining data
        self.flush_buffers()?;

        // Close and return the inner writer
        let inner = self.writer.into_inner()?;
        Ok(inner)
    }

    /// Get current write statistics (without closing).
    pub fn stats(&self) -> SpectraWriterStats {
        SpectraWriterStats {
            spectra_written: self.spectra_written,
            row_groups_written: 0, // Unknown until finish
            file_size_bytes: 0,    // Unknown until finish
        }
    }

    /// Get the number of spectra written so far.
    pub fn spectra_written(&self) -> u64 {
        self.spectra_written
    }

    /// Get the number of spectra currently buffered (not yet flushed).
    pub fn buffered_count(&self) -> usize {
        self.buffers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_spectra_writer_config_default() {
        let config = SpectraWriterConfig::default();
        assert_eq!(config.row_group_size, 10_000);
        assert!(config.write_statistics);
    }

    #[test]
    fn test_spectra_writer_basic() {
        let buffer = Cursor::new(Vec::new());
        let config = SpectraWriterConfig {
            row_group_size: 100,
            ..Default::default()
        };

        let mut writer = SpectraWriter::new(buffer, &config).expect("Failed to create writer");

        // Write some spectra
        for i in 0..50 {
            let metadata = SpectrumMetadata::new_ms1(i, Some(i as i32 + 1), i as f32 * 0.1, 1, 100);
            writer
                .write_spectrum_metadata(&metadata)
                .expect("Failed to write spectrum");
        }

        assert_eq!(writer.spectra_written(), 50);
        assert_eq!(writer.buffered_count(), 50);

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.spectra_written, 50);
    }

    #[test]
    fn test_spectra_writer_with_ms2() {
        let buffer = Cursor::new(Vec::new());
        let config = SpectraWriterConfig::default();

        let mut writer = SpectraWriter::new(buffer, &config).expect("Failed to create writer");

        // Write MS2 spectrum with precursor info
        let mut metadata = SpectrumMetadata::new_ms2(0, Some(1), 60.0, 1, 500, 456.789);
        metadata.precursor_charge = Some(2);
        metadata.precursor_intensity = Some(10000.0);
        metadata.collision_energy = Some(30.0);
        metadata.total_ion_current = Some(1000000.0);
        metadata.base_peak_mz = Some(234.567);
        metadata.base_peak_intensity = Some(50000.0);

        writer
            .write_spectrum_metadata_with_offset(&metadata, 1024)
            .expect("Failed to write spectrum");

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.spectra_written, 1);
    }

    #[test]
    fn test_spectra_writer_batch() {
        let buffer = Cursor::new(Vec::new());
        let config = SpectraWriterConfig::default();

        let mut writer = SpectraWriter::new(buffer, &config).expect("Failed to create writer");

        // Create a batch of spectra
        let spectra: Vec<SpectrumMetadata> = (0..100)
            .map(|i| SpectrumMetadata::new_ms1(i, Some(i as i32), i as f32 * 0.5, 1, 200))
            .collect();

        // Write as batch with offsets
        let batch: Vec<_> = spectra.iter().enumerate().map(|(i, m)| (m, i as u64 * 1000)).collect();
        writer
            .write_spectrum_metadata_batch(batch)
            .expect("Failed to write batch");

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.spectra_written, 100);
    }

    #[test]
    fn test_spectra_writer_flush_on_full_buffer() {
        let buffer = Cursor::new(Vec::new());
        let config = SpectraWriterConfig {
            row_group_size: 10, // Small buffer to trigger flush
            ..Default::default()
        };

        let mut writer = SpectraWriter::new(buffer, &config).expect("Failed to create writer");

        // Write more than buffer size
        for i in 0..25 {
            let metadata = SpectrumMetadata::new_ms1(i, Some(i as i32), i as f32, 1, 50);
            writer
                .write_spectrum_metadata(&metadata)
                .expect("Failed to write spectrum");
        }

        // Should have flushed twice (10 + 10), with 5 remaining
        assert_eq!(writer.buffered_count(), 5);
        assert_eq!(writer.spectra_written(), 25);

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.spectra_written, 25);
        assert!(stats.row_groups_written >= 2);
    }

    #[test]
    fn test_spectra_writer_imaging_data() {
        let buffer = Cursor::new(Vec::new());
        let config = SpectraWriterConfig::default();

        let mut writer = SpectraWriter::new(buffer, &config).expect("Failed to create writer");

        // Write imaging spectrum with pixel coordinates
        let mut metadata = SpectrumMetadata::new_ms1(0, None, 0.0, 1, 1000);
        metadata.pixel_x = Some(10);
        metadata.pixel_y = Some(20);
        metadata.pixel_z = Some(1);

        writer
            .write_spectrum_metadata(&metadata)
            .expect("Failed to write spectrum");

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.spectra_written, 1);
    }
}
