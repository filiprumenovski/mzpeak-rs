//! # Peaks Writer for mzPeak v2.0
//!
//! This module provides the `PeaksWriterV2` for writing peak-level data
//! to the `peaks.parquet` file in the mzPeak v2.0 container format.
//!
//! ## Design
//!
//! The v2.0 peaks table has a simplified schema with only 3-4 columns:
//! - spectrum_id (UInt32) - uses DELTA_BINARY_PACKED encoding
//! - mz (Float64) - uses BYTE_STREAM_SPLIT encoding
//! - intensity (Float32) - uses BYTE_STREAM_SPLIT encoding
//! - ion_mobility (Float64, optional) - uses BYTE_STREAM_SPLIT encoding
//!
//! ## Usage
//!
//! ```rust,ignore
//! use mzpeak::writer::{PeaksWriterV2, PeaksWriterV2Config};
//! use mzpeak::writer::types::PeakArraysV2;
//!
//! let file = std::fs::File::create("peaks.parquet")?;
//! let config = PeaksWriterV2Config::default();
//! let mut writer = PeaksWriterV2::new(file, &config, true)?; // true = has ion mobility
//!
//! // Write peaks for a spectrum
//! let peaks = PeakArraysV2::new(vec![100.0, 200.0], vec![1000.0, 500.0]);
//! writer.write_peaks(0, &peaks)?;
//!
//! // Finish and get stats
//! let stats = writer.finish()?;
//! ```

use std::collections::HashMap;
use std::io::{Seek, Write};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Builder, Float64Builder, UInt32Builder};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::KeyValue;
use parquet::schema::types::ColumnPath;

use crate::schema::create_peaks_schema_v2_arc;

use super::config::CompressionType;
use super::error::WriterError;
use super::types::PeakArraysV2;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for the PeaksWriterV2
#[derive(Debug, Clone)]
pub struct PeaksWriterV2Config {
    /// Compression type to use
    pub compression: CompressionType,

    /// Target row group size (number of peaks per group)
    /// Smaller = better random access, larger = better compression
    pub row_group_size: usize,

    /// Data page size in bytes
    pub data_page_size: usize,

    /// Whether to write statistics for columns
    pub write_statistics: bool,

    /// Enable BYTE_STREAM_SPLIT encoding for floating-point columns
    pub use_byte_stream_split: bool,

    /// Optional key-value metadata to include in the file
    pub metadata: HashMap<String, String>,
}

impl Default for PeaksWriterV2Config {
    fn default() -> Self {
        Self {
            // ZSTD level 9 for good compression
            compression: CompressionType::Zstd(9),
            // 500k peaks per row group balances compression vs random access
            row_group_size: 500_000,
            // 1MB data pages
            data_page_size: 1024 * 1024,
            write_statistics: true,
            // BYTE_STREAM_SPLIT improves compression for floating-point data
            use_byte_stream_split: true,
            metadata: HashMap::new(),
        }
    }
}

impl PeaksWriterV2Config {
    /// Create writer properties from this configuration
    fn to_writer_properties(&self, has_ion_mobility: bool) -> WriterProperties {
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
            .set_statistics_enabled(statistics)
            .set_max_row_group_size(self.row_group_size);

        // Disable dictionary encoding for all columns (high-cardinality data)
        builder = builder.set_dictionary_enabled(false);

        // Use DELTA_BINARY_PACKED for spectrum_id column
        // This encoding is optimal for monotonically increasing/grouped integers
        builder = builder.set_column_encoding(
            ColumnPath::new(vec!["spectrum_id".to_string()]),
            Encoding::DELTA_BINARY_PACKED,
        );

        // Use BYTE_STREAM_SPLIT for floating-point columns
        if self.use_byte_stream_split {
            let mut float_columns = vec!["mz", "intensity"];
            if has_ion_mobility {
                float_columns.push("ion_mobility");
            }
            for col in float_columns {
                builder = builder.set_column_encoding(
                    ColumnPath::new(vec![col.to_string()]),
                    Encoding::BYTE_STREAM_SPLIT,
                );
            }
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

/// Statistics from a completed peaks write operation
#[derive(Debug, Clone)]
pub struct PeaksWriterV2Stats {
    /// Number of peaks written
    pub peaks_written: u64,
    /// Number of spectra written
    pub spectra_written: u64,
    /// Number of Parquet row groups written
    pub row_groups_written: usize,
    /// Total file size in bytes (approximate)
    pub file_size_bytes: u64,
}

impl std::fmt::Display for PeaksWriterV2Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Wrote {} peaks from {} spectra in {} row groups ({} bytes)",
            self.peaks_written, self.spectra_written, self.row_groups_written, self.file_size_bytes
        )
    }
}

// =============================================================================
// Column Buffers
// =============================================================================

/// Buffered column data for efficient batch writing
#[derive(Debug)]
struct ColumnBuffers {
    spectrum_id: Vec<u32>,
    mz: Vec<f64>,
    intensity: Vec<f32>,
    ion_mobility: Option<Vec<f64>>,
}

impl ColumnBuffers {
    fn new(has_ion_mobility: bool, capacity: usize) -> Self {
        Self {
            spectrum_id: Vec::with_capacity(capacity),
            mz: Vec::with_capacity(capacity),
            intensity: Vec::with_capacity(capacity),
            ion_mobility: if has_ion_mobility {
                Some(Vec::with_capacity(capacity))
            } else {
                None
            },
        }
    }

    fn len(&self) -> usize {
        self.mz.len()
    }

    fn is_empty(&self) -> bool {
        self.mz.is_empty()
    }

    fn clear(&mut self) {
        self.spectrum_id.clear();
        self.mz.clear();
        self.intensity.clear();
        if let Some(ref mut im) = self.ion_mobility {
            im.clear();
        }
    }

    /// Push peaks for a spectrum into the buffers
    fn push_spectrum(&mut self, spectrum_id: u32, peaks: &PeakArraysV2) {
        let peak_count = peaks.len();

        // Extend spectrum_id with repeated values
        self.spectrum_id.extend(std::iter::repeat(spectrum_id).take(peak_count));

        // Extend mz and intensity
        self.mz.extend_from_slice(&peaks.mz);
        self.intensity.extend_from_slice(&peaks.intensity);

        // Extend ion_mobility if present
        if let Some(ref mut im_buf) = self.ion_mobility {
            if let Some(ref im_data) = peaks.ion_mobility {
                im_buf.extend_from_slice(im_data);
            } else {
                // If peaks don't have ion mobility but we expect it, fill with NaN
                im_buf.extend(std::iter::repeat(f64::NAN).take(peak_count));
            }
        }
    }
}

// =============================================================================
// PeaksWriterV2 Implementation
// =============================================================================

/// Writer for peaks.parquet files in mzPeak v2.0 format.
///
/// This writer handles one row per peak with the simplified v2.0 schema.
/// It buffers rows and flushes them to row groups for efficient Parquet writing.
///
/// # Example
///
/// ```rust,ignore
/// use mzpeak::writer::{PeaksWriterV2, PeaksWriterV2Config};
/// use mzpeak::writer::types::PeakArraysV2;
/// use std::fs::File;
///
/// let file = File::create("peaks.parquet")?;
/// let config = PeaksWriterV2Config::default();
/// let mut writer = PeaksWriterV2::new(file, &config, false)?; // 3D data
///
/// // Write peaks for multiple spectra
/// for i in 0..100 {
///     let peaks = PeakArraysV2::new(
///         vec![100.0 + i as f64, 200.0 + i as f64],
///         vec![1000.0, 500.0],
///     );
///     writer.write_peaks(i, &peaks)?;
/// }
///
/// let stats = writer.finish()?;
/// println!("Written: {}", stats);
/// ```
pub struct PeaksWriterV2<W: Write + Seek> {
    writer: ArrowWriter<W>,
    schema: Arc<arrow::datatypes::Schema>,
    row_group_size: usize,
    has_ion_mobility: bool,
    peaks_written: u64,
    spectra_written: u64,
    buffers: ColumnBuffers,
}

impl<W: Write + Seek + Send> PeaksWriterV2<W> {
    fn validate_ion_mobility(&self, peaks: &PeakArraysV2) -> Result<(), WriterError> {
        match (self.has_ion_mobility, peaks.ion_mobility.as_ref()) {
            (true, Some(_)) => Ok(()),
            (false, None) => Ok(()),
            (true, None) => Err(WriterError::InvalidData(
                "ion_mobility missing for modality requiring it".to_string(),
            )),
            (false, Some(_)) => Err(WriterError::InvalidData(
                "ion_mobility present for modality without it".to_string(),
            )),
        }
    }

    /// Create a new PeaksWriterV2 with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `writer` - The underlying writer (file, buffer, etc.)
    /// * `config` - Writer configuration
    /// * `has_ion_mobility` - Whether to include the ion_mobility column
    ///
    /// # Returns
    ///
    /// A new PeaksWriterV2 ready to write peak data.
    pub fn new(
        writer: W,
        config: &PeaksWriterV2Config,
        has_ion_mobility: bool,
    ) -> Result<Self, WriterError> {
        let schema = create_peaks_schema_v2_arc(has_ion_mobility);
        let props = config.to_writer_properties(has_ion_mobility);

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            row_group_size: config.row_group_size,
            has_ion_mobility,
            peaks_written: 0,
            spectra_written: 0,
            buffers: ColumnBuffers::new(has_ion_mobility, config.row_group_size),
        })
    }

    /// Write peaks for a single spectrum.
    ///
    /// # Arguments
    ///
    /// * `spectrum_id` - The spectrum identifier (0-indexed)
    /// * `peaks` - The peak data arrays
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if writing fails.
    pub fn write_peaks(&mut self, spectrum_id: u32, peaks: &PeakArraysV2) -> Result<(), WriterError> {
        if peaks.is_empty() {
            return Ok(());
        }

        self.validate_ion_mobility(peaks)?;
        self.buffers.push_spectrum(spectrum_id, peaks);
        self.peaks_written += peaks.len() as u64;
        self.spectra_written += 1;

        // Flush if buffer is full
        if self.buffers.len() >= self.row_group_size {
            self.flush_buffers()?;
        }

        Ok(())
    }

    /// Write peaks for multiple spectra in a batch.
    ///
    /// # Arguments
    ///
    /// * `batch` - Iterator of (spectrum_id, peaks) tuples
    pub fn write_peaks_batch<'a, I>(&mut self, batch: I) -> Result<(), WriterError>
    where
        I: IntoIterator<Item = (u32, &'a PeakArraysV2)>,
    {
        for (spectrum_id, peaks) in batch {
            if peaks.is_empty() {
                continue;
            }

            self.validate_ion_mobility(peaks)?;
            self.buffers.push_spectrum(spectrum_id, peaks);
            self.peaks_written += peaks.len() as u64;
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

        let arrays = self.build_arrays();
        let record_batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.writer.write(&record_batch)?;
        self.buffers.clear();

        Ok(())
    }

    /// Build Arrow arrays from the buffered data.
    fn build_arrays(&self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = vec![
            // spectrum_id (UInt32)
            Self::build_u32_array(&self.buffers.spectrum_id),
            // mz (Float64)
            Self::build_f64_array(&self.buffers.mz),
            // intensity (Float32)
            Self::build_f32_array(&self.buffers.intensity),
        ];

        // ion_mobility (Float64, optional)
        if let Some(ref im) = self.buffers.ion_mobility {
            arrays.push(Self::build_f64_array(im));
        }

        arrays
    }

    // =========================================================================
    // Array Builder Helpers
    // =========================================================================

    #[inline]
    fn build_u32_array(data: &[u32]) -> ArrayRef {
        let mut builder = UInt32Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    #[inline]
    fn build_f64_array(data: &[f64]) -> ArrayRef {
        let mut builder = Float64Builder::with_capacity(data.len());
        builder.append_slice(data);
        Arc::new(builder.finish())
    }

    #[inline]
    fn build_f32_array(data: &[f32]) -> ArrayRef {
        let mut builder = Float32Builder::with_capacity(data.len());
        builder.append_slice(data);
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
    pub fn finish(mut self) -> Result<PeaksWriterV2Stats, WriterError> {
        // Flush any remaining data
        self.flush_buffers()?;

        // Close the writer
        let file_metadata = self.writer.close()?;

        Ok(PeaksWriterV2Stats {
            peaks_written: self.peaks_written,
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
    pub fn stats(&self) -> PeaksWriterV2Stats {
        PeaksWriterV2Stats {
            peaks_written: self.peaks_written,
            spectra_written: self.spectra_written,
            row_groups_written: 0, // Unknown until finish
            file_size_bytes: 0,    // Unknown until finish
        }
    }

    /// Get the number of peaks written so far.
    pub fn peaks_written(&self) -> u64 {
        self.peaks_written
    }

    /// Get the number of spectra written so far.
    pub fn spectra_written(&self) -> u64 {
        self.spectra_written
    }

    /// Get the number of peaks currently buffered (not yet flushed).
    pub fn buffered_count(&self) -> usize {
        self.buffers.len()
    }

    /// Returns whether this writer includes ion mobility data.
    pub fn has_ion_mobility(&self) -> bool {
        self.has_ion_mobility
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_peaks_writer_v2_config_default() {
        let config = PeaksWriterV2Config::default();
        assert_eq!(config.row_group_size, 500_000);
        assert!(config.write_statistics);
        assert!(config.use_byte_stream_split);
    }

    #[test]
    fn test_peaks_writer_v2_basic_3d() {
        let buffer = Cursor::new(Vec::new());
        let config = PeaksWriterV2Config {
            row_group_size: 100,
            ..Default::default()
        };

        let mut writer = PeaksWriterV2::new(buffer, &config, false).expect("Failed to create writer");

        // Write peaks for a few spectra
        for i in 0..5 {
            let peaks = PeakArraysV2::new(
                vec![100.0 + i as f64, 200.0 + i as f64, 300.0 + i as f64],
                vec![1000.0, 500.0, 250.0],
            );
            writer.write_peaks(i, &peaks).expect("Failed to write peaks");
        }

        assert_eq!(writer.peaks_written(), 15);
        assert_eq!(writer.spectra_written(), 5);
        assert!(!writer.has_ion_mobility());

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.peaks_written, 15);
        assert_eq!(stats.spectra_written, 5);
    }

    #[test]
    fn test_peaks_writer_v2_basic_4d() {
        let buffer = Cursor::new(Vec::new());
        let config = PeaksWriterV2Config::default();

        let mut writer = PeaksWriterV2::new(buffer, &config, true).expect("Failed to create writer");

        // Write peaks with ion mobility
        let peaks = PeakArraysV2::with_ion_mobility(
            vec![100.0, 200.0],
            vec![1000.0, 500.0],
            vec![1.5, 1.6],
        );
        writer.write_peaks(0, &peaks).expect("Failed to write peaks");

        assert!(writer.has_ion_mobility());
        assert_eq!(writer.peaks_written(), 2);

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.peaks_written, 2);
    }

    #[test]
    fn test_peaks_writer_v2_flush_on_full_buffer() {
        let buffer = Cursor::new(Vec::new());
        let config = PeaksWriterV2Config {
            row_group_size: 10, // Small buffer to trigger flush
            ..Default::default()
        };

        let mut writer = PeaksWriterV2::new(buffer, &config, false).expect("Failed to create writer");

        // Write more peaks than buffer size
        // Each spectrum has 3 peaks:
        // After spectrum 0: buffer=3
        // After spectrum 1: buffer=6
        // After spectrum 2: buffer=9
        // After spectrum 3: buffer=12 >= 10, flush! buffer=0
        // After spectrum 4: buffer=3
        for i in 0..5 {
            let peaks = PeakArraysV2::new(
                vec![100.0, 200.0, 300.0], // 3 peaks each
                vec![1000.0, 500.0, 250.0],
            );
            writer.write_peaks(i, &peaks).expect("Failed to write peaks");
        }

        // 15 peaks total, buffer size 10, flushed once after spectrum 3
        assert_eq!(writer.peaks_written(), 15);
        assert_eq!(writer.buffered_count(), 3); // Only spectrum 4's peaks remain after flush

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.peaks_written, 15);
        assert!(stats.row_groups_written >= 1);
    }

    #[test]
    fn test_peaks_writer_v2_empty_spectrum() {
        let buffer = Cursor::new(Vec::new());
        let config = PeaksWriterV2Config::default();

        let mut writer = PeaksWriterV2::new(buffer, &config, false).expect("Failed to create writer");

        // Write empty spectrum
        let empty_peaks = PeakArraysV2::new(vec![], vec![]);
        writer.write_peaks(0, &empty_peaks).expect("Failed to write peaks");

        // Empty spectrum should not increment counters
        assert_eq!(writer.peaks_written(), 0);
        assert_eq!(writer.spectra_written(), 0);

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.peaks_written, 0);
    }

    #[test]
    fn test_peaks_writer_v2_batch() {
        let buffer = Cursor::new(Vec::new());
        let config = PeaksWriterV2Config::default();

        let mut writer = PeaksWriterV2::new(buffer, &config, false).expect("Failed to create writer");

        // Create batch of spectra
        let spectra: Vec<PeakArraysV2> = (0..10)
            .map(|i| PeakArraysV2::new(vec![100.0 + i as f64], vec![1000.0]))
            .collect();

        // Write as batch
        let batch: Vec<_> = spectra.iter().enumerate().map(|(i, p)| (i as u32, p)).collect();
        writer.write_peaks_batch(batch).expect("Failed to write batch");

        assert_eq!(writer.peaks_written(), 10);
        assert_eq!(writer.spectra_written(), 10);

        let stats = writer.finish().expect("Failed to finish writer");
        assert_eq!(stats.peaks_written, 10);
    }
}
