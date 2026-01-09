//! # mzPeak Writer Module
//!
//! This module provides the core functionality for writing mass spectrometry data
//! to the mzPeak Parquet format.
//!
//! ## Design Principles
//!
//! 1. **Streaming Architecture**: Data is written in batches to handle large files
//!    without loading everything into memory.
//!
//! 2. **RLE Optimization**: Data is sorted and grouped by spectrum_id to maximize
//!    Run-Length Encoding compression on repeated metadata.
//!
//! 3. **Self-Contained Files**: All metadata (SDRF, instrument config, etc.) is
//!    embedded in the Parquet footer's key_value_metadata.
//!
//! 4. **Configurable Compression**: Supports ZSTD (default), Snappy, and uncompressed.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::format::KeyValue;

use crate::metadata::MzPeakMetadata;
use crate::schema::{columns, create_mzpeak_schema_arc};

/// Errors that can occur during writing
#[derive(Debug, thiserror::Error)]
pub enum WriterError {
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

    #[error("Writer not initialized")]
    NotInitialized,
}

/// Compression options for mzPeak files
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// ZSTD compression (recommended, best compression ratio)
    Zstd(i32),
    /// Snappy compression (faster, slightly larger files)
    Snappy,
    /// No compression (fastest write, largest files)
    Uncompressed,
}

impl Default for CompressionType {
    fn default() -> Self {
        // ZSTD level 3 is a good balance of speed and compression
        // For maximum compression, use Zstd(9) or higher
        Self::Zstd(3)
    }
}

impl CompressionType {
    /// Maximum compression (slower write, smallest files)
    pub fn max_compression() -> Self {
        Self::Zstd(22)
    }

    /// Balanced compression (recommended default)
    pub fn balanced() -> Self {
        Self::Zstd(3)
    }

    /// Fast compression (faster write, larger files)
    pub fn fast() -> Self {
        Self::Snappy
    }
}

/// Configuration for the mzPeak writer
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Compression type to use
    pub compression: CompressionType,

    /// Target row group size (number of rows per group)
    /// Smaller = better random access, larger = better compression
    pub row_group_size: usize,

    /// Data page size in bytes
    pub data_page_size: usize,

    /// Whether to write statistics for columns
    pub write_statistics: bool,

    /// Dictionary encoding threshold (0.0 to disable)
    pub dictionary_page_size_limit: usize,

    /// Maximum peaks per file before rotating (None = no rotation)
    pub max_peaks_per_file: Option<usize>,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            // ZSTD level 9 for better compression (was level 3)
            // This is a good balance for archival storage
            // Use Zstd(3) or Snappy for faster writing if needed
            compression: CompressionType::Zstd(9),
            // 100k peaks per row group is a good balance
            row_group_size: 100_000,
            // 1MB data pages
            data_page_size: 1024 * 1024,
            write_statistics: true,
            // 1MB dictionary page limit
            dictionary_page_size_limit: 1024 * 1024,
            // Default to 50M peaks per file for sharding
            max_peaks_per_file: Some(50_000_000),
        }
    }
}

impl WriterConfig {
    /// Configuration optimized for maximum compression (slower write)
    pub fn max_compression() -> Self {
        Self {
            compression: CompressionType::Zstd(22),
            row_group_size: 500_000, // Larger row groups = better compression
            data_page_size: 2 * 1024 * 1024, // 2MB pages
            write_statistics: true,
            dictionary_page_size_limit: 2 * 1024 * 1024,
            max_peaks_per_file: Some(100_000_000),
        }
    }

    /// Configuration optimized for fast writing (larger files)
    pub fn fast_write() -> Self {
        Self {
            compression: CompressionType::Snappy,
            row_group_size: 50_000,
            data_page_size: 512 * 1024,
            write_statistics: true,
            dictionary_page_size_limit: 512 * 1024,
            max_peaks_per_file: Some(50_000_000),
        }
    }

    /// Balanced configuration (default)
    pub fn balanced() -> Self {
        Self::default()
    }

    /// Create writer properties from this configuration
    fn to_writer_properties(&self, metadata: &HashMap<String, String>) -> WriterProperties {
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

        // Enable dictionary encoding for columns that benefit from it (repeated metadata)
        // These columns have the same value for all peaks in a spectrum, so dictionary
        // encoding + RLE will achieve excellent compression.
        // Note: Parquet automatically uses RLE for dictionary-encoded data.
        let dict_columns = [
            columns::SPECTRUM_ID,
            columns::SCAN_NUMBER,
            columns::MS_LEVEL,
            columns::RETENTION_TIME,
            columns::POLARITY,
            columns::PRECURSOR_MZ,
            columns::PRECURSOR_CHARGE,
            columns::PRECURSOR_INTENSITY,
            columns::ISOLATION_WINDOW_LOWER,
            columns::ISOLATION_WINDOW_UPPER,
            columns::COLLISION_ENERGY,
            columns::TOTAL_ION_CURRENT,
            columns::BASE_PEAK_MZ,
            columns::BASE_PEAK_INTENSITY,
            columns::INJECTION_TIME,
            // MSI columns also benefit from dictionary encoding (same value per spectrum)
            columns::PIXEL_X,
            columns::PIXEL_Y,
            columns::PIXEL_Z,
        ];

        for col in dict_columns {
            builder = builder.set_column_dictionary_enabled(
                parquet::schema::types::ColumnPath::new(vec![col.to_string()]),
                true,
            );
        }

        // m/z, intensity, and ion_mobility columns: disable dictionary (high cardinality data)
        // PLAIN encoding with compression works best for these
        builder = builder.set_column_dictionary_enabled(
            parquet::schema::types::ColumnPath::new(vec![columns::MZ.to_string()]),
            false,
        );
        builder = builder.set_column_dictionary_enabled(
            parquet::schema::types::ColumnPath::new(vec![columns::INTENSITY.to_string()]),
            false,
        );
        builder = builder.set_column_dictionary_enabled(
            parquet::schema::types::ColumnPath::new(vec![columns::ION_MOBILITY.to_string()]),
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

/// Represents a single peak in the "Long" format
#[derive(Debug, Clone)]
pub struct Peak {
    pub mz: f64,
    pub intensity: f32,
    pub ion_mobility: Option<f64>,
}

/// Represents a complete spectrum with all its metadata and peaks
#[derive(Debug, Clone)]
pub struct Spectrum {
    /// Unique spectrum identifier (typically 0-indexed)
    pub spectrum_id: i64,

    /// Native scan number from the instrument
    pub scan_number: i64,

    /// MS level (1, 2, 3, ...)
    pub ms_level: i16,

    /// Retention time in seconds
    pub retention_time: f32,

    /// Polarity: 1 for positive, -1 for negative
    pub polarity: i8,

    /// Precursor m/z (for MS2+)
    pub precursor_mz: Option<f64>,

    /// Precursor charge state
    pub precursor_charge: Option<i16>,

    /// Precursor intensity
    pub precursor_intensity: Option<f32>,

    /// Isolation window lower offset
    pub isolation_window_lower: Option<f32>,

    /// Isolation window upper offset
    pub isolation_window_upper: Option<f32>,

    /// Collision energy in eV
    pub collision_energy: Option<f32>,

    /// Total ion current
    pub total_ion_current: Option<f64>,

    /// Base peak m/z
    pub base_peak_mz: Option<f64>,

    /// Base peak intensity
    pub base_peak_intensity: Option<f32>,

    /// Ion injection time in ms
    pub injection_time: Option<f32>,

    // MSI (Mass Spectrometry Imaging) spatial coordinates
    /// X coordinate for imaging data (pixels)
    pub pixel_x: Option<i32>,

    /// Y coordinate for imaging data (pixels)
    pub pixel_y: Option<i32>,

    /// Z coordinate for 3D imaging data (pixels)
    pub pixel_z: Option<i32>,

    /// The actual peak data (m/z, intensity pairs)
    pub peaks: Vec<Peak>,
}

impl Spectrum {
    /// Create a new MS1 spectrum
    pub fn new_ms1(
        spectrum_id: i64,
        scan_number: i64,
        retention_time: f32,
        polarity: i8,
        peaks: Vec<Peak>,
    ) -> Self {
        Self {
            spectrum_id,
            scan_number,
            ms_level: 1,
            retention_time,
            polarity,
            precursor_mz: None,
            precursor_charge: None,
            precursor_intensity: None,
            isolation_window_lower: None,
            isolation_window_upper: None,
            collision_energy: None,
            total_ion_current: None,
            base_peak_mz: None,
            base_peak_intensity: None,
            injection_time: None,
            pixel_x: None,
            pixel_y: None,
            pixel_z: None,
            peaks,
        }
    }

    /// Create a new MS2 spectrum with precursor information
    pub fn new_ms2(
        spectrum_id: i64,
        scan_number: i64,
        retention_time: f32,
        polarity: i8,
        precursor_mz: f64,
        peaks: Vec<Peak>,
    ) -> Self {
        Self {
            spectrum_id,
            scan_number,
            ms_level: 2,
            retention_time,
            polarity,
            precursor_mz: Some(precursor_mz),
            precursor_charge: None,
            precursor_intensity: None,
            isolation_window_lower: None,
            isolation_window_upper: None,
            collision_energy: None,
            total_ion_current: None,
            base_peak_mz: None,
            base_peak_intensity: None,
            injection_time: None,
            pixel_x: None,
            pixel_y: None,
            pixel_z: None,
            peaks,
        }
    }

    /// Calculate and set spectrum statistics (TIC, base peak)
    pub fn compute_statistics(&mut self) {
        if self.peaks.is_empty() {
            return;
        }

        let mut tic: f64 = 0.0;
        let mut max_intensity: f32 = 0.0;
        let mut max_mz: f64 = 0.0;

        for peak in &self.peaks {
            tic += peak.intensity as f64;
            if peak.intensity > max_intensity {
                max_intensity = peak.intensity;
                max_mz = peak.mz;
            }
        }

        self.total_ion_current = Some(tic);
        self.base_peak_mz = Some(max_mz);
        self.base_peak_intensity = Some(max_intensity);
    }

    /// Get the number of peaks in this spectrum
    pub fn peak_count(&self) -> usize {
        self.peaks.len()
    }
}

/// Builder for constructing Spectrum objects fluently
pub struct SpectrumBuilder {
    spectrum: Spectrum,
}

impl SpectrumBuilder {
    pub fn new(spectrum_id: i64, scan_number: i64) -> Self {
        Self {
            spectrum: Spectrum {
                spectrum_id,
                scan_number,
                ms_level: 1,
                retention_time: 0.0,
                polarity: 1,
                precursor_mz: None,
                precursor_charge: None,
                precursor_intensity: None,
                isolation_window_lower: None,
                isolation_window_upper: None,
                collision_energy: None,
                total_ion_current: None,
                base_peak_mz: None,
                base_peak_intensity: None,
                injection_time: None,
                pixel_x: None,
                pixel_y: None,
                pixel_z: None,
                peaks: Vec::new(),
            },
        }
    }

    pub fn ms_level(mut self, level: i16) -> Self {
        self.spectrum.ms_level = level;
        self
    }

    pub fn retention_time(mut self, rt: f32) -> Self {
        self.spectrum.retention_time = rt;
        self
    }

    pub fn polarity(mut self, polarity: i8) -> Self {
        self.spectrum.polarity = polarity;
        self
    }

    pub fn precursor(mut self, mz: f64, charge: Option<i16>, intensity: Option<f32>) -> Self {
        self.spectrum.precursor_mz = Some(mz);
        self.spectrum.precursor_charge = charge;
        self.spectrum.precursor_intensity = intensity;
        self
    }

    pub fn isolation_window(mut self, lower: f32, upper: f32) -> Self {
        self.spectrum.isolation_window_lower = Some(lower);
        self.spectrum.isolation_window_upper = Some(upper);
        self
    }

    pub fn collision_energy(mut self, ce: f32) -> Self {
        self.spectrum.collision_energy = Some(ce);
        self
    }

    pub fn injection_time(mut self, time_ms: f32) -> Self {
        self.spectrum.injection_time = Some(time_ms);
        self
    }

    /// Set MSI pixel coordinates (for imaging mass spectrometry)
    pub fn pixel(mut self, x: i32, y: i32) -> Self {
        self.spectrum.pixel_x = Some(x);
        self.spectrum.pixel_y = Some(y);
        self
    }

    /// Set MSI pixel coordinates including Z (for 3D imaging)
    pub fn pixel_3d(mut self, x: i32, y: i32, z: i32) -> Self {
        self.spectrum.pixel_x = Some(x);
        self.spectrum.pixel_y = Some(y);
        self.spectrum.pixel_z = Some(z);
        self
    }

    pub fn peaks(mut self, peaks: Vec<Peak>) -> Self {
        self.spectrum.peaks = peaks;
        self
    }

    pub fn add_peak(mut self, mz: f64, intensity: f32) -> Self {
        self.spectrum.peaks.push(Peak { mz, intensity, ion_mobility: None });
        self
    }

    pub fn add_peak_with_im(mut self, mz: f64, intensity: f32, ion_mobility: f64) -> Self {
        self.spectrum.peaks.push(Peak { mz, intensity, ion_mobility: Some(ion_mobility) });
        self
    }

    pub fn build(mut self) -> Spectrum {
        self.spectrum.compute_statistics();
        self.spectrum
    }
}

/// Streaming writer for mzPeak Parquet files
pub struct MzPeakWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    schema: Arc<Schema>,
    config: WriterConfig,
    spectra_written: usize,
    peaks_written: usize,
}

impl MzPeakWriter<File> {
    /// Create a new writer to a file path
    pub fn new_file<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        let file = File::create(path)?;
        Self::new(file, metadata, config)
    }
}

impl<W: Write + Send> MzPeakWriter<W> {
    /// Create a new writer to any Write implementation
    pub fn new(
        writer: W,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        let schema = create_mzpeak_schema_arc();
        let parquet_metadata = metadata.to_parquet_metadata()?;
        let props = config.to_writer_properties(&parquet_metadata);

        let arrow_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

        Ok(Self {
            writer: arrow_writer,
            schema,
            config,
            spectra_written: 0,
            peaks_written: 0,
        })
    }

    /// Write a batch of spectra to the file
    ///
    /// Spectra are expanded into the "Long" format where each peak is a row.
    pub fn write_spectra(&mut self, spectra: &[Spectrum]) -> Result<(), WriterError> {
        if spectra.is_empty() {
            return Ok(());
        }

        // Calculate total number of peaks
        let total_peaks: usize = spectra.iter().map(|s| s.peaks.len()).sum();

        if total_peaks == 0 {
            return Ok(());
        }

        // Build arrays for each column
        let mut spectrum_id_builder = Int64Builder::with_capacity(total_peaks);
        let mut scan_number_builder = Int64Builder::with_capacity(total_peaks);
        let mut ms_level_builder = Int16Builder::with_capacity(total_peaks);
        let mut retention_time_builder = Float32Builder::with_capacity(total_peaks);
        let mut polarity_builder = Int8Builder::with_capacity(total_peaks);
        let mut mz_builder = Float64Builder::with_capacity(total_peaks);
        let mut intensity_builder = Float32Builder::with_capacity(total_peaks);
        let mut ion_mobility_builder = Float64Builder::with_capacity(total_peaks);
        let mut precursor_mz_builder = Float64Builder::with_capacity(total_peaks);
        let mut precursor_charge_builder = Int16Builder::with_capacity(total_peaks);
        let mut precursor_intensity_builder = Float32Builder::with_capacity(total_peaks);
        let mut isolation_lower_builder = Float32Builder::with_capacity(total_peaks);
        let mut isolation_upper_builder = Float32Builder::with_capacity(total_peaks);
        let mut collision_energy_builder = Float32Builder::with_capacity(total_peaks);
        let mut tic_builder = Float64Builder::with_capacity(total_peaks);
        let mut base_peak_mz_builder = Float64Builder::with_capacity(total_peaks);
        let mut base_peak_intensity_builder = Float32Builder::with_capacity(total_peaks);
        let mut injection_time_builder = Float32Builder::with_capacity(total_peaks);
        // MSI pixel coordinate builders
        let mut pixel_x_builder = Int32Builder::with_capacity(total_peaks);
        let mut pixel_y_builder = Int32Builder::with_capacity(total_peaks);
        let mut pixel_z_builder = Int32Builder::with_capacity(total_peaks);

        // Expand spectra into long format
        for spectrum in spectra {
            for peak in &spectrum.peaks {
                // Required columns (repeated for each peak in spectrum)
                spectrum_id_builder.append_value(spectrum.spectrum_id);
                scan_number_builder.append_value(spectrum.scan_number);
                ms_level_builder.append_value(spectrum.ms_level);
                retention_time_builder.append_value(spectrum.retention_time);
                polarity_builder.append_value(spectrum.polarity);

                // Peak data (unique per peak)
                mz_builder.append_value(peak.mz);
                intensity_builder.append_value(peak.intensity);

                // Ion mobility (optional, unique per peak)
                match peak.ion_mobility {
                    Some(v) => ion_mobility_builder.append_value(v),
                    None => ion_mobility_builder.append_null(),
                }

                // Optional columns (repeated for each peak in spectrum)
                match spectrum.precursor_mz {
                    Some(v) => precursor_mz_builder.append_value(v),
                    None => precursor_mz_builder.append_null(),
                }

                match spectrum.precursor_charge {
                    Some(v) => precursor_charge_builder.append_value(v),
                    None => precursor_charge_builder.append_null(),
                }

                match spectrum.precursor_intensity {
                    Some(v) => precursor_intensity_builder.append_value(v),
                    None => precursor_intensity_builder.append_null(),
                }

                match spectrum.isolation_window_lower {
                    Some(v) => isolation_lower_builder.append_value(v),
                    None => isolation_lower_builder.append_null(),
                }

                match spectrum.isolation_window_upper {
                    Some(v) => isolation_upper_builder.append_value(v),
                    None => isolation_upper_builder.append_null(),
                }

                match spectrum.collision_energy {
                    Some(v) => collision_energy_builder.append_value(v),
                    None => collision_energy_builder.append_null(),
                }

                match spectrum.total_ion_current {
                    Some(v) => tic_builder.append_value(v),
                    None => tic_builder.append_null(),
                }

                match spectrum.base_peak_mz {
                    Some(v) => base_peak_mz_builder.append_value(v),
                    None => base_peak_mz_builder.append_null(),
                }

                match spectrum.base_peak_intensity {
                    Some(v) => base_peak_intensity_builder.append_value(v),
                    None => base_peak_intensity_builder.append_null(),
                }

                match spectrum.injection_time {
                    Some(v) => injection_time_builder.append_value(v),
                    None => injection_time_builder.append_null(),
                }

                // MSI pixel coordinates (optional)
                match spectrum.pixel_x {
                    Some(v) => pixel_x_builder.append_value(v),
                    None => pixel_x_builder.append_null(),
                }

                match spectrum.pixel_y {
                    Some(v) => pixel_y_builder.append_value(v),
                    None => pixel_y_builder.append_null(),
                }

                match spectrum.pixel_z {
                    Some(v) => pixel_z_builder.append_value(v),
                    None => pixel_z_builder.append_null(),
                }
            }
        }

        // Build the arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(spectrum_id_builder.finish()),
            Arc::new(scan_number_builder.finish()),
            Arc::new(ms_level_builder.finish()),
            Arc::new(retention_time_builder.finish()),
            Arc::new(polarity_builder.finish()),
            Arc::new(mz_builder.finish()),
            Arc::new(intensity_builder.finish()),
            Arc::new(ion_mobility_builder.finish()),
            Arc::new(precursor_mz_builder.finish()),
            Arc::new(precursor_charge_builder.finish()),
            Arc::new(precursor_intensity_builder.finish()),
            Arc::new(isolation_lower_builder.finish()),
            Arc::new(isolation_upper_builder.finish()),
            Arc::new(collision_energy_builder.finish()),
            Arc::new(tic_builder.finish()),
            Arc::new(base_peak_mz_builder.finish()),
            Arc::new(base_peak_intensity_builder.finish()),
            Arc::new(injection_time_builder.finish()),
            // MSI pixel coordinates
            Arc::new(pixel_x_builder.finish()),
            Arc::new(pixel_y_builder.finish()),
            Arc::new(pixel_z_builder.finish()),
        ];

        // Create record batch
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;

        // Write the batch
        self.writer.write(&batch)?;

        self.spectra_written += spectra.len();
        self.peaks_written += total_peaks;

        Ok(())
    }

    /// Write a single spectrum
    pub fn write_spectrum(&mut self, spectrum: &Spectrum) -> Result<(), WriterError> {
        self.write_spectra(&[spectrum.clone()])
    }

    /// Flush any buffered data and finalize the file
    pub fn finish(self) -> Result<WriterStats, WriterError> {
        let file_metadata = self.writer.close()?;

        Ok(WriterStats {
            spectra_written: self.spectra_written,
            peaks_written: self.peaks_written,
            row_groups_written: file_metadata.row_groups.len(),
            file_size_bytes: file_metadata
                .row_groups
                .iter()
                .map(|rg| rg.total_byte_size as u64)
                .sum(),
        })
    }

    /// Flush any buffered data, finalize the file, and return the underlying writer
    ///
    /// This is useful when the writer is backed by an in-memory buffer and you need
    /// to access the written data.
    pub fn finish_into_inner(self) -> Result<W, WriterError> {
        let inner = self.writer.into_inner()?;
        Ok(inner)
    }

    /// Get current statistics
    pub fn stats(&self) -> WriterStats {
        WriterStats {
            spectra_written: self.spectra_written,
            peaks_written: self.peaks_written,
            row_groups_written: 0, // Unknown until finish
            file_size_bytes: 0,    // Unknown until finish
        }
    }
}

/// Statistics from a completed write operation
#[derive(Debug, Clone)]
pub struct WriterStats {
    pub spectra_written: usize,
    pub peaks_written: usize,
    pub row_groups_written: usize,
    pub file_size_bytes: u64,
}

impl std::fmt::Display for WriterStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Wrote {} spectra ({} peaks) in {} row groups",
            self.spectra_written, self.peaks_written, self.row_groups_written
        )
    }
}

/// Rolling writer that automatically shards output into multiple files
pub struct RollingWriter {
    base_path: std::path::PathBuf,
    metadata: MzPeakMetadata,
    config: WriterConfig,
    current_writer: Option<MzPeakWriter<File>>,
    current_part: usize,
    total_spectra_written: usize,
    total_peaks_written: usize,
    part_stats: Vec<WriterStats>,
}

impl RollingWriter {
    /// Create a new rolling writer
    pub fn new<P: AsRef<Path>>(
        base_path: P,
        metadata: MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        let base_path = base_path.as_ref().to_path_buf();
        
        Ok(Self {
            base_path,
            metadata,
            config,
            current_writer: None,
            current_part: 0,
            total_spectra_written: 0,
            total_peaks_written: 0,
            part_stats: Vec::new(),
        })
    }

    /// Get the path for a specific part number
    fn part_path(&self, part: usize) -> std::path::PathBuf {
        if part == 0 {
            self.base_path.clone()
        } else {
            let stem = self.base_path.file_stem().unwrap_or_default().to_string_lossy();
            let extension = self.base_path.extension().unwrap_or_default().to_string_lossy();
            let parent = self.base_path.parent().unwrap_or_else(|| Path::new("."));
            
            if extension.is_empty() {
                parent.join(format!("{}-part-{:04}", stem, part))
            } else {
                parent.join(format!("{}-part-{:04}.{}", stem, part, extension))
            }
        }
    }

    /// Rotate to a new file
    fn rotate_file(&mut self) -> Result<(), WriterError> {
        // Finish current writer if exists
        if let Some(writer) = self.current_writer.take() {
            let stats = writer.finish()?;
            self.part_stats.push(stats);
        }

        // Create new writer for next part
        self.current_part += 1;
        let part_path = self.part_path(self.current_part - 1);
        
        let writer = MzPeakWriter::new_file(part_path, &self.metadata, self.config.clone())?;
        self.current_writer = Some(writer);
        
        Ok(())
    }

    /// Write a batch of spectra, automatically rotating files if needed
    pub fn write_spectra(&mut self, spectra: &[Spectrum]) -> Result<(), WriterError> {
        if spectra.is_empty() {
            return Ok(());
        }

        // Initialize first writer if needed
        if self.current_writer.is_none() {
            self.rotate_file()?;
        }

        let writer = self.current_writer.as_mut().unwrap();
        
        // Check if we need to rotate based on config
        if let Some(max_peaks) = self.config.max_peaks_per_file {
            let peaks_in_batch: usize = spectra.iter().map(|s| s.peaks.len()).sum();
            
            // If adding this batch would exceed limit, rotate first
            if writer.peaks_written > 0 && writer.peaks_written + peaks_in_batch > max_peaks {
                self.rotate_file()?;
                let writer = self.current_writer.as_mut().unwrap();
                writer.write_spectra(spectra)?;
            } else {
                writer.write_spectra(spectra)?;
            }
        } else {
            writer.write_spectra(spectra)?;
        }

        self.total_spectra_written += spectra.len();
        self.total_peaks_written += spectra.iter().map(|s| s.peaks.len()).sum::<usize>();

        Ok(())
    }

    /// Write a single spectrum
    pub fn write_spectrum(&mut self, spectrum: &Spectrum) -> Result<(), WriterError> {
        self.write_spectra(&[spectrum.clone()])
    }

    /// Finish writing and return combined statistics
    pub fn finish(mut self) -> Result<RollingWriterStats, WriterError> {
        // Finish current writer if exists
        if let Some(writer) = self.current_writer.take() {
            let stats = writer.finish()?;
            self.part_stats.push(stats);
        }

        Ok(RollingWriterStats {
            total_spectra_written: self.total_spectra_written,
            total_peaks_written: self.total_peaks_written,
            files_written: self.part_stats.len(),
            part_stats: self.part_stats,
        })
    }

    /// Get current statistics
    pub fn stats(&self) -> RollingWriterStats {
        RollingWriterStats {
            total_spectra_written: self.total_spectra_written,
            total_peaks_written: self.total_peaks_written,
            files_written: self.part_stats.len() + if self.current_writer.is_some() { 1 } else { 0 },
            part_stats: self.part_stats.clone(),
        }
    }
}

/// Statistics from a rolling writer operation
#[derive(Debug, Clone)]
pub struct RollingWriterStats {
    /// Total number of spectra written across all files
    pub total_spectra_written: usize,
    /// Total number of peaks written across all files
    pub total_peaks_written: usize,
    /// Number of output files created
    pub files_written: usize,
    /// Statistics for each individual file part
    pub part_stats: Vec<WriterStats>,
}

impl std::fmt::Display for RollingWriterStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Wrote {} spectra ({} peaks) across {} file(s)",
            self.total_spectra_written, self.total_peaks_written, self.files_written
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_spectrum_builder() {
        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(2)
            .retention_time(100.5)
            .polarity(1)
            .precursor(500.25, Some(2), Some(1e6))
            .collision_energy(30.0)
            .add_peak(100.0, 1000.0)
            .add_peak(200.0, 2000.0)
            .add_peak(300.0, 500.0)
            .build();

        assert_eq!(spectrum.spectrum_id, 0);
        assert_eq!(spectrum.ms_level, 2);
        assert_eq!(spectrum.peaks.len(), 3);
        assert!(spectrum.total_ion_current.is_some());
        assert_eq!(spectrum.base_peak_intensity, Some(2000.0));
    }

    #[test]
    fn test_write_spectra() -> Result<(), WriterError> {
        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();

        let buffer = Cursor::new(Vec::new());
        let mut writer = MzPeakWriter::new(buffer, &metadata, config)?;

        let spectrum = SpectrumBuilder::new(0, 1)
            .ms_level(1)
            .retention_time(60.0)
            .polarity(1)
            .add_peak(400.0, 10000.0)
            .add_peak(500.0, 20000.0)
            .build();

        writer.write_spectrum(&spectrum)?;

        let stats = writer.finish()?;
        assert_eq!(stats.spectra_written, 1);
        assert_eq!(stats.peaks_written, 2);

        Ok(())
    }
}
