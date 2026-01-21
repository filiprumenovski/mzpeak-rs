//! # mzPeak v2.0 Dataset Writer
//!
//! This module provides the `MzPeakDatasetWriterV2` which orchestrates the creation
//! of mzPeak v2.0 datasets with the two-table normalized architecture.
//!
//! ## v2.0 Container Format
//!
//! ```text
//! {name}.mzpeak (ZIP archive)
//! ├── mimetype                    # "application/vnd.mzpeak+v2" (uncompressed, first entry)
//! ├── manifest.json               # Schema version and modality declaration
//! ├── metadata.json               # Human-readable metadata (Deflate compressed)
//! ├── spectra/spectra.parquet     # Spectrum-level metadata (one row per spectrum)
//! └── peaks/peaks.parquet         # Peak-level data (one row per peak)
//! ```
//!
//! ## Design Rationale
//!
//! The v2.0 schema separates spectrum metadata from peak data:
//! - **spectra.parquet**: One row per spectrum with metadata, peak_offset, peak_count
//! - **peaks.parquet**: One row per peak with spectrum_id, mz, intensity, [ion_mobility]
//!
//! This normalized architecture provides:
//! - 30-40% smaller file sizes through reduced data duplication
//! - Faster metadata-only queries (no need to scan peak data)
//! - Better compression ratios with optimized encodings
//!
//! ## Usage
//!
//! ```rust,ignore
//! use mzpeak::dataset::MzPeakDatasetWriterV2;
//! use mzpeak::schema::manifest::Modality;
//! use mzpeak::writer::types::{SpectrumV2, SpectrumMetadata, PeakArraysV2};
//!
//! let mut writer = MzPeakDatasetWriterV2::new(
//!     "output.mzpeak",
//!     Modality::LcMs,
//!     None, // vendor_hints
//! )?;
//!
//! // Write a spectrum
//! let metadata = SpectrumMetadata::new_ms1(0, Some(1), 60.0, 1, 100);
//! let peaks = PeakArraysV2::new(vec![400.0, 500.0], vec![1000.0, 500.0]);
//! writer.write_spectrum_v2(&metadata, &peaks)?;
//!
//! // Finalize
//! let stats = writer.close()?;
//! ```

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
use zip::write::SimpleFileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;

use crate::metadata::{MzPeakMetadata, VendorHints};
use crate::schema::manifest::{Manifest, Modality};
use crate::writer::{
    PeakArraysV2, PeaksWriterV2, PeaksWriterV2Config, PeaksWriterV2Stats, SpectraWriter,
    SpectraWriterConfig, SpectraWriterStats, SpectrumMetadata, SpectrumV2,
};

use super::error::DatasetError;

// =============================================================================
// v2.0 Mimetype
// =============================================================================

/// MIME type for mzPeak v2.0 container format
pub const MZPEAK_V2_MIMETYPE: &str = "application/vnd.mzpeak+v2";

// =============================================================================
// Statistics
// =============================================================================

/// Statistics from a completed v2.0 dataset write operation
#[derive(Debug, Clone)]
pub struct DatasetV2Stats {
    /// Statistics from the spectra writer
    pub spectra_stats: SpectraWriterStats,
    /// Statistics from the peaks writer
    pub peaks_stats: PeaksWriterV2Stats,
    /// Total file size in bytes
    pub total_size_bytes: u64,
}

impl std::fmt::Display for DatasetV2Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "v2.0 Dataset: {} spectra, {} peaks, {} bytes total",
            self.spectra_stats.spectra_written,
            self.peaks_stats.peaks_written,
            self.total_size_bytes
        )
    }
}

// =============================================================================
// Temp File Buffer (reused from writer_impl.rs)
// =============================================================================

/// Buffer that writes Parquet data to a temp file for later ZIP inclusion.
struct ParquetTempFile {
    temp_file: NamedTempFile,
    writer: BufWriter<File>,
}

impl ParquetTempFile {
    fn new() -> std::io::Result<Self> {
        let temp_file = NamedTempFile::new()?;
        let file = temp_file.reopen()?;
        let writer = BufWriter::new(file);
        Ok(Self { temp_file, writer })
    }

    fn size(&self) -> std::io::Result<u64> {
        self.temp_file.as_file().metadata().map(|m| m.len())
    }

    fn into_reader(mut self) -> std::io::Result<(u64, BufReader<File>)> {
        self.writer.flush()?;
        let size = self.size()?;
        let mut file = self.temp_file.reopen()?;
        file.seek(SeekFrom::Start(0))?;
        Ok((size, BufReader::new(file)))
    }
}

impl Write for ParquetTempFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl Seek for ParquetTempFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.writer.flush()?;
        self.writer.get_mut().seek(pos)
    }
}

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for MzPeakDatasetWriterV2
#[derive(Debug, Clone)]
pub struct DatasetWriterV2Config {
    /// Configuration for the spectra writer
    pub spectra_config: SpectraWriterConfig,
    /// Configuration for the peaks writer
    pub peaks_config: PeaksWriterV2Config,
}

impl Default for DatasetWriterV2Config {
    fn default() -> Self {
        Self {
            spectra_config: SpectraWriterConfig::default(),
            peaks_config: PeaksWriterV2Config::default(),
        }
    }
}

// =============================================================================
// MzPeakDatasetWriterV2 Implementation
// =============================================================================

/// Orchestrator for creating mzPeak v2.0 datasets
///
/// This writer coordinates two sub-writers:
/// - `SpectraWriter`: Writes spectrum-level metadata to spectra/spectra.parquet
/// - `PeaksWriterV2`: Writes peak-level data to peaks/peaks.parquet
///
/// The v2.0 format uses a normalized two-table architecture that provides
/// significant storage efficiency improvements over v1.0.
pub struct MzPeakDatasetWriterV2 {
    /// Output path for the container
    output_path: PathBuf,

    /// ZIP writer for the container
    zip_writer: ZipWriter<BufWriter<File>>,

    /// Spectra writer (writes to temp file)
    spectra_writer: Option<SpectraWriter<ParquetTempFile>>,

    /// Peaks writer (writes to temp file)
    peaks_writer: Option<PeaksWriterV2<ParquetTempFile>>,

    /// Data modality
    modality: Modality,

    /// Optional metadata
    metadata: Option<MzPeakMetadata>,

    /// Vendor hints for provenance
    vendor_hints: Option<VendorHints>,

    /// Whether precursor info has been written
    has_precursor_info: bool,

    /// Current peak offset (byte position in peaks file)
    current_peak_offset: u64,

    /// Total peaks written
    peaks_written: u64,

    /// Total spectra written
    spectra_written: u64,

    /// Flag indicating if the dataset is finalized
    finalized: bool,
}

impl MzPeakDatasetWriterV2 {
    /// Create a new v2.0 dataset writer at the specified path.
    ///
    /// # Arguments
    ///
    /// * `path` - Output path (should end with `.mzpeak`)
    /// * `modality` - Data modality (LC-MS, LC-IMS-MS, MSI, MSI-IMS)
    /// * `vendor_hints` - Optional vendor hints for provenance tracking
    ///
    /// # Returns
    ///
    /// A new MzPeakDatasetWriterV2 ready to write spectra.
    pub fn new<P: AsRef<Path>>(
        path: P,
        modality: Modality,
        vendor_hints: Option<VendorHints>,
    ) -> Result<Self, DatasetError> {
        Self::with_config(path, modality, vendor_hints, DatasetWriterV2Config::default())
    }

    /// Create a new v2.0 dataset writer with custom configuration.
    pub fn with_config<P: AsRef<Path>>(
        path: P,
        modality: Modality,
        vendor_hints: Option<VendorHints>,
        config: DatasetWriterV2Config,
    ) -> Result<Self, DatasetError> {
        let output_path = path.as_ref().to_path_buf();

        // Validate path
        if output_path.to_string_lossy().is_empty() {
            return Err(DatasetError::InvalidPath("Empty path".to_string()));
        }

        // Check if file already exists
        if output_path.exists() {
            return Err(DatasetError::AlreadyExists(
                output_path.to_string_lossy().to_string(),
            ));
        }

        // Create parent directories if needed
        if let Some(parent) = output_path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        // Create ZIP file
        let file = File::create(&output_path)?;
        let buf_writer = BufWriter::new(file);
        let mut zip_writer = ZipWriter::new(buf_writer);

        // Write v2.0 mimetype as first entry (MUST be uncompressed and first)
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o644);
        zip_writer.start_file("mimetype", options)?;
        zip_writer.write_all(MZPEAK_V2_MIMETYPE.as_bytes())?;

        // Initialize spectra writer to temp file
        let spectra_buffer = ParquetTempFile::new()?;
        let spectra_writer = SpectraWriter::new(spectra_buffer, &config.spectra_config)?;

        // Initialize peaks writer to temp file
        let has_ion_mobility = modality.has_ion_mobility();
        let peaks_buffer = ParquetTempFile::new()?;
        let peaks_writer = PeaksWriterV2::new(peaks_buffer, &config.peaks_config, has_ion_mobility)?;

        Ok(Self {
            output_path,
            zip_writer,
            spectra_writer: Some(spectra_writer),
            peaks_writer: Some(peaks_writer),
            modality,
            metadata: None,
            vendor_hints,
            has_precursor_info: false,
            current_peak_offset: 0,
            peaks_written: 0,
            spectra_written: 0,
            finalized: false,
        })
    }

    /// Set optional metadata for the dataset.
    pub fn set_metadata(&mut self, metadata: MzPeakMetadata) {
        self.metadata = Some(metadata);
    }

    /// Write a single spectrum using v2 types.
    ///
    /// # Arguments
    ///
    /// * `metadata` - Spectrum-level metadata
    /// * `peaks` - Peak-level data arrays
    pub fn write_spectrum_v2(
        &mut self,
        metadata: &SpectrumMetadata,
        peaks: &PeakArraysV2,
    ) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        // Track if this has precursor info
        if metadata.precursor_mz.is_some() {
            self.has_precursor_info = true;
        }

        // Write spectrum metadata with current peak offset
        let spectra_writer = self
            .spectra_writer
            .as_mut()
            .ok_or(DatasetError::NotInitialized)?;
        spectra_writer.write_spectrum_metadata_with_offset(metadata, self.current_peak_offset)?;

        // Write peaks
        let peaks_writer = self
            .peaks_writer
            .as_mut()
            .ok_or(DatasetError::NotInitialized)?;
        peaks_writer.write_peaks(metadata.spectrum_id, peaks)?;

        // Update offset tracking
        // Note: We track row count, not byte offset. The peak_offset column
        // stores the row index in peaks.parquet where this spectrum's peaks start.
        self.current_peak_offset += peaks.len() as u64;
        self.peaks_written += peaks.len() as u64;
        self.spectra_written += 1;

        Ok(())
    }

    /// Write a combined SpectrumV2 (convenience method).
    pub fn write_spectrum(&mut self, spectrum: &SpectrumV2) -> Result<(), DatasetError> {
        self.write_spectrum_v2(&spectrum.metadata, &spectrum.peaks)
    }

    /// Write multiple spectra in a batch.
    pub fn write_spectra(&mut self, spectra: &[SpectrumV2]) -> Result<(), DatasetError> {
        for spectrum in spectra {
            self.write_spectrum(spectrum)?;
        }
        Ok(())
    }

    /// Get current statistics (without closing).
    pub fn stats(&self) -> (u64, u64) {
        (self.spectra_written, self.peaks_written)
    }

    /// Get the data modality.
    pub fn modality(&self) -> Modality {
        self.modality
    }

    /// Build the manifest JSON content.
    fn build_manifest(&self) -> Manifest {
        let created = chrono::Utc::now().to_rfc3339();
        let converter = format!("mzpeak-rs v{}", env!("CARGO_PKG_VERSION"));

        let mut manifest = Manifest::new(
            self.modality,
            self.has_precursor_info,
            self.spectra_written,
            self.peaks_written,
            created,
            converter,
        );

        manifest.vendor_hints = self.vendor_hints.clone();

        manifest
    }

    /// Build the metadata JSON content.
    fn build_metadata_json(&self) -> Result<String, DatasetError> {
        let mut json_map = serde_json::Map::new();

        json_map.insert(
            "format_version".to_string(),
            serde_json::Value::String("2.0".to_string()),
        );

        json_map.insert(
            "created".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );

        json_map.insert(
            "converter".to_string(),
            serde_json::Value::String(format!("mzpeak-rs v{}", env!("CARGO_PKG_VERSION"))),
        );

        // Add optional MzPeakMetadata fields if present
        if let Some(ref metadata) = self.metadata {
            if let Some(ref sdrf) = metadata.sdrf {
                let sdrf_json = serde_json::to_value(sdrf)?;
                json_map.insert("sdrf".to_string(), sdrf_json);
            }

            if let Some(ref instrument) = metadata.instrument {
                let instrument_json = serde_json::to_value(instrument)?;
                json_map.insert("instrument".to_string(), instrument_json);
            }

            if let Some(ref lc) = metadata.lc_config {
                let lc_json = serde_json::to_value(lc)?;
                json_map.insert("lc_config".to_string(), lc_json);
            }

            if let Some(ref run) = metadata.run_parameters {
                let run_json = serde_json::to_value(run)?;
                json_map.insert("run_parameters".to_string(), run_json);
            }

            if let Some(ref source) = metadata.source_file {
                let source_json = serde_json::to_value(source)?;
                json_map.insert("source_file".to_string(), source_json);
            }

            if let Some(ref history) = metadata.processing_history {
                let history_json = serde_json::to_value(history)?;
                json_map.insert("processing_history".to_string(), history_json);
            }

            if let Some(ref hints) = metadata.vendor_hints {
                let hints_json = serde_json::to_value(hints)?;
                json_map.insert("vendor_hints".to_string(), hints_json);
            }
        }

        let json_value = serde_json::Value::Object(json_map);
        Ok(serde_json::to_string_pretty(&json_value)?)
    }

    /// Close the dataset and finalize all writers.
    ///
    /// This ensures:
    /// 1. Both writers are properly finished and flushed
    /// 2. The manifest.json and metadata.json files are written
    /// 3. All entries are added to the ZIP and finalized
    ///
    /// # Returns
    ///
    /// Statistics about the completed write operation.
    pub fn close(mut self) -> Result<DatasetV2Stats, DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        // Build JSON content before consuming writers
        let manifest = self.build_manifest();
        let manifest_json = serde_json::to_string_pretty(&manifest)?;
        let metadata_json = self.build_metadata_json()?;

        // Finalize spectra writer
        let spectra_stats;
        let spectra_reader;
        if let Some(writer) = self.spectra_writer.take() {
            let temp_file = writer.finish_into_inner()?;
            let (size, reader) = temp_file.into_reader()?;
            spectra_stats = SpectraWriterStats {
                spectra_written: self.spectra_written,
                row_groups_written: 0,
                file_size_bytes: size,
            };
            spectra_reader = reader;
        } else {
            return Err(DatasetError::NotInitialized);
        }

        // Finalize peaks writer
        let peaks_stats;
        let peaks_reader;
        if let Some(writer) = self.peaks_writer.take() {
            let temp_file = writer.finish_into_inner()?;
            let (size, reader) = temp_file.into_reader()?;
            peaks_stats = PeaksWriterV2Stats {
                peaks_written: self.peaks_written,
                spectra_written: self.spectra_written,
                row_groups_written: 0,
                file_size_bytes: size,
            };
            peaks_reader = reader;
        } else {
            return Err(DatasetError::NotInitialized);
        }

        // Write manifest.json (Deflate compressed)
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .unix_permissions(0o644);
        self.zip_writer.start_file("manifest.json", options)?;
        self.zip_writer.write_all(manifest_json.as_bytes())?;

        // Write metadata.json (Deflate compressed)
        self.zip_writer.start_file("metadata.json", options)?;
        self.zip_writer.write_all(metadata_json.as_bytes())?;

        // Write spectra/spectra.parquet (MUST be uncompressed/Stored for seekability)
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o644);
        self.zip_writer.start_file("spectra/spectra.parquet", options)?;
        stream_copy_to_zip(spectra_reader, &mut self.zip_writer)?;

        // Write peaks/peaks.parquet (MUST be uncompressed/Stored for seekability)
        self.zip_writer.start_file("peaks/peaks.parquet", options)?;
        stream_copy_to_zip(peaks_reader, &mut self.zip_writer)?;

        // Finalize the ZIP archive
        let inner = self.zip_writer.finish()?;
        inner.into_inner().map_err(|e| {
            DatasetError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to flush ZIP buffer: {}", e.error()),
            ))
        })?;

        // Get final file size
        let total_size = fs::metadata(&self.output_path)?.len();

        self.finalized = true;

        Ok(DatasetV2Stats {
            spectra_stats,
            peaks_stats,
            total_size_bytes: total_size,
        })
    }

    /// Get the output path.
    pub fn output_path(&self) -> &Path {
        &self.output_path
    }
}

/// Copy data from a reader to a ZIP writer with bounded memory.
const STREAM_COPY_BUFFER_SIZE: usize = 64 * 1024;

fn stream_copy_to_zip<R: Read, W: Write + Seek>(
    mut reader: R,
    zip_writer: &mut ZipWriter<W>,
) -> std::io::Result<u64> {
    let mut buffer = [0u8; STREAM_COPY_BUFFER_SIZE];
    let mut total_written = 0u64;

    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        zip_writer.write_all(&buffer[..bytes_read])?;
        total_written += bytes_read as u64;
    }

    Ok(total_written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_dataset_writer_v2_basic() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("test.mzpeak");

        let mut writer =
            MzPeakDatasetWriterV2::new(&output_path, Modality::LcMs, None).expect("Failed to create writer");

        // Write some spectra
        for i in 0..10 {
            let metadata = SpectrumMetadata::new_ms1(i, Some(i as i32 + 1), i as f32 * 0.1, 1, 50);
            let peaks = PeakArraysV2::new(
                vec![100.0 + i as f64, 200.0 + i as f64],
                vec![1000.0, 500.0],
            );
            writer.write_spectrum_v2(&metadata, &peaks).expect("Failed to write spectrum");
        }

        let (spectra_count, peaks_count) = writer.stats();
        assert_eq!(spectra_count, 10);
        assert_eq!(peaks_count, 20);

        let stats = writer.close().expect("Failed to close writer");
        assert_eq!(stats.spectra_stats.spectra_written, 10);
        assert_eq!(stats.peaks_stats.peaks_written, 20);
        assert!(stats.total_size_bytes > 0);

        // Verify the file exists
        assert!(output_path.exists());
    }

    #[test]
    fn test_dataset_writer_v2_with_ion_mobility() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("test_4d.mzpeak");

        let mut writer =
            MzPeakDatasetWriterV2::new(&output_path, Modality::LcImsMs, None).expect("Failed to create writer");

        // Write spectrum with ion mobility
        let metadata = SpectrumMetadata::new_ms1(0, Some(1), 60.0, 1, 100);
        let peaks = PeakArraysV2::with_ion_mobility(
            vec![100.0, 200.0],
            vec![1000.0, 500.0],
            vec![1.5, 1.6],
        );
        writer.write_spectrum_v2(&metadata, &peaks).expect("Failed to write spectrum");

        let stats = writer.close().expect("Failed to close writer");
        assert_eq!(stats.peaks_stats.peaks_written, 2);
    }

    #[test]
    fn test_dataset_writer_v2_with_ms2() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("test_ms2.mzpeak");

        let mut writer =
            MzPeakDatasetWriterV2::new(&output_path, Modality::LcMs, None).expect("Failed to create writer");

        // Write MS1
        let ms1_metadata = SpectrumMetadata::new_ms1(0, Some(1), 60.0, 1, 100);
        let ms1_peaks = PeakArraysV2::new(vec![500.0], vec![10000.0]);
        writer.write_spectrum_v2(&ms1_metadata, &ms1_peaks).unwrap();

        // Write MS2 with precursor
        let mut ms2_metadata = SpectrumMetadata::new_ms2(1, Some(2), 60.1, 1, 50, 456.789);
        ms2_metadata.precursor_charge = Some(2);
        ms2_metadata.collision_energy = Some(30.0);
        let ms2_peaks = PeakArraysV2::new(vec![100.0, 200.0], vec![500.0, 250.0]);
        writer.write_spectrum_v2(&ms2_metadata, &ms2_peaks).unwrap();

        let stats = writer.close().expect("Failed to close writer");
        assert_eq!(stats.spectra_stats.spectra_written, 2);
        assert_eq!(stats.peaks_stats.peaks_written, 3);
    }

    #[test]
    fn test_dataset_writer_v2_spectrum_v2_type() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("test_v2_type.mzpeak");

        let mut writer =
            MzPeakDatasetWriterV2::new(&output_path, Modality::LcMs, None).expect("Failed to create writer");

        // Use SpectrumV2 convenience type
        let spectrum = SpectrumV2::new(
            SpectrumMetadata::new_ms1(0, Some(1), 60.0, 1, 100),
            PeakArraysV2::new(vec![100.0], vec![1000.0]),
        );
        writer.write_spectrum(&spectrum).unwrap();

        let stats = writer.close().expect("Failed to close writer");
        assert_eq!(stats.spectra_stats.spectra_written, 1);
    }

    #[test]
    fn test_dataset_writer_v2_already_exists() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("existing.mzpeak");

        // Create the file first
        std::fs::write(&output_path, "dummy").unwrap();

        // Try to create writer - should fail
        let result = MzPeakDatasetWriterV2::new(&output_path, Modality::LcMs, None);
        assert!(matches!(result, Err(DatasetError::AlreadyExists(_))));
    }

    #[test]
    fn test_dataset_writer_v2_vendor_hints() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("test_hints.mzpeak");

        let vendor_hints = VendorHints {
            original_vendor: Some("Thermo Scientific".to_string()),
            original_format: Some("RAW".to_string()),
            instrument_model: Some("Q Exactive HF".to_string()),
            conversion_path: vec!["RAW".to_string(), "mzML".to_string(), "mzpeak".to_string()],
        };

        let mut writer = MzPeakDatasetWriterV2::new(&output_path, Modality::LcMs, Some(vendor_hints))
            .expect("Failed to create writer");

        let metadata = SpectrumMetadata::new_ms1(0, Some(1), 60.0, 1, 100);
        let peaks = PeakArraysV2::new(vec![100.0], vec![1000.0]);
        writer.write_spectrum_v2(&metadata, &peaks).unwrap();

        let stats = writer.close().expect("Failed to close writer");
        assert_eq!(stats.spectra_stats.spectra_written, 1);
    }
}
