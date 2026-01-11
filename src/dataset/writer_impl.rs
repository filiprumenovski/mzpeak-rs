use std::fs::{self, File};
use std::io::{BufWriter, Cursor, Seek, Write};
use std::path::{Path, PathBuf};

use zip::write::SimpleFileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;

use crate::chromatogram_writer::{
    Chromatogram, ChromatogramWriter, ChromatogramWriterConfig, ChromatogramWriterStats,
};
use crate::mobilogram_writer::{
    Mobilogram, MobilogramWriter, MobilogramWriterConfig, MobilogramWriterStats,
};
use crate::metadata::MzPeakMetadata;
use crate::schema::MZPEAK_MIMETYPE;
use crate::writer::{MzPeakWriter, Spectrum, WriterConfig, WriterStats};

use super::error::DatasetError;
use super::stats::DatasetStats;
use super::types::OutputMode;

/// Wrapper that writes Parquet data to an in-memory buffer for later ZIP inclusion.
/// This is necessary because the ZIP writer needs to know the uncompressed size upfront
/// for Stored entries, so we buffer the entire Parquet file.
struct ParquetBuffer {
    buffer: Cursor<Vec<u8>>,
}

impl ParquetBuffer {
    fn new() -> Self {
        Self {
            buffer: Cursor::new(Vec::new()),
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.buffer.into_inner()
    }
}

impl Write for ParquetBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.flush()
    }
}

impl Seek for ParquetBuffer {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.buffer.seek(pos)
    }
}

/// Internal sink abstraction for writing to either directory or container
enum DatasetSink {
    /// Directory mode: writes directly to files
    Directory {
        root_path: PathBuf,
        peak_writer: Option<MzPeakWriter<File>>,
        chromatogram_writer: Option<ChromatogramWriter<File>>,
        mobilogram_writer: Option<MobilogramWriter<File>>,
    },
    /// Container mode: writes to a ZIP archive
    Container {
        output_path: PathBuf,
        zip_writer: ZipWriter<BufWriter<File>>,
        peak_writer: Option<MzPeakWriter<ParquetBuffer>>,
        chromatogram_writer: Option<ChromatogramWriter<ParquetBuffer>>,
        mobilogram_writer: Option<MobilogramWriter<ParquetBuffer>>,
    },
}

/// Orchestrator for creating mzPeak datasets
///
/// Supports two output modes:
/// - **Container Mode** (default): Single `.mzpeak` ZIP archive
/// - **Directory Mode** (legacy): Directory bundle with separate files
pub struct MzPeakDatasetWriter {
    /// Internal sink (directory or container)
    sink: DatasetSink,

    /// Output mode being used
    mode: OutputMode,

    /// Copy of metadata for JSON export
    metadata: MzPeakMetadata,

    /// Writer configuration
    config: WriterConfig,

    /// Flag indicating if the dataset is finalized
    finalized: bool,
}

impl MzPeakDatasetWriter {
    /// Create a new dataset at the specified path
    ///
    /// Mode is automatically selected based on the path:
    /// - If path ends with `.mzpeak` and is not an existing directory → Container Mode
    /// - Otherwise → Directory Mode
    ///
    /// # Arguments
    ///
    /// * `path` - Output path (e.g., "output.mzpeak")
    /// * `metadata` - Metadata to embed in the dataset
    /// * `config` - Writer configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file/directory already exists
    /// - Creation fails
    pub fn new<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, DatasetError> {
        let path = path.as_ref();

        // Determine mode: Container if .mzpeak extension and not an existing directory
        let use_container = path
            .extension()
            .map(|ext| ext == "mzpeak")
            .unwrap_or(false)
            && !path.is_dir();

        if use_container {
            Self::new_container(path, metadata, config)
        } else {
            Self::new_directory(path, metadata, config)
        }
    }

    /// Create a new dataset in Container Mode (ZIP archive)
    pub fn new_container<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
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

        // Write mimetype as first entry (MUST be uncompressed and first)
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .unix_permissions(0o644);
        zip_writer.start_file("mimetype", options)?;
        zip_writer.write_all(MZPEAK_MIMETYPE.as_bytes())?;

        // Initialize peak writer to buffer
        let peak_buffer = ParquetBuffer::new();
        let peak_writer = MzPeakWriter::new(peak_buffer, metadata, config.clone())?;

        // Initialize chromatogram writer to buffer
        let chrom_buffer = ParquetBuffer::new();
        let chrom_config = ChromatogramWriterConfig::default();
        let chrom_writer = ChromatogramWriter::new(chrom_buffer, metadata, chrom_config)
            .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?;

        // Initialize mobilogram writer to buffer
        let mob_buffer = ParquetBuffer::new();
        let mob_config = MobilogramWriterConfig::default();
        let mob_writer = MobilogramWriter::new(mob_buffer, metadata, mob_config)
            .map_err(|e| DatasetError::MobilogramWriterError(e.to_string()))?;

        Ok(Self {
            sink: DatasetSink::Container {
                output_path,
                zip_writer,
                peak_writer: Some(peak_writer),
                chromatogram_writer: Some(chrom_writer),
                mobilogram_writer: Some(mob_writer),
            },
            mode: OutputMode::Container,
            metadata: metadata.clone(),
            config,
            finalized: false,
        })
    }

    /// Create a new dataset in Directory Mode (legacy bundle)
    pub fn new_directory<P: AsRef<Path>>(
        path: P,
        metadata: &MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, DatasetError> {
        let root_path = path.as_ref().to_path_buf();

        // Validate path
        if root_path.to_string_lossy().is_empty() {
            return Err(DatasetError::InvalidPath("Empty path".to_string()));
        }

        // Check if dataset already exists
        if root_path.exists() {
            return Err(DatasetError::AlreadyExists(
                root_path.to_string_lossy().to_string(),
            ));
        }

        // Create root directory
        fs::create_dir_all(&root_path)?;

        // Create subdirectories
        let peaks_dir = root_path.join("peaks");
        let chromatograms_dir = root_path.join("chromatograms");
        let mobilograms_dir = root_path.join("mobilograms");

        fs::create_dir(&peaks_dir)?;
        fs::create_dir(&chromatograms_dir)?;
        fs::create_dir(&mobilograms_dir)?;

        // Initialize peak writer
        let peak_file_path = peaks_dir.join("peaks.parquet");
        let peak_writer = MzPeakWriter::new_file(&peak_file_path, metadata, config.clone())?;

        // Initialize chromatogram writer
        let chrom_file_path = chromatograms_dir.join("chromatograms.parquet");
        let chrom_config = ChromatogramWriterConfig::default();
        let chrom_writer = ChromatogramWriter::new_file(&chrom_file_path, metadata, chrom_config)
            .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?;

        // Initialize mobilogram writer
        let mob_file_path = mobilograms_dir.join("mobilograms.parquet");
        let mob_config = MobilogramWriterConfig::default();
        let mob_writer = MobilogramWriter::new_file(&mob_file_path, metadata, mob_config)
            .map_err(|e| DatasetError::MobilogramWriterError(e.to_string()))?;

        Ok(Self {
            sink: DatasetSink::Directory {
                root_path,
                peak_writer: Some(peak_writer),
                chromatogram_writer: Some(chrom_writer),
                mobilogram_writer: Some(mob_writer),
            },
            mode: OutputMode::Directory,
            metadata: metadata.clone(),
            config,
            finalized: false,
        })
    }

    /// Get the output mode being used
    pub fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Write a single spectrum to the dataset
    ///
    /// This delegates to the internal peak writer.
    pub fn write_spectrum(&mut self, spectrum: &Spectrum) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectrum(spectrum)?;
            }
            DatasetSink::Container { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectrum(spectrum)?;
            }
        }
        Ok(())
    }

    /// Write multiple spectra to the dataset
    ///
    /// This delegates to the internal peak writer.
    pub fn write_spectra(&mut self, spectra: &[Spectrum]) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectra(spectra)?;
            }
            DatasetSink::Container { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectra(spectra)?;
            }
        }
        Ok(())
    }

    /// Write a single chromatogram to the dataset
    pub fn write_chromatogram(&mut self, chromatogram: &Chromatogram) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory {
                chromatogram_writer,
                ..
            } => {
                let writer = chromatogram_writer
                    .as_mut()
                    .ok_or(DatasetError::NotInitialized)?;
                writer
                    .write_chromatogram(chromatogram)
                    .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?;
            }
            DatasetSink::Container {
                chromatogram_writer,
                ..
            } => {
                let writer = chromatogram_writer
                    .as_mut()
                    .ok_or(DatasetError::NotInitialized)?;
                writer
                    .write_chromatogram(chromatogram)
                    .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Write multiple chromatograms to the dataset
    pub fn write_chromatograms(
        &mut self,
        chromatograms: &[Chromatogram],
    ) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory {
                chromatogram_writer,
                ..
            } => {
                let writer = chromatogram_writer
                    .as_mut()
                    .ok_or(DatasetError::NotInitialized)?;
                writer
                    .write_chromatograms(chromatograms)
                    .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?;
            }
            DatasetSink::Container {
                chromatogram_writer,
                ..
            } => {
                let writer = chromatogram_writer
                    .as_mut()
                    .ok_or(DatasetError::NotInitialized)?;
                writer
                    .write_chromatograms(chromatograms)
                    .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Write a single mobilogram to the dataset
    pub fn write_mobilogram(&mut self, mobilogram: &Mobilogram) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory {
                mobilogram_writer,
                ..
            } => {
                let writer = mobilogram_writer
                    .as_mut()
                    .ok_or(DatasetError::NotInitialized)?;
                writer
                    .write_mobilogram(mobilogram)
                    .map_err(|e| DatasetError::MobilogramWriterError(e.to_string()))?;
            }
            DatasetSink::Container {
                mobilogram_writer,
                ..
            } => {
                let writer = mobilogram_writer
                    .as_mut()
                    .ok_or(DatasetError::NotInitialized)?;
                writer
                    .write_mobilogram(mobilogram)
                    .map_err(|e| DatasetError::MobilogramWriterError(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Write multiple mobilograms to the dataset
    pub fn write_mobilograms(
        &mut self,
        mobilograms: &[Mobilogram],
    ) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory {
                mobilogram_writer,
                ..
            } => {
                let writer = mobilogram_writer
                    .as_mut()
                    .ok_or(DatasetError::NotInitialized)?;
                writer
                    .write_mobilograms(mobilograms)
                    .map_err(|e| DatasetError::MobilogramWriterError(e.to_string()))?;
            }
            DatasetSink::Container {
                mobilogram_writer,
                ..
            } => {
                let writer = mobilogram_writer
                    .as_mut()
                    .ok_or(DatasetError::NotInitialized)?;
                writer
                    .write_mobilograms(mobilograms)
                    .map_err(|e| DatasetError::MobilogramWriterError(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Get current statistics from the peak writer
    pub fn stats(&self) -> Option<WriterStats> {
        match &self.sink {
            DatasetSink::Directory { peak_writer, .. } => peak_writer.as_ref().map(|w| w.stats()),
            DatasetSink::Container { peak_writer, .. } => peak_writer.as_ref().map(|w| w.stats()),
        }
    }

    /// Build the metadata JSON content
    fn build_metadata_json(&self) -> Result<String, DatasetError> {
        let mut json_map = serde_json::Map::new();

        json_map.insert(
            "format_version".to_string(),
            serde_json::Value::String(crate::schema::MZPEAK_FORMAT_VERSION.to_string()),
        );

        json_map.insert(
            "created".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );

        json_map.insert(
            "converter".to_string(),
            serde_json::Value::String(format!("mzpeak-rs v{}", env!("CARGO_PKG_VERSION"))),
        );

        // Add SDRF metadata
        if let Some(ref sdrf) = self.metadata.sdrf {
            let sdrf_json = serde_json::to_value(sdrf)?;
            json_map.insert("sdrf".to_string(), sdrf_json);
        }

        // Add instrument config
        if let Some(ref instrument) = self.metadata.instrument {
            let instrument_json = serde_json::to_value(instrument)?;
            json_map.insert("instrument".to_string(), instrument_json);
        }

        // Add LC config
        if let Some(ref lc) = self.metadata.lc_config {
            let lc_json = serde_json::to_value(lc)?;
            json_map.insert("lc_config".to_string(), lc_json);
        }

        // Add run parameters
        if let Some(ref run) = self.metadata.run_parameters {
            let run_json = serde_json::to_value(run)?;
            json_map.insert("run_parameters".to_string(), run_json);
        }

        // Add source file info
        if let Some(ref source) = self.metadata.source_file {
            let source_json = serde_json::to_value(source)?;
            json_map.insert("source_file".to_string(), source_json);
        }

        // Add processing history
        if let Some(ref history) = self.metadata.processing_history {
            let history_json = serde_json::to_value(history)?;
            json_map.insert("processing_history".to_string(), history_json);
        }

        let json_value = serde_json::Value::Object(json_map);
        Ok(serde_json::to_string_pretty(&json_value)?)
    }

    /// Close the dataset and finalize all writers
    ///
    /// This ensures:
    /// 1. The peak writer is properly finished and flushed
    /// 2. The metadata.json file is written
    /// 3. For Container Mode: All entries are added to the ZIP and finalized
    ///
    /// After calling close(), the dataset is marked as complete and valid.
    pub fn close(mut self) -> Result<DatasetStats, DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        // Build metadata JSON before consuming sink (to avoid borrow issues)
        let json_string = self.build_metadata_json()?;

        let (peak_stats, chromatogram_stats, mobilogram_stats, total_size) = match self.sink {
            DatasetSink::Directory {
                root_path,
                mut peak_writer,
                mut chromatogram_writer,
                mut mobilogram_writer,
            } => {
                // Finalize peak writer
                let peak_stats = if let Some(writer) = peak_writer.take() {
                    writer.finish()?
                } else {
                    return Err(DatasetError::NotInitialized);
                };

                // Finalize chromatogram writer
                let chromatogram_stats = if let Some(writer) = chromatogram_writer.take() {
                    Some(
                        writer
                            .finish()
                            .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?,
                    )
                } else {
                    None
                };

                // Finalize mobilogram writer
                let mobilogram_stats = if let Some(writer) = mobilogram_writer.take() {
                    Some(
                        writer
                            .finish()
                            .map_err(|e| DatasetError::MobilogramWriterError(e.to_string()))?,
                    )
                } else {
                    None
                };

                // Write metadata.json to root directory
                let metadata_path = root_path.join("metadata.json");
                let mut file = File::create(metadata_path)?;
                file.write_all(json_string.as_bytes())?;
                file.flush()?;

                // Calculate total dataset size
                let total_size = calculate_directory_size(&root_path)?;

                (peak_stats, chromatogram_stats, mobilogram_stats, total_size)
            }
            DatasetSink::Container {
                output_path,
                mut zip_writer,
                mut peak_writer,
                mut chromatogram_writer,
                mut mobilogram_writer,
            } => {
                // Finalize peak writer and get the buffer
                let (peak_stats, parquet_data) = if let Some(writer) = peak_writer.take() {
                    let buffer = writer.finish_into_inner()?;
                    let data = buffer.into_inner();
                    let stats = WriterStats {
                        spectra_written: 0, // Stats not tracked in container mode buffer
                        peaks_written: 0,
                        row_groups_written: 0,
                        file_size_bytes: data.len() as u64,
                    };
                    (stats, data)
                } else {
                    return Err(DatasetError::NotInitialized);
                };

                // Finalize chromatogram writer and get the buffer
                let (chromatogram_stats, chrom_data_opt) =
                    if let Some(writer) = chromatogram_writer.take() {
                        // Check if any chromatograms were written
                        let stats = writer.stats();
                        if stats.chromatograms_written > 0 {
                            // Extract the buffer
                            match writer.finish_into_inner() {
                                Ok(buffer) => {
                                    let data = buffer.into_inner();
                                    let final_stats = ChromatogramWriterStats {
                                        chromatograms_written: stats.chromatograms_written,
                                        data_points_written: stats.data_points_written,
                                        row_groups_written: 0, // Estimated
                                        file_size_bytes: data.len() as u64,
                                    };
                                    (Some(final_stats), Some(data))
                                }
                                Err(e) => {
                                    log::warn!("Failed to finalize chromatogram writer: {}", e);
                                    (None, None)
                                }
                            }
                        } else {
                            // No chromatograms written, don't include in ZIP
                            (None, None)
                        }
                    } else {
                        (None, None)
                    };

                // Finalize mobilogram writer and get the buffer
                let (mobilogram_stats, mob_data_opt) = if let Some(writer) = mobilogram_writer.take()
                {
                    // Check if any mobilograms were written
                    let stats = writer.stats();
                    if stats.mobilograms_written > 0 {
                        // Extract the buffer
                        match writer.finish_into_inner() {
                            Ok(buffer) => {
                                let data = buffer.into_inner();
                                let final_stats = MobilogramWriterStats {
                                    mobilograms_written: stats.mobilograms_written,
                                    data_points_written: stats.data_points_written,
                                    row_groups_written: 0, // Estimated
                                    file_size_bytes: data.len() as u64,
                                };
                                (Some(final_stats), Some(data))
                            }
                            Err(e) => {
                                log::warn!("Failed to finalize mobilogram writer: {}", e);
                                (None, None)
                            }
                        }
                    } else {
                        // No mobilograms written, don't include in ZIP
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                // Write metadata.json (Deflate compressed)
                let options = SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Deflated)
                    .unix_permissions(0o644);
                zip_writer.start_file("metadata.json", options)?;
                zip_writer.write_all(json_string.as_bytes())?;

                // Write peaks/peaks.parquet (MUST be uncompressed/Stored for seekability)
                let options = SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Stored)
                    .unix_permissions(0o644);
                zip_writer.start_file("peaks/peaks.parquet", options)?;
                zip_writer.write_all(&parquet_data)?;

                // Write chromatograms/chromatograms.parquet if available (MUST be uncompressed/Stored for seekability)
                if let Some(chrom_data) = chrom_data_opt {
                    let options = SimpleFileOptions::default()
                        .compression_method(CompressionMethod::Stored)
                        .unix_permissions(0o644);
                    zip_writer.start_file("chromatograms/chromatograms.parquet", options)?;
                    zip_writer.write_all(&chrom_data)?;
                }

                // Write mobilograms/mobilograms.parquet if available (MUST be uncompressed/Stored for seekability)
                if let Some(mob_data) = mob_data_opt {
                    let options = SimpleFileOptions::default()
                        .compression_method(CompressionMethod::Stored)
                        .unix_permissions(0o644);
                    zip_writer.start_file("mobilograms/mobilograms.parquet", options)?;
                    zip_writer.write_all(&mob_data)?;
                }

                // Finalize the ZIP archive
                let inner = zip_writer.finish()?;
                inner.into_inner().map_err(|e| {
                    DatasetError::IoError(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to flush ZIP buffer: {}", e.error()),
                    ))
                })?;

                // Get final file size
                let total_size = fs::metadata(&output_path)?.len();

                (peak_stats, chromatogram_stats, mobilogram_stats, total_size)
            }
        };

        self.finalized = true;

        let chromatograms_written = chromatogram_stats
            .as_ref()
            .map(|s| s.chromatograms_written)
            .unwrap_or(0);
        let mobilograms_written = mobilogram_stats
            .as_ref()
            .map(|s| s.mobilograms_written)
            .unwrap_or(0);

        Ok(DatasetStats {
            peak_stats,
            chromatogram_stats,
            chromatograms_written,
            mobilogram_stats,
            mobilograms_written,
            total_size_bytes: total_size,
        })
    }

    /// Get the root path of the dataset (directory mode) or output file (container mode)
    pub fn output_path(&self) -> &Path {
        match &self.sink {
            DatasetSink::Directory { root_path, .. } => root_path,
            DatasetSink::Container { output_path, .. } => output_path,
        }
    }

    /// Get the root path of the dataset (for backwards compatibility)
    #[deprecated(since = "0.2.0", note = "Use output_path() instead")]
    pub fn root_path(&self) -> &Path {
        self.output_path()
    }

    /// Get the peaks directory path (only valid in Directory mode)
    pub fn peaks_dir(&self) -> Option<PathBuf> {
        match &self.sink {
            DatasetSink::Directory { root_path, .. } => Some(root_path.join("peaks")),
            DatasetSink::Container { .. } => None,
        }
    }

    /// Get the chromatograms directory path (only valid in Directory mode)
    pub fn chromatograms_dir(&self) -> Option<PathBuf> {
        match &self.sink {
            DatasetSink::Directory { root_path, .. } => Some(root_path.join("chromatograms")),
            DatasetSink::Container { .. } => None,
        }
    }

    /// Get the mobilograms directory path (only valid in Directory mode)
    pub fn mobilograms_dir(&self) -> Option<PathBuf> {
        match &self.sink {
            DatasetSink::Directory { root_path, .. } => Some(root_path.join("mobilograms")),
            DatasetSink::Container { .. } => None,
        }
    }
}

/// Calculate the total size of a directory recursively
fn calculate_directory_size(path: &Path) -> Result<u64, std::io::Error> {
    let mut total_size = 0u64;

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_dir() {
                total_size += calculate_directory_size(&entry.path())?;
            } else {
                total_size += metadata.len();
            }
        }
    } else {
        total_size = fs::metadata(path)?.len();
    }

    Ok(total_size)
}
