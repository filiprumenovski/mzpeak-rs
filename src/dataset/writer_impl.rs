use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
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
use crate::writer::{MzPeakWriter, SpectrumArrays, WriterConfig, WriterStats};

use super::error::DatasetError;
use super::stats::DatasetStats;
use super::types::OutputMode;

/// Buffer that writes Parquet data to a temp file for later ZIP inclusion.
///
/// This replaces the previous in-memory buffer approach (Issue 000 fix) to provide
/// bounded memory usage. The temp file is automatically cleaned up when dropped.
///
/// # Memory Bounds
///
/// Memory usage is now O(buffer_size) instead of O(file_size), where buffer_size
/// is the internal buffer of BufWriter (typically 8KB).
struct ParquetTempFile {
    /// The temp file holding the Parquet data
    temp_file: NamedTempFile,
    /// Buffered writer for efficient writes
    writer: BufWriter<File>,
}

impl ParquetTempFile {
    fn new() -> std::io::Result<Self> {
        let temp_file = NamedTempFile::new()?;
        // Clone the file handle for writing
        let file = temp_file.reopen()?;
        let writer = BufWriter::new(file);
        Ok(Self { temp_file, writer })
    }

    /// Get the size of the written data
    fn size(&self) -> std::io::Result<u64> {
        self.temp_file.as_file().metadata().map(|m| m.len())
    }

    /// Consume this buffer and return a reader for streaming the data to ZIP
    fn into_reader(mut self) -> std::io::Result<(u64, BufReader<File>)> {
        // Flush any buffered data
        self.writer.flush()?;

        // Get the file size before reopening
        let size = self.size()?;

        // Reopen the temp file for reading from the beginning
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
        // Flush before seeking to ensure data is on disk
        self.writer.flush()?;
        // Get the underlying file and seek it
        self.writer.get_mut().seek(pos)
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
    /// Uses temp files for bounded memory (Issue 000 fix)
    Container {
        output_path: PathBuf,
        zip_writer: ZipWriter<BufWriter<File>>,
        peak_writer: Option<MzPeakWriter<ParquetTempFile>>,
        chromatogram_writer: Option<ChromatogramWriter<ParquetTempFile>>,
        mobilogram_writer: Option<MobilogramWriter<ParquetTempFile>>,
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

        // Initialize peak writer to temp file (bounded memory - Issue 000 fix)
        let peak_buffer = ParquetTempFile::new()?;
        let peak_writer = MzPeakWriter::new(peak_buffer, metadata, config.clone())?;

        // Initialize chromatogram writer to temp file
        let chrom_buffer = ParquetTempFile::new()?;
        let chrom_config = ChromatogramWriterConfig::default();
        let chrom_writer = ChromatogramWriter::new(chrom_buffer, metadata, chrom_config)
            .map_err(|e| DatasetError::ChromatogramWriterError(e.to_string()))?;

        // Initialize mobilogram writer to temp file
        let mob_buffer = ParquetTempFile::new()?;
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
            finalized: false,
        })
    }

    /// Get the output mode being used
    pub fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Write a single spectrum with SoA peak layout to the dataset.
    pub fn write_spectrum_arrays(
        &mut self,
        spectrum: &SpectrumArrays,
    ) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectrum_arrays(spectrum)?;
            }
            DatasetSink::Container { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectrum_arrays(spectrum)?;
            }
        }
        Ok(())
    }

    /// Write multiple spectra with SoA peak layout to the dataset.
    pub fn write_spectra_arrays(
        &mut self,
        spectra: &[SpectrumArrays],
    ) -> Result<(), DatasetError> {
        if self.finalized {
            return Err(DatasetError::NotInitialized);
        }

        match &mut self.sink {
            DatasetSink::Directory { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectra_arrays(spectra)?;
            }
            DatasetSink::Container { peak_writer, .. } => {
                let writer = peak_writer.as_mut().ok_or(DatasetError::NotInitialized)?;
                writer.write_spectra_arrays(spectra)?;
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
                // Finalize peak writer and get streaming reader (Issue 000 fix - bounded memory)
                let (peak_stats, peak_reader) = if let Some(writer) = peak_writer.take() {
                    let temp_file = writer.finish_into_inner()?;
                    let (size, reader) = temp_file.into_reader()?;
                    let stats = WriterStats {
                        spectra_written: 0, // Stats not tracked in container mode buffer
                        peaks_written: 0,
                        row_groups_written: 0,
                        file_size_bytes: size,
                    };
                    (stats, reader)
                } else {
                    return Err(DatasetError::NotInitialized);
                };

                // Finalize chromatogram writer and get streaming reader
                let (chromatogram_stats, chrom_reader_opt) =
                    if let Some(writer) = chromatogram_writer.take() {
                        // Check if any chromatograms were written
                        let stats = writer.stats();
                        if stats.chromatograms_written > 0 {
                            // Extract the temp file and get a reader
                            match writer.finish_into_inner() {
                                Ok(temp_file) => match temp_file.into_reader() {
                                    Ok((size, reader)) => {
                                        let final_stats = ChromatogramWriterStats {
                                            chromatograms_written: stats.chromatograms_written,
                                            data_points_written: stats.data_points_written,
                                            row_groups_written: 0, // Estimated
                                            file_size_bytes: size,
                                        };
                                        (Some(final_stats), Some(reader))
                                    }
                                    Err(e) => {
                                        log::warn!("Failed to read chromatogram temp file: {}", e);
                                        (None, None)
                                    }
                                },
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

                // Finalize mobilogram writer and get streaming reader
                let (mobilogram_stats, mob_reader_opt) = if let Some(writer) = mobilogram_writer.take()
                {
                    // Check if any mobilograms were written
                    let stats = writer.stats();
                    if stats.mobilograms_written > 0 {
                        // Extract the temp file and get a reader
                        match writer.finish_into_inner() {
                            Ok(temp_file) => match temp_file.into_reader() {
                                Ok((size, reader)) => {
                                    let final_stats = MobilogramWriterStats {
                                        mobilograms_written: stats.mobilograms_written,
                                        data_points_written: stats.data_points_written,
                                        row_groups_written: 0, // Estimated
                                        file_size_bytes: size,
                                    };
                                    (Some(final_stats), Some(reader))
                                }
                                Err(e) => {
                                    log::warn!("Failed to read mobilogram temp file: {}", e);
                                    (None, None)
                                }
                            },
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
                // Stream from temp file to ZIP with bounded memory (Issue 000 fix)
                let options = SimpleFileOptions::default()
                    .compression_method(CompressionMethod::Stored)
                    .unix_permissions(0o644);
                zip_writer.start_file("peaks/peaks.parquet", options)?;
                stream_copy_to_zip(peak_reader, &mut zip_writer)?;

                // Write chromatograms/chromatograms.parquet if available (MUST be uncompressed/Stored for seekability)
                if let Some(chrom_reader) = chrom_reader_opt {
                    let options = SimpleFileOptions::default()
                        .compression_method(CompressionMethod::Stored)
                        .unix_permissions(0o644);
                    zip_writer.start_file("chromatograms/chromatograms.parquet", options)?;
                    stream_copy_to_zip(chrom_reader, &mut zip_writer)?;
                }

                // Write mobilograms/mobilograms.parquet if available (MUST be uncompressed/Stored for seekability)
                if let Some(mob_reader) = mob_reader_opt {
                    let options = SimpleFileOptions::default()
                        .compression_method(CompressionMethod::Stored)
                        .unix_permissions(0o644);
                    zip_writer.start_file("mobilograms/mobilograms.parquet", options)?;
                    stream_copy_to_zip(mob_reader, &mut zip_writer)?;
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

/// Copy data from a reader to a ZIP writer with bounded memory
///
/// Uses a fixed-size buffer (64KB) for streaming copy, ensuring memory usage
/// is O(buffer_size) instead of O(file_size). This is the Issue 000 fix.
const STREAM_COPY_BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer

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
