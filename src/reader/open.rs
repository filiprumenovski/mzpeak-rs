use std::fs::File;
use std::path::Path;

use parquet::file::reader::SerializedFileReader;

use super::config::ReaderSource;
use super::zip_chunk_reader::{SharedZipEntryReader, ZipEntryChunkReader};
use super::{MzPeakReader, ReaderConfig, ReaderError};

impl MzPeakReader {
    /// Open an mzPeak file or directory
    ///
    /// Automatically detects the format:
    /// - `.mzpeak` files are treated as ZIP containers
    /// - `.parquet` files are read directly
    /// - Directories are treated as dataset bundles
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, ReaderError> {
        Self::open_with_config(path, ReaderConfig::default())
    }

    /// Open an mzPeak file with custom configuration
    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        config: ReaderConfig,
    ) -> Result<Self, ReaderError> {
        let path = path.as_ref();

        if path.is_dir() {
            // Directory bundle - look for peaks/peaks.parquet
            let peaks_path = path.join("peaks").join("peaks.parquet");
            if !peaks_path.exists() {
                return Err(ReaderError::InvalidFormat(format!(
                    "Directory bundle missing peaks/peaks.parquet: {}",
                    path.display()
                )));
            }
            Self::open_parquet_file(&peaks_path, config)
        } else if path.extension().map(|e| e == "mzpeak").unwrap_or(false) {
            // ZIP container format
            Self::open_container(path, config)
        } else {
            // Assume single Parquet file
            Self::open_parquet_file(path, config)
        }
    }

    /// Open a ZIP container format file
    ///
    /// Uses `SharedZipEntryReader` for streaming access without loading the
    /// entire Parquet file into memory (Issue 002 fix).
    fn open_container<P: AsRef<Path>>(path: P, config: ReaderConfig) -> Result<Self, ReaderError> {
        let zip_path = path.as_ref().to_path_buf();

        // Create seekable chunk reader for the peaks parquet entry
        // This validates that the entry is Stored (uncompressed) and fails fast if not
        let chunk_reader = ZipEntryChunkReader::new(&zip_path, "peaks/peaks.parquet")?;
        let chunk_reader = SharedZipEntryReader::new(chunk_reader);

        // Extract metadata using the chunk reader
        let file_metadata = Self::extract_file_metadata_from_chunk_reader(&chunk_reader)?;

        Ok(Self {
            source: ReaderSource::ZipContainer {
                chunk_reader,
                zip_path,
            },
            config,
            file_metadata,
        })
    }

    /// Open a single Parquet file directly
    fn open_parquet_file<P: AsRef<Path>>(
        path: P,
        config: ReaderConfig,
    ) -> Result<Self, ReaderError> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let parquet_reader = SerializedFileReader::new(file)?;

        let file_metadata = Self::extract_file_metadata(&parquet_reader)?;

        Ok(Self {
            source: ReaderSource::FilePath(path),
            config,
            file_metadata,
        })
    }
}
