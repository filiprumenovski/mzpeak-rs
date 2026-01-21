use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use parquet::file::reader::SerializedFileReader;
use zip::ZipArchive;

use crate::dataset::MZPEAK_V2_MIMETYPE;

use super::config::ReaderSource;
use super::zip_chunk_reader::{SharedZipEntryReader, ZipEntryChunkReader};
use super::{MzPeakReader, ReaderConfig, ReaderError};

impl MzPeakReader {
    /// Open an mzPeak file or directory
    ///
    /// Automatically detects the format:
    /// - `.mzpeak` files are treated as ZIP containers (v1 or v2)
    /// - `.parquet` files are read directly
    /// - Directories are treated as dataset bundles (v1 or v2)
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
            // Directory bundle - check for v2 or v1 format
            let spectra_path = path.join("spectra").join("spectra.parquet");
            let peaks_path = path.join("peaks").join("peaks.parquet");

            if spectra_path.exists() && peaks_path.exists() {
                // v2 format directory bundle
                Self::open_directory_v2(path, config)
            } else if peaks_path.exists() {
                // v1 format directory bundle
                Self::open_parquet_file(&peaks_path, config)
            } else {
                return Err(ReaderError::InvalidFormat(format!(
                    "Directory bundle missing peaks/peaks.parquet: {}",
                    path.display()
                )));
            }
        } else if path.extension().map(|e| e == "mzpeak").unwrap_or(false) {
            // ZIP container format - detect v1 or v2
            Self::open_container(path, config)
        } else {
            // Assume single Parquet file
            Self::open_parquet_file(path, config)
        }
    }

    /// Detect if a ZIP container is v2 format by checking the mimetype
    fn is_v2_container<P: AsRef<Path>>(zip_path: P) -> Result<bool, ReaderError> {
        let file = File::open(zip_path.as_ref())?;
        let mut archive = ZipArchive::new(BufReader::new(file))?;

        // Try to read mimetype file
        let has_mimetype = archive.by_name("mimetype").is_ok();
        drop(archive); // Explicitly drop before reopening

        if has_mimetype {
            // Re-open to read the content
            let file = File::open(zip_path.as_ref())?;
            let mut archive = ZipArchive::new(BufReader::new(file))?;
            let mut entry = archive.by_name("mimetype")?;
            let mut mimetype = String::new();
            entry.read_to_string(&mut mimetype)?;
            return Ok(mimetype.trim() == MZPEAK_V2_MIMETYPE);
        }

        // No mimetype file - check for spectra/spectra.parquet
        let file = File::open(zip_path.as_ref())?;
        let mut archive = ZipArchive::new(BufReader::new(file))?;
        let has_spectra = archive.by_name("spectra/spectra.parquet").is_ok();
        Ok(has_spectra)
    }

    /// Open a ZIP container format file (v1 or v2)
    ///
    /// Uses `SharedZipEntryReader` for streaming access without loading the
    /// entire Parquet file into memory (Issue 002 fix).
    fn open_container<P: AsRef<Path>>(path: P, config: ReaderConfig) -> Result<Self, ReaderError> {
        let zip_path = path.as_ref().to_path_buf();

        // Detect v2 format
        if Self::is_v2_container(&zip_path)? {
            Self::open_container_v2(zip_path, config)
        } else {
            Self::open_container_v1(zip_path, config)
        }
    }

    /// Open a v1 format ZIP container
    fn open_container_v1(
        zip_path: std::path::PathBuf,
        config: ReaderConfig,
    ) -> Result<Self, ReaderError> {
        // Create seekable chunk reader for the peaks parquet entry
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

    /// Open a v2 format ZIP container with separate spectra and peaks tables
    fn open_container_v2(
        zip_path: std::path::PathBuf,
        config: ReaderConfig,
    ) -> Result<Self, ReaderError> {
        // Create seekable chunk readers for both parquet entries
        let peaks_chunk_reader = ZipEntryChunkReader::new(&zip_path, "peaks/peaks.parquet")?;
        let peaks_chunk_reader = SharedZipEntryReader::new(peaks_chunk_reader);

        let spectra_chunk_reader = ZipEntryChunkReader::new(&zip_path, "spectra/spectra.parquet")?;
        let spectra_chunk_reader = SharedZipEntryReader::new(spectra_chunk_reader);

        // Extract metadata from peaks file (for compatibility with v1 API)
        let file_metadata = Self::extract_file_metadata_from_chunk_reader(&peaks_chunk_reader)?;

        Ok(Self {
            source: ReaderSource::ZipContainerV2 {
                peaks_chunk_reader,
                spectra_chunk_reader,
                zip_path,
            },
            config,
            file_metadata,
        })
    }

    /// Open a v2 format directory bundle
    fn open_directory_v2<P: AsRef<Path>>(
        path: P,
        config: ReaderConfig,
    ) -> Result<Self, ReaderError> {
        let path = path.as_ref();
        let peaks_path = path.join("peaks").join("peaks.parquet");
        let spectra_path = path.join("spectra").join("spectra.parquet");

        // Extract metadata from peaks file
        let file = File::open(&peaks_path)?;
        let parquet_reader = SerializedFileReader::new(file)?;
        let file_metadata = Self::extract_file_metadata(&parquet_reader)?;

        Ok(Self {
            source: ReaderSource::DirectoryV2 {
                peaks_path,
                spectra_path,
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
