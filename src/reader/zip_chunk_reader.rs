//! Seekable reader for stored ZIP entries
//!
//! This module provides a [`ZipEntryChunkReader`] that implements parquet's
//! [`ChunkReader`] trait, enabling streaming reads directly from ZIP containers
//! without loading the entire file into memory.
//!
//! # Requirements
//!
//! The ZIP entry MUST be stored with `Stored` (no compression) method.
//! This is required by the mzPeak format specification to enable random access.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use zip::ZipArchive;

use super::ReaderError;

/// Zero-copy reader for stored ZIP entries
///
/// Implements parquet's [`ChunkReader`] trait to enable streaming reads
/// directly from ZIP container without loading entire file into memory.
///
/// # Thread Safety
///
/// This struct uses interior mutability via `Mutex` to allow concurrent reads
/// from different file handles (via `try_clone`), while the logical offset/size
/// remain immutable.
pub struct ZipEntryChunkReader {
    /// Path to the ZIP file (used for try_clone)
    zip_path: std::path::PathBuf,
    /// Byte offset of entry data within ZIP
    entry_offset: u64,
    /// Size of uncompressed entry
    entry_size: u64,
}

impl ZipEntryChunkReader {
    /// Create a new chunk reader for a stored ZIP entry
    ///
    /// # Arguments
    /// * `zip_path` - Path to the .mzpeak ZIP file
    /// * `entry_name` - Name of the entry (e.g., "peaks/peaks.parquet")
    ///
    /// # Errors
    /// Returns error if:
    /// - Entry is compressed (must be Stored)
    /// - Entry is not found
    /// - I/O error occurs
    ///
    /// # Example
    /// ```rust,no_run
    /// use mzpeak::reader::zip_chunk_reader::ZipEntryChunkReader;
    ///
    /// let reader = ZipEntryChunkReader::new("data.mzpeak", "peaks/peaks.parquet")?;
    /// # Ok::<(), mzpeak::reader::ReaderError>(())
    /// ```
    pub fn new<P: AsRef<Path>>(zip_path: P, entry_name: &str) -> Result<Self, ReaderError> {
        let zip_path = zip_path.as_ref();
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(BufReader::new(file))?;

        let entry = archive.by_name(entry_name).map_err(|_| {
            ReaderError::InvalidFormat(format!("ZIP container missing {}", entry_name))
        })?;

        // Verify entry is Stored (uncompressed) for direct seeking
        if entry.compression() != zip::CompressionMethod::Stored {
            return Err(ReaderError::InvalidFormat(format!(
                "ZIP entry '{}' must be Stored (uncompressed) for streaming access, found {:?}. \
                 The mzPeak format requires Stored entries for efficient random access.",
                entry_name,
                entry.compression()
            )));
        }

        let entry_offset = entry.data_start();
        let entry_size = entry.size();

        Ok(Self {
            zip_path: zip_path.to_path_buf(),
            entry_offset,
            entry_size,
        })
    }

    /// Returns the size of the entry in bytes
    pub fn entry_size(&self) -> u64 {
        self.entry_size
    }

    /// Returns the byte offset of the entry within the ZIP file
    pub fn entry_offset(&self) -> u64 {
        self.entry_offset
    }
}

impl std::fmt::Debug for ZipEntryChunkReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZipEntryChunkReader")
            .field("zip_path", &self.zip_path)
            .field("entry_offset", &self.entry_offset)
            .field("entry_size", &self.entry_size)
            .finish()
    }
}

impl Length for ZipEntryChunkReader {
    fn len(&self) -> u64 {
        self.entry_size
    }
}

/// A reader for a slice of a ZIP entry
///
/// This wraps a file handle positioned at the correct offset within the ZIP
/// and limits reads to not exceed the entry boundary.
pub struct ZipEntrySliceReader {
    /// The underlying file handle
    file: File,
    /// Current position within the logical slice
    position: u64,
    /// Maximum position (entry_size - start offset)
    max_len: u64,
}

impl Read for ZipEntrySliceReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.max_len.saturating_sub(self.position);
        if remaining == 0 {
            return Ok(0);
        }

        let to_read = std::cmp::min(buf.len() as u64, remaining) as usize;
        let n = self.file.read(&mut buf[..to_read])?;
        self.position += n as u64;
        Ok(n)
    }
}

impl ChunkReader for ZipEntryChunkReader {
    type T = ZipEntrySliceReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut file = File::open(&self.zip_path).map_err(|e| {
            parquet::errors::ParquetError::General(format!("Failed to open ZIP file: {}", e))
        })?;

        // Seek to the correct position within the entry
        file.seek(SeekFrom::Start(self.entry_offset + start))
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!("Failed to seek in ZIP: {}", e))
            })?;

        let max_len = self.entry_size.saturating_sub(start);

        Ok(ZipEntrySliceReader {
            file,
            position: 0,
            max_len,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut file = File::open(&self.zip_path).map_err(|e| {
            parquet::errors::ParquetError::General(format!("Failed to open ZIP file: {}", e))
        })?;

        // Seek to the correct position within the entry
        file.seek(SeekFrom::Start(self.entry_offset + start))
            .map_err(|e| {
                parquet::errors::ParquetError::General(format!("Failed to seek in ZIP: {}", e))
            })?;

        // Clamp length to not exceed entry boundary
        let remaining = self.entry_size.saturating_sub(start) as usize;
        let actual_length = std::cmp::min(length, remaining);

        let mut buf = vec![0u8; actual_length];
        file.read_exact(&mut buf).map_err(|e| {
            parquet::errors::ParquetError::General(format!("Failed to read from ZIP: {}", e))
        })?;

        Ok(Bytes::from(buf))
    }
}

// SAFETY: ZipEntryChunkReader is Send + Sync because:
// - zip_path is PathBuf (Send + Sync)
// - entry_offset and entry_size are u64 (Send + Sync)
// - Each method call opens a new file handle, so there's no shared mutable state
unsafe impl Send for ZipEntryChunkReader {}
unsafe impl Sync for ZipEntryChunkReader {}

/// Arc-wrapped ZipEntryChunkReader for sharing across threads
///
/// This is a newtype wrapper that implements ChunkReader, working around
/// Rust's orphan rules while providing the same functionality.
#[derive(Debug, Clone)]
pub struct SharedZipEntryReader(pub std::sync::Arc<ZipEntryChunkReader>);

impl SharedZipEntryReader {
    /// Create a new shared reader from a ZipEntryChunkReader
    pub fn new(reader: ZipEntryChunkReader) -> Self {
        Self(std::sync::Arc::new(reader))
    }

    /// Get a reference to the inner reader
    pub fn inner(&self) -> &ZipEntryChunkReader {
        &self.0
    }
}

impl Length for SharedZipEntryReader {
    fn len(&self) -> u64 {
        self.0.entry_size
    }
}

impl ChunkReader for SharedZipEntryReader {
    type T = ZipEntrySliceReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.0.get_read(start)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.0.get_bytes(start, length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    fn create_test_zip(compression: zip::CompressionMethod) -> NamedTempFile {
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let file = File::create(temp_file.path()).expect("Failed to create file");
        let mut zip = ZipWriter::new(file);

        let options = SimpleFileOptions::default().compression_method(compression);
        zip.start_file("peaks/peaks.parquet", options)
            .expect("Failed to start file");
        zip.write_all(b"PAR1test_data_here_12345PAR1")
            .expect("Failed to write");
        zip.finish().expect("Failed to finish");

        temp_file
    }

    #[test]
    fn test_stored_entry_opens_successfully() {
        let temp = create_test_zip(zip::CompressionMethod::Stored);
        let reader = ZipEntryChunkReader::new(temp.path(), "peaks/peaks.parquet");
        assert!(reader.is_ok());

        let reader = reader.expect("should open");
        assert_eq!(reader.entry_size(), 28); // "PAR1test_data_here_12345PAR1"
    }

    #[test]
    fn test_compressed_entry_fails() {
        let temp = create_test_zip(zip::CompressionMethod::Deflated);
        let result = ZipEntryChunkReader::new(temp.path(), "peaks/peaks.parquet");
        assert!(result.is_err());

        let err = result.expect_err("should fail on compressed");
        assert!(
            format!("{}", err).contains("Stored"),
            "Error should mention Stored requirement"
        );
    }

    #[test]
    fn test_missing_entry_fails() {
        let temp = create_test_zip(zip::CompressionMethod::Stored);
        let result = ZipEntryChunkReader::new(temp.path(), "nonexistent/file.parquet");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_bytes_reads_correctly() {
        let temp = create_test_zip(zip::CompressionMethod::Stored);
        let reader =
            ZipEntryChunkReader::new(temp.path(), "peaks/peaks.parquet").expect("should open");

        // Read first 4 bytes (PAR1)
        let bytes = reader.get_bytes(0, 4).expect("should read");
        assert_eq!(&bytes[..], b"PAR1");

        // Read middle bytes
        let bytes = reader.get_bytes(4, 10).expect("should read");
        assert_eq!(&bytes[..], b"test_data_");

        // Read last 4 bytes
        let bytes = reader.get_bytes(24, 4).expect("should read");
        assert_eq!(&bytes[..], b"PAR1");
    }

    #[test]
    fn test_get_read_reads_correctly() {
        let temp = create_test_zip(zip::CompressionMethod::Stored);
        let reader =
            ZipEntryChunkReader::new(temp.path(), "peaks/peaks.parquet").expect("should open");

        let mut slice_reader = reader.get_read(0).expect("should get reader");
        let mut buf = [0u8; 28];
        slice_reader.read_exact(&mut buf).expect("should read");
        assert_eq!(&buf[..], b"PAR1test_data_here_12345PAR1");
    }

    #[test]
    fn test_length_trait() {
        let temp = create_test_zip(zip::CompressionMethod::Stored);
        let reader =
            ZipEntryChunkReader::new(temp.path(), "peaks/peaks.parquet").expect("should open");

        assert_eq!(Length::len(&reader), 28);
    }
}
