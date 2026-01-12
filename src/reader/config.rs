use super::zip_chunk_reader::SharedZipEntryReader;

/// Configuration for reading mzPeak files
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Batch size for reading records
    pub batch_size: usize,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self { batch_size: 65536 }
    }
}

/// Source type for the reader
///
/// For ZIP containers, uses `SharedZipEntryReader` for streaming access
/// without loading the entire file into memory (Issue 002 fix).
pub(super) enum ReaderSource {
    /// File path for file-based reading (single Parquet file)
    FilePath(std::path::PathBuf),
    /// Seekable reader for ZIP container format (.mzpeak files)
    /// Uses `SharedZipEntryReader` for bounded memory usage
    ZipContainer {
        /// Seekable reader for the peaks/peaks.parquet entry
        chunk_reader: SharedZipEntryReader,
        /// Path to the ZIP file (for subfile access and error messages)
        zip_path: std::path::PathBuf,
    },
}
