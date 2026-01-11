use bytes::Bytes;

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

/// Source type for the reader (stores path or bytes for re-reading)
pub(super) enum ReaderSource {
    /// File path for file-based reading
    FilePath(std::path::PathBuf),
    /// Bytes for in-memory reading (ZIP containers), with original path for re-opening
    ZipContainer {
        peaks_bytes: Bytes,
        zip_path: std::path::PathBuf,
    },
}
