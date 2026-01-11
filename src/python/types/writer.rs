use pyo3::prelude::*;

use crate::writer::{CompressionType, WriterConfig, WriterStats};

/// Configuration for mzPeak writers
#[pyclass(name = "WriterConfig")]
#[derive(Clone)]
pub struct PyWriterConfig {
    pub(crate) inner: WriterConfig,
}

#[pymethods]
impl PyWriterConfig {
    /// Create a new writer configuration
    ///
    /// Args:
    ///     compression: Compression type ("zstd", "snappy", or "none")
    ///     compression_level: ZSTD compression level (1-22, default 9)
    ///     row_group_size: Number of rows per row group (default 100000)
    ///     data_page_size: Data page size in bytes (default 1MB)
    #[new]
    #[pyo3(signature = (compression="zstd", compression_level=9, row_group_size=100000, data_page_size=1048576))]
    fn new(
        compression: &str,
        compression_level: i32,
        row_group_size: usize,
        data_page_size: usize,
    ) -> PyResult<Self> {
        let compression_type = match compression.to_lowercase().as_str() {
            "zstd" => CompressionType::Zstd(compression_level),
            "snappy" => CompressionType::Snappy,
            "none" | "uncompressed" => CompressionType::Uncompressed,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unknown compression type: {}. Use 'zstd', 'snappy', or 'none'.",
                    compression
                )))
            }
        };

        Ok(Self {
            inner: WriterConfig {
                compression: compression_type,
                row_group_size,
                data_page_size,
                ..Default::default()
            },
        })
    }

    /// Create default configuration
    #[staticmethod]
    fn default() -> Self {
        Self {
            inner: WriterConfig::default(),
        }
    }

    /// Row group size
    #[getter]
    fn row_group_size(&self) -> usize {
        self.inner.row_group_size
    }

    /// Data page size in bytes
    #[getter]
    fn data_page_size(&self) -> usize {
        self.inner.data_page_size
    }

    fn __repr__(&self) -> String {
        format!(
            "WriterConfig(row_group_size={}, data_page_size={})",
            self.inner.row_group_size, self.inner.data_page_size
        )
    }
}

impl Default for PyWriterConfig {
    fn default() -> Self {
        Self {
            inner: WriterConfig::default(),
        }
    }
}

/// Statistics from a writer operation
#[pyclass(name = "WriterStats")]
#[derive(Clone)]
pub struct PyWriterStats {
    inner: WriterStats,
}

#[pymethods]
impl PyWriterStats {
    /// Number of spectra written
    #[getter]
    fn spectra_written(&self) -> usize {
        self.inner.spectra_written
    }

    /// Number of peaks written
    #[getter]
    fn peaks_written(&self) -> usize {
        self.inner.peaks_written
    }

    /// Number of row groups written
    #[getter]
    fn row_groups_written(&self) -> usize {
        self.inner.row_groups_written
    }

    /// Output file size in bytes
    #[getter]
    fn file_size_bytes(&self) -> u64 {
        self.inner.file_size_bytes
    }

    fn __repr__(&self) -> String {
        format!(
            "WriterStats(spectra={}, peaks={}, size={} bytes)",
            self.inner.spectra_written, self.inner.peaks_written, self.inner.file_size_bytes
        )
    }
}

impl From<WriterStats> for PyWriterStats {
    fn from(stats: WriterStats) -> Self {
        Self { inner: stats }
    }
}
