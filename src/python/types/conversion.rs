use pyo3::prelude::*;

use crate::mzml::converter::{ConversionConfig, ConversionStats};

/// Configuration for mzML conversion
#[pyclass(name = "ConversionConfig")]
#[derive(Clone)]
pub struct PyConversionConfig {
    pub(crate) inner: ConversionConfig,
}

#[pymethods]
impl PyConversionConfig {
    /// Create a new conversion configuration
    ///
    /// Args:
    ///     batch_size: Number of spectra to process per batch (default 100)
    ///     preserve_precision: Keep original numeric precision (default True)
    ///     include_chromatograms: Include chromatogram data (default True)
    ///     progress_interval: Log progress every N spectra (default 1000)
    #[new]
    #[pyo3(signature = (batch_size=100, preserve_precision=true, include_chromatograms=true, progress_interval=1000))]
    fn new(
        batch_size: usize,
        preserve_precision: bool,
        include_chromatograms: bool,
        progress_interval: usize,
    ) -> Self {
        let mut config = ConversionConfig::default();
        config.batch_size = batch_size;
        config.preserve_precision = preserve_precision;
        config.include_chromatograms = include_chromatograms;
        config.progress_interval = progress_interval;
        Self { inner: config }
    }

    /// Create default configuration
    #[staticmethod]
    fn default() -> Self {
        Self {
            inner: ConversionConfig::default(),
        }
    }

    /// Batch size for processing
    #[getter]
    fn batch_size(&self) -> usize {
        self.inner.batch_size
    }

    /// Whether to preserve original numeric precision
    #[getter]
    fn preserve_precision(&self) -> bool {
        self.inner.preserve_precision
    }

    /// Whether to include chromatogram data
    #[getter]
    fn include_chromatograms(&self) -> bool {
        self.inner.include_chromatograms
    }

    /// Progress logging interval
    #[getter]
    fn progress_interval(&self) -> usize {
        self.inner.progress_interval
    }

    fn __repr__(&self) -> String {
        format!(
            "ConversionConfig(batch_size={}, preserve_precision={}, include_chromatograms={})",
            self.inner.batch_size, self.inner.preserve_precision, self.inner.include_chromatograms
        )
    }
}

/// Statistics from a conversion operation
#[pyclass(name = "ConversionStats")]
#[derive(Clone)]
pub struct PyConversionStats {
    inner: ConversionStats,
}

#[pymethods]
impl PyConversionStats {
    /// Total number of spectra converted
    #[getter]
    fn spectra_count(&self) -> usize {
        self.inner.spectra_count
    }

    /// Total number of peaks converted
    #[getter]
    fn peak_count(&self) -> usize {
        self.inner.peak_count
    }

    /// Number of MS1 spectra
    #[getter]
    fn ms1_spectra(&self) -> usize {
        self.inner.ms1_spectra
    }

    /// Number of MS2 spectra
    #[getter]
    fn ms2_spectra(&self) -> usize {
        self.inner.ms2_spectra
    }

    /// Number of MSn spectra (n > 2)
    #[getter]
    fn msn_spectra(&self) -> usize {
        self.inner.msn_spectra
    }

    /// Number of chromatograms converted
    #[getter]
    fn chromatograms_converted(&self) -> usize {
        self.inner.chromatograms_converted
    }

    /// Source file size in bytes
    #[getter]
    fn source_file_size(&self) -> u64 {
        self.inner.source_file_size
    }

    /// Output file size in bytes
    #[getter]
    fn output_file_size(&self) -> u64 {
        self.inner.output_file_size
    }

    /// Compression ratio achieved
    #[getter]
    fn compression_ratio(&self) -> f64 {
        self.inner.compression_ratio
    }

    fn __repr__(&self) -> String {
        format!(
            "ConversionStats(spectra={}, peaks={}, compression_ratio={:.2}x)",
            self.inner.spectra_count, self.inner.peak_count, self.inner.compression_ratio
        )
    }
}

impl From<ConversionStats> for PyConversionStats {
    fn from(stats: ConversionStats) -> Self {
        Self { inner: stats }
    }
}
