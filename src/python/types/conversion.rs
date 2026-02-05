use pyo3::prelude::*;

use crate::mzml::converter::{ConversionConfig, ConversionStats, OutputFormat, StreamingConfig};
use crate::schema::manifest::Modality;
use crate::writer::{CompressionType, WriterConfig};

/// Output format for conversion (v1 legacy or v2 container)
#[pyclass(name = "OutputFormat")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyOutputFormat {
    /// mzPeak v2.0 container format (default, recommended)
    V2Container = 0,
    /// Legacy v1 Parquet file (.mzpeak.parquet)
    V1Parquet = 1,
}

impl From<PyOutputFormat> for OutputFormat {
    fn from(py_fmt: PyOutputFormat) -> Self {
        match py_fmt {
            PyOutputFormat::V2Container => Self::V2Container,
            PyOutputFormat::V1Parquet => Self::V1Parquet,
        }
    }
}

impl From<OutputFormat> for PyOutputFormat {
    fn from(fmt: OutputFormat) -> Self {
        match fmt {
            OutputFormat::V2Container => Self::V2Container,
            OutputFormat::V1Parquet => Self::V1Parquet,
        }
    }
}

/// Data modality for conversion output
#[pyclass(name = "Modality")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyModality {
    /// LC-MS: 3D data (RT, m/z, intensity)
    LcMs = 0,
    /// LC-IMS-MS: 4D data with ion mobility
    LcImsMs = 1,
    /// MSI: Mass spectrometry imaging without ion mobility
    Msi = 2,
    /// MSI-IMS: Mass spectrometry imaging with ion mobility
    MsiIms = 3,
}

impl From<PyModality> for Modality {
    fn from(py_mod: PyModality) -> Self {
        match py_mod {
            PyModality::LcMs => Self::LcMs,
            PyModality::LcImsMs => Self::LcImsMs,
            PyModality::Msi => Self::Msi,
            PyModality::MsiIms => Self::MsiIms,
        }
    }
}

impl From<Modality> for PyModality {
    fn from(m: Modality) -> Self {
        match m {
            Modality::LcMs => Self::LcMs,
            Modality::LcImsMs => Self::LcImsMs,
            Modality::Msi => Self::Msi,
            Modality::MsiIms => Self::MsiIms,
        }
    }
}

#[pymethods]
impl PyModality {
    /// Check if this modality includes ion mobility data
    fn has_ion_mobility(&self) -> bool {
        Modality::from(*self).has_ion_mobility()
    }

    /// Check if this modality includes imaging data
    fn has_imaging(&self) -> bool {
        Modality::from(*self).has_imaging()
    }

    /// Create modality from flags
    #[staticmethod]
    fn from_flags(has_ion_mobility: bool, has_imaging: bool) -> Self {
        Modality::from_flags(has_ion_mobility, has_imaging).into()
    }

    fn __repr__(&self) -> String {
        match self {
            Self::LcMs => "Modality.LcMs".to_string(),
            Self::LcImsMs => "Modality.LcImsMs".to_string(),
            Self::Msi => "Modality.Msi".to_string(),
            Self::MsiIms => "Modality.MsiIms".to_string(),
        }
    }
}

/// Streaming configuration for memory-bounded conversion
#[pyclass(name = "StreamingConfig")]
#[derive(Clone)]
pub struct PyStreamingConfig {
    pub(crate) inner: StreamingConfig,
}

#[pymethods]
impl PyStreamingConfig {
    /// Create a new streaming configuration
    ///
    /// Args:
    ///     input_buffer_size: Size of input buffer in bytes (default 64KB)
    ///     streaming_mode: Enable streaming mode for bounded memory (default True)
    #[new]
    #[pyo3(signature = (input_buffer_size=65536, streaming_mode=true))]
    fn new(input_buffer_size: usize, streaming_mode: bool) -> Self {
        Self {
            inner: StreamingConfig {
                input_buffer_size,
                max_container_buffer_bytes: None,
                streaming_mode,
            },
        }
    }

    /// Create default streaming configuration
    #[staticmethod]
    fn default() -> Self {
        Self {
            inner: StreamingConfig::default(),
        }
    }

    /// Create config optimized for low memory usage
    #[staticmethod]
    fn low_memory() -> Self {
        Self {
            inner: StreamingConfig::low_memory(),
        }
    }

    /// Create config optimized for throughput
    #[staticmethod]
    fn high_throughput() -> Self {
        Self {
            inner: StreamingConfig::high_throughput(),
        }
    }

    /// Input buffer size in bytes
    #[getter]
    fn input_buffer_size(&self) -> usize {
        self.inner.input_buffer_size
    }

    /// Whether streaming mode is enabled
    #[getter]
    fn streaming_mode(&self) -> bool {
        self.inner.streaming_mode
    }

    fn __repr__(&self) -> String {
        format!(
            "StreamingConfig(input_buffer_size={}, streaming_mode={})",
            self.inner.input_buffer_size, self.inner.streaming_mode
        )
    }
}

/// Configuration for mzML conversion
///
/// Provides full control over conversion settings including output format,
/// compression, streaming behavior, and metadata options.
///
/// Example:
///     >>> config = ConversionConfig(
///     ...     batch_size=200,
///     ...     output_format=OutputFormat.V2Container,
///     ...     compression_level=9,
///     ... )
///     >>> stats = mzpeak.convert("input.mzML", "output.mzpeak", config)
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
    ///     output_format: Output format (V2Container or V1Parquet, default V2Container)
    ///     modality: Data modality override (auto-detect if None)
    ///     compression_level: ZSTD compression level 1-22 (default 9)
    ///     row_group_size: Rows per Parquet row group (default 100000)
    ///     sdrf_path: Optional path to SDRF metadata file
    ///     streaming_config: Optional streaming configuration
    #[new]
    #[pyo3(signature = (
        batch_size=100,
        preserve_precision=true,
        include_chromatograms=true,
        progress_interval=1000,
        output_format=None,
        modality=None,
        compression_level=None,
        row_group_size=None,
        sdrf_path=None,
        streaming_config=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        batch_size: usize,
        preserve_precision: bool,
        include_chromatograms: bool,
        progress_interval: usize,
        output_format: Option<PyOutputFormat>,
        modality: Option<PyModality>,
        compression_level: Option<i32>,
        row_group_size: Option<usize>,
        sdrf_path: Option<String>,
        streaming_config: Option<PyStreamingConfig>,
    ) -> Self {
        let mut config = ConversionConfig::default();
        config.batch_size = batch_size;
        config.preserve_precision = preserve_precision;
        config.include_chromatograms = include_chromatograms;
        config.progress_interval = progress_interval;

        if let Some(fmt) = output_format {
            config.output_format = fmt.into();
        }
        if let Some(mod_) = modality {
            config.modality = Some(mod_.into());
        }
        if let Some(level) = compression_level {
            config.writer_config.compression = CompressionType::Zstd(level);
        }
        if let Some(size) = row_group_size {
            config.writer_config.row_group_size = size;
        }
        config.sdrf_path = sdrf_path;
        if let Some(sc) = streaming_config {
            config.streaming_config = sc.inner;
        }

        Self { inner: config }
    }

    /// Create default configuration
    #[staticmethod]
    fn default() -> Self {
        Self {
            inner: ConversionConfig::default(),
        }
    }

    /// Create configuration optimized for maximum compression
    #[staticmethod]
    fn max_compression() -> Self {
        Self {
            inner: ConversionConfig::max_compression(),
        }
    }

    /// Create configuration optimized for fast writing
    #[staticmethod]
    fn fast_write() -> Self {
        Self {
            inner: ConversionConfig::fast_write(),
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

    /// Output format (V2Container or V1Parquet)
    #[getter]
    fn output_format(&self) -> PyOutputFormat {
        self.inner.output_format.into()
    }

    /// Data modality override (None for auto-detect)
    #[getter]
    fn modality(&self) -> Option<PyModality> {
        self.inner.modality.map(|m| m.into())
    }

    /// Compression level (for ZSTD)
    #[getter]
    fn compression_level(&self) -> Option<i32> {
        match self.inner.writer_config.compression {
            CompressionType::Zstd(level) => Some(level),
            _ => None,
        }
    }

    /// Row group size for Parquet output
    #[getter]
    fn row_group_size(&self) -> usize {
        self.inner.writer_config.row_group_size
    }

    /// SDRF metadata file path
    #[getter]
    fn sdrf_path(&self) -> Option<String> {
        self.inner.sdrf_path.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "ConversionConfig(batch_size={}, preserve_precision={}, output_format={:?}, compression_level={:?})",
            self.inner.batch_size,
            self.inner.preserve_precision,
            self.output_format(),
            self.compression_level()
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
