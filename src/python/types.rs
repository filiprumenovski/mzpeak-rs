//! Python-friendly data types for mzPeak
//!
//! Provides Python classes with property accessors for core mzPeak types.

use pyo3::prelude::*;
use std::collections::HashMap;

use crate::chromatogram_writer::Chromatogram;
use crate::mobilogram_writer::Mobilogram;
use crate::mzml::converter::{ConversionConfig, ConversionStats};
use crate::reader::{FileMetadata, FileSummary};
use crate::writer::{CompressionType, Peak, Spectrum, WriterConfig, WriterStats};

/// A single mass spectrometry peak (m/z, intensity pair)
#[pyclass(name = "Peak")]
#[derive(Clone)]
pub struct PyPeak {
    inner: Peak,
}

#[pymethods]
impl PyPeak {
    /// Create a new peak
    ///
    /// Args:
    ///     mz: Mass-to-charge ratio
    ///     intensity: Signal intensity
    ///     ion_mobility: Optional ion mobility value
    #[new]
    #[pyo3(signature = (mz, intensity, ion_mobility=None))]
    fn new(mz: f64, intensity: f32, ion_mobility: Option<f64>) -> Self {
        Self {
            inner: Peak {
                mz,
                intensity,
                ion_mobility,
            },
        }
    }

    /// Mass-to-charge ratio
    #[getter]
    fn mz(&self) -> f64 {
        self.inner.mz
    }

    /// Signal intensity
    #[getter]
    fn intensity(&self) -> f32 {
        self.inner.intensity
    }

    /// Ion mobility value (if available)
    #[getter]
    fn ion_mobility(&self) -> Option<f64> {
        self.inner.ion_mobility
    }

    fn __repr__(&self) -> String {
        match self.inner.ion_mobility {
            Some(im) => format!(
                "Peak(mz={:.4}, intensity={:.1}, ion_mobility={:.4})",
                self.inner.mz, self.inner.intensity, im
            ),
            None => format!(
                "Peak(mz={:.4}, intensity={:.1})",
                self.inner.mz, self.inner.intensity
            ),
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<Peak> for PyPeak {
    fn from(peak: Peak) -> Self {
        Self { inner: peak }
    }
}

impl From<PyPeak> for Peak {
    fn from(py_peak: PyPeak) -> Self {
        py_peak.inner
    }
}

/// A mass spectrum containing peaks and metadata
#[pyclass(name = "Spectrum")]
#[derive(Clone)]
pub struct PySpectrum {
    pub(crate) inner: Spectrum,
}

#[pymethods]
impl PySpectrum {
    /// Create a new spectrum
    #[new]
    #[pyo3(signature = (spectrum_id, scan_number, ms_level, retention_time, polarity, peaks=None))]
    fn new(
        spectrum_id: i64,
        scan_number: i64,
        ms_level: i16,
        retention_time: f32,
        polarity: i8,
        peaks: Option<Vec<PyPeak>>,
    ) -> Self {
        Self {
            inner: Spectrum {
                spectrum_id,
                scan_number,
                ms_level,
                retention_time,
                polarity,
                peaks: peaks
                    .map(|p| p.into_iter().map(|pp| pp.inner).collect())
                    .unwrap_or_default(),
                precursor_mz: None,
                precursor_charge: None,
                precursor_intensity: None,
                isolation_window_lower: None,
                isolation_window_upper: None,
                collision_energy: None,
                total_ion_current: None,
                base_peak_mz: None,
                base_peak_intensity: None,
                injection_time: None,
                pixel_x: None,
                pixel_y: None,
                pixel_z: None,
            },
        }
    }

    /// Unique spectrum identifier
    #[getter]
    fn spectrum_id(&self) -> i64 {
        self.inner.spectrum_id
    }

    /// Native scan number
    #[getter]
    fn scan_number(&self) -> i64 {
        self.inner.scan_number
    }

    /// MS level (1 for MS1, 2 for MS2, etc.)
    #[getter]
    fn ms_level(&self) -> i16 {
        self.inner.ms_level
    }

    /// Retention time in seconds
    #[getter]
    fn retention_time(&self) -> f32 {
        self.inner.retention_time
    }

    /// Polarity (1 for positive, -1 for negative)
    #[getter]
    fn polarity(&self) -> i8 {
        self.inner.polarity
    }

    /// List of peaks in this spectrum
    #[getter]
    fn peaks(&self) -> Vec<PyPeak> {
        self.inner.peaks.iter().cloned().map(PyPeak::from).collect()
    }

    /// Number of peaks in this spectrum
    #[getter]
    fn num_peaks(&self) -> usize {
        self.inner.peaks.len()
    }

    /// Precursor m/z (for MS2+ spectra)
    #[getter]
    fn precursor_mz(&self) -> Option<f64> {
        self.inner.precursor_mz
    }

    /// Precursor charge state
    #[getter]
    fn precursor_charge(&self) -> Option<i16> {
        self.inner.precursor_charge
    }

    /// Precursor intensity
    #[getter]
    fn precursor_intensity(&self) -> Option<f32> {
        self.inner.precursor_intensity
    }

    /// Lower isolation window offset
    #[getter]
    fn isolation_window_lower(&self) -> Option<f32> {
        self.inner.isolation_window_lower
    }

    /// Upper isolation window offset
    #[getter]
    fn isolation_window_upper(&self) -> Option<f32> {
        self.inner.isolation_window_upper
    }

    /// Collision energy in eV
    #[getter]
    fn collision_energy(&self) -> Option<f32> {
        self.inner.collision_energy
    }

    /// Total ion current
    #[getter]
    fn total_ion_current(&self) -> Option<f64> {
        self.inner.total_ion_current
    }

    /// Base peak m/z
    #[getter]
    fn base_peak_mz(&self) -> Option<f64> {
        self.inner.base_peak_mz
    }

    /// Base peak intensity
    #[getter]
    fn base_peak_intensity(&self) -> Option<f32> {
        self.inner.base_peak_intensity
    }

    /// Ion injection time in milliseconds
    #[getter]
    fn injection_time(&self) -> Option<f32> {
        self.inner.injection_time
    }

    /// MSI pixel X coordinate
    #[getter]
    fn pixel_x(&self) -> Option<i32> {
        self.inner.pixel_x
    }

    /// MSI pixel Y coordinate
    #[getter]
    fn pixel_y(&self) -> Option<i32> {
        self.inner.pixel_y
    }

    /// MSI pixel Z coordinate
    #[getter]
    fn pixel_z(&self) -> Option<i32> {
        self.inner.pixel_z
    }

    fn __repr__(&self) -> String {
        format!(
            "Spectrum(id={}, scan={}, ms_level={}, rt={:.2}s, {} peaks)",
            self.inner.spectrum_id,
            self.inner.scan_number,
            self.inner.ms_level,
            self.inner.retention_time,
            self.inner.peaks.len()
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __len__(&self) -> usize {
        self.inner.peaks.len()
    }
}

impl From<Spectrum> for PySpectrum {
    fn from(spectrum: Spectrum) -> Self {
        Self { inner: spectrum }
    }
}

impl From<PySpectrum> for Spectrum {
    fn from(py_spectrum: PySpectrum) -> Self {
        py_spectrum.inner
    }
}

/// Summary statistics for an mzPeak file
#[pyclass(name = "FileSummary")]
#[derive(Clone)]
pub struct PyFileSummary {
    inner: FileSummary,
}

#[pymethods]
impl PyFileSummary {
    /// Total number of peaks across all spectra
    #[getter]
    fn total_peaks(&self) -> i64 {
        self.inner.total_peaks
    }

    /// Total number of spectra
    #[getter]
    fn num_spectra(&self) -> i64 {
        self.inner.num_spectra
    }

    /// Number of MS1 spectra
    #[getter]
    fn num_ms1_spectra(&self) -> i64 {
        self.inner.num_ms1_spectra
    }

    /// Number of MS2 spectra
    #[getter]
    fn num_ms2_spectra(&self) -> i64 {
        self.inner.num_ms2_spectra
    }

    /// Retention time range as (min, max) tuple in seconds
    #[getter]
    fn rt_range(&self) -> Option<(f32, f32)> {
        self.inner.rt_range
    }

    /// m/z range as (min, max) tuple
    #[getter]
    fn mz_range(&self) -> Option<(f64, f64)> {
        self.inner.mz_range
    }

    /// Format version string
    #[getter]
    fn format_version(&self) -> String {
        self.inner.format_version.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "FileSummary(spectra={}, peaks={}, ms1={}, ms2={})",
            self.inner.num_spectra,
            self.inner.total_peaks,
            self.inner.num_ms1_spectra,
            self.inner.num_ms2_spectra
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<FileSummary> for PyFileSummary {
    fn from(summary: FileSummary) -> Self {
        Self { inner: summary }
    }
}

/// Metadata from an mzPeak file
#[pyclass(name = "FileMetadata")]
#[derive(Clone)]
pub struct PyFileMetadata {
    inner: FileMetadata,
}

#[pymethods]
impl PyFileMetadata {
    /// Format version string
    #[getter]
    fn format_version(&self) -> String {
        self.inner.format_version.clone()
    }

    /// Total number of rows (peaks) in the file
    #[getter]
    fn total_rows(&self) -> i64 {
        self.inner.total_rows
    }

    /// Number of row groups in the Parquet file
    #[getter]
    fn num_row_groups(&self) -> usize {
        self.inner.num_row_groups
    }

    /// Key-value metadata from the file
    #[getter]
    fn key_value_metadata(&self) -> HashMap<String, String> {
        self.inner.key_value_metadata.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "FileMetadata(version='{}', rows={}, row_groups={})",
            self.inner.format_version, self.inner.total_rows, self.inner.num_row_groups
        )
    }
}

impl From<FileMetadata> for PyFileMetadata {
    fn from(metadata: FileMetadata) -> Self {
        Self { inner: metadata }
    }
}

/// A chromatogram (time-intensity trace)
#[pyclass(name = "Chromatogram")]
#[derive(Clone)]
pub struct PyChromatogram {
    pub(crate) inner: Chromatogram,
}

#[pymethods]
impl PyChromatogram {
    /// Create a new chromatogram
    #[new]
    fn new(
        chromatogram_id: String,
        chromatogram_type: String,
        time_array: Vec<f64>,
        intensity_array: Vec<f32>,
    ) -> Self {
        Self {
            inner: Chromatogram {
                chromatogram_id,
                chromatogram_type,
                time_array,
                intensity_array,
            },
        }
    }

    /// Chromatogram identifier
    #[getter]
    fn chromatogram_id(&self) -> String {
        self.inner.chromatogram_id.clone()
    }

    /// Chromatogram type (e.g., "TIC", "BPC")
    #[getter]
    fn chromatogram_type(&self) -> String {
        self.inner.chromatogram_type.clone()
    }

    /// Time values in seconds
    #[getter]
    fn time_array(&self) -> Vec<f64> {
        self.inner.time_array.clone()
    }

    /// Intensity values
    #[getter]
    fn intensity_array(&self) -> Vec<f32> {
        self.inner.intensity_array.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Chromatogram(id='{}', type='{}', {} points)",
            self.inner.chromatogram_id,
            self.inner.chromatogram_type,
            self.inner.time_array.len()
        )
    }

    fn __len__(&self) -> usize {
        self.inner.time_array.len()
    }
}

impl From<Chromatogram> for PyChromatogram {
    fn from(chrom: Chromatogram) -> Self {
        Self { inner: chrom }
    }
}

impl From<PyChromatogram> for Chromatogram {
    fn from(py_chrom: PyChromatogram) -> Self {
        py_chrom.inner
    }
}

/// A mobilogram (ion mobility-intensity trace)
#[pyclass(name = "Mobilogram")]
#[derive(Clone)]
pub struct PyMobilogram {
    pub(crate) inner: Mobilogram,
}

#[pymethods]
impl PyMobilogram {
    /// Create a new mobilogram
    #[new]
    fn new(
        mobilogram_id: String,
        mobilogram_type: String,
        mobility_array: Vec<f64>,
        intensity_array: Vec<f32>,
    ) -> Self {
        Self {
            inner: Mobilogram {
                mobilogram_id,
                mobilogram_type,
                mobility_array,
                intensity_array,
            },
        }
    }

    /// Mobilogram identifier
    #[getter]
    fn mobilogram_id(&self) -> String {
        self.inner.mobilogram_id.clone()
    }

    /// Mobilogram type
    #[getter]
    fn mobilogram_type(&self) -> String {
        self.inner.mobilogram_type.clone()
    }

    /// Ion mobility values
    #[getter]
    fn mobility_array(&self) -> Vec<f64> {
        self.inner.mobility_array.clone()
    }

    /// Intensity values
    #[getter]
    fn intensity_array(&self) -> Vec<f32> {
        self.inner.intensity_array.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Mobilogram(id='{}', type='{}', {} points)",
            self.inner.mobilogram_id,
            self.inner.mobilogram_type,
            self.inner.mobility_array.len()
        )
    }

    fn __len__(&self) -> usize {
        self.inner.mobility_array.len()
    }
}

impl From<Mobilogram> for PyMobilogram {
    fn from(mob: Mobilogram) -> Self {
        Self { inner: mob }
    }
}

impl From<PyMobilogram> for Mobilogram {
    fn from(py_mob: PyMobilogram) -> Self {
        py_mob.inner
    }
}

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
