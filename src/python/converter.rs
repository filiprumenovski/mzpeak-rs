//! Python bindings for mzML to mzPeak conversion
//!
//! Provides high-level conversion API with progress reporting and GIL release.

use pyo3::prelude::*;

use crate::mzml::converter::{ConversionConfig, MzMLConverter};
use crate::python::exceptions::IntoPyResult;
use crate::python::types::{PyConversionConfig, PyConversionStats};

/// Converter for mzML files to mzPeak format
///
/// Provides methods for converting mzML files with various options
/// including sharding for large files.
///
/// Example:
///     >>> converter = mzpeak.MzMLConverter()
///     >>> stats = converter.convert("input.mzML", "output.mzpeak")
///     >>> print(f"Converted {stats.spectra_count} spectra")
#[pyclass(name = "MzMLConverter")]
pub struct PyMzMLConverter {
    config: ConversionConfig,
}

#[pymethods]
impl PyMzMLConverter {
    /// Create a new converter with optional configuration
    ///
    /// Args:
    ///     config: Optional ConversionConfig for batch size, precision settings, etc.
    #[new]
    #[pyo3(signature = (config=None))]
    fn new(config: Option<PyConversionConfig>) -> Self {
        Self {
            config: config.map(|c| c.inner).unwrap_or_default(),
        }
    }

    /// Set the batch size for processing
    ///
    /// Args:
    ///     batch_size: Number of spectra to process per batch
    ///
    /// Returns:
    ///     Self for method chaining
    fn with_batch_size(mut slf: PyRefMut<'_, Self>, batch_size: usize) -> PyRefMut<'_, Self> {
        slf.config.batch_size = batch_size;
        slf
    }

    /// Convert an mzML file to mzPeak format
    ///
    /// Args:
    ///     input_path: Path to input mzML file
    ///     output_path: Path for output mzPeak file/directory
    ///
    /// Returns:
    ///     ConversionStats with details about the conversion
    fn convert(
        &self,
        py: Python<'_>,
        input_path: String,
        output_path: String,
    ) -> PyResult<PyConversionStats> {
        let converter = MzMLConverter::with_config(self.config.clone());

        // Release GIL during the potentially long conversion
        let stats = py.allow_threads(|| converter.convert(&input_path, &output_path).into_py_result())?;

        Ok(PyConversionStats::from(stats))
    }

    /// Convert an mzML file with automatic file sharding
    ///
    /// Creates multiple output files when the data exceeds the specified
    /// maximum peaks per file, useful for very large datasets.
    ///
    /// Args:
    ///     input_path: Path to input mzML file
    ///     output_path: Base path for output files (will add _001, _002, etc.)
    ///     max_peaks_per_file: Maximum peaks per output file (default: 50 million)
    ///
    /// Returns:
    ///     ConversionStats with details about the conversion
    #[pyo3(signature = (input_path, output_path, max_peaks_per_file=50_000_000))]
    fn convert_with_sharding(
        &self,
        py: Python<'_>,
        input_path: String,
        output_path: String,
        max_peaks_per_file: usize,
    ) -> PyResult<PyConversionStats> {
        // Clone config and set max_peaks_per_file
        let mut config = self.config.clone();
        config.writer_config.max_peaks_per_file = Some(max_peaks_per_file);
        let converter = MzMLConverter::with_config(config);

        // Release GIL during the potentially long conversion
        let stats = py.allow_threads(|| {
            converter
                .convert_with_sharding(&input_path, &output_path)
                .into_py_result()
        })?;

        Ok(PyConversionStats::from(stats))
    }

    fn __repr__(&self) -> String {
        format!(
            "MzMLConverter(batch_size={}, preserve_precision={})",
            self.config.batch_size, self.config.preserve_precision
        )
    }
}

/// Convert an mzML file to mzPeak format (convenience function)
///
/// This is a module-level function for simple one-shot conversions.
///
/// Args:
///     input_path: Path to input mzML file
///     output_path: Path for output mzPeak file/directory
///     config: Optional ConversionConfig
///
/// Returns:
///     ConversionStats with details about the conversion
///
/// Example:
///     >>> import mzpeak
///     >>> stats = mzpeak.convert("input.mzML", "output.mzpeak")
///     >>> print(f"Converted {stats.spectra_count} spectra")
#[pyfunction]
#[pyo3(signature = (input_path, output_path, config=None))]
pub fn convert(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    config: Option<PyConversionConfig>,
) -> PyResult<PyConversionStats> {
    let conversion_config = config.map(|c| c.inner).unwrap_or_default();
    let converter = MzMLConverter::with_config(conversion_config);

    // Release GIL during the potentially long conversion
    let stats = py.allow_threads(|| converter.convert(&input_path, &output_path).into_py_result())?;

    Ok(PyConversionStats::from(stats))
}

/// Convert an mzML file with automatic file sharding (convenience function)
///
/// Creates multiple output files when the data exceeds the specified
/// maximum peaks per file.
///
/// Args:
///     input_path: Path to input mzML file
///     output_path: Base path for output files
///     max_peaks_per_file: Maximum peaks per output file (default: 50 million)
///     config: Optional ConversionConfig
///
/// Returns:
///     ConversionStats with details about the conversion
///
/// Example:
///     >>> import mzpeak
///     >>> stats = mzpeak.convert_with_sharding("large.mzML", "output", max_peaks_per_file=10_000_000)
#[pyfunction]
#[pyo3(signature = (input_path, output_path, max_peaks_per_file=50_000_000, config=None))]
pub fn convert_with_sharding(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    max_peaks_per_file: usize,
    config: Option<PyConversionConfig>,
) -> PyResult<PyConversionStats> {
    let mut conversion_config = config.map(|c| c.inner).unwrap_or_default();
    conversion_config.writer_config.max_peaks_per_file = Some(max_peaks_per_file);
    let converter = MzMLConverter::with_config(conversion_config);

    // Release GIL during the potentially long conversion
    let stats = py.allow_threads(|| {
        converter
            .convert_with_sharding(&input_path, &output_path)
            .into_py_result()
    })?;

    Ok(PyConversionStats::from(stats))
}
