//! Python bindings for mzPeak - High-performance mass spectrometry data format
//!
//! This module provides Python bindings via PyO3, enabling seamless integration
//! with the Python data science ecosystem (pandas, polars, pyarrow).
//!
//! # Features
//!
//! - Zero-copy Arrow integration for efficient data transfer
//! - Context manager support for safe resource management
//! - Full type hints via .pyi stub files
//! - GIL release during heavy I/O operations
//!
//! # Example
//!
//! ```python
//! import mzpeak
//!
//! # Convert mzML to mzPeak format
//! mzpeak.convert("input.mzML", "output.mzpeak")
//!
//! # Read and analyze data
//! with mzpeak.MzPeakReader("output.mzpeak") as reader:
//!     summary = reader.summary()
//!     print(f"Total spectra: {summary.num_spectra}")
//!     
//!     # Zero-copy Arrow access
//!     table = reader.to_arrow()
//!     df = reader.to_pandas()
//! ```

mod converter;
mod cv;
pub(crate) mod exceptions;
mod metadata;
mod reader;
mod types;
mod validator;
mod writer;

use pyo3::prelude::*;

/// Initialize the mzpeak Python module
#[pymodule]
fn mzpeak(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize logging bridge to Python's logging module
    pyo3_log::init();

    // Register exception types
    m.add("MzPeakException", py.get_type::<exceptions::MzPeakException>())?;
    m.add("MzPeakIOError", py.get_type::<exceptions::MzPeakIOError>())?;
    m.add("MzPeakFormatError", py.get_type::<exceptions::MzPeakFormatError>())?;
    m.add("MzPeakValidationError", py.get_type::<exceptions::MzPeakValidationError>())?;

    // Register data types
    m.add_class::<types::PyPeak>()?;
    m.add_class::<types::PySpectrum>()?;
    m.add_class::<types::PySpectrumArrays>()?;
    m.add_class::<types::PySpectrumArraysView>()?;
    m.add_class::<types::PySpectrumMetadata>()?;
    m.add_class::<types::PyPeakArraysV2>()?;
    m.add_class::<types::PySpectrumV2>()?;
    m.add_class::<types::PySpectrumMetadataView>()?;
    m.add_class::<types::PyFileSummary>()?;
    m.add_class::<types::PyFileMetadata>()?;
    m.add_class::<types::PyChromatogram>()?;
    m.add_class::<types::PyMobilogram>()?;
    m.add_class::<types::PyWriterConfig>()?;
    m.add_class::<types::PyWriterStats>()?;
    m.add_class::<types::PyConversionConfig>()?;
    m.add_class::<types::PyConversionStats>()?;
    m.add_class::<types::PyDatasetV2Stats>()?;

    // Register conversion enums and config types
    m.add_class::<types::PyOutputFormat>()?;
    m.add_class::<types::PyModality>()?;
    m.add_class::<types::PyStreamingConfig>()?;

    // Register metadata types
    m.add_class::<metadata::PyMzPeakMetadata>()?;
    m.add_class::<metadata::PyInstrumentConfig>()?;
    m.add_class::<metadata::PyMassAnalyzerConfig>()?;
    m.add_class::<metadata::PyLcConfig>()?;
    m.add_class::<metadata::PyColumnInfo>()?;
    m.add_class::<metadata::PyMobilePhase>()?;
    m.add_class::<metadata::PyGradientProgram>()?;
    m.add_class::<metadata::PyGradientStep>()?;
    m.add_class::<metadata::PyRunParameters>()?;
    m.add_class::<metadata::PySdrfMetadata>()?;
    m.add_class::<metadata::PySourceFileInfo>()?;
    m.add_class::<metadata::PyProcessingHistory>()?;
    m.add_class::<metadata::PyProcessingStep>()?;
    m.add_class::<metadata::PyImagingMetadata>()?;
    m.add_class::<metadata::PyVendorHints>()?;

    // Register validation types
    m.add_class::<validator::PyCheckStatus>()?;
    m.add_class::<validator::PyValidationCheck>()?;
    m.add_class::<validator::PyValidationReport>()?;
    m.add_function(wrap_pyfunction!(validator::py_validate_mzpeak_file, m)?)?;

    // Register CV types
    m.add_class::<cv::PyCvTerm>()?;
    m.add_class::<cv::PyCvParamList>()?;
    m.add_class::<cv::PyMsTerms>()?;
    m.add_class::<cv::PyUnitTerms>()?;

    // Register reader classes
    m.add_class::<reader::PyMzPeakReader>()?;
    m.add_class::<reader::PySpectrumIterator>()?;
    m.add_class::<reader::PyStreamingSpectrumArraysIterator>()?;
    m.add_class::<reader::PyStreamingSpectrumArraysViewIterator>()?;
    m.add_class::<reader::PySpectrumMetadataViewIterator>()?;

    // Register writer classes
    m.add_class::<writer::PyMzPeakWriter>()?;
    m.add_class::<writer::PyMzPeakDatasetWriter>()?;
    m.add_class::<writer::PyMzPeakDatasetWriterV2>()?;
    m.add_class::<writer::PySpectrumBuilder>()?;
    m.add_class::<writer::PyRollingWriter>()?;
    m.add_class::<writer::PyRollingWriterStats>()?;
    m.add_class::<writer::PyAsyncMzPeakWriter>()?;
    m.add_class::<writer::PyOwnedColumnarBatch>()?;
    m.add_class::<writer::PyIngestSpectrum>()?;
    m.add_class::<writer::PyIngestSpectrumConverter>()?;

    // Register converter classes
    m.add_class::<converter::PyMzMLConverter>()?;

    // Register TDF converter if feature enabled
    #[cfg(feature = "tdf")]
    {
        m.add_class::<converter::PyTdfConverter>()?;
        m.add_class::<converter::PyTdfConversionStats>()?;
        m.add_function(wrap_pyfunction!(converter::convert_tdf, m)?)?;
    }

    // Register Thermo converter if feature enabled
    #[cfg(feature = "thermo")]
    {
        m.add_class::<converter::PyThermoConverter>()?;
        m.add_class::<converter::PyThermoConversionStats>()?;
        m.add_function(wrap_pyfunction!(converter::convert_thermo, m)?)?;
    }

    // Register module-level convenience functions
    m.add_function(wrap_pyfunction!(converter::convert, m)?)?;
    m.add_function(wrap_pyfunction!(converter::convert_with_sharding, m)?)?;

    // Add version and format constants
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FORMAT_VERSION", crate::schema::MZPEAK_FORMAT_VERSION)?;
    m.add("MIMETYPE", crate::schema::MZPEAK_MIMETYPE)?;

    Ok(())
}

