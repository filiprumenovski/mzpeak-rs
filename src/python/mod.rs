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
mod exceptions;
mod reader;
mod types;
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
    m.add_class::<types::PyFileSummary>()?;
    m.add_class::<types::PyFileMetadata>()?;
    m.add_class::<types::PyChromatogram>()?;
    m.add_class::<types::PyMobilogram>()?;
    m.add_class::<types::PyWriterConfig>()?;
    m.add_class::<types::PyWriterStats>()?;
    m.add_class::<types::PyConversionConfig>()?;
    m.add_class::<types::PyConversionStats>()?;

    // Register reader classes
    m.add_class::<reader::PyMzPeakReader>()?;
    m.add_class::<reader::PySpectrumIterator>()?;
    m.add_class::<reader::PyStreamingSpectrumArraysIterator>()?;

    // Register writer classes
    m.add_class::<writer::PyMzPeakWriter>()?;
    m.add_class::<writer::PyMzPeakDatasetWriter>()?;
    m.add_class::<writer::PySpectrumBuilder>()?;

    // Register converter class
    m.add_class::<converter::PyMzMLConverter>()?;

    // Register module-level convenience functions
    m.add_function(wrap_pyfunction!(converter::convert, m)?)?;
    m.add_function(wrap_pyfunction!(converter::convert_with_sharding, m)?)?;

    // Add version and format constants
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FORMAT_VERSION", crate::schema::MZPEAK_FORMAT_VERSION)?;
    m.add("MIMETYPE", crate::schema::MZPEAK_MIMETYPE)?;

    Ok(())
}
