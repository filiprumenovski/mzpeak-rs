use pyo3::prelude::*;
use std::collections::HashMap;

use crate::python::metadata::PyMzPeakMetadata;
use crate::reader::{FileMetadata, FileSummary};

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

    /// Get the parsed structured metadata (if available).
    ///
    /// Returns the MzPeakMetadata object containing instrument config,
    /// LC config, run parameters, SDRF metadata, and other structured
    /// information parsed from the Parquet footer.
    ///
    /// Returns None if the file doesn't contain structured metadata
    /// or if parsing failed.
    #[getter]
    fn parsed_metadata(&self) -> Option<PyMzPeakMetadata> {
        self.inner.mzpeak_metadata.clone().map(PyMzPeakMetadata::from)
    }

    /// Check if this file has parsed structured metadata
    fn has_parsed_metadata(&self) -> bool {
        self.inner.mzpeak_metadata.is_some()
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
