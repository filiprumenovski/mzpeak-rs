//! Python bindings for MzPeakReader
//!
//! Provides read access to mzPeak files with zero-copy Arrow integration.

use arrow::array::RecordBatch;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::prelude::*;

#[pyclass(name = "_ArrowCStream")]
struct PyArrowCStream {
    capsule: PyObject,
}

#[pymethods]
impl PyArrowCStream {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__(&self, py: Python<'_>, requested_schema: Option<PyObject>) -> PyObject {
        let _ = requested_schema;
        self.capsule.clone_ref(py)
    }
}

use crate::python::exceptions::IntoPyResult;
use crate::python::types::{
    PyChromatogram, PyFileMetadata, PyFileSummary, PyMobilogram, PySpectrum, PySpectrumArrays,
    PySpectrumArraysView, PySpectrumMetadataView,
};
use crate::reader::{
    MzPeakReader, ReaderConfig, SpectrumMetadataIterator, StreamingSpectrumArraysViewIterator,
};

/// Reader for mzPeak format files
///
/// Supports reading from single Parquet files, dataset bundles (directories),
/// and ZIP container files.
///
/// Example:
///     >>> with mzpeak.MzPeakReader("data.mzpeak") as reader:
///     ...     summary = reader.summary()
///     ...     print(f"Total spectra: {summary.num_spectra}")
///     ...     table = reader.to_arrow()
#[pyclass(name = "MzPeakReader")]
pub struct PyMzPeakReader {
    inner: Option<MzPeakReader>,
    path: String,
}

#[pymethods]
impl PyMzPeakReader {
    /// Open an mzPeak file for reading
    ///
    /// Args:
    ///     path: Path to the mzPeak file, directory, or ZIP container
    ///     batch_size: Optional batch size for reading (default: 65536)
    ///
    /// Returns:
    ///     MzPeakReader instance
    #[new]
    #[pyo3(signature = (path, batch_size=None))]
    fn new(path: String, batch_size: Option<usize>) -> PyResult<Self> {
        let config = batch_size.map(|bs| ReaderConfig { batch_size: bs });

        let reader = if let Some(cfg) = config {
            MzPeakReader::open_with_config(&path, cfg)
        } else {
            MzPeakReader::open(&path)
        }
        .into_py_result()?;

        Ok(Self {
            inner: Some(reader),
            path,
        })
    }

    /// Open an mzPeak file (alternative constructor)
    #[staticmethod]
    #[pyo3(signature = (path, batch_size=None))]
    fn open(path: String, batch_size: Option<usize>) -> PyResult<Self> {
        Self::new(path, batch_size)
    }

    /// Get file metadata
    ///
    /// Returns:
    ///     FileMetadata with format version, row counts, and key-value metadata
    fn metadata(&self) -> PyResult<PyFileMetadata> {
        let reader = self.get_reader()?;
        Ok(PyFileMetadata::from(reader.metadata().clone()))
    }

    /// Get file summary statistics
    ///
    /// Returns:
    ///     FileSummary with spectrum counts, peak counts, and data ranges
    fn summary(&self, py: Python<'_>) -> PyResult<PyFileSummary> {
        let reader = self.get_reader()?;
        // Release GIL during potentially slow operation
        py.allow_threads(|| reader.summary().into_py_result())
            .map(PyFileSummary::from)
    }

    /// Get total number of peaks in the file
    fn total_peaks(&self) -> PyResult<i64> {
        let reader = self.get_reader()?;
        Ok(reader.total_peaks())
    }

    /// Get a single spectrum by ID
    ///
    /// Args:
    ///     spectrum_id: The spectrum identifier
    ///
    /// Returns:
    ///     Spectrum object or None if not found
    fn get_spectrum(&self, py: Python<'_>, spectrum_id: i64) -> PyResult<Option<PySpectrum>> {
        let reader = self.get_reader()?;
        let spectrum =
            py.allow_threads(|| reader.get_spectrum_arrays(spectrum_id).into_py_result())?;
        match spectrum {
            Some(view) => {
                let owned = view.to_owned().into_py_result()?;
                Ok(Some(PySpectrum::from(owned)))
            }
            None => Ok(None),
        }
    }

    /// Get a single spectrum by ID as SoA arrays
    ///
    /// Args:
    ///     spectrum_id: The spectrum identifier
    ///
    /// Returns:
    ///     SpectrumArrays object or None if not found
    fn get_spectrum_arrays(
        &self,
        py: Python<'_>,
        spectrum_id: i64,
    ) -> PyResult<Option<PySpectrumArrays>> {
        let reader = self.get_reader()?;
        let spectrum =
            py.allow_threads(|| reader.get_spectrum_arrays(spectrum_id).into_py_result())?;
        match spectrum {
            Some(view) => {
                let owned = view.to_owned().into_py_result()?;
                Ok(Some(PySpectrumArrays::from_arrays(py, owned)))
            }
            None => Ok(None),
        }
    }

    /// Get a single spectrum by ID as SoA array views (zero-copy)
    ///
    /// Args:
    ///     spectrum_id: The spectrum identifier
    ///
    /// Returns:
    ///     SpectrumArraysView object or None if not found
    fn get_spectrum_arrays_view(
        &self,
        py: Python<'_>,
        spectrum_id: i64,
    ) -> PyResult<Option<PySpectrumArraysView>> {
        let reader = self.get_reader()?;
        let spectrum =
            py.allow_threads(|| reader.get_spectrum_arrays(spectrum_id).into_py_result())?;
        Ok(spectrum.map(PySpectrumArraysView::from_view))
    }

    /// Get multiple spectra by their IDs
    ///
    /// Args:
    ///     spectrum_ids: List of spectrum identifiers
    ///
    /// Returns:
    ///     List of Spectrum objects
    fn get_spectra(&self, py: Python<'_>, spectrum_ids: Vec<i64>) -> PyResult<Vec<PySpectrum>> {
        let reader = self.get_reader()?;
        let spectra =
            py.allow_threads(|| reader.get_spectra_arrays(&spectrum_ids).into_py_result())?;
        let mut out = Vec::with_capacity(spectra.len());
        for view in spectra {
            let owned = view.to_owned().into_py_result()?;
            out.push(PySpectrum::from(owned));
        }
        Ok(out)
    }

    /// Get multiple spectra by their IDs as SoA arrays
    ///
    /// Args:
    ///     spectrum_ids: List of spectrum identifiers
    ///
    /// Returns:
    ///     List of SpectrumArrays objects
    fn get_spectra_arrays(
        &self,
        py: Python<'_>,
        spectrum_ids: Vec<i64>,
    ) -> PyResult<Vec<PySpectrumArrays>> {
        let reader = self.get_reader()?;
        let spectra =
            py.allow_threads(|| reader.get_spectra_arrays(&spectrum_ids).into_py_result())?;
        let mut out = Vec::with_capacity(spectra.len());
        for view in spectra {
            let owned = view.to_owned().into_py_result()?;
            out.push(PySpectrumArrays::from_arrays(py, owned));
        }
        Ok(out)
    }

    /// Get multiple spectra by their IDs as SoA array views (zero-copy)
    ///
    /// Args:
    ///     spectrum_ids: List of spectrum identifiers
    ///
    /// Returns:
    ///     List of SpectrumArraysView objects
    fn get_spectra_arrays_views(
        &self,
        py: Python<'_>,
        spectrum_ids: Vec<i64>,
    ) -> PyResult<Vec<PySpectrumArraysView>> {
        let reader = self.get_reader()?;
        let spectra =
            py.allow_threads(|| reader.get_spectra_arrays(&spectrum_ids).into_py_result())?;
        Ok(spectra
            .into_iter()
            .map(PySpectrumArraysView::from_view)
            .collect())
    }

    /// Get all spectra from the file
    ///
    /// Warning: This loads all spectra into memory. For large files,
    /// consider using iter_spectra() or to_arrow() instead.
    ///
    /// Returns:
    ///     List of all Spectrum objects
    fn all_spectra(&self, py: Python<'_>) -> PyResult<Vec<PySpectrum>> {
        let reader = self.get_reader()?;
        let spectra = py.allow_threads(|| reader.iter_spectra_arrays().into_py_result())?;
        let mut out = Vec::with_capacity(spectra.len());
        for view in spectra {
            let owned = view.to_owned().into_py_result()?;
            out.push(PySpectrum::from(owned));
        }
        Ok(out)
    }

    /// Get all spectra from the file as SoA arrays
    ///
    /// Warning: This loads all spectra into memory. For large files,
    /// consider using iter_spectra_arrays() instead.
    ///
    /// Returns:
    ///     List of SpectrumArrays objects
    fn all_spectra_arrays(&self, py: Python<'_>) -> PyResult<Vec<PySpectrumArrays>> {
        let reader = self.get_reader()?;
        let spectra = py.allow_threads(|| reader.iter_spectra_arrays().into_py_result())?;
        let mut out = Vec::with_capacity(spectra.len());
        for view in spectra {
            let owned = view.to_owned().into_py_result()?;
            out.push(PySpectrumArrays::from_arrays(py, owned));
        }
        Ok(out)
    }

    /// Get all spectra from the file as SoA array views (zero-copy)
    ///
    /// Warning: This loads all spectra into memory. For large files,
    /// consider using iter_spectra_arrays_views() instead.
    ///
    /// Returns:
    ///     List of SpectrumArraysView objects
    fn all_spectra_arrays_views(&self, py: Python<'_>) -> PyResult<Vec<PySpectrumArraysView>> {
        let reader = self.get_reader()?;
        let spectra = py.allow_threads(|| reader.iter_spectra_arrays().into_py_result())?;
        Ok(spectra
            .into_iter()
            .map(PySpectrumArraysView::from_view)
            .collect())
    }

    /// Get spectra within a retention time range
    ///
    /// Args:
    ///     min_rt: Minimum retention time in seconds
    ///     max_rt: Maximum retention time in seconds
    ///
    /// Returns:
    ///     List of Spectrum objects within the RT range
    fn spectra_by_rt_range(
        &self,
        py: Python<'_>,
        min_rt: f32,
        max_rt: f32,
    ) -> PyResult<Vec<PySpectrum>> {
        let reader = self.get_reader()?;
        let spectra =
            py.allow_threads(|| reader.spectra_by_rt_range_arrays(min_rt, max_rt).into_py_result())?;
        let mut out = Vec::with_capacity(spectra.len());
        for view in spectra {
            let owned = view.to_owned().into_py_result()?;
            out.push(PySpectrum::from(owned));
        }
        Ok(out)
    }

    /// Get spectra within a retention time range as SoA arrays
    ///
    /// Args:
    ///     min_rt: Minimum retention time in seconds
    ///     max_rt: Maximum retention time in seconds
    ///
    /// Returns:
    ///     List of SpectrumArrays objects within the RT range
    fn spectra_by_rt_range_arrays(
        &self,
        py: Python<'_>,
        min_rt: f32,
        max_rt: f32,
    ) -> PyResult<Vec<PySpectrumArrays>> {
        let reader = self.get_reader()?;
        let spectra =
            py.allow_threads(|| reader.spectra_by_rt_range_arrays(min_rt, max_rt).into_py_result())?;
        let mut out = Vec::with_capacity(spectra.len());
        for view in spectra {
            let owned = view.to_owned().into_py_result()?;
            out.push(PySpectrumArrays::from_arrays(py, owned));
        }
        Ok(out)
    }

    /// Get spectra by MS level
    ///
    /// Args:
    ///     ms_level: MS level (1, 2, etc.)
    ///
    /// Returns:
    ///     List of Spectrum objects with the specified MS level
    fn spectra_by_ms_level(&self, py: Python<'_>, ms_level: i16) -> PyResult<Vec<PySpectrum>> {
        let reader = self.get_reader()?;
        let spectra =
            py.allow_threads(|| reader.spectra_by_ms_level_arrays(ms_level).into_py_result())?;
        let mut out = Vec::with_capacity(spectra.len());
        for view in spectra {
            let owned = view.to_owned().into_py_result()?;
            out.push(PySpectrum::from(owned));
        }
        Ok(out)
    }

    /// Get spectra by MS level as SoA arrays
    ///
    /// Args:
    ///     ms_level: MS level (1, 2, etc.)
    ///
    /// Returns:
    ///     List of SpectrumArrays objects with the specified MS level
    fn spectra_by_ms_level_arrays(
        &self,
        py: Python<'_>,
        ms_level: i16,
    ) -> PyResult<Vec<PySpectrumArrays>> {
        let reader = self.get_reader()?;
        let spectra =
            py.allow_threads(|| reader.spectra_by_ms_level_arrays(ms_level).into_py_result())?;
        let mut out = Vec::with_capacity(spectra.len());
        for view in spectra {
            let owned = view.to_owned().into_py_result()?;
            out.push(PySpectrumArrays::from_arrays(py, owned));
        }
        Ok(out)
    }

    /// Get all spectrum IDs in the file
    ///
    /// Returns:
    ///     List of spectrum identifiers
    fn spectrum_ids(&self, py: Python<'_>) -> PyResult<Vec<i64>> {
        let reader = self.get_reader()?;
        py.allow_threads(|| reader.spectrum_ids().into_py_result())
    }

    /// Read chromatogram data
    ///
    /// Returns:
    ///     List of Chromatogram objects (empty list if no chromatograms present)
    fn read_chromatograms(&self, py: Python<'_>) -> PyResult<Vec<PyChromatogram>> {
        let reader = self.get_reader()?;
        let result = py.allow_threads(|| reader.read_chromatograms().into_py_result())?;
        Ok(result.into_iter().map(PyChromatogram::from).collect())
    }

    /// Read mobilogram data
    ///
    /// Returns:
    ///     List of Mobilogram objects (empty list if no mobilograms present)
    fn read_mobilograms(&self, py: Python<'_>) -> PyResult<Vec<PyMobilogram>> {
        let reader = self.get_reader()?;
        let result = py.allow_threads(|| reader.read_mobilograms().into_py_result())?;
        Ok(result.into_iter().map(PyMobilogram::from).collect())
    }

    /// Return a streaming iterator over all spectra (truly lazy)
    ///
    /// This is memory-efficient for large files as it reads spectra lazily
    /// from the underlying Parquet data. Memory usage is bounded by batch_size.
    ///
    /// **Issue 004 Fix**: This iterator is now truly streaming and does not
    /// load all spectra into memory upfront.
    ///
    /// Returns:
    ///     Iterator yielding Spectrum objects
    fn iter_spectra(&self) -> PyResult<PyStreamingSpectrumIterator> {
        let reader = self.get_reader()?;
        let streaming_iter = reader.iter_spectra_arrays_streaming().into_py_result()?;
        Ok(PyStreamingSpectrumIterator::new(streaming_iter))
    }

    /// Return a streaming iterator over all spectra as SoA arrays
    ///
    /// Returns:
    ///     Iterator yielding SpectrumArrays objects
    fn iter_spectra_arrays(&self) -> PyResult<PyStreamingSpectrumArraysIterator> {
        let reader = self.get_reader()?;
        let streaming_iter = reader.iter_spectra_arrays_streaming().into_py_result()?;
        Ok(PyStreamingSpectrumArraysIterator::new(streaming_iter))
    }

    /// Return a streaming iterator over all spectra as SoA array views (zero-copy)
    ///
    /// Returns:
    ///     Iterator yielding SpectrumArraysView objects
    fn iter_spectra_arrays_views(&self) -> PyResult<PyStreamingSpectrumArraysViewIterator> {
        let reader = self.get_reader()?;
        let streaming_iter = reader.iter_spectra_arrays_streaming().into_py_result()?;
        Ok(PyStreamingSpectrumArraysViewIterator::new(streaming_iter))
    }

    /// Export data as a streaming PyArrow RecordBatchReader (Issue 005 fix)
    ///
    /// Returns a streaming reader that pulls batches on-demand from the underlying
    /// Parquet data. Memory usage is bounded by batch_size, not file size.
    ///
    /// This implements the Arrow C Stream protocol (`__arrow_c_stream__`) for
    /// efficient interop with PyArrow, Polars, and other Arrow-compatible libraries.
    ///
    /// Returns:
    ///     pyarrow.RecordBatchReader that streams batches on-demand
    ///
    /// Raises:
    ///     ImportError: If pyarrow is not installed
    fn to_arrow_stream(&self, py: Python<'_>) -> PyResult<PyObject> {
        let reader = self.get_reader()?;
        let batch_iter = reader.iter_batches().into_py_result()?;
        let schema = reader.schema();

        // Wrap in our streaming reader
        let streaming_reader = PyStreamingArrowReader::new(batch_iter, schema);
        let py_reader = Py::new(py, streaming_reader)?;

        // Return PyArrow RecordBatchReader from our stream
        let pa = py.import("pyarrow")?;
        let pa_reader = pa
            .getattr("RecordBatchReader")?
            .call_method1("from_stream", (py_reader,))?;
        Ok(pa_reader.into())
    }

    /// Export data as a PyArrow Table (convenience method)
    ///
    /// For large files, prefer `to_arrow_stream()` which doesn't materialize
    /// all batches into memory at once.
    ///
    /// Returns:
    ///     pyarrow.Table containing all peak data
    ///
    /// Raises:
    ///     ImportError: If pyarrow is not installed
    fn to_arrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        // Use streaming reader, then read all into table
        let stream_reader = self.to_arrow_stream(py)?;
        stream_reader.call_method0(py, "read_all")
    }

    /// Export data as a pandas DataFrame
    ///
    /// Internally uses zero-copy Arrow handoff for efficiency.
    ///
    /// Returns:
    ///     pandas.DataFrame containing all peak data
    ///
    /// Raises:
    ///     ImportError: If pandas or pyarrow is not installed
    fn to_pandas(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.to_arrow(py)?;
        table.call_method0(py, "to_pandas")
    }

    /// Export data as a polars DataFrame
    ///
    /// Internally uses zero-copy Arrow handoff for efficiency.
    ///
    /// Returns:
    ///     polars.DataFrame containing all peak data
    ///
    /// Raises:
    ///     ImportError: If polars is not installed
    fn to_polars(&self, py: Python<'_>) -> PyResult<PyObject> {
        let table = self.to_arrow(py)?;
        let polars = py.import("polars")?;
        polars.call_method1("from_arrow", (table,)).map(|df| df.into())
    }

    // =========================================================================
    // v2 Format Reader Methods
    // =========================================================================

    /// Check if this is a v2 format container with separate spectra table.
    ///
    /// Returns `True` if the container has a `spectra/spectra.parquet` table.
    ///
    /// Returns:
    ///     bool: True if v2 format with spectra table
    fn has_spectra_table(&self) -> PyResult<bool> {
        let reader = self.get_reader()?;
        Ok(reader.has_spectra_table())
    }

    /// Check if this is a v2 format container.
    ///
    /// Alias for `has_spectra_table()` for clarity.
    ///
    /// Returns:
    ///     bool: True if v2 format
    fn is_v2_format(&self) -> PyResult<bool> {
        let reader = self.get_reader()?;
        Ok(reader.is_v2_format())
    }

    /// Get total number of spectra in a v2 container.
    ///
    /// Returns the row count from the spectra table.
    /// Only available for v2 format containers.
    ///
    /// Returns:
    ///     int: Total number of spectra
    ///
    /// Raises:
    ///     MzPeakFormatError: If not a v2 format container
    fn total_spectra(&self, py: Python<'_>) -> PyResult<i64> {
        let reader = self.get_reader()?;
        py.allow_threads(|| reader.total_spectra().into_py_result())
    }

    /// Iterate over spectrum metadata from the v2 spectra table.
    ///
    /// Returns a streaming iterator that yields spectrum metadata one at a time.
    /// This is memory-efficient as it doesn't load all spectra at once.
    /// Only available for v2 format containers.
    ///
    /// Returns:
    ///     Iterator[SpectrumMetadataView]: Iterator over spectrum metadata
    ///
    /// Raises:
    ///     MzPeakFormatError: If not a v2 format container
    ///
    /// Example:
    ///     >>> for meta in reader.iter_spectra_metadata():
    ///     ...     print(f"Spectrum {meta.spectrum_id}: {meta.peak_count} peaks")
    fn iter_spectra_metadata(&self) -> PyResult<PySpectrumMetadataViewIterator> {
        let reader = self.get_reader()?;
        let iter = reader.iter_spectra_metadata().into_py_result()?;
        Ok(PySpectrumMetadataViewIterator::new(iter))
    }

    /// Read a batch of spectrum metadata from the v2 spectra table.
    ///
    /// Returns up to `batch_size` spectrum metadata records starting from `offset`.
    /// Only available for v2 format containers.
    ///
    /// Args:
    ///     offset: The starting spectrum index (0-based)
    ///     batch_size: Maximum number of spectra to return
    ///
    /// Returns:
    ///     List[SpectrumMetadataView]: Batch of spectrum metadata
    ///
    /// Raises:
    ///     MzPeakFormatError: If not a v2 format container
    ///
    /// Example:
    ///     >>> # Read spectra 100-199
    ///     >>> batch = reader.read_spectra_batch(100, 100)
    ///     >>> print(f"Read {len(batch)} spectra")
    fn read_spectra_batch(
        &self,
        py: Python<'_>,
        offset: usize,
        batch_size: usize,
    ) -> PyResult<Vec<PySpectrumMetadataView>> {
        let reader = self.get_reader()?;
        let batch = py.allow_threads(|| {
            reader.read_spectra_batch(offset, batch_size).into_py_result()
        })?;
        Ok(batch.into_iter().map(PySpectrumMetadataView::from).collect())
    }

    /// Get spectrum metadata by ID (v2 format only).
    ///
    /// Returns the spectrum metadata for the given spectrum_id, or None if not found.
    /// Only available for v2 format containers.
    ///
    /// Args:
    ///     spectrum_id: The spectrum identifier (0-indexed)
    ///
    /// Returns:
    ///     Optional[SpectrumMetadataView]: Spectrum metadata or None if not found
    ///
    /// Raises:
    ///     MzPeakFormatError: If not a v2 format container
    fn get_spectrum_metadata(
        &self,
        py: Python<'_>,
        spectrum_id: u32,
    ) -> PyResult<Option<PySpectrumMetadataView>> {
        let reader = self.get_reader()?;
        let result = py.allow_threads(|| {
            reader.get_spectrum_metadata(spectrum_id).into_py_result()
        })?;
        Ok(result.map(PySpectrumMetadataView::from))
    }

    /// Query spectrum metadata by retention time range (v2 format only).
    ///
    /// Returns all spectrum metadata within the given RT range (inclusive).
    /// Only available for v2 format containers.
    ///
    /// Args:
    ///     start_rt: Start retention time in seconds (inclusive)
    ///     end_rt: End retention time in seconds (inclusive)
    ///
    /// Returns:
    ///     List[SpectrumMetadataView]: Spectrum metadata within the RT range
    ///
    /// Raises:
    ///     MzPeakFormatError: If not a v2 format container
    fn spectra_metadata_by_rt_range(
        &self,
        py: Python<'_>,
        start_rt: f32,
        end_rt: f32,
    ) -> PyResult<Vec<PySpectrumMetadataView>> {
        let reader = self.get_reader()?;
        let results = py.allow_threads(|| {
            reader.spectra_metadata_by_rt_range(start_rt, end_rt).into_py_result()
        })?;
        Ok(results.into_iter().map(PySpectrumMetadataView::from).collect())
    }

    /// Query spectrum metadata by MS level (v2 format only).
    ///
    /// Returns all spectrum metadata with the given MS level.
    /// Only available for v2 format containers.
    ///
    /// Args:
    ///     ms_level: MS level (1 for MS1, 2 for MS2, etc.)
    ///
    /// Returns:
    ///     List[SpectrumMetadataView]: Spectrum metadata with the specified MS level
    ///
    /// Raises:
    ///     MzPeakFormatError: If not a v2 format container
    fn spectra_metadata_by_ms_level(
        &self,
        py: Python<'_>,
        ms_level: u8,
    ) -> PyResult<Vec<PySpectrumMetadataView>> {
        let reader = self.get_reader()?;
        let results = py.allow_threads(|| {
            reader.spectra_metadata_by_ms_level(ms_level).into_py_result()
        })?;
        Ok(results.into_iter().map(PySpectrumMetadataView::from).collect())
    }

    /// Context manager entry
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager exit - close the reader
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, pyo3::types::PyType>>,
        _exc_val: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_tb: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false) // Don't suppress exceptions
    }

    /// Close the reader and release resources
    fn close(&mut self) -> PyResult<()> {
        self.inner = None;
        Ok(())
    }

    /// Check if the reader is open
    fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    fn __repr__(&self) -> String {
        if self.inner.is_some() {
            format!("MzPeakReader('{}', open=True)", self.path)
        } else {
            format!("MzPeakReader('{}', open=False)", self.path)
        }
    }
}

impl PyMzPeakReader {
    fn get_reader(&self) -> PyResult<&MzPeakReader> {
        self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Reader is closed")
        })
    }
}

/// Convert an Arrow RecordBatch to a PyArrow RecordBatch using the C Data Interface.
///
/// # Memory Safety
///
/// This function implements zero-copy data transfer between Rust and Python using
/// the Apache Arrow C Data Interface (PEP 688 / Arrow PyCapsule Protocol):
///
/// 1. **Ownership Transfer**: The `FFI_ArrowArrayStream` is heap-allocated via
///    `Box::into_raw()` and ownership is transferred to Python via `PyCapsule`.
///    The Rust side relinquishes ownership of the memory.
///
/// 2. **Lifecycle Management**: The capsule has a custom destructor (`drop_ffi_stream`)
///    registered via `PyCapsule_New`. When Python's garbage collector frees the
///    capsule, the destructor is invoked, which reclaims the Rust-allocated stream
///    via `Box::from_raw()`.
///
/// 3. **Zero-Copy Guarantee**: The Arrow arrays' underlying buffers are shared
///    directly between Rust and Python - no data serialization or copying occurs.
///    The `Bytes` type used for buffer storage in Arrow is reference-counted,
///    allowing safe concurrent access.
///
/// 4. **Thread Safety**: The GIL is held during all FFI operations. The `Bytes`
///    backing store uses atomic reference counting (`Arc`) and is `Send + Sync`.
///
/// 5. **Error Recovery**: If capsule creation fails, we immediately reclaim the
///    stream via `Box::from_raw()` to prevent memory leaks.
///
/// # Protocol Compliance
///
/// This implementation follows the Arrow C Stream specification (ARROW-15656) and
/// the Python PyCapsule Protocol. The capsule name `"arrow_array_stream\0"` is
/// the standard identifier for Arrow stream capsules.
fn record_batch_to_pyarrow(py: Python<'_>, batch: RecordBatch) -> PyResult<PyObject> {
    let pa = py.import("pyarrow")?;

    // Create a RecordBatchReader from the single batch
    let schema = batch.schema();
    let batches = vec![batch];
    let reader = arrow::record_batch::RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    );

    // Create FFI stream from reader using Arrow 54 API
    // This wraps the reader in a C-compatible struct that Python can consume
    let ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));

    // SAFETY: We box the stream and convert to raw pointer for FFI transfer.
    // The PyCapsule takes ownership and will call our destructor when freed.
    let stream_box = Box::new(ffi_stream);
    let stream_ptr = Box::into_raw(stream_box);

    // Capsule name must be a NUL-terminated C string.
    // Use an explicit byte string to keep MSRV-compatible (avoid `c"..."` literals).
    let capsule_name = b"arrow_array_stream\0";
    // SAFETY: PyCapsule_New takes ownership of stream_ptr. The destructor
    // (drop_ffi_stream) will be called when Python GC collects the capsule.
    let capsule = unsafe {
        pyo3::ffi::PyCapsule_New(
            stream_ptr as *mut std::ffi::c_void,
            capsule_name.as_ptr() as *const std::ffi::c_char,
            Some(drop_ffi_stream),
        )
    };

    if capsule.is_null() {
        // Clean up the stream if capsule creation failed
        // SAFETY: We still own stream_ptr since capsule creation failed
        unsafe { drop(Box::from_raw(stream_ptr)); }
        return Err(pyo3::exceptions::PyMemoryError::new_err(
            "Failed to create PyCapsule for Arrow stream"
        ));
    }

    // SAFETY: capsule is non-null and we transfer ownership to Python
    let capsule_obj: PyObject = unsafe { PyObject::from_owned_ptr(py, capsule) };

    // In pyarrow>=10, `RecordBatchReader.from_stream` expects an object implementing
    // the Arrow PyCapsule Protocol (`__arrow_c_stream__`), not a raw capsule.
    let stream_obj = Py::new(
        py,
        PyArrowCStream {
            capsule: capsule_obj.clone_ref(py),
        },
    )?;

    let pa_reader = pa
        .getattr("RecordBatchReader")?
        .call_method1("from_stream", (stream_obj,))?;

    // Read the batch from the reader
    let batch = pa_reader.call_method0("read_next_batch")?;

    Ok(batch.into())
}

/// Destructor for the FFI stream capsule.
///
/// # Safety
///
/// This function is called by Python's garbage collector when the PyCapsule is freed.
/// It reclaims the Rust-allocated `FFI_ArrowArrayStream` to prevent memory leaks.
/// The function is marked `unsafe extern "C"` as required by the PyCapsule API.
unsafe extern "C" fn drop_ffi_stream(capsule: *mut pyo3::ffi::PyObject) {
    let capsule_name = b"arrow_array_stream\0";
    let ptr = pyo3::ffi::PyCapsule_GetPointer(
        capsule,
        capsule_name.as_ptr() as *const std::ffi::c_char,
    );
    if !ptr.is_null() {
        // SAFETY: ptr was created by Box::into_raw in record_batch_to_pyarrow
        drop(Box::from_raw(ptr as *mut FFI_ArrowArrayStream));
    }
}

/// Legacy iterator over spectra (loads all into memory)
///
/// This is kept for backwards compatibility but is not recommended for large files.
#[pyclass(name = "_EagerSpectrumIterator")]
pub struct PySpectrumIterator {
    spectra: Vec<PySpectrum>,
    index: usize,
}

#[pymethods]
impl PySpectrumIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PySpectrum> {
        if self.index < self.spectra.len() {
            let spectrum = self.spectra[self.index].clone();
            self.index += 1;
            Some(spectrum)
        } else {
            None
        }
    }

    fn __len__(&self) -> usize {
        self.spectra.len()
    }
}

/// Streaming iterator over spectra (Issue 004 fix)
///
/// This iterator is truly lazy and reads spectra on-demand from the underlying
/// Parquet data. Memory usage is bounded by batch_size, not file size.
///
/// The GIL is released during batch reads for better Python threading performance.
///
/// Note: This iterator is marked `unsendable` because it holds an internal iterator
/// that is `Send` but not `Sync`. This is safe because Python's GIL ensures
/// single-threaded access to the iterator within a Python context.
#[pyclass(name = "SpectrumIterator", unsendable)]
pub struct PyStreamingSpectrumIterator {
    inner: Option<StreamingSpectrumArraysViewIterator>,
}

impl PyStreamingSpectrumIterator {
    pub fn new(inner: StreamingSpectrumArraysViewIterator) -> Self {
        Self { inner: Some(inner) }
    }
}

/// Streaming iterator over spectra with SoA arrays
#[pyclass(name = "SpectrumArraysIterator", unsendable)]
pub struct PyStreamingSpectrumArraysIterator {
    inner: Option<StreamingSpectrumArraysViewIterator>,
}

impl PyStreamingSpectrumArraysIterator {
    pub fn new(inner: StreamingSpectrumArraysViewIterator) -> Self {
        Self { inner: Some(inner) }
    }
}

/// Streaming iterator over spectra with SoA array views (zero-copy)
#[pyclass(name = "SpectrumArraysViewIterator", unsendable)]
pub struct PyStreamingSpectrumArraysViewIterator {
    inner: Option<StreamingSpectrumArraysViewIterator>,
}

impl PyStreamingSpectrumArraysViewIterator {
    pub fn new(inner: StreamingSpectrumArraysViewIterator) -> Self {
        Self { inner: Some(inner) }
    }
}

#[pymethods]
impl PyStreamingSpectrumArraysIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PySpectrumArrays>> {
        let inner = self.inner.as_mut().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Iterator exhausted or closed")
        })?;

        // Release GIL during potentially slow batch reads
        let result = py.allow_threads(|| inner.next());

        match result {
            Some(Ok(view)) => {
                let owned = view.to_owned().into_py_result()?;
                Ok(Some(PySpectrumArrays::from_arrays(py, owned)))
            }
            Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Error reading spectrum arrays: {}",
                e
            ))),
            None => {
                // Iterator exhausted, drop inner to release resources
                self.inner = None;
                Ok(None)
            }
        }
    }
}

#[pymethods]
impl PyStreamingSpectrumArraysViewIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PySpectrumArraysView>> {
        let inner = self.inner.as_mut().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Iterator exhausted or closed")
        })?;

        let result = py.allow_threads(|| inner.next());

        match result {
            Some(Ok(spectrum)) => Ok(Some(PySpectrumArraysView::from_view(spectrum))),
            Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Error reading spectrum arrays view: {}",
                e
            ))),
            None => {
                self.inner = None;
                Ok(None)
            }
        }
    }
}

#[pymethods]
impl PyStreamingSpectrumIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PySpectrum>> {
        let inner = self.inner.as_mut().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Iterator exhausted or closed")
        })?;

        // Release GIL during potentially slow batch reads
        let result = py.allow_threads(|| inner.next());

        match result {
            Some(Ok(view)) => {
                let owned = view.to_owned().into_py_result()?;
                Ok(Some(PySpectrum::from(owned)))
            }
            Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Error reading spectrum: {}",
                e
            ))),
            None => {
                // Iterator exhausted, drop inner to release resources
                self.inner = None;
                Ok(None)
            }
        }
    }
}

/// Streaming iterator over spectrum metadata from v2 spectra table
///
/// This iterator is memory-efficient and reads spectrum metadata on-demand
/// from the underlying Parquet spectra table in v2 format containers.
#[pyclass(name = "SpectrumMetadataViewIterator", unsendable)]
pub struct PySpectrumMetadataViewIterator {
    inner: Option<SpectrumMetadataIterator>,
}

impl PySpectrumMetadataViewIterator {
    pub fn new(inner: SpectrumMetadataIterator) -> Self {
        Self { inner: Some(inner) }
    }
}

#[pymethods]
impl PySpectrumMetadataViewIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PySpectrumMetadataView>> {
        let inner = self.inner.as_mut().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Iterator exhausted or closed")
        })?;

        // Release GIL during potentially slow batch reads
        let result = py.allow_threads(|| inner.next());

        match result {
            Some(Ok(view)) => Ok(Some(PySpectrumMetadataView::from(view))),
            Some(Err(e)) => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Error reading spectrum metadata: {}",
                e
            ))),
            None => {
                // Iterator exhausted, drop inner to release resources
                self.inner = None;
                Ok(None)
            }
        }
    }
}

/// Streaming Arrow reader implementing `__arrow_c_stream__` (Issue 005 fix)
///
/// This wrapper holds a Rust `RecordBatchIterator` and exposes it via the
/// Arrow C Stream protocol for efficient interop with PyArrow.
///
/// Memory usage is bounded by batch_size, not file size, as batches are
/// produced on-demand rather than pre-materialized.
#[pyclass(name = "_StreamingArrowReader", unsendable)]
pub struct PyStreamingArrowReader {
    /// Iterator over RecordBatches
    batch_iter: Option<crate::reader::RecordBatchIterator>,
    /// Schema for the stream
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    /// Whether the stream has been consumed
    exhausted: bool,
}

impl PyStreamingArrowReader {
    pub fn new(
        batch_iter: crate::reader::RecordBatchIterator,
        schema: std::sync::Arc<arrow::datatypes::Schema>,
    ) -> Self {
        Self {
            batch_iter: Some(batch_iter),
            schema,
            exhausted: false,
        }
    }
}

#[pymethods]
impl PyStreamingArrowReader {
    /// Implement Arrow C Stream protocol
    ///
    /// Returns a PyCapsule containing an FFI_ArrowArrayStream that PyArrow
    /// can consume for streaming record batch access.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__(&mut self, py: Python<'_>, requested_schema: Option<PyObject>) -> PyResult<PyObject> {
        let _ = requested_schema; // We don't support schema negotiation

        if self.exhausted {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Arrow stream has already been consumed"
            ));
        }

        let batch_iter = self.batch_iter.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Arrow stream has already been consumed")
        })?;
        self.exhausted = true;

        // Create a RecordBatchReader from our iterator
        let schema = self.schema.clone();
        let reader = StreamingBatchReader::new(batch_iter, schema);

        // Create FFI stream from reader
        let ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));

        // SAFETY: We box the stream and convert to raw pointer for FFI transfer.
        // The PyCapsule takes ownership and will call our destructor when freed.
        let stream_box = Box::new(ffi_stream);
        let stream_ptr = Box::into_raw(stream_box);

        // Capsule name must be a NUL-terminated C string
        let capsule_name = b"arrow_array_stream\0";

        // SAFETY: PyCapsule_New takes ownership of stream_ptr. The destructor
        // (drop_ffi_stream) will be called when Python GC collects the capsule.
        let capsule = unsafe {
            pyo3::ffi::PyCapsule_New(
                stream_ptr as *mut std::ffi::c_void,
                capsule_name.as_ptr() as *const std::ffi::c_char,
                Some(drop_ffi_stream),
            )
        };

        if capsule.is_null() {
            // Clean up the stream if capsule creation failed
            // SAFETY: We still own stream_ptr since capsule creation failed
            unsafe { drop(Box::from_raw(stream_ptr)); }
            return Err(pyo3::exceptions::PyMemoryError::new_err(
                "Failed to create PyCapsule for Arrow stream"
            ));
        }

        // SAFETY: capsule is non-null and we transfer ownership to Python
        let capsule_obj: PyObject = unsafe { PyObject::from_owned_ptr(py, capsule) };
        Ok(capsule_obj)
    }
}

/// Adapter that implements arrow's RecordBatchReader trait for our RecordBatchIterator
struct StreamingBatchReader {
    iter: crate::reader::RecordBatchIterator,
    schema: std::sync::Arc<arrow::datatypes::Schema>,
}

impl StreamingBatchReader {
    fn new(iter: crate::reader::RecordBatchIterator, schema: std::sync::Arc<arrow::datatypes::Schema>) -> Self {
        Self { iter, schema }
    }
}

impl arrow::record_batch::RecordBatchReader for StreamingBatchReader {
    fn schema(&self) -> std::sync::Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

impl Iterator for StreamingBatchReader {
    type Item = Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|r| r.map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e))))
    }
}
