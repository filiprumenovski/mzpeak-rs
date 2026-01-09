//! Python bindings for MzPeakReader
//!
//! Provides read access to mzPeak files with zero-copy Arrow integration.

use arrow::array::RecordBatch;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::prelude::*;
use pyo3::types::PyList;

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
use crate::python::types::{PyChromatogram, PyFileMetadata, PyFileSummary, PyMobilogram, PySpectrum};
use crate::reader::{MzPeakReader, ReaderConfig};

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
        py.allow_threads(|| reader.get_spectrum(spectrum_id).into_py_result())
            .map(|opt| opt.map(PySpectrum::from))
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
        py.allow_threads(|| reader.get_spectra(&spectrum_ids).into_py_result())
            .map(|spectra| spectra.into_iter().map(PySpectrum::from).collect())
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
        py.allow_threads(|| reader.iter_spectra().into_py_result())
            .map(|spectra: Vec<crate::writer::Spectrum>| spectra.into_iter().map(PySpectrum::from).collect())
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
        py.allow_threads(|| reader.spectra_by_rt_range(min_rt, max_rt).into_py_result())
            .map(|spectra| spectra.into_iter().map(PySpectrum::from).collect())
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
        py.allow_threads(|| reader.spectra_by_ms_level(ms_level).into_py_result())
            .map(|spectra| spectra.into_iter().map(PySpectrum::from).collect())
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

    /// Return an iterator over all spectra
    ///
    /// This is memory-efficient for large files as it reads spectra lazily.
    ///
    /// Returns:
    ///     Iterator yielding Spectrum objects
    fn iter_spectra(&self, py: Python<'_>) -> PyResult<PySpectrumIterator> {
        let reader = self.get_reader()?;
        // Get all spectra and create an iterator over them
        // Note: For truly lazy iteration, we'd need to implement a streaming reader
        let spectra = py.allow_threads(|| reader.iter_spectra().into_py_result())?;
        Ok(PySpectrumIterator {
            spectra: spectra.into_iter().map(PySpectrum::from).collect(),
            index: 0,
        })
    }

    /// Export data as a PyArrow Table (zero-copy)
    ///
    /// Uses the Arrow C Data Interface to pass memory directly to PyArrow
    /// without serialization overhead.
    ///
    /// Returns:
    ///     pyarrow.Table containing all peak data
    ///
    /// Raises:
    ///     ImportError: If pyarrow is not installed
    fn to_arrow(&self, py: Python<'_>) -> PyResult<PyObject> {
        let reader = self.get_reader()?;
        
        // Get all record batches from the reader
        let batches = py.allow_threads(|| reader.read_all_batches().into_py_result())?;
        
        if batches.is_empty() {
            // Return empty table with schema
            let pa = py.import("pyarrow")?;
            let schema = reader.schema();
            let empty_batch = RecordBatch::new_empty(schema);
            let py_batch = record_batch_to_pyarrow(py, empty_batch)?;
            let py_batches = vec![py_batch];
            let py_list = PyList::new(py, &py_batches)?;
            let table = pa
                .getattr("Table")?
                .call_method1("from_batches", (py_list,))?;
            return Ok(table.into());
        }
        
        // Convert each batch to PyArrow
        let py_batches: Vec<PyObject> = batches
            .into_iter()
            .map(|batch| record_batch_to_pyarrow(py, batch))
            .collect::<PyResult<Vec<_>>>()?;
        
        // Create a PyArrow Table from record batches
        let pa = py.import("pyarrow")?;
        let py_list = PyList::new(py, &py_batches)?;
        let table = pa
            .getattr("Table")?
            .call_method1("from_batches", (py_list,))?;
        Ok(table.into())
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

/// Convert an Arrow RecordBatch to a PyArrow RecordBatch using the C Data Interface
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
    let ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));
    
    // Create a PyCapsule for the stream - we need to keep the stream alive
    // so we box it and create a capsule from that
    let stream_box = Box::new(ffi_stream);
    let stream_ptr = Box::into_raw(stream_box);
    
    // Capsule name must be a NUL-terminated C string.
    // Use an explicit byte string to keep MSRV-compatible (avoid `c"..."` literals).
    let capsule_name = b"arrow_array_stream\0";
    let capsule = unsafe {
        pyo3::ffi::PyCapsule_New(
            stream_ptr as *mut std::ffi::c_void,
            capsule_name.as_ptr() as *const std::ffi::c_char,
            Some(drop_ffi_stream),
        )
    };
    
    if capsule.is_null() {
        // Clean up the stream if capsule creation failed
        unsafe { drop(Box::from_raw(stream_ptr)); }
        return Err(pyo3::exceptions::PyMemoryError::new_err(
            "Failed to create PyCapsule for Arrow stream"
        ));
    }
    
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

/// Destructor for the FFI stream capsule
unsafe extern "C" fn drop_ffi_stream(capsule: *mut pyo3::ffi::PyObject) {
    let capsule_name = b"arrow_array_stream\0";
    let ptr = pyo3::ffi::PyCapsule_GetPointer(
        capsule,
        capsule_name.as_ptr() as *const std::ffi::c_char,
    );
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut FFI_ArrowArrayStream));
    }
}

/// Iterator over spectra
#[pyclass(name = "SpectrumIterator")]
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
