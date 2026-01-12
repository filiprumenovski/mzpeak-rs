use std::ffi::c_void;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Float64Array};
use numpy::{PyArrayDescr, PyArrayDescrMethods};
use numpy::npyffi::{self, npy_intp};
use pyo3::exceptions::{PyMemoryError, PyValueError};
use pyo3::prelude::*;

use crate::python::types::PySpectrumArrays;
use crate::reader::SpectrumArraysView;

const ARRAY_CAPSULE_NAME: &[u8] = b"mzpeak_arrow_array\0";

struct ArrowArrayHolder {
    _array: ArrayRef,
}

unsafe extern "C" fn drop_arrow_array_holder(capsule: *mut pyo3::ffi::PyObject) {
    let ptr = pyo3::ffi::PyCapsule_GetPointer(
        capsule,
        ARRAY_CAPSULE_NAME.as_ptr() as *const std::ffi::c_char,
    );
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut ArrowArrayHolder));
    }
}

fn create_array_capsule(
    _py: Python<'_>,
    array: ArrayRef,
) -> PyResult<*mut pyo3::ffi::PyObject> {
    let holder = Box::new(ArrowArrayHolder { _array: array });
    let holder_ptr = Box::into_raw(holder);
    let capsule = unsafe {
        pyo3::ffi::PyCapsule_New(
            holder_ptr as *mut c_void,
            ARRAY_CAPSULE_NAME.as_ptr() as *const std::ffi::c_char,
            Some(drop_arrow_array_holder),
        )
    };
    if capsule.is_null() {
        unsafe { drop(Box::from_raw(holder_ptr)); }
        return Err(PyMemoryError::new_err(
            "Failed to create capsule for Arrow array",
        ));
    }
    Ok(capsule)
}

fn numpy_view_from_f64(py: Python<'_>, array: &Float64Array) -> PyResult<PyObject> {
    let values = array.values();
    let len = values.len();
    let mut dims = [len as npy_intp];
    let mut strides = [std::mem::size_of::<f64>() as npy_intp];
    let dtype = PyArrayDescr::of::<f64>(py);
    let capsule = create_array_capsule(py, Arc::new(array.clone()))?;

    let array_ptr = unsafe {
        npyffi::PY_ARRAY_API.PyArray_NewFromDescr(
            py,
            npyffi::PY_ARRAY_API.get_type_object(py, npyffi::NpyTypes::PyArray_Type),
            dtype.into_dtype_ptr(),
            1,
            dims.as_mut_ptr(),
            strides.as_mut_ptr(),
            values.as_ptr() as *mut c_void,
            npyffi::NPY_ARRAY_C_CONTIGUOUS | npyffi::NPY_ARRAY_ALIGNED,
            std::ptr::null_mut(),
        )
    };
    if array_ptr.is_null() {
        unsafe { pyo3::ffi::Py_DECREF(capsule); }
        return Err(PyMemoryError::new_err(
            "Failed to create NumPy array view",
        ));
    }

    let set_result = unsafe {
        npyffi::PY_ARRAY_API.PyArray_SetBaseObject(
            py,
            array_ptr as *mut npyffi::PyArrayObject,
            capsule,
        )
    };
    if set_result != 0 {
        unsafe {
            pyo3::ffi::Py_DECREF(capsule);
            pyo3::ffi::Py_DECREF(array_ptr);
        }
        return Err(PyMemoryError::new_err(
            "Failed to set base object for NumPy array",
        ));
    }

    Ok(unsafe { PyObject::from_owned_ptr(py, array_ptr) })
}

fn numpy_view_from_f32(py: Python<'_>, array: &Float32Array) -> PyResult<PyObject> {
    let values = array.values();
    let len = values.len();
    let mut dims = [len as npy_intp];
    let mut strides = [std::mem::size_of::<f32>() as npy_intp];
    let dtype = PyArrayDescr::of::<f32>(py);
    let capsule = create_array_capsule(py, Arc::new(array.clone()))?;

    let array_ptr = unsafe {
        npyffi::PY_ARRAY_API.PyArray_NewFromDescr(
            py,
            npyffi::PY_ARRAY_API.get_type_object(py, npyffi::NpyTypes::PyArray_Type),
            dtype.into_dtype_ptr(),
            1,
            dims.as_mut_ptr(),
            strides.as_mut_ptr(),
            values.as_ptr() as *mut c_void,
            npyffi::NPY_ARRAY_C_CONTIGUOUS | npyffi::NPY_ARRAY_ALIGNED,
            std::ptr::null_mut(),
        )
    };
    if array_ptr.is_null() {
        unsafe { pyo3::ffi::Py_DECREF(capsule); }
        return Err(PyMemoryError::new_err(
            "Failed to create NumPy array view",
        ));
    }

    let set_result = unsafe {
        npyffi::PY_ARRAY_API.PyArray_SetBaseObject(
            py,
            array_ptr as *mut npyffi::PyArrayObject,
            capsule,
        )
    };
    if set_result != 0 {
        unsafe {
            pyo3::ffi::Py_DECREF(capsule);
            pyo3::ffi::Py_DECREF(array_ptr);
        }
        return Err(PyMemoryError::new_err(
            "Failed to set base object for NumPy array",
        ));
    }

    Ok(unsafe { PyObject::from_owned_ptr(py, array_ptr) })
}

/// View-backed mass spectrum with zero-copy array access.
#[pyclass(name = "SpectrumArraysView")]
pub struct PySpectrumArraysView {
    inner: SpectrumArraysView,
}

impl PySpectrumArraysView {
    pub(crate) fn from_view(view: SpectrumArraysView) -> Self {
        Self { inner: view }
    }
}

#[pymethods]
impl PySpectrumArraysView {
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

    /// Number of peaks in this spectrum
    #[getter]
    fn num_peaks(&self) -> usize {
        self.inner.peak_count()
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

    /// Return m/z array view(s) without copying.
    #[getter]
    fn mz_array_views(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let arrays = self.inner.mz_arrays().map_err(|e| {
            PyValueError::new_err(format!("Failed to read mz array views: {}", e))
        })?;
        arrays
            .iter()
            .map(|array| numpy_view_from_f64(py, array))
            .collect()
    }

    /// Return intensity array view(s) without copying.
    #[getter]
    fn intensity_array_views(&self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        let arrays = self.inner.intensity_arrays().map_err(|e| {
            PyValueError::new_err(format!("Failed to read intensity array views: {}", e))
        })?;
        arrays
            .iter()
            .map(|array| numpy_view_from_f32(py, array))
            .collect()
    }

    /// Return a single m/z array view when contiguous.
    #[getter]
    fn mz_array_view(&self, py: Python<'_>) -> PyResult<PyObject> {
        let arrays = self.inner.mz_arrays().map_err(|e| {
            PyValueError::new_err(format!("Failed to read mz array views: {}", e))
        })?;
        if arrays.len() != 1 {
            return Err(PyValueError::new_err(
                "Spectrum spans multiple batches; use mz_array_views",
            ));
        }
        numpy_view_from_f64(py, &arrays[0])
    }

    /// Return a single intensity array view when contiguous.
    #[getter]
    fn intensity_array_view(&self, py: Python<'_>) -> PyResult<PyObject> {
        let arrays = self.inner.intensity_arrays().map_err(|e| {
            PyValueError::new_err(format!("Failed to read intensity array views: {}", e))
        })?;
        if arrays.len() != 1 {
            return Err(PyValueError::new_err(
                "Spectrum spans multiple batches; use intensity_array_views",
            ));
        }
        numpy_view_from_f32(py, &arrays[0])
    }

    /// Materialize the view into an owned SpectrumArrays object.
    fn to_owned(&self, py: Python<'_>) -> PyResult<PySpectrumArrays> {
        let spectrum = self.inner.to_owned().map_err(|e| {
            PyValueError::new_err(format!("Failed to materialize spectrum arrays: {}", e))
        })?;
        Ok(PySpectrumArrays::from_arrays(py, spectrum))
    }

    fn __repr__(&self) -> String {
        format!(
            "SpectrumArraysView(id={}, scan={}, ms_level={}, rt={:.2}s, {} peaks)",
            self.inner.spectrum_id,
            self.inner.scan_number,
            self.inner.ms_level,
            self.inner.retention_time,
            self.inner.peak_count()
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __len__(&self) -> usize {
        self.inner.peak_count()
    }
}
