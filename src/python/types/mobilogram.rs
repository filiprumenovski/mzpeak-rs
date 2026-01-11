use pyo3::prelude::*;

use crate::mobilogram_writer::Mobilogram;

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
