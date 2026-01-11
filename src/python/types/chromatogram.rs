use pyo3::prelude::*;

use crate::chromatogram_writer::Chromatogram;

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
