use pyo3::prelude::*;

use crate::writer::Peak;

/// A single mass spectrometry peak (m/z, intensity pair)
#[pyclass(name = "Peak")]
#[derive(Clone)]
pub struct PyPeak {
    pub(crate) inner: Peak,
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
