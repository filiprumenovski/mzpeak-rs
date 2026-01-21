use pyo3::prelude::*;

/// A single mass spectrometry peak (m/z, intensity pair)
#[pyclass(name = "Peak")]
#[derive(Clone, Debug)]
pub struct PyPeak {
    mz: f64,
    intensity: f32,
    ion_mobility: Option<f64>,
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
            mz,
            intensity,
            ion_mobility,
        }
    }

    /// Mass-to-charge ratio
    #[getter]
    fn mz(&self) -> f64 {
        self.mz
    }

    /// Signal intensity
    #[getter]
    fn intensity(&self) -> f32 {
        self.intensity
    }

    /// Ion mobility value (if available)
    #[getter]
    fn ion_mobility(&self) -> Option<f64> {
        self.ion_mobility
    }

    fn __repr__(&self) -> String {
        match self.ion_mobility {
            Some(im) => format!(
                "Peak(mz={:.4}, intensity={:.1}, ion_mobility={:.4})",
                self.mz, self.intensity, im
            ),
            None => format!(
                "Peak(mz={:.4}, intensity={:.1})",
                self.mz, self.intensity
            ),
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl PyPeak {
    pub(crate) fn from_values(mz: f64, intensity: f32, ion_mobility: Option<f64>) -> Self {
        Self {
            mz,
            intensity,
            ion_mobility,
        }
    }

    pub(crate) fn mz_value(&self) -> f64 {
        self.mz
    }

    pub(crate) fn intensity_value(&self) -> f32 {
        self.intensity
    }

    pub(crate) fn ion_mobility_value(&self) -> Option<f64> {
        self.ion_mobility
    }
}
