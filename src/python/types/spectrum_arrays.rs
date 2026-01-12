use numpy::{IntoPyArray, PyReadonlyArray1};
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;

use crate::writer::{OptionalColumnBuf, SpectrumArrays};

struct IonMobilityArrays {
    values: PyObject,
    validity: Option<PyObject>,
}

/// A mass spectrum with SoA peak arrays and metadata
#[pyclass(name = "SpectrumArrays")]
pub struct PySpectrumArrays {
    spectrum_id: i64,
    scan_number: i64,
    ms_level: i16,
    retention_time: f32,
    polarity: i8,
    precursor_mz: Option<f64>,
    precursor_charge: Option<i16>,
    precursor_intensity: Option<f32>,
    isolation_window_lower: Option<f32>,
    isolation_window_upper: Option<f32>,
    collision_energy: Option<f32>,
    total_ion_current: Option<f64>,
    base_peak_mz: Option<f64>,
    base_peak_intensity: Option<f32>,
    injection_time: Option<f32>,
    pixel_x: Option<i32>,
    pixel_y: Option<i32>,
    pixel_z: Option<i32>,
    num_peaks: usize,
    mz: PyObject,
    intensity: PyObject,
    ion_mobility: Option<IonMobilityArrays>,
}

impl PySpectrumArrays {
    pub(crate) fn to_rust(&self, py: Python<'_>) -> PyResult<SpectrumArrays> {
        let mz = extract_vec::<f64>(py, &self.mz, "mz")?;
        let intensity = extract_vec::<f32>(py, &self.intensity, "intensity")?;
        if intensity.len() != mz.len() {
            return Err(PyValueError::new_err(format!(
                "intensity length {} does not match mz length {}",
                intensity.len(),
                mz.len()
            )));
        }

        let ion_mobility = match &self.ion_mobility {
            None => OptionalColumnBuf::all_null(mz.len()),
            Some(arrays) => {
                let values = extract_vec::<f64>(py, &arrays.values, "ion_mobility")?;
                if values.len() != mz.len() {
                    return Err(PyValueError::new_err(format!(
                        "ion_mobility length {} does not match mz length {}",
                        values.len(),
                        mz.len()
                    )));
                }
                match &arrays.validity {
                    None => OptionalColumnBuf::AllPresent(values),
                    Some(validity_obj) => {
                        let validity = extract_vec::<bool>(py, validity_obj, "ion_mobility_validity")?;
                        if validity.len() != mz.len() {
                            return Err(PyValueError::new_err(format!(
                                "ion_mobility_validity length {} does not match mz length {}",
                                validity.len(),
                                mz.len()
                            )));
                        }
                        OptionalColumnBuf::WithValidity { values, validity }
                    }
                }
            }
        };

        Ok(SpectrumArrays {
            spectrum_id: self.spectrum_id,
            scan_number: self.scan_number,
            ms_level: self.ms_level,
            retention_time: self.retention_time,
            polarity: self.polarity,
            precursor_mz: self.precursor_mz,
            precursor_charge: self.precursor_charge,
            precursor_intensity: self.precursor_intensity,
            isolation_window_lower: self.isolation_window_lower,
            isolation_window_upper: self.isolation_window_upper,
            collision_energy: self.collision_energy,
            total_ion_current: self.total_ion_current,
            base_peak_mz: self.base_peak_mz,
            base_peak_intensity: self.base_peak_intensity,
            injection_time: self.injection_time,
            pixel_x: self.pixel_x,
            pixel_y: self.pixel_y,
            pixel_z: self.pixel_z,
            peaks: crate::writer::PeakArrays {
                mz,
                intensity,
                ion_mobility,
            },
        })
    }

    pub(crate) fn from_arrays(py: Python<'_>, spectrum: SpectrumArrays) -> Self {
        let SpectrumArrays {
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            isolation_window_lower,
            isolation_window_upper,
            collision_energy,
            total_ion_current,
            base_peak_mz,
            base_peak_intensity,
            injection_time,
            pixel_x,
            pixel_y,
            pixel_z,
            peaks,
        } = spectrum;

        let num_peaks = peaks.mz.len();
        let mz = peaks.mz.into_pyarray(py).to_object(py);
        let intensity = peaks.intensity.into_pyarray(py).to_object(py);
        let ion_mobility = match peaks.ion_mobility {
            OptionalColumnBuf::AllNull { .. } => None,
            OptionalColumnBuf::AllPresent(values) => Some(IonMobilityArrays {
                values: values.into_pyarray(py).to_object(py),
                validity: None,
            }),
            OptionalColumnBuf::WithValidity { values, validity } => Some(IonMobilityArrays {
                values: values.into_pyarray(py).to_object(py),
                validity: Some(validity.into_pyarray(py).to_object(py)),
            }),
        };

        Self {
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            isolation_window_lower,
            isolation_window_upper,
            collision_energy,
            total_ion_current,
            base_peak_mz,
            base_peak_intensity,
            injection_time,
            pixel_x,
            pixel_y,
            pixel_z,
            num_peaks,
            mz,
            intensity,
            ion_mobility,
        }
    }
}

#[pymethods]
impl PySpectrumArrays {
    /// Create a new SpectrumArrays object from NumPy arrays
    #[new]
    #[pyo3(signature = (
        spectrum_id,
        scan_number,
        ms_level,
        retention_time,
        polarity,
        mz,
        intensity,
        ion_mobility=None,
        ion_mobility_validity=None,
        precursor_mz=None,
        precursor_charge=None,
        precursor_intensity=None,
        isolation_window_lower=None,
        isolation_window_upper=None,
        collision_energy=None,
        total_ion_current=None,
        base_peak_mz=None,
        base_peak_intensity=None,
        injection_time=None,
        pixel_x=None,
        pixel_y=None,
        pixel_z=None
    ))]
    fn new(
        py: Python<'_>,
        spectrum_id: i64,
        scan_number: i64,
        ms_level: i16,
        retention_time: f32,
        polarity: i8,
        mz: PyObject,
        intensity: PyObject,
        ion_mobility: Option<PyObject>,
        ion_mobility_validity: Option<PyObject>,
        precursor_mz: Option<f64>,
        precursor_charge: Option<i16>,
        precursor_intensity: Option<f32>,
        isolation_window_lower: Option<f32>,
        isolation_window_upper: Option<f32>,
        collision_energy: Option<f32>,
        total_ion_current: Option<f64>,
        base_peak_mz: Option<f64>,
        base_peak_intensity: Option<f32>,
        injection_time: Option<f32>,
        pixel_x: Option<i32>,
        pixel_y: Option<i32>,
        pixel_z: Option<i32>,
    ) -> PyResult<Self> {
        let mz_array: PyReadonlyArray1<f64> = mz.extract(py)?;
        let intensity_array: PyReadonlyArray1<f32> = intensity.extract(py)?;
        let num_peaks = mz_array.len()?;
        let intensity_len = intensity_array.len()?;
        if intensity_len != num_peaks {
            return Err(PyValueError::new_err(format!(
                "intensity length {} does not match mz length {}",
                intensity_len,
                num_peaks,
            )));
        }

        let ion_mobility_arrays = match ion_mobility {
            None => {
                if ion_mobility_validity.is_some() {
                    return Err(PyValueError::new_err(
                        "ion_mobility_validity provided without ion_mobility values",
                    ));
                }
                None
            }
            Some(values_obj) => {
                let values: PyReadonlyArray1<f64> = values_obj.extract(py)?;
                let values_len = values.len()?;
                if values_len != num_peaks {
                    return Err(PyValueError::new_err(format!(
                        "ion_mobility length {} does not match mz length {}",
                        values_len,
                        num_peaks,
                    )));
                }
                let validity = match ion_mobility_validity {
                    None => None,
                    Some(validity_obj) => {
                        let validity_array: PyReadonlyArray1<bool> =
                            validity_obj.extract(py)?;
                        let validity_len = validity_array.len()?;
                        if validity_len != num_peaks {
                            return Err(PyValueError::new_err(format!(
                                "ion_mobility_validity length {} does not match mz length {}",
                                validity_len,
                                num_peaks,
                            )));
                        }
                        Some(validity_obj)
                    }
                };
                Some(IonMobilityArrays {
                    values: values_obj,
                    validity,
                })
            }
        };

        Ok(Self {
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            isolation_window_lower,
            isolation_window_upper,
            collision_energy,
            total_ion_current,
            base_peak_mz,
            base_peak_intensity,
            injection_time,
            pixel_x,
            pixel_y,
            pixel_z,
            num_peaks,
            mz,
            intensity,
            ion_mobility: ion_mobility_arrays,
        })
    }

    /// Unique spectrum identifier
    #[getter]
    fn spectrum_id(&self) -> i64 {
        self.spectrum_id
    }

    /// Native scan number
    #[getter]
    fn scan_number(&self) -> i64 {
        self.scan_number
    }

    /// MS level (1 for MS1, 2 for MS2, etc.)
    #[getter]
    fn ms_level(&self) -> i16 {
        self.ms_level
    }

    /// Retention time in seconds
    #[getter]
    fn retention_time(&self) -> f32 {
        self.retention_time
    }

    /// Polarity (1 for positive, -1 for negative)
    #[getter]
    fn polarity(&self) -> i8 {
        self.polarity
    }

    /// m/z array (NumPy)
    #[getter]
    fn mz_array(&self, py: Python<'_>) -> PyObject {
        self.mz.clone_ref(py)
    }

    /// Intensity array (NumPy)
    #[getter]
    fn intensity_array(&self, py: Python<'_>) -> PyObject {
        self.intensity.clone_ref(py)
    }

    /// Ion mobility array (NumPy), optionally with a validity mask
    ///
    /// Returns:
    /// - None if no ion mobility data is present
    /// - values array if all values are present
    /// - (values, validity) tuple for sparse data
    #[getter]
    fn ion_mobility_array(&self, py: Python<'_>) -> PyObject {
        match &self.ion_mobility {
            None => py.None(),
            Some(arrays) => match &arrays.validity {
                None => arrays.values.clone_ref(py),
                Some(validity) => (arrays.values.clone_ref(py), validity.clone_ref(py)).into_py(py),
            },
        }
    }

    /// Number of peaks in this spectrum
    #[getter]
    fn num_peaks(&self) -> usize {
        self.num_peaks
    }

    /// Precursor m/z (for MS2+ spectra)
    #[getter]
    fn precursor_mz(&self) -> Option<f64> {
        self.precursor_mz
    }

    /// Precursor charge state
    #[getter]
    fn precursor_charge(&self) -> Option<i16> {
        self.precursor_charge
    }

    /// Precursor intensity
    #[getter]
    fn precursor_intensity(&self) -> Option<f32> {
        self.precursor_intensity
    }

    /// Lower isolation window offset
    #[getter]
    fn isolation_window_lower(&self) -> Option<f32> {
        self.isolation_window_lower
    }

    /// Upper isolation window offset
    #[getter]
    fn isolation_window_upper(&self) -> Option<f32> {
        self.isolation_window_upper
    }

    /// Collision energy in eV
    #[getter]
    fn collision_energy(&self) -> Option<f32> {
        self.collision_energy
    }

    /// Total ion current
    #[getter]
    fn total_ion_current(&self) -> Option<f64> {
        self.total_ion_current
    }

    /// Base peak m/z
    #[getter]
    fn base_peak_mz(&self) -> Option<f64> {
        self.base_peak_mz
    }

    /// Base peak intensity
    #[getter]
    fn base_peak_intensity(&self) -> Option<f32> {
        self.base_peak_intensity
    }

    /// Ion injection time in milliseconds
    #[getter]
    fn injection_time(&self) -> Option<f32> {
        self.injection_time
    }

    /// MSI pixel X coordinate
    #[getter]
    fn pixel_x(&self) -> Option<i32> {
        self.pixel_x
    }

    /// MSI pixel Y coordinate
    #[getter]
    fn pixel_y(&self) -> Option<i32> {
        self.pixel_y
    }

    /// MSI pixel Z coordinate
    #[getter]
    fn pixel_z(&self) -> Option<i32> {
        self.pixel_z
    }

    fn __repr__(&self) -> String {
        format!(
            "SpectrumArrays(id={}, scan={}, ms_level={}, rt={:.2}s, {} peaks)",
            self.spectrum_id, self.scan_number, self.ms_level, self.retention_time, self.num_peaks
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __len__(&self) -> usize {
        self.num_peaks
    }
}

fn extract_vec<T: numpy::Element + Copy>(
    py: Python<'_>,
    obj: &PyObject,
    label: &str,
) -> PyResult<Vec<T>> {
    let array: PyReadonlyArray1<T> = obj.extract(py)?;
    let slice = array
        .as_slice()
        .map_err(|_| PyValueError::new_err(format!("{} must be a contiguous 1D array", label)))?;
    Ok(slice.to_vec())
}
