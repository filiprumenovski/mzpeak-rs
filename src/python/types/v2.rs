use numpy::{IntoPyArray, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::dataset::DatasetV2Stats;
use crate::reader::SpectrumMetadataView;
use crate::writer::{PeakArraysV2, SpectrumMetadata, SpectrumV2};

#[pyclass(name = "SpectrumMetadata")]
#[derive(Clone)]
pub struct PySpectrumMetadata {
    pub(crate) inner: SpectrumMetadata,
}

#[pymethods]
impl PySpectrumMetadata {
    #[new]
    #[pyo3(signature = (
        spectrum_id,
        scan_number,
        ms_level,
        retention_time,
        polarity,
        peak_count,
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
        spectrum_id: u32,
        scan_number: Option<i32>,
        ms_level: u8,
        retention_time: f32,
        polarity: i8,
        peak_count: u32,
        precursor_mz: Option<f64>,
        precursor_charge: Option<i8>,
        precursor_intensity: Option<f32>,
        isolation_window_lower: Option<f32>,
        isolation_window_upper: Option<f32>,
        collision_energy: Option<f32>,
        total_ion_current: Option<f64>,
        base_peak_mz: Option<f64>,
        base_peak_intensity: Option<f32>,
        injection_time: Option<f32>,
        pixel_x: Option<u16>,
        pixel_y: Option<u16>,
        pixel_z: Option<u16>,
    ) -> PyResult<Self> {
        if ms_level < 1 {
            return Err(PyValueError::new_err("ms_level must be >= 1"));
        }
        if !matches!(polarity, -1 | 0 | 1) {
            return Err(PyValueError::new_err(
                "polarity must be -1, 0, or 1",
            ));
        }

        Ok(Self {
            inner: SpectrumMetadata {
                spectrum_id,
                scan_number,
                ms_level,
                retention_time,
                polarity,
                peak_count,
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
            },
        })
    }

    #[staticmethod]
    #[pyo3(signature = (spectrum_id, scan_number, retention_time, polarity, peak_count))]
    fn new_ms1(
        spectrum_id: u32,
        scan_number: Option<i32>,
        retention_time: f32,
        polarity: i8,
        peak_count: u32,
    ) -> PyResult<Self> {
        if !matches!(polarity, -1 | 0 | 1) {
            return Err(PyValueError::new_err(
                "polarity must be -1, 0, or 1",
            ));
        }
        Ok(Self {
            inner: SpectrumMetadata::new_ms1(
                spectrum_id,
                scan_number,
                retention_time,
                polarity,
                peak_count,
            ),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (spectrum_id, scan_number, retention_time, polarity, peak_count, precursor_mz))]
    fn new_ms2(
        spectrum_id: u32,
        scan_number: Option<i32>,
        retention_time: f32,
        polarity: i8,
        peak_count: u32,
        precursor_mz: f64,
    ) -> PyResult<Self> {
        if !matches!(polarity, -1 | 0 | 1) {
            return Err(PyValueError::new_err(
                "polarity must be -1, 0, or 1",
            ));
        }
        Ok(Self {
            inner: SpectrumMetadata::new_ms2(
                spectrum_id,
                scan_number,
                retention_time,
                polarity,
                peak_count,
                precursor_mz,
            ),
        })
    }

    #[getter]
    fn spectrum_id(&self) -> u32 {
        self.inner.spectrum_id
    }

    #[getter]
    fn scan_number(&self) -> Option<i32> {
        self.inner.scan_number
    }

    #[getter]
    fn ms_level(&self) -> u8 {
        self.inner.ms_level
    }

    #[getter]
    fn retention_time(&self) -> f32 {
        self.inner.retention_time
    }

    #[getter]
    fn polarity(&self) -> i8 {
        self.inner.polarity
    }

    #[getter]
    fn peak_count(&self) -> u32 {
        self.inner.peak_count
    }

    #[getter]
    fn precursor_mz(&self) -> Option<f64> {
        self.inner.precursor_mz
    }

    #[getter]
    fn precursor_charge(&self) -> Option<i8> {
        self.inner.precursor_charge
    }

    #[getter]
    fn precursor_intensity(&self) -> Option<f32> {
        self.inner.precursor_intensity
    }

    #[getter]
    fn isolation_window_lower(&self) -> Option<f32> {
        self.inner.isolation_window_lower
    }

    #[getter]
    fn isolation_window_upper(&self) -> Option<f32> {
        self.inner.isolation_window_upper
    }

    #[getter]
    fn collision_energy(&self) -> Option<f32> {
        self.inner.collision_energy
    }

    #[getter]
    fn total_ion_current(&self) -> Option<f64> {
        self.inner.total_ion_current
    }

    #[getter]
    fn base_peak_mz(&self) -> Option<f64> {
        self.inner.base_peak_mz
    }

    #[getter]
    fn base_peak_intensity(&self) -> Option<f32> {
        self.inner.base_peak_intensity
    }

    #[getter]
    fn injection_time(&self) -> Option<f32> {
        self.inner.injection_time
    }

    #[getter]
    fn pixel_x(&self) -> Option<u16> {
        self.inner.pixel_x
    }

    #[getter]
    fn pixel_y(&self) -> Option<u16> {
        self.inner.pixel_y
    }

    #[getter]
    fn pixel_z(&self) -> Option<u16> {
        self.inner.pixel_z
    }

    fn __repr__(&self) -> String {
        format!(
            "SpectrumMetadata(id={}, ms_level={}, rt={:.2}s, peaks={})",
            self.inner.spectrum_id,
            self.inner.ms_level,
            self.inner.retention_time,
            self.inner.peak_count
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl PySpectrumMetadata {
    pub(crate) fn to_rust(&self) -> SpectrumMetadata {
        self.inner.clone()
    }
}

impl From<SpectrumMetadata> for PySpectrumMetadata {
    fn from(metadata: SpectrumMetadata) -> Self {
        Self { inner: metadata }
    }
}

#[pyclass(name = "PeakArraysV2")]
pub struct PyPeakArraysV2 {
    mz: PyObject,
    intensity: PyObject,
    ion_mobility: Option<PyObject>,
    num_peaks: usize,
}

#[pymethods]
impl PyPeakArraysV2 {
    #[new]
    #[pyo3(signature = (mz, intensity, ion_mobility=None))]
    fn new(
        py: Python<'_>,
        mz: PyObject,
        intensity: PyObject,
        ion_mobility: Option<PyObject>,
    ) -> PyResult<Self> {
        let mz_array: PyReadonlyArray1<f64> = mz.extract(py)?;
        let intensity_array: PyReadonlyArray1<f32> = intensity.extract(py)?;
        let num_peaks = mz_array.len()?;
        let intensity_len = intensity_array.len()?;

        if intensity_len != num_peaks {
            return Err(PyValueError::new_err(format!(
                "intensity length {} does not match mz length {}",
                intensity_len, num_peaks,
            )));
        }

        if let Some(im) = ion_mobility.as_ref() {
            let im_array: PyReadonlyArray1<f64> = im.extract(py)?;
            let im_len = im_array.len()?;
            if im_len != num_peaks {
                return Err(PyValueError::new_err(format!(
                    "ion_mobility length {} does not match mz length {}",
                    im_len, num_peaks,
                )));
            }
        }

        Ok(Self {
            mz,
            intensity,
            ion_mobility,
            num_peaks,
        })
    }

    #[getter]
    fn mz_array(&self) -> PyObject {
        self.mz.clone()
    }

    #[getter]
    fn intensity_array(&self) -> PyObject {
        self.intensity.clone()
    }

    #[getter]
    fn ion_mobility_array(&self) -> Option<PyObject> {
        self.ion_mobility.clone()
    }

    fn __len__(&self) -> usize {
        self.num_peaks
    }

    fn __repr__(&self) -> String {
        format!("PeakArraysV2(peaks={})", self.num_peaks)
    }
}

impl PyPeakArraysV2 {
    pub(crate) fn to_rust(&self, py: Python<'_>) -> PyResult<PeakArraysV2> {
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
            None => None,
            Some(obj) => {
                let values = extract_vec::<f64>(py, obj, "ion_mobility")?;
                if values.len() != mz.len() {
                    return Err(PyValueError::new_err(format!(
                        "ion_mobility length {} does not match mz length {}",
                        values.len(),
                        mz.len()
                    )));
                }
                Some(values)
            }
        };

        Ok(PeakArraysV2 {
            mz,
            intensity,
            ion_mobility,
        })
    }

    pub(crate) fn from_peaks(py: Python<'_>, peaks: PeakArraysV2) -> Self {
        let num_peaks = peaks.mz.len();
        let mz = peaks.mz.into_pyarray(py).to_object(py);
        let intensity = peaks.intensity.into_pyarray(py).to_object(py);
        let ion_mobility = peaks
            .ion_mobility
            .map(|values| values.into_pyarray(py).to_object(py));

        Self {
            mz,
            intensity,
            ion_mobility,
            num_peaks,
        }
    }
}

#[pyclass(name = "SpectrumV2")]
#[derive(Clone)]
pub struct PySpectrumV2 {
    pub(crate) inner: SpectrumV2,
}

#[pymethods]
impl PySpectrumV2 {
    #[new]
    fn new(
        py: Python<'_>,
        metadata: PyRef<'_, PySpectrumMetadata>,
        peaks: PyRef<'_, PyPeakArraysV2>,
    ) -> PyResult<Self> {
        let peaks_rust = peaks.to_rust(py)?;
        let metadata_rust = metadata.to_rust();
        let expected = peaks_rust.len() as u32;
        if metadata_rust.peak_count != expected {
            return Err(PyValueError::new_err(format!(
                "peak_count {} does not match peaks length {}",
                metadata_rust.peak_count, expected
            )));
        }

        Ok(Self {
            inner: SpectrumV2::new(metadata_rust, peaks_rust),
        })
    }

    #[getter]
    fn metadata(&self) -> PySpectrumMetadata {
        PySpectrumMetadata::from(self.inner.metadata.clone())
    }

    #[getter]
    fn peaks(&self, py: Python<'_>) -> PyPeakArraysV2 {
        PyPeakArraysV2::from_peaks(py, self.inner.peaks.clone())
    }

    #[getter]
    fn peak_count(&self) -> u32 {
        self.inner.peak_count()
    }

    fn __repr__(&self) -> String {
        format!(
            "SpectrumV2(id={}, ms_level={}, peaks={})",
            self.inner.metadata.spectrum_id,
            self.inner.metadata.ms_level,
            self.inner.peak_count()
        )
    }
}

#[pyclass(name = "DatasetV2Stats")]
#[derive(Clone)]
pub struct PyDatasetV2Stats {
    inner: DatasetV2Stats,
}

#[pymethods]
impl PyDatasetV2Stats {
    #[getter]
    fn spectra_written(&self) -> u64 {
        self.inner.spectra_stats.spectra_written
    }

    #[getter]
    fn peaks_written(&self) -> u64 {
        self.inner.peaks_stats.peaks_written
    }

    #[getter]
    fn spectra_row_groups(&self) -> usize {
        self.inner.spectra_stats.row_groups_written
    }

    #[getter]
    fn peaks_row_groups(&self) -> usize {
        self.inner.peaks_stats.row_groups_written
    }

    #[getter]
    fn spectra_file_size_bytes(&self) -> u64 {
        self.inner.spectra_stats.file_size_bytes
    }

    #[getter]
    fn peaks_file_size_bytes(&self) -> u64 {
        self.inner.peaks_stats.file_size_bytes
    }

    #[getter]
    fn total_size_bytes(&self) -> u64 {
        self.inner.total_size_bytes
    }

    fn __repr__(&self) -> String {
        format!(
            "DatasetV2Stats(spectra={}, peaks={}, size={} bytes)",
            self.inner.spectra_stats.spectra_written,
            self.inner.peaks_stats.peaks_written,
            self.inner.total_size_bytes
        )
    }
}

impl From<DatasetV2Stats> for PyDatasetV2Stats {
    fn from(stats: DatasetV2Stats) -> Self {
        Self { inner: stats }
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

// =============================================================================
// v2 Reader Types
// =============================================================================

/// Read-only view of spectrum metadata from v2 spectra table.
///
/// This class provides access to spectrum metadata without peak data.
/// Use `peak_offset` and `peak_count` to locate peak data in the peaks table.
#[pyclass(name = "SpectrumMetadataView")]
#[derive(Clone)]
pub struct PySpectrumMetadataView {
    pub(crate) inner: SpectrumMetadataView,
}

#[pymethods]
impl PySpectrumMetadataView {
    #[getter]
    fn spectrum_id(&self) -> u32 {
        self.inner.spectrum_id
    }

    #[getter]
    fn scan_number(&self) -> Option<i32> {
        self.inner.scan_number
    }

    #[getter]
    fn ms_level(&self) -> u8 {
        self.inner.ms_level
    }

    #[getter]
    fn retention_time(&self) -> f32 {
        self.inner.retention_time
    }

    #[getter]
    fn polarity(&self) -> i8 {
        self.inner.polarity
    }

    #[getter]
    fn peak_offset(&self) -> u64 {
        self.inner.peak_offset
    }

    #[getter]
    fn peak_count(&self) -> u32 {
        self.inner.peak_count
    }

    #[getter]
    fn precursor_mz(&self) -> Option<f64> {
        self.inner.precursor_mz
    }

    #[getter]
    fn precursor_charge(&self) -> Option<i8> {
        self.inner.precursor_charge
    }

    #[getter]
    fn precursor_intensity(&self) -> Option<f32> {
        self.inner.precursor_intensity
    }

    #[getter]
    fn isolation_window_lower(&self) -> Option<f32> {
        self.inner.isolation_window_lower
    }

    #[getter]
    fn isolation_window_upper(&self) -> Option<f32> {
        self.inner.isolation_window_upper
    }

    #[getter]
    fn collision_energy(&self) -> Option<f32> {
        self.inner.collision_energy
    }

    #[getter]
    fn total_ion_current(&self) -> Option<f64> {
        self.inner.total_ion_current
    }

    #[getter]
    fn base_peak_mz(&self) -> Option<f64> {
        self.inner.base_peak_mz
    }

    #[getter]
    fn base_peak_intensity(&self) -> Option<f32> {
        self.inner.base_peak_intensity
    }

    #[getter]
    fn injection_time(&self) -> Option<f32> {
        self.inner.injection_time
    }

    #[getter]
    fn pixel_x(&self) -> Option<u16> {
        self.inner.pixel_x
    }

    #[getter]
    fn pixel_y(&self) -> Option<u16> {
        self.inner.pixel_y
    }

    #[getter]
    fn pixel_z(&self) -> Option<u16> {
        self.inner.pixel_z
    }

    fn __repr__(&self) -> String {
        format!(
            "SpectrumMetadataView(id={}, ms_level={}, rt={:.2}s, peaks={})",
            self.inner.spectrum_id,
            self.inner.ms_level,
            self.inner.retention_time,
            self.inner.peak_count
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<SpectrumMetadataView> for PySpectrumMetadataView {
    fn from(view: SpectrumMetadataView) -> Self {
        Self { inner: view }
    }
}
