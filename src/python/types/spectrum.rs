use pyo3::prelude::*;

use crate::writer::Spectrum;

use super::peak::PyPeak;

/// A mass spectrum containing peaks and metadata
#[pyclass(name = "Spectrum")]
#[derive(Clone)]
pub struct PySpectrum {
    pub(crate) inner: Spectrum,
}

#[pymethods]
impl PySpectrum {
    /// Create a new spectrum
    #[new]
    #[pyo3(signature = (spectrum_id, scan_number, ms_level, retention_time, polarity, peaks=None))]
    fn new(
        spectrum_id: i64,
        scan_number: i64,
        ms_level: i16,
        retention_time: f32,
        polarity: i8,
        peaks: Option<Vec<PyPeak>>,
    ) -> Self {
        Self {
            inner: Spectrum {
                spectrum_id,
                scan_number,
                ms_level,
                retention_time,
                polarity,
                peaks: peaks
                    .map(|p| p.into_iter().map(|pp| pp.inner).collect())
                    .unwrap_or_default(),
                precursor_mz: None,
                precursor_charge: None,
                precursor_intensity: None,
                isolation_window_lower: None,
                isolation_window_upper: None,
                collision_energy: None,
                total_ion_current: None,
                base_peak_mz: None,
                base_peak_intensity: None,
                injection_time: None,
                pixel_x: None,
                pixel_y: None,
                pixel_z: None,
            },
        }
    }

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

    /// List of peaks in this spectrum
    #[getter]
    fn peaks(&self) -> Vec<PyPeak> {
        self.inner.peaks.iter().cloned().map(PyPeak::from).collect()
    }

    /// Number of peaks in this spectrum
    #[getter]
    fn num_peaks(&self) -> usize {
        self.inner.peaks.len()
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

    fn __repr__(&self) -> String {
        format!(
            "Spectrum(id={}, scan={}, ms_level={}, rt={:.2}s, {} peaks)",
            self.inner.spectrum_id,
            self.inner.scan_number,
            self.inner.ms_level,
            self.inner.retention_time,
            self.inner.peaks.len()
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    fn __len__(&self) -> usize {
        self.inner.peaks.len()
    }
}

impl From<Spectrum> for PySpectrum {
    fn from(spectrum: Spectrum) -> Self {
        Self { inner: spectrum }
    }
}

impl From<PySpectrum> for Spectrum {
    fn from(py_spectrum: PySpectrum) -> Self {
        py_spectrum.inner
    }
}
