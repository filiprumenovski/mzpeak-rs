use pyo3::prelude::*;

use crate::writer::{OptionalColumnBuf, PeakArrays, SpectrumArrays};

use super::peak::PyPeak;

/// A mass spectrum containing peaks and metadata
#[pyclass(name = "Spectrum")]
#[derive(Clone)]
pub struct PySpectrum {
    pub(crate) inner: SpectrumArrays,
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
        let peak_list = peaks.unwrap_or_default();
        let peak_arrays = build_peak_arrays(&peak_list);

        Self {
            inner: SpectrumArrays {
                spectrum_id,
                scan_number,
                ms_level,
                retention_time,
                polarity,
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
                peaks: peak_arrays,
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
        let mut out = Vec::with_capacity(self.inner.peaks.len());
        let mz = &self.inner.peaks.mz;
        let intensity = &self.inner.peaks.intensity;

        match &self.inner.peaks.ion_mobility {
            OptionalColumnBuf::AllNull { .. } => {
                for (mz_val, intensity_val) in mz.iter().zip(intensity.iter()) {
                    out.push(PyPeak::from_values(*mz_val, *intensity_val, None));
                }
            }
            OptionalColumnBuf::AllPresent(values) => {
                for ((mz_val, intensity_val), im_val) in mz
                    .iter()
                    .zip(intensity.iter())
                    .zip(values.iter())
                {
                    out.push(PyPeak::from_values(*mz_val, *intensity_val, Some(*im_val)));
                }
            }
            OptionalColumnBuf::WithValidity { values, validity } => {
                for i in 0..mz.len() {
                    let im = if validity.get(i).copied().unwrap_or(false) {
                        values.get(i).copied()
                    } else {
                        None
                    };
                    out.push(PyPeak::from_values(mz[i], intensity[i], im));
                }
            }
        }

        out
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

    fn __repr__(&self) -> String {
        format!(
            "Spectrum(id={}, scan={}, ms_level={}, rt={:.2}s, {} peaks)",
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

impl PySpectrum {
    pub(crate) fn into_arrays(self) -> SpectrumArrays {
        self.inner
    }

    pub(crate) fn to_arrays(&self) -> SpectrumArrays {
        self.inner.clone()
    }
}

impl From<SpectrumArrays> for PySpectrum {
    fn from(spectrum: SpectrumArrays) -> Self {
        Self { inner: spectrum }
    }
}

pub(crate) fn build_peak_arrays(peaks: &[PyPeak]) -> PeakArrays {
    let len = peaks.len();
    let mut mz = Vec::with_capacity(len);
    let mut intensity = Vec::with_capacity(len);
    let mut ion_mobility_values = Vec::with_capacity(len);
    let mut validity = Vec::with_capacity(len);
    let mut any_im = false;
    let mut all_im = true;

    for peak in peaks {
        mz.push(peak.mz_value());
        intensity.push(peak.intensity_value());
        match peak.ion_mobility_value() {
            Some(im) => {
                any_im = true;
                ion_mobility_values.push(im);
                validity.push(true);
            }
            None => {
                all_im = false;
                ion_mobility_values.push(0.0);
                validity.push(false);
            }
        }
    }

    let ion_mobility = if !any_im {
        OptionalColumnBuf::AllNull { len }
    } else if all_im {
        OptionalColumnBuf::AllPresent(ion_mobility_values)
    } else {
        OptionalColumnBuf::WithValidity {
            values: ion_mobility_values,
            validity,
        }
    };

    PeakArrays {
        mz,
        intensity,
        ion_mobility,
    }
}
