//! Python bindings for MzPeakWriter and MzPeakDatasetWriter
//!
//! Provides write access to create mzPeak files with context manager support.

use pyo3::prelude::*;
use std::fs::File;

use crate::dataset::{MzPeakDatasetWriter, OutputMode};
use crate::metadata::MzPeakMetadata;
use crate::python::exceptions::IntoPyResult;
use crate::python::types::{
    PyChromatogram, PyMobilogram, PySpectrum, PySpectrumArrays, PyWriterConfig, PyWriterStats,
};
use crate::writer::{MzPeakWriter, Peak, Spectrum, SpectrumArrays, SpectrumBuilder};

/// Writer for creating mzPeak Parquet files
///
/// Supports streaming writes with automatic batching and compression.
/// Use as a context manager to ensure proper file finalization.
///
/// Example:
///     >>> with mzpeak.MzPeakWriter("output.parquet") as writer:
///     ...     spectrum = mzpeak.SpectrumBuilder(1, 1) \
///     ...         .ms_level(1) \
///     ...         .retention_time(60.0) \
///     ...         .add_peak(400.0, 10000.0) \
///     ...         .build()
///     ...     writer.write_spectrum(spectrum)
#[pyclass(name = "MzPeakWriter", unsendable)]
pub struct PyMzPeakWriter {
    inner: Option<MzPeakWriter<File>>,
    path: String,
    closed: bool,
}

#[pymethods]
impl PyMzPeakWriter {
    /// Create a new mzPeak writer
    ///
    /// Args:
    ///     path: Output file path (should end with .parquet or .mzpeak.parquet)
    ///     config: Optional WriterConfig for compression and batching settings
    ///
    /// Returns:
    ///     MzPeakWriter instance
    #[new]
    #[pyo3(signature = (path, config=None))]
    fn new(path: String, config: Option<PyWriterConfig>) -> PyResult<Self> {
        let writer_config = config.map(|c| c.inner).unwrap_or_default();
        let metadata = MzPeakMetadata::new();

        let writer =
            MzPeakWriter::new_file(&path, &metadata, writer_config).into_py_result()?;

        Ok(Self {
            inner: Some(writer),
            path,
            closed: false,
        })
    }

    /// Write a single spectrum
    ///
    /// Args:
    ///     spectrum: Spectrum object to write
    fn write_spectrum(&mut self, py: Python<'_>, spectrum: PySpectrum) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        py.allow_threads(|| writer.write_spectrum(&spectrum.inner).into_py_result())
    }

    /// Write a single spectrum using SoA arrays
    ///
    /// Args:
    ///     spectrum: SpectrumArrays object to write
    fn write_spectrum_arrays(
        &mut self,
        py: Python<'_>,
        spectrum: PyRef<'_, PySpectrumArrays>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let rust_spectrum = spectrum.to_rust(py)?;
        py.allow_threads(|| writer.write_spectrum_arrays(&rust_spectrum).into_py_result())
    }

    /// Write multiple spectra in a batch
    ///
    /// Args:
    ///     spectra: List of Spectrum objects to write
    fn write_spectra(&mut self, py: Python<'_>, spectra: Vec<PySpectrum>) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let rust_spectra: Vec<Spectrum> = spectra.into_iter().map(|s| s.inner).collect();
        py.allow_threads(|| writer.write_spectra(&rust_spectra).into_py_result())
    }

    /// Write multiple spectra using SoA arrays
    ///
    /// Args:
    ///     spectra: List of SpectrumArrays objects to write
    fn write_spectra_arrays(
        &mut self,
        py: Python<'_>,
        spectra: Vec<Py<PySpectrumArrays>>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let mut rust_spectra: Vec<SpectrumArrays> = Vec::with_capacity(spectra.len());
        for spectrum in spectra {
            let spectrum_ref = spectrum.bind(py).borrow();
            rust_spectra.push(spectrum_ref.to_rust(py)?);
        }
        py.allow_threads(|| writer.write_spectra_arrays(&rust_spectra).into_py_result())
    }


    /// Get current writer statistics
    ///
    /// Returns:
    ///     WriterStats with counts of spectra and peaks written
    fn stats(&self) -> PyResult<PyWriterStats> {
        let writer = self.get_writer()?;
        Ok(PyWriterStats::from(writer.stats()))
    }

    /// Finalize and close the writer
    ///
    /// This must be called to ensure all data is flushed and the file
    /// is properly finalized. Using the context manager handles this
    /// automatically.
    ///
    /// Returns:
    ///     WriterStats with final statistics
    fn close(&mut self, py: Python<'_>) -> PyResult<PyWriterStats> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is already closed",
            ));
        }

        let writer = self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })?;

        let stats = py.allow_threads(|| writer.finish().into_py_result())?;
        self.closed = true;
        Ok(PyWriterStats::from(stats))
    }

    /// Check if the writer is open
    fn is_open(&self) -> bool {
        self.inner.is_some() && !self.closed
    }

    /// Context manager entry
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager exit - finalize the writer
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, pyo3::types::PyType>>,
        _exc_val: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_tb: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<bool> {
        if !self.closed && self.inner.is_some() {
            self.close(py)?;
        }
        Ok(false) // Don't suppress exceptions
    }

    fn __repr__(&self) -> String {
        if self.is_open() {
            format!("MzPeakWriter('{}', open=True)", self.path)
        } else {
            format!("MzPeakWriter('{}', open=False)", self.path)
        }
    }
}

impl PyMzPeakWriter {
    fn get_writer(&self) -> PyResult<&MzPeakWriter<File>> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is closed",
            ));
        }
        self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })
    }

    fn get_writer_mut(&mut self) -> PyResult<&mut MzPeakWriter<File>> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is closed",
            ));
        }
        self.inner.as_mut().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })
    }
}

/// Writer for creating mzPeak dataset bundles
///
/// Creates directory-based or ZIP container datasets with separate files
/// for peaks, chromatograms, mobilograms, and metadata.
///
/// Example:
///     >>> with mzpeak.MzPeakDatasetWriter("output.mzpeak") as writer:
///     ...     spectrum = mzpeak.SpectrumBuilder(1, 1) \
///     ...         .ms_level(1) \
///     ...         .retention_time(60.0) \
///     ...         .add_peak(400.0, 10000.0) \
///     ...         .build()
///     ...     writer.write_spectrum(spectrum)
#[pyclass(name = "MzPeakDatasetWriter", unsendable)]
pub struct PyMzPeakDatasetWriter {
    inner: Option<MzPeakDatasetWriter>,
    path: String,
    closed: bool,
    output_mode: OutputMode,
}

#[pymethods]
impl PyMzPeakDatasetWriter {
    /// Create a new dataset writer
    ///
    /// Args:
    ///     path: Output path (will create .mzpeak directory or ZIP file)
    ///     config: Optional WriterConfig for compression settings
    ///     use_container: If True, create a ZIP container; otherwise create a directory
    ///
    /// Returns:
    ///     MzPeakDatasetWriter instance
    #[new]
    #[pyo3(signature = (path, config=None, use_container=true))]
    fn new(path: String, config: Option<PyWriterConfig>, use_container: bool) -> PyResult<Self> {
        let writer_config = config.map(|c| c.inner).unwrap_or_default();
        let metadata = MzPeakMetadata::new();

        let (writer, mode) = if use_container {
            (MzPeakDatasetWriter::new_container(&path, &metadata, writer_config), OutputMode::Container)
        } else {
            (MzPeakDatasetWriter::new_directory(&path, &metadata, writer_config), OutputMode::Directory)
        };
        let writer = writer.into_py_result()?;

        Ok(Self {
            inner: Some(writer),
            path,
            closed: false,
            output_mode: mode,
        })
    }

    /// Write a single spectrum
    ///
    /// Args:
    ///     spectrum: Spectrum object to write
    fn write_spectrum(&mut self, py: Python<'_>, spectrum: PySpectrum) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        py.allow_threads(|| writer.write_spectrum(&spectrum.inner).into_py_result())
    }

    /// Write a single spectrum using SoA arrays
    ///
    /// Args:
    ///     spectrum: SpectrumArrays object to write
    fn write_spectrum_arrays(
        &mut self,
        py: Python<'_>,
        spectrum: PyRef<'_, PySpectrumArrays>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let rust_spectrum = spectrum.to_rust(py)?;
        py.allow_threads(|| writer.write_spectrum_arrays(&rust_spectrum).into_py_result())
    }

    /// Write multiple spectra in a batch
    ///
    /// Args:
    ///     spectra: List of Spectrum objects to write
    fn write_spectra(&mut self, py: Python<'_>, spectra: Vec<PySpectrum>) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let rust_spectra: Vec<Spectrum> = spectra.into_iter().map(|s| s.inner).collect();
        py.allow_threads(|| writer.write_spectra(&rust_spectra).into_py_result())
    }

    /// Write multiple spectra using SoA arrays
    ///
    /// Args:
    ///     spectra: List of SpectrumArrays objects to write
    fn write_spectra_arrays(
        &mut self,
        py: Python<'_>,
        spectra: Vec<Py<PySpectrumArrays>>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let mut rust_spectra: Vec<SpectrumArrays> = Vec::with_capacity(spectra.len());
        for spectrum in spectra {
            let spectrum_ref = spectrum.bind(py).borrow();
            rust_spectra.push(spectrum_ref.to_rust(py)?);
        }
        py.allow_threads(|| writer.write_spectra_arrays(&rust_spectra).into_py_result())
    }

    /// Write a chromatogram
    ///
    /// Args:
    ///     chromatogram: Chromatogram object to write
    fn write_chromatogram(
        &mut self,
        py: Python<'_>,
        chromatogram: PyChromatogram,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        py.allow_threads(|| writer.write_chromatogram(&chromatogram.inner).into_py_result())
    }

    /// Write multiple chromatograms
    ///
    /// Args:
    ///     chromatograms: List of Chromatogram objects to write
    fn write_chromatograms(
        &mut self,
        py: Python<'_>,
        chromatograms: Vec<PyChromatogram>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let rust_chroms: Vec<crate::chromatogram_writer::Chromatogram> = chromatograms.into_iter().map(|c| c.inner).collect();
        py.allow_threads(|| writer.write_chromatograms(&rust_chroms).into_py_result())
    }

    /// Write a mobilogram
    ///
    /// Args:
    ///     mobilogram: Mobilogram object to write
    fn write_mobilogram(&mut self, py: Python<'_>, mobilogram: PyMobilogram) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        py.allow_threads(|| writer.write_mobilogram(&mobilogram.inner).into_py_result())
    }

    /// Write multiple mobilograms
    ///
    /// Args:
    ///     mobilograms: List of Mobilogram objects to write
    fn write_mobilograms(
        &mut self,
        py: Python<'_>,
        mobilograms: Vec<PyMobilogram>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let rust_mobs: Vec<crate::mobilogram_writer::Mobilogram> = mobilograms.into_iter().map(|m| m.inner).collect();
        py.allow_threads(|| writer.write_mobilograms(&rust_mobs).into_py_result())
    }

    /// Get the output mode (directory or container)
    fn output_mode(&self) -> PyResult<String> {
        Ok(format!("{:?}", self.output_mode))
    }

    /// Finalize and close the dataset writer
    ///
    /// Returns:
    ///     Dictionary with final statistics
    fn close(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is already closed",
            ));
        }

        let writer = self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })?;

        let stats = py.allow_threads(|| writer.close().into_py_result())?;
        self.closed = true;

        // Convert stats to Python dict
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("peaks_written", stats.peak_stats.peaks_written)?;
        dict.set_item("spectra_written", stats.peak_stats.spectra_written)?;
        dict.set_item("chromatograms_written", stats.chromatograms_written)?;
        dict.set_item("mobilograms_written", stats.mobilograms_written)?;
        Ok(dict.into())
    }

    /// Check if the writer is open
    fn is_open(&self) -> bool {
        self.inner.is_some() && !self.closed
    }

    /// Context manager entry
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager exit - finalize the writer
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, pyo3::types::PyType>>,
        _exc_val: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_tb: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<bool> {
        if !self.closed && self.inner.is_some() {
            self.close(py)?;
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        if self.is_open() {
            format!("MzPeakDatasetWriter('{}', open=True)", self.path)
        } else {
            format!("MzPeakDatasetWriter('{}', open=False)", self.path)
        }
    }
}

impl PyMzPeakDatasetWriter {
    fn get_writer(&self) -> PyResult<&MzPeakDatasetWriter> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is closed",
            ));
        }
        self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })
    }

    fn get_writer_mut(&mut self) -> PyResult<&mut MzPeakDatasetWriter> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is closed",
            ));
        }
        self.inner.as_mut().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })
    }
}

/// Builder for creating Spectrum objects with a fluent API
///
/// Example:
///     >>> spectrum = mzpeak.SpectrumBuilder(1, 1) \
///     ...     .ms_level(1) \
///     ...     .retention_time(60.0) \
///     ...     .polarity(1) \
///     ...     .add_peak(400.0, 10000.0) \
///     ...     .add_peak(500.0, 20000.0) \
///     ...     .build()
#[pyclass(name = "SpectrumBuilder")]
pub struct PySpectrumBuilder {
    inner: SpectrumBuilder,
}

#[pymethods]
impl PySpectrumBuilder {
    /// Create a new spectrum builder
    ///
    /// Args:
    ///     spectrum_id: Unique spectrum identifier
    ///     scan_number: Native scan number
    #[new]
    fn new(spectrum_id: i64, scan_number: i64) -> Self {
        Self {
            inner: SpectrumBuilder::new(spectrum_id, scan_number),
        }
    }

    /// Set the MS level
    fn ms_level(mut slf: PyRefMut<'_, Self>, level: i16) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).ms_level(level);
        slf
    }

    /// Set the retention time in seconds
    fn retention_time(mut slf: PyRefMut<'_, Self>, rt: f32) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).retention_time(rt);
        slf
    }

    /// Set the polarity (1 for positive, -1 for negative)
    fn polarity(mut slf: PyRefMut<'_, Self>, polarity: i8) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).polarity(polarity);
        slf
    }

    /// Set precursor information
    ///
    /// Args:
    ///     mz: Precursor m/z
    ///     charge: Optional charge state
    ///     intensity: Optional precursor intensity
    #[pyo3(signature = (mz, charge=None, intensity=None))]
    fn precursor(
        mut slf: PyRefMut<'_, Self>,
        mz: f64,
        charge: Option<i16>,
        intensity: Option<f32>,
    ) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).precursor(mz, charge, intensity);
        slf
    }

    /// Set the isolation window
    ///
    /// Args:
    ///     lower: Lower offset from precursor m/z
    ///     upper: Upper offset from precursor m/z
    fn isolation_window(
        mut slf: PyRefMut<'_, Self>,
        lower: f32,
        upper: f32,
    ) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).isolation_window(lower, upper);
        slf
    }

    /// Set the collision energy in eV
    fn collision_energy(mut slf: PyRefMut<'_, Self>, ce: f32) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).collision_energy(ce);
        slf
    }

    /// Set the ion injection time in milliseconds
    fn injection_time(mut slf: PyRefMut<'_, Self>, time_ms: f32) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).injection_time(time_ms);
        slf
    }

    /// Set MSI pixel coordinates (2D)
    fn pixel(mut slf: PyRefMut<'_, Self>, x: i32, y: i32) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).pixel(x, y);
        slf
    }

    /// Set MSI pixel coordinates (3D)
    fn pixel_3d(mut slf: PyRefMut<'_, Self>, x: i32, y: i32, z: i32) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).pixel_3d(x, y, z);
        slf
    }

    /// Set all peaks at once
    ///
    /// Args:
    ///     peaks: List of Peak objects
    fn peaks(mut slf: PyRefMut<'_, Self>, peaks: Vec<PyPeak>) -> PyRefMut<'_, Self> {
        let rust_peaks: Vec<Peak> = peaks.into_iter().map(|p| p.into()).collect();
        slf.inner = std::mem::take(&mut slf.inner).peaks(rust_peaks);
        slf
    }

    /// Add a single peak
    ///
    /// Args:
    ///     mz: Mass-to-charge ratio
    ///     intensity: Signal intensity
    fn add_peak(mut slf: PyRefMut<'_, Self>, mz: f64, intensity: f32) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).add_peak(mz, intensity);
        slf
    }

    /// Add a peak with ion mobility
    ///
    /// Args:
    ///     mz: Mass-to-charge ratio
    ///     intensity: Signal intensity
    ///     ion_mobility: Ion mobility value
    fn add_peak_with_im(
        mut slf: PyRefMut<'_, Self>,
        mz: f64,
        intensity: f32,
        ion_mobility: f64,
    ) -> PyRefMut<'_, Self> {
        slf.inner = std::mem::take(&mut slf.inner).add_peak_with_im(mz, intensity, ion_mobility);
        slf
    }

    /// Build the final Spectrum object
    ///
    /// Returns:
    ///     Spectrum object with all configured properties
    fn build(&mut self) -> PySpectrum {
        let spectrum = std::mem::take(&mut self.inner).build();
        PySpectrum::from(spectrum)
    }

    fn __repr__(&self) -> String {
        "SpectrumBuilder(...)".to_string()
    }
}

use crate::python::types::PyPeak;

impl Default for SpectrumBuilder {
    fn default() -> Self {
        Self::new(0, 0)
    }
}
