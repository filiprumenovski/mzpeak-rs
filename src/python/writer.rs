//! Python bindings for MzPeakWriter and MzPeakDatasetWriter
//!
//! Provides write access to create mzPeak files with context manager support.

use numpy::{IntoPyArray, PyReadonlyArray1};
use pyo3::prelude::*;
use std::fs::File;
use std::path::PathBuf;

use crate::dataset::{DatasetWriterV2Config, MzPeakDatasetWriter, MzPeakDatasetWriterV2, OutputMode};
use crate::formats::ingest::{IngestSpectrum, IngestSpectrumConverter};
use crate::metadata::MzPeakMetadata;
use crate::python::exceptions::IntoPyResult;
use crate::python::types::{
    build_peak_arrays, PyChromatogram, PyDatasetV2Stats, PyMobilogram, PyPeakArraysV2,
    PySpectrum, PySpectrumArrays, PySpectrumMetadata, PySpectrumV2, PyWriterConfig, PyWriterStats,
};
use crate::schema::manifest::Modality;
use crate::writer::{
    AsyncMzPeakWriter, MzPeakWriter, OptionalColumnBuf, OwnedColumnarBatch, PeakArrays,
    PeaksWriterV2Config, RollingWriter, RollingWriterStats, SpectraWriterConfig, SpectrumArrays,
    SpectrumV2, WriterConfig, WriterStats,
};

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
        let spectrum_arrays = spectrum.into_arrays();
        py.allow_threads(|| writer.write_spectrum_owned(spectrum_arrays).into_py_result())
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
        let rust_spectra: Vec<SpectrumArrays> =
            spectra.into_iter().map(|s| s.into_arrays()).collect();
        py.allow_threads(|| writer.write_spectra_owned(rust_spectra).into_py_result())
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
        let spectrum_arrays = spectrum.into_arrays();
        py.allow_threads(|| writer.write_spectrum_owned(spectrum_arrays).into_py_result())
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
        let rust_spectra: Vec<SpectrumArrays> =
            spectra.into_iter().map(|s| s.into_arrays()).collect();
        py.allow_threads(|| writer.write_spectra_owned(rust_spectra).into_py_result())
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

/// Writer for creating mzPeak v2.0 dataset containers
///
/// Creates a normalized two-table container with spectra and peaks parquet files.
#[pyclass(name = "MzPeakDatasetWriterV2", unsendable)]
pub struct PyMzPeakDatasetWriterV2 {
    inner: Option<MzPeakDatasetWriterV2>,
    path: String,
    closed: bool,
    modality: Modality,
}

#[pymethods]
impl PyMzPeakDatasetWriterV2 {
    /// Create a new v2 dataset writer
    ///
    /// Args:
    ///     path: Output .mzpeak container path
    ///     modality: Data modality ("lc-ms", "lc-ims-ms", "msi", "msi-ims")
    ///     config: Optional WriterConfig for compression settings
    #[new]
    #[pyo3(signature = (path, modality="lc-ms", config=None))]
    fn new(path: String, modality: &str, config: Option<PyWriterConfig>) -> PyResult<Self> {
        let modality = parse_modality(modality)?;
        let writer_config = config.map(|c| c.inner).unwrap_or_default();

        let dataset_config = DatasetWriterV2Config {
            spectra_config: SpectraWriterConfig {
                compression: writer_config.compression,
                data_page_size: writer_config.data_page_size,
                write_statistics: writer_config.write_statistics,
                ..Default::default()
            },
            peaks_config: PeaksWriterV2Config {
                compression: writer_config.compression,
                row_group_size: writer_config.row_group_size,
                data_page_size: writer_config.data_page_size,
                write_statistics: writer_config.write_statistics,
                use_byte_stream_split: writer_config.use_byte_stream_split,
                ..Default::default()
            },
        };

        let writer =
            MzPeakDatasetWriterV2::with_config(&path, modality, None, dataset_config)
                .into_py_result()?;

        Ok(Self {
            inner: Some(writer),
            path,
            closed: false,
            modality,
        })
    }

    /// Write a single spectrum using v2 metadata + peaks
    fn write_spectrum_v2(
        &mut self,
        py: Python<'_>,
        metadata: PyRef<'_, PySpectrumMetadata>,
        peaks: PyRef<'_, PyPeakArraysV2>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let peaks_rust = peaks.to_rust(py)?;
        let metadata_rust = metadata.to_rust();
        let expected = peaks_rust.len() as u32;
        if metadata_rust.peak_count != expected {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "peak_count {} does not match peaks length {}",
                metadata_rust.peak_count, expected
            )));
        }
        py.allow_threads(|| writer.write_spectrum_v2(&metadata_rust, &peaks_rust).into_py_result())
    }

    /// Write a single SpectrumV2
    fn write_spectrum(
        &mut self,
        py: Python<'_>,
        spectrum: PyRef<'_, PySpectrumV2>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let spectrum_owned = spectrum.inner.clone();
        py.allow_threads(|| writer.write_spectrum(&spectrum_owned).into_py_result())
    }

    /// Write multiple SpectrumV2 objects
    fn write_spectra(
        &mut self,
        py: Python<'_>,
        spectra: Vec<Py<PySpectrumV2>>,
    ) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let mut rust_spectra: Vec<SpectrumV2> = Vec::with_capacity(spectra.len());
        for spectrum in spectra {
            let spectrum_ref = spectrum.bind(py).borrow();
            rust_spectra.push(spectrum_ref.inner.clone());
        }
        py.allow_threads(|| writer.write_spectra(&rust_spectra).into_py_result())
    }

    /// Get current stats (spectra/peaks counts)
    fn stats(&self, py: Python<'_>) -> PyResult<PyObject> {
        let writer = self.get_writer()?;
        let (spectra_written, peaks_written) = writer.stats();
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("spectra_written", spectra_written)?;
        dict.set_item("peaks_written", peaks_written)?;
        Ok(dict.into())
    }

    /// Finalize and close the dataset writer
    fn close(&mut self, py: Python<'_>) -> PyResult<PyDatasetV2Stats> {
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
        Ok(PyDatasetV2Stats::from(stats))
    }

    /// Get the modality
    fn modality(&self) -> String {
        modality_to_str(self.modality).to_string()
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
            format!(
                "MzPeakDatasetWriterV2('{}', modality='{}', open=True)",
                self.path,
                modality_to_str(self.modality)
            )
        } else {
            format!(
                "MzPeakDatasetWriterV2('{}', modality='{}', open=False)",
                self.path,
                modality_to_str(self.modality)
            )
        }
    }
}

impl PyMzPeakDatasetWriterV2 {
    fn get_writer(&self) -> PyResult<&MzPeakDatasetWriterV2> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is closed",
            ));
        }
        self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })
    }

    fn get_writer_mut(&mut self) -> PyResult<&mut MzPeakDatasetWriterV2> {
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
#[derive(Debug, Clone)]
struct SpectrumBuilderState {
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
    peaks: Vec<PyPeak>,
}

impl SpectrumBuilderState {
    fn new(spectrum_id: i64, scan_number: i64) -> Self {
        Self {
            spectrum_id,
            scan_number,
            ms_level: 1,
            retention_time: 0.0,
            polarity: 1,
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
            peaks: Vec::new(),
        }
    }
}

#[pyclass(name = "SpectrumBuilder")]
pub struct PySpectrumBuilder {
    inner: SpectrumBuilderState,
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
            inner: SpectrumBuilderState::new(spectrum_id, scan_number),
        }
    }

    /// Set the MS level
    fn ms_level(mut slf: PyRefMut<'_, Self>, level: i16) -> PyRefMut<'_, Self> {
        slf.inner.ms_level = level;
        slf
    }

    /// Set the retention time in seconds
    fn retention_time(mut slf: PyRefMut<'_, Self>, rt: f32) -> PyRefMut<'_, Self> {
        slf.inner.retention_time = rt;
        slf
    }

    /// Set the polarity (1 for positive, -1 for negative)
    fn polarity(mut slf: PyRefMut<'_, Self>, polarity: i8) -> PyRefMut<'_, Self> {
        slf.inner.polarity = polarity;
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
        slf.inner.precursor_mz = Some(mz);
        slf.inner.precursor_charge = charge;
        slf.inner.precursor_intensity = intensity;
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
        slf.inner.isolation_window_lower = Some(lower);
        slf.inner.isolation_window_upper = Some(upper);
        slf
    }

    /// Set the collision energy in eV
    fn collision_energy(mut slf: PyRefMut<'_, Self>, ce: f32) -> PyRefMut<'_, Self> {
        slf.inner.collision_energy = Some(ce);
        slf
    }

    /// Set the ion injection time in milliseconds
    fn injection_time(mut slf: PyRefMut<'_, Self>, time_ms: f32) -> PyRefMut<'_, Self> {
        slf.inner.injection_time = Some(time_ms);
        slf
    }

    /// Set MSI pixel coordinates (2D)
    fn pixel(mut slf: PyRefMut<'_, Self>, x: i32, y: i32) -> PyRefMut<'_, Self> {
        slf.inner.pixel_x = Some(x);
        slf.inner.pixel_y = Some(y);
        slf
    }

    /// Set MSI pixel coordinates (3D)
    fn pixel_3d(mut slf: PyRefMut<'_, Self>, x: i32, y: i32, z: i32) -> PyRefMut<'_, Self> {
        slf.inner.pixel_x = Some(x);
        slf.inner.pixel_y = Some(y);
        slf.inner.pixel_z = Some(z);
        slf
    }

    /// Set all peaks at once
    ///
    /// Args:
    ///     peaks: List of Peak objects
    fn peaks(mut slf: PyRefMut<'_, Self>, peaks: Vec<PyPeak>) -> PyRefMut<'_, Self> {
        slf.inner.peaks = peaks;
        slf
    }

    /// Add a single peak
    ///
    /// Args:
    ///     mz: Mass-to-charge ratio
    ///     intensity: Signal intensity
    fn add_peak(mut slf: PyRefMut<'_, Self>, mz: f64, intensity: f32) -> PyRefMut<'_, Self> {
        slf.inner
            .peaks
            .push(PyPeak::from_values(mz, intensity, None));
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
        slf.inner
            .peaks
            .push(PyPeak::from_values(mz, intensity, Some(ion_mobility)));
        slf
    }

    /// Build the final Spectrum object
    ///
    /// Returns:
    ///     Spectrum object with all configured properties
    fn build(&mut self) -> PySpectrum {
        let state = std::mem::replace(
            &mut self.inner,
            SpectrumBuilderState::new(0, 0),
        );
        let peaks = build_peak_arrays(&state.peaks);
        let mut spectrum = SpectrumArrays {
            spectrum_id: state.spectrum_id,
            scan_number: state.scan_number,
            ms_level: state.ms_level,
            retention_time: state.retention_time,
            polarity: state.polarity,
            precursor_mz: state.precursor_mz,
            precursor_charge: state.precursor_charge,
            precursor_intensity: state.precursor_intensity,
            isolation_window_lower: state.isolation_window_lower,
            isolation_window_upper: state.isolation_window_upper,
            collision_energy: state.collision_energy,
            total_ion_current: state.total_ion_current,
            base_peak_mz: state.base_peak_mz,
            base_peak_intensity: state.base_peak_intensity,
            injection_time: state.injection_time,
            pixel_x: state.pixel_x,
            pixel_y: state.pixel_y,
            pixel_z: state.pixel_z,
            peaks,
        };
        if spectrum.total_ion_current.is_none() {
            spectrum.compute_statistics();
        }
        PySpectrum::from(spectrum)
    }

    fn __repr__(&self) -> String {
        "SpectrumBuilder(...)".to_string()
    }
}

use crate::python::types::PyPeak;

fn parse_modality(value: &str) -> PyResult<Modality> {
    let normalized = value.trim().to_lowercase();
    match normalized.as_str() {
        "lc-ms" | "lc_ms" | "lcms" => Ok(Modality::LcMs),
        "lc-ims-ms" | "lc_ims_ms" | "lcimsms" => Ok(Modality::LcImsMs),
        "msi" => Ok(Modality::Msi),
        "msi-ims" | "msi_ims" | "msiims" => Ok(Modality::MsiIms),
        _ => Err(pyo3::exceptions::PyValueError::new_err(
            "Unknown modality. Use 'lc-ms', 'lc-ims-ms', 'msi', or 'msi-ims'.",
        )),
    }
}

fn modality_to_str(modality: Modality) -> &'static str {
    match modality {
        Modality::LcMs => "lc-ms",
        Modality::LcImsMs => "lc-ims-ms",
        Modality::Msi => "msi",
        Modality::MsiIms => "msi-ims",
    }
}

// ============================================================================
// Rolling Writer - Auto-sharding by peak count
// ============================================================================

/// Statistics from a rolling writer operation
#[pyclass(name = "RollingWriterStats")]
#[derive(Clone)]
pub struct PyRollingWriterStats {
    inner: RollingWriterStats,
}

#[pymethods]
impl PyRollingWriterStats {
    /// Total number of spectra written across all files
    #[getter]
    fn total_spectra_written(&self) -> usize {
        self.inner.total_spectra_written
    }

    /// Total number of peaks written across all files
    #[getter]
    fn total_peaks_written(&self) -> usize {
        self.inner.total_peaks_written
    }

    /// Number of output files created
    #[getter]
    fn files_written(&self) -> usize {
        self.inner.files_written
    }

    /// Statistics for each individual file part
    #[getter]
    fn part_stats(&self) -> Vec<PyWriterStats> {
        self.inner
            .part_stats
            .iter()
            .map(|s| PyWriterStats::from(s.clone()))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "RollingWriterStats(spectra={}, peaks={}, files={})",
            self.inner.total_spectra_written,
            self.inner.total_peaks_written,
            self.inner.files_written
        )
    }
}

impl From<RollingWriterStats> for PyRollingWriterStats {
    fn from(stats: RollingWriterStats) -> Self {
        Self { inner: stats }
    }
}

/// Rolling writer that automatically shards output into multiple files
///
/// Useful for processing very large datasets that need to be split across
/// multiple files based on peak count limits.
///
/// Example:
///     >>> with mzpeak.RollingWriter("output.parquet", max_peaks_per_file=10_000_000) as writer:
///     ...     for spectrum in spectra:
///     ...         writer.write_spectrum(spectrum)
///     ...     stats = writer.finish()
///     >>> print(f"Wrote {stats.files_written} files")
#[pyclass(name = "RollingWriter", unsendable)]
pub struct PyRollingWriter {
    inner: Option<RollingWriter>,
    base_path: String,
    closed: bool,
}

#[pymethods]
impl PyRollingWriter {
    /// Create a new rolling writer
    ///
    /// Args:
    ///     base_path: Base output file path (files will be named base-part-NNNN.parquet)
    ///     max_peaks_per_file: Maximum peaks per output file (default 50M)
    ///     config: Optional WriterConfig for compression settings
    ///
    /// Returns:
    ///     RollingWriter instance
    #[new]
    #[pyo3(signature = (base_path, max_peaks_per_file=50_000_000, config=None))]
    fn new(
        base_path: String,
        max_peaks_per_file: usize,
        config: Option<PyWriterConfig>,
    ) -> PyResult<Self> {
        let mut writer_config = config.map(|c| c.inner).unwrap_or_default();
        writer_config.max_peaks_per_file = Some(max_peaks_per_file);
        let metadata = MzPeakMetadata::new();

        let writer =
            RollingWriter::new(&base_path, metadata, writer_config).into_py_result()?;

        Ok(Self {
            inner: Some(writer),
            base_path,
            closed: false,
        })
    }

    /// Write a single spectrum
    ///
    /// Args:
    ///     spectrum: Spectrum object to write
    fn write_spectrum(&mut self, py: Python<'_>, spectrum: PySpectrum) -> PyResult<()> {
        let writer = self.get_writer_mut()?;
        let spectrum_arrays = spectrum.into_arrays();
        py.allow_threads(|| writer.write_spectrum_owned(spectrum_arrays).into_py_result())
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
        let rust_spectra: Vec<SpectrumArrays> =
            spectra.into_iter().map(|s| s.into_arrays()).collect();
        py.allow_threads(|| writer.write_spectra_owned(rust_spectra).into_py_result())
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
    ///     RollingWriterStats with counts of spectra and peaks written
    fn stats(&self) -> PyResult<PyRollingWriterStats> {
        let writer = self.get_writer()?;
        Ok(PyRollingWriterStats::from(writer.stats()))
    }

    /// Finalize and close the writer
    ///
    /// Returns:
    ///     RollingWriterStats with final statistics
    fn finish(&mut self, py: Python<'_>) -> PyResult<PyRollingWriterStats> {
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
        Ok(PyRollingWriterStats::from(stats))
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
            self.finish(py)?;
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        if self.is_open() {
            format!("RollingWriter('{}', open=True)", self.base_path)
        } else {
            format!("RollingWriter('{}', open=False)", self.base_path)
        }
    }
}

impl PyRollingWriter {
    fn get_writer(&self) -> PyResult<&RollingWriter> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is closed",
            ));
        }
        self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })
    }

    fn get_writer_mut(&mut self) -> PyResult<&mut RollingWriter> {
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

// ============================================================================
// Async Writer - Background compression and I/O
// ============================================================================

/// Async writer that offloads compression and I/O to a background thread
///
/// Ideal for high-throughput pipelines where the producer can prepare batches
/// while previous batches are being written. Uses zero-copy transfer of batches.
///
/// Example:
///     >>> writer = mzpeak.AsyncMzPeakWriter("output.parquet")
///     >>> for batch in batches:
///     ...     writer.write_batch(batch)  # Non-blocking until backpressure
///     >>> stats = writer.finish()  # Wait for background thread
#[pyclass(name = "AsyncMzPeakWriter", unsendable)]
pub struct PyAsyncMzPeakWriter {
    inner: Option<AsyncMzPeakWriter>,
    path: String,
    closed: bool,
}

#[pymethods]
impl PyAsyncMzPeakWriter {
    /// Create a new async writer
    ///
    /// Args:
    ///     path: Output file path
    ///     config: Optional WriterConfig for compression settings
    ///     buffer_capacity: Number of batches to buffer (default 8)
    ///
    /// Returns:
    ///     AsyncMzPeakWriter instance
    #[new]
    #[pyo3(signature = (path, config=None, buffer_capacity=8))]
    fn new(
        path: String,
        config: Option<PyWriterConfig>,
        buffer_capacity: usize,
    ) -> PyResult<Self> {
        let mut writer_config = config.map(|c| c.inner).unwrap_or_default();
        writer_config.async_buffer_capacity = buffer_capacity;
        let metadata = MzPeakMetadata::new();

        let file = File::create(&path).into_py_result()?;
        let writer =
            AsyncMzPeakWriter::new(file, metadata, writer_config).into_py_result()?;

        Ok(Self {
            inner: Some(writer),
            path,
            closed: false,
        })
    }

    /// Write an owned columnar batch (zero-copy transfer)
    ///
    /// Args:
    ///     batch: OwnedColumnarBatch to write
    ///
    /// Note: This may block if the buffer is full (backpressure)
    fn write_batch(&self, py: Python<'_>, batch: PyRef<'_, PyOwnedColumnarBatch>) -> PyResult<()> {
        let writer = self.get_writer()?;
        let rust_batch = batch.to_rust(py)?;
        py.allow_threads(|| writer.write_owned_batch(rust_batch).into_py_result())
    }

    /// Check if the background writer has encountered an error
    ///
    /// Returns:
    ///     None if no error, raises exception if error occurred
    fn check_error(&self) -> PyResult<()> {
        let writer = self.get_writer()?;
        writer.check_error().into_py_result()
    }

    /// Finalize and close the writer
    ///
    /// Waits for the background thread to complete all pending writes
    /// and finalize the Parquet file.
    ///
    /// Returns:
    ///     WriterStats with final statistics
    fn finish(&mut self, py: Python<'_>) -> PyResult<PyWriterStats> {
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
            self.finish(py)?;
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        if self.is_open() {
            format!("AsyncMzPeakWriter('{}', open=True)", self.path)
        } else {
            format!("AsyncMzPeakWriter('{}', open=False)", self.path)
        }
    }
}

impl PyAsyncMzPeakWriter {
    fn get_writer(&self) -> PyResult<&AsyncMzPeakWriter> {
        if self.closed {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Writer is closed",
            ));
        }
        self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Writer is not initialized")
        })
    }
}

// ============================================================================
// Owned Columnar Batch - Zero-copy bulk writes
// ============================================================================

/// Owned columnar batch for zero-copy writing to Arrow/Parquet
///
/// This struct takes full ownership of all data vectors, enabling true zero-copy
/// transfer to the underlying Arrow backend. Use this for maximum write performance.
///
/// Example:
///     >>> import numpy as np
///     >>> batch = mzpeak.OwnedColumnarBatch(
///     ...     mz=np.array([100.0, 200.0, 300.0], dtype=np.float64),
///     ...     intensity=np.array([1000.0, 2000.0, 500.0], dtype=np.float32),
///     ...     spectrum_id=np.array([0, 0, 0], dtype=np.int64),
///     ...     scan_number=np.array([1, 1, 1], dtype=np.int64),
///     ...     ms_level=np.array([1, 1, 1], dtype=np.int16),
///     ...     retention_time=np.array([60.0, 60.0, 60.0], dtype=np.float32),
///     ...     polarity=np.array([1, 1, 1], dtype=np.int8),
///     ... )
///     >>> writer.write_batch(batch)
#[pyclass(name = "OwnedColumnarBatch")]
pub struct PyOwnedColumnarBatch {
    // Required columns (stored as PyObject to avoid copying on access)
    mz: PyObject,
    intensity: PyObject,
    spectrum_id: PyObject,
    scan_number: PyObject,
    ms_level: PyObject,
    retention_time: PyObject,
    polarity: PyObject,
    // Optional columns
    ion_mobility: Option<PyObject>,
    precursor_mz: Option<PyObject>,
    precursor_charge: Option<PyObject>,
    precursor_intensity: Option<PyObject>,
    isolation_window_lower: Option<PyObject>,
    isolation_window_upper: Option<PyObject>,
    collision_energy: Option<PyObject>,
    total_ion_current: Option<PyObject>,
    base_peak_mz: Option<PyObject>,
    base_peak_intensity: Option<PyObject>,
    injection_time: Option<PyObject>,
    pixel_x: Option<PyObject>,
    pixel_y: Option<PyObject>,
    pixel_z: Option<PyObject>,
    // Cached length
    len: usize,
}

#[pymethods]
impl PyOwnedColumnarBatch {
    /// Create a new owned columnar batch from numpy arrays
    ///
    /// Args:
    ///     mz: Float64 array of m/z values
    ///     intensity: Float32 array of intensity values
    ///     spectrum_id: Int64 array of spectrum IDs
    ///     scan_number: Int64 array of scan numbers
    ///     ms_level: Int16 array of MS levels
    ///     retention_time: Float32 array of retention times
    ///     polarity: Int8 array of polarity values (1 or -1)
    ///     ion_mobility: Optional Float64 array of ion mobility values
    ///     precursor_mz: Optional Float64 array of precursor m/z
    ///     precursor_charge: Optional Int16 array of precursor charges
    ///     precursor_intensity: Optional Float32 array of precursor intensities
    ///     isolation_window_lower: Optional Float32 array
    ///     isolation_window_upper: Optional Float32 array
    ///     collision_energy: Optional Float32 array
    ///     total_ion_current: Optional Float64 array
    ///     base_peak_mz: Optional Float64 array
    ///     base_peak_intensity: Optional Float32 array
    ///     injection_time: Optional Float32 array
    ///     pixel_x: Optional Int32 array (MSI)
    ///     pixel_y: Optional Int32 array (MSI)
    ///     pixel_z: Optional Int32 array (MSI)
    #[new]
    #[pyo3(signature = (
        mz, intensity, spectrum_id, scan_number, ms_level, retention_time, polarity,
        ion_mobility=None, precursor_mz=None, precursor_charge=None, precursor_intensity=None,
        isolation_window_lower=None, isolation_window_upper=None, collision_energy=None,
        total_ion_current=None, base_peak_mz=None, base_peak_intensity=None,
        injection_time=None, pixel_x=None, pixel_y=None, pixel_z=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        mz: PyObject,
        intensity: PyObject,
        spectrum_id: PyObject,
        scan_number: PyObject,
        ms_level: PyObject,
        retention_time: PyObject,
        polarity: PyObject,
        ion_mobility: Option<PyObject>,
        precursor_mz: Option<PyObject>,
        precursor_charge: Option<PyObject>,
        precursor_intensity: Option<PyObject>,
        isolation_window_lower: Option<PyObject>,
        isolation_window_upper: Option<PyObject>,
        collision_energy: Option<PyObject>,
        total_ion_current: Option<PyObject>,
        base_peak_mz: Option<PyObject>,
        base_peak_intensity: Option<PyObject>,
        injection_time: Option<PyObject>,
        pixel_x: Option<PyObject>,
        pixel_y: Option<PyObject>,
        pixel_z: Option<PyObject>,
    ) -> PyResult<Self> {
        // Validate array lengths match
        let mz_arr: PyReadonlyArray1<f64> = mz.extract(py)?;
        let len = mz_arr.len()?;

        // Validate required columns
        validate_array_len::<f32>(py, &intensity, "intensity", len)?;
        validate_array_len::<i64>(py, &spectrum_id, "spectrum_id", len)?;
        validate_array_len::<i64>(py, &scan_number, "scan_number", len)?;
        validate_array_len::<i16>(py, &ms_level, "ms_level", len)?;
        validate_array_len::<f32>(py, &retention_time, "retention_time", len)?;
        validate_array_len::<i8>(py, &polarity, "polarity", len)?;

        // Validate optional columns if present
        if let Some(ref arr) = ion_mobility {
            validate_array_len::<f64>(py, arr, "ion_mobility", len)?;
        }
        if let Some(ref arr) = precursor_mz {
            validate_array_len::<f64>(py, arr, "precursor_mz", len)?;
        }
        if let Some(ref arr) = precursor_charge {
            validate_array_len::<i16>(py, arr, "precursor_charge", len)?;
        }
        if let Some(ref arr) = precursor_intensity {
            validate_array_len::<f32>(py, arr, "precursor_intensity", len)?;
        }
        if let Some(ref arr) = isolation_window_lower {
            validate_array_len::<f32>(py, arr, "isolation_window_lower", len)?;
        }
        if let Some(ref arr) = isolation_window_upper {
            validate_array_len::<f32>(py, arr, "isolation_window_upper", len)?;
        }
        if let Some(ref arr) = collision_energy {
            validate_array_len::<f32>(py, arr, "collision_energy", len)?;
        }
        if let Some(ref arr) = total_ion_current {
            validate_array_len::<f64>(py, arr, "total_ion_current", len)?;
        }
        if let Some(ref arr) = base_peak_mz {
            validate_array_len::<f64>(py, arr, "base_peak_mz", len)?;
        }
        if let Some(ref arr) = base_peak_intensity {
            validate_array_len::<f32>(py, arr, "base_peak_intensity", len)?;
        }
        if let Some(ref arr) = injection_time {
            validate_array_len::<f32>(py, arr, "injection_time", len)?;
        }
        if let Some(ref arr) = pixel_x {
            validate_array_len::<i32>(py, arr, "pixel_x", len)?;
        }
        if let Some(ref arr) = pixel_y {
            validate_array_len::<i32>(py, arr, "pixel_y", len)?;
        }
        if let Some(ref arr) = pixel_z {
            validate_array_len::<i32>(py, arr, "pixel_z", len)?;
        }

        Ok(Self {
            mz,
            intensity,
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            ion_mobility,
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
            len,
        })
    }

    /// Create a batch with only required columns (optional columns set to all-null)
    #[staticmethod]
    fn with_required(
        py: Python<'_>,
        mz: PyObject,
        intensity: PyObject,
        spectrum_id: PyObject,
        scan_number: PyObject,
        ms_level: PyObject,
        retention_time: PyObject,
        polarity: PyObject,
    ) -> PyResult<Self> {
        Self::new(
            py,
            mz,
            intensity,
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
        )
    }

    /// Number of peaks in this batch
    fn __len__(&self) -> usize {
        self.len
    }

    /// Number of peaks in this batch
    #[getter]
    fn num_peaks(&self) -> usize {
        self.len
    }

    fn __repr__(&self) -> String {
        format!("OwnedColumnarBatch(peaks={})", self.len)
    }
}

impl PyOwnedColumnarBatch {
    /// Convert to Rust OwnedColumnarBatch (copies the data)
    pub(crate) fn to_rust(&self, py: Python<'_>) -> PyResult<OwnedColumnarBatch> {
        let len = self.len;

        let mz = extract_vec_copy::<f64>(py, &self.mz, "mz")?;
        let intensity = extract_vec_copy::<f32>(py, &self.intensity, "intensity")?;
        let spectrum_id = extract_vec_copy::<i64>(py, &self.spectrum_id, "spectrum_id")?;
        let scan_number = extract_vec_copy::<i64>(py, &self.scan_number, "scan_number")?;
        let ms_level = extract_vec_copy::<i16>(py, &self.ms_level, "ms_level")?;
        let retention_time = extract_vec_copy::<f32>(py, &self.retention_time, "retention_time")?;
        let polarity = extract_vec_copy::<i8>(py, &self.polarity, "polarity")?;

        let ion_mobility = extract_optional_column::<f64>(py, &self.ion_mobility, len)?;
        let precursor_mz = extract_optional_column::<f64>(py, &self.precursor_mz, len)?;
        let precursor_charge = extract_optional_column::<i16>(py, &self.precursor_charge, len)?;
        let precursor_intensity = extract_optional_column::<f32>(py, &self.precursor_intensity, len)?;
        let isolation_window_lower = extract_optional_column::<f32>(py, &self.isolation_window_lower, len)?;
        let isolation_window_upper = extract_optional_column::<f32>(py, &self.isolation_window_upper, len)?;
        let collision_energy = extract_optional_column::<f32>(py, &self.collision_energy, len)?;
        let total_ion_current = extract_optional_column::<f64>(py, &self.total_ion_current, len)?;
        let base_peak_mz = extract_optional_column::<f64>(py, &self.base_peak_mz, len)?;
        let base_peak_intensity = extract_optional_column::<f32>(py, &self.base_peak_intensity, len)?;
        let injection_time = extract_optional_column::<f32>(py, &self.injection_time, len)?;
        let pixel_x = extract_optional_column::<i32>(py, &self.pixel_x, len)?;
        let pixel_y = extract_optional_column::<i32>(py, &self.pixel_y, len)?;
        let pixel_z = extract_optional_column::<i32>(py, &self.pixel_z, len)?;

        Ok(OwnedColumnarBatch {
            mz,
            intensity,
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            ion_mobility,
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
        })
    }
}

// ============================================================================
// Thin-Waist Ingestion Types
// ============================================================================

/// Thin-waist ingestion spectrum for contract-enforced writing
///
/// This provides a validated spectrum type that enforces the ingestion contract
/// invariants (valid ms_level, polarity, finite retention_time, matching array lengths).
///
/// Example:
///     >>> import numpy as np
///     >>> converter = mzpeak.IngestSpectrumConverter()
///     >>> ingest = mzpeak.IngestSpectrum(
///     ...     spectrum_id=0,
///     ...     scan_number=1,
///     ...     ms_level=1,
///     ...     retention_time=60.0,
///     ...     polarity=1,
///     ...     mz=np.array([100.0, 200.0], dtype=np.float64),
///     ...     intensity=np.array([1000.0, 2000.0], dtype=np.float32),
///     ... )
///     >>> spectrum_arrays = converter.convert(ingest)
#[pyclass(name = "IngestSpectrum")]
pub struct PyIngestSpectrum {
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
    mz: PyObject,
    intensity: PyObject,
    ion_mobility: Option<PyObject>,
}

#[pymethods]
impl PyIngestSpectrum {
    /// Create a new ingestion spectrum
    #[new]
    #[pyo3(signature = (
        spectrum_id, scan_number, ms_level, retention_time, polarity, mz, intensity,
        ion_mobility=None, precursor_mz=None, precursor_charge=None, precursor_intensity=None,
        isolation_window_lower=None, isolation_window_upper=None, collision_energy=None,
        total_ion_current=None, base_peak_mz=None, base_peak_intensity=None,
        injection_time=None, pixel_x=None, pixel_y=None, pixel_z=None
    ))]
    #[allow(clippy::too_many_arguments)]
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
        // Validate array lengths
        let mz_arr: PyReadonlyArray1<f64> = mz.extract(py)?;
        let len = mz_arr.len()?;
        validate_array_len::<f32>(py, &intensity, "intensity", len)?;
        if let Some(ref arr) = ion_mobility {
            validate_array_len::<f64>(py, arr, "ion_mobility", len)?;
        }

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
            mz,
            intensity,
            ion_mobility,
        })
    }

    #[getter]
    fn spectrum_id(&self) -> i64 {
        self.spectrum_id
    }

    #[getter]
    fn scan_number(&self) -> i64 {
        self.scan_number
    }

    #[getter]
    fn ms_level(&self) -> i16 {
        self.ms_level
    }

    #[getter]
    fn retention_time(&self) -> f32 {
        self.retention_time
    }

    #[getter]
    fn polarity(&self) -> i8 {
        self.polarity
    }

    #[getter]
    fn mz_array(&self) -> PyObject {
        self.mz.clone()
    }

    #[getter]
    fn intensity_array(&self) -> PyObject {
        self.intensity.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "IngestSpectrum(id={}, ms_level={}, rt={:.2}s)",
            self.spectrum_id, self.ms_level, self.retention_time
        )
    }
}

impl PyIngestSpectrum {
    pub(crate) fn to_rust(&self, py: Python<'_>) -> PyResult<IngestSpectrum> {
        let mz = extract_vec_copy::<f64>(py, &self.mz, "mz")?;
        let intensity = extract_vec_copy::<f32>(py, &self.intensity, "intensity")?;
        
        let ion_mobility = match &self.ion_mobility {
            None => OptionalColumnBuf::all_null(mz.len()),
            Some(obj) => {
                let values = extract_vec_copy::<f64>(py, obj, "ion_mobility")?;
                OptionalColumnBuf::AllPresent(values)
            }
        };

        Ok(IngestSpectrum {
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
            peaks: PeakArrays {
                mz,
                intensity,
                ion_mobility,
            },
        })
    }
}

/// Stateful converter from IngestSpectrum to SpectrumArrays with contract enforcement
///
/// Validates that spectrum IDs are contiguous and enforces all ingestion contract
/// invariants (ms_level >= 1, valid polarity, finite retention_time, etc.).
///
/// Example:
///     >>> converter = mzpeak.IngestSpectrumConverter()
///     >>> for ingest_spectrum in spectra:
///     ...     spectrum_arrays = converter.convert(ingest_spectrum)
///     ...     writer.write_spectrum_arrays(spectrum_arrays)
#[pyclass(name = "IngestSpectrumConverter")]
pub struct PyIngestSpectrumConverter {
    inner: IngestSpectrumConverter,
}

#[pymethods]
impl PyIngestSpectrumConverter {
    /// Create a new contract-enforcing converter
    #[new]
    fn new() -> Self {
        Self {
            inner: IngestSpectrumConverter::new(),
        }
    }

    /// Convert an ingestion spectrum to SpectrumArrays with contract validation
    ///
    /// Args:
    ///     ingest: IngestSpectrum to convert
    ///
    /// Returns:
    ///     SpectrumArrays suitable for writing
    ///
    /// Raises:
    ///     MzPeakValidationError: If contract validation fails
    fn convert(&mut self, py: Python<'_>, ingest: PyRef<'_, PyIngestSpectrum>) -> PyResult<PySpectrumArrays> {
        let rust_ingest = ingest.to_rust(py)?;
        let spectrum_arrays = self.inner.convert(rust_ingest).into_py_result()?;
        Ok(PySpectrumArrays::from_rust(py, spectrum_arrays))
    }

    fn __repr__(&self) -> String {
        "IngestSpectrumConverter()".to_string()
    }
}

// ============================================================================
// Helper functions
// ============================================================================

fn validate_array_len<T: numpy::Element>(
    py: Python<'_>,
    obj: &PyObject,
    name: &str,
    expected_len: usize,
) -> PyResult<()> {
    let arr: PyReadonlyArray1<T> = obj.extract(py)?;
    let len = arr.len()?;
    if len != expected_len {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "{} length {} does not match expected length {}",
            name, len, expected_len
        )));
    }
    Ok(())
}

fn extract_vec_copy<T: numpy::Element + Copy>(
    py: Python<'_>,
    obj: &PyObject,
    label: &str,
) -> PyResult<Vec<T>> {
    let array: PyReadonlyArray1<T> = obj.extract(py)?;
    let slice = array
        .as_slice()
        .map_err(|_| pyo3::exceptions::PyValueError::new_err(format!(
            "{} must be a contiguous 1D array", label
        )))?;
    Ok(slice.to_vec())
}

fn extract_optional_column<T: numpy::Element + Copy>(
    py: Python<'_>,
    opt: &Option<PyObject>,
    len: usize,
) -> PyResult<OptionalColumnBuf<T>> {
    match opt {
        None => Ok(OptionalColumnBuf::all_null(len)),
        Some(obj) => {
            let values = extract_vec_copy::<T>(py, obj, "optional_column")?;
            Ok(OptionalColumnBuf::AllPresent(values))
        }
    }
}
