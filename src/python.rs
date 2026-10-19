//! Small Python interface: conversion, streaming spectra and v2 writing.
use crate::{
    convert::{convert as convert_file, ConvertOptions},
    dataset::{DatasetWriterV2Config, MzPeakDatasetWriterV2},
    reader::{MzPeakReader, ReaderConfig, StreamingSpectrumArraysViewIterator},
    schema::manifest::Modality,
    writer::{
        CompressionType, PeakArraysV2, PeaksWriterV2Config, SpectraWriterConfig, SpectrumMetadata,
    },
};
use numpy::{IntoPyArray, PyArray1, PyReadonlyArray1};
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyDict};
fn error(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}
fn object<T: serde::Serialize>(py: Python<'_>, value: &T) -> PyResult<PyObject> {
    let encoded = serde_json::to_string(value).map_err(error)?;
    Ok(py
        .import_bound("json")?
        .call_method1("loads", (encoded,))?
        .unbind())
}
#[pyfunction]
#[pyo3(signature=(input,output,*,compression_level=3,batch_size=256,rawr_binary=None))]
fn convert(
    py: Python<'_>,
    input: String,
    output: String,
    compression_level: i32,
    batch_size: usize,
    rawr_binary: Option<String>,
) -> PyResult<PyObject> {
    let mut options = ConvertOptions {
        batch_size,
        rawr_binary: rawr_binary.map(Into::into),
        ..Default::default()
    };
    options.writer_config.compression = CompressionType::Zstd(compression_level);
    let stats = py
        .allow_threads(|| convert_file(input, output, &options))
        .map_err(error)?;
    object(py, &stats)
}
#[pyfunction]
fn validate(py: Python<'_>, path: String) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| crate::validator::validate_mzpeak_file(path))
        .map_err(error)?;
    object(py, &result)
}
#[pyclass(name = "Spectrum")]
struct PySpectrum {
    #[pyo3(get)]
    metadata: PyObject,
    #[pyo3(get)]
    mz: Py<PyArray1<f64>>,
    #[pyo3(get)]
    intensity: Py<PyArray1<f32>>,
    #[pyo3(get)]
    ion_mobility: Option<Py<PyArray1<f64>>>,
    len: usize,
}
#[pymethods]
impl PySpectrum {
    fn __len__(&self) -> usize {
        self.len
    }
}
#[pyclass(name = "Reader")]
struct PyReader {
    reader: MzPeakReader,
}
#[pymethods]
impl PyReader {
    #[new]
    #[pyo3(signature=(path,*,batch_size=65536))]
    fn new(py: Python<'_>, path: String, batch_size: usize) -> PyResult<Self> {
        Ok(Self {
            reader: py
                .allow_threads(|| MzPeakReader::open_with_config(path, ReaderConfig { batch_size }))
                .map_err(error)?,
        })
    }
    fn metadata(&self, py: Python<'_>) -> PyResult<PyObject> {
        object(py, self.reader.metadata())
    }
    fn manifest(&self, py: Python<'_>) -> PyResult<PyObject> {
        object(py, self.reader.manifest())
    }
    fn __iter__(&self, py: Python<'_>) -> PyResult<PySpectra> {
        Ok(PySpectra {
            iter: py
                .allow_threads(|| self.reader.iter_spectra_arrays_streaming())
                .map_err(error)?,
        })
    }
}
#[pyclass(name = "_Spectra")]
struct PySpectra {
    iter: StreamingSpectrumArraysViewIterator,
}
#[pymethods]
impl PySpectra {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PySpectrum>> {
        let next = py.allow_threads(|| self.iter.next());
        let Some(spectrum) = next else {
            return Ok(None);
        };
        let spectrum = spectrum.map_err(error)?.to_owned().map_err(error)?;
        let len = spectrum.peaks.len();
        Ok(Some(PySpectrum {
            metadata: object(py, &spectrum.metadata)?,
            mz: spectrum.peaks.mz.into_pyarray_bound(py).unbind(),
            intensity: spectrum.peaks.intensity.into_pyarray_bound(py).unbind(),
            ion_mobility: spectrum
                .peaks
                .ion_mobility
                .map(|v| v.into_pyarray_bound(py).unbind()),
            len,
        }))
    }
}
#[pyclass(name = "Writer")]
struct PyWriter {
    writer: Option<MzPeakDatasetWriterV2>,
}
#[pymethods]
impl PyWriter {
    #[new]
    #[pyo3(signature=(path,*,ion_mobility=false,imaging=false,compression_level=3))]
    fn new(
        py: Python<'_>,
        path: String,
        ion_mobility: bool,
        imaging: bool,
        compression_level: i32,
    ) -> PyResult<Self> {
        let config = DatasetWriterV2Config {
            spectra_config: SpectraWriterConfig {
                compression: CompressionType::Zstd(compression_level),
                ..Default::default()
            },
            peaks_config: PeaksWriterV2Config {
                compression: CompressionType::Zstd(compression_level),
                ..Default::default()
            },
        };
        Ok(Self {
            writer: Some(
                py.allow_threads(|| {
                    MzPeakDatasetWriterV2::with_config(
                        path,
                        Modality::from_flags(ion_mobility, imaging),
                        None,
                        config,
                    )
                })
                .map_err(error)?,
            ),
        })
    }
    #[pyo3(signature=(metadata,mz,intensity,ion_mobility=None))]
    fn write(
        &mut self,
        py: Python<'_>,
        metadata: &Bound<'_, PyDict>,
        mz: PyReadonlyArray1<'_, f64>,
        intensity: PyReadonlyArray1<'_, f32>,
        ion_mobility: Option<PyReadonlyArray1<'_, f64>>,
    ) -> PyResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| error("writer is closed"))?;
        let text = py
            .import_bound("json")?
            .call_method1("dumps", (metadata,))?
            .extract::<String>()?;
        let mut decoded: SpectrumMetadata = serde_json::from_str(&text).map_err(error)?;
        let peaks = PeakArraysV2 {
            mz: mz.as_slice().map_err(error)?.to_vec(),
            intensity: intensity.as_slice().map_err(error)?.to_vec(),
            ion_mobility: ion_mobility
                .map(|v| v.as_slice().map(|s| s.to_vec()))
                .transpose()
                .map_err(error)?,
        };
        if metadata.get_item("spectrum_id")?.is_none() {
            decoded.spectrum_id = writer.stats().0.try_into().map_err(error)?;
        }
        let count: u32 = peaks.len().try_into().map_err(error)?;
        if metadata.get_item("peak_count")?.is_some() && decoded.peak_count != count {
            return Err(error("peak_count does not match arrays"));
        }
        decoded.peak_count = count;
        py.allow_threads(|| writer.write_spectrum_v2(&decoded, &peaks))
            .map_err(error)
    }
    fn set_metadata(&mut self, py: Python<'_>, metadata: &Bound<'_, PyDict>) -> PyResult<()> {
        let text = py
            .import_bound("json")?
            .call_method1("dumps", (metadata,))?
            .extract::<String>()?;
        let decoded = serde_json::from_str(&text).map_err(error)?;
        self.writer
            .as_mut()
            .ok_or_else(|| error("writer is closed"))?
            .set_metadata(decoded);
        Ok(())
    }
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| error("writer is closed"))?;
        py.allow_threads(|| writer.close()).map_err(error)?;
        Ok(())
    }
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __exit__(
        &mut self,
        py: Python<'_>,
        exception_type: Option<&Bound<'_, PyAny>>,
        _exception: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        if exception_type.is_some() {
            self.writer.take();
        } else if self.writer.is_some() {
            self.close(py)?;
        }
        Ok(())
    }
}
#[pymodule]
fn mzpeak(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(convert, m)?)?;
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_class::<PyReader>()?;
    m.add_class::<PyWriter>()?;
    m.add_class::<PySpectrum>()?;
    m.add_class::<PySpectra>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FORMAT_VERSION", "2.0")?;
    Ok(())
}
