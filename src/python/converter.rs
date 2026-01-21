//! Python bindings for mzML to mzPeak conversion
//!
//! Provides high-level conversion API with progress reporting and GIL release.
//! Supports mzML, Bruker TDF, and Thermo RAW formats.

use pyo3::prelude::*;

use crate::mzml::converter::{ConversionConfig, MzMLConverter};
use crate::python::exceptions::IntoPyResult;
use crate::python::types::{PyConversionConfig, PyConversionStats, PyModality};

#[cfg(feature = "tdf")]
use crate::tdf::{TdfConversionConfig, TdfConversionStats, TdfConverter};

#[cfg(feature = "thermo")]
use crate::thermo::{ThermoConverter, ThermoStreamer};
#[cfg(feature = "thermo")]
use crate::dataset::{DatasetWriterV2Config, MzPeakDatasetWriterV2};
#[cfg(feature = "thermo")]
use crate::ingest::IngestSpectrumConverter;
#[cfg(feature = "thermo")]
use crate::metadata::MzPeakMetadata;
#[cfg(feature = "thermo")]
use crate::schema::manifest::Modality;
#[cfg(feature = "thermo")]
use crate::writer::{
    CompressionType, MzPeakWriter, PeaksWriterV2Config, SpectraWriterConfig, SpectrumV2,
    WriterConfig,
};

/// Converter for mzML files to mzPeak format
///
/// Provides methods for converting mzML files with various options
/// including sharding for large files.
///
/// Example:
///     >>> converter = mzpeak.MzMLConverter()
///     >>> stats = converter.convert("input.mzML", "output.mzpeak")
///     >>> print(f"Converted {stats.spectra_count} spectra")
#[pyclass(name = "MzMLConverter")]
pub struct PyMzMLConverter {
    config: ConversionConfig,
}

#[pymethods]
impl PyMzMLConverter {
    /// Create a new converter with optional configuration
    ///
    /// Args:
    ///     config: Optional ConversionConfig for batch size, precision settings, etc.
    #[new]
    #[pyo3(signature = (config=None))]
    fn new(config: Option<PyConversionConfig>) -> Self {
        Self {
            config: config.map(|c| c.inner).unwrap_or_default(),
        }
    }

    /// Set the batch size for processing
    ///
    /// Args:
    ///     batch_size: Number of spectra to process per batch
    ///
    /// Returns:
    ///     Self for method chaining
    fn with_batch_size(mut slf: PyRefMut<'_, Self>, batch_size: usize) -> PyRefMut<'_, Self> {
        slf.config.batch_size = batch_size;
        slf
    }

    /// Convert an mzML file to mzPeak format
    ///
    /// Args:
    ///     input_path: Path to input mzML file
    ///     output_path: Path for output mzPeak file/directory
    ///
    /// Returns:
    ///     ConversionStats with details about the conversion
    fn convert(
        &self,
        py: Python<'_>,
        input_path: String,
        output_path: String,
    ) -> PyResult<PyConversionStats> {
        let converter = MzMLConverter::with_config(self.config.clone());

        // Release GIL during the potentially long conversion
        let stats = py.allow_threads(|| converter.convert(&input_path, &output_path).into_py_result())?;

        Ok(PyConversionStats::from(stats))
    }

    /// Convert an mzML file with automatic file sharding
    ///
    /// Creates multiple output files when the data exceeds the specified
    /// maximum peaks per file, useful for very large datasets.
    ///
    /// Args:
    ///     input_path: Path to input mzML file
    ///     output_path: Base path for output files (will add _001, _002, etc.)
    ///     max_peaks_per_file: Maximum peaks per output file (default: 50 million)
    ///
    /// Returns:
    ///     ConversionStats with details about the conversion
    #[pyo3(signature = (input_path, output_path, max_peaks_per_file=50_000_000))]
    fn convert_with_sharding(
        &self,
        py: Python<'_>,
        input_path: String,
        output_path: String,
        max_peaks_per_file: usize,
    ) -> PyResult<PyConversionStats> {
        // Clone config and set max_peaks_per_file
        let mut config = self.config.clone();
        config.writer_config.max_peaks_per_file = Some(max_peaks_per_file);
        let converter = MzMLConverter::with_config(config);

        // Release GIL during the potentially long conversion
        let stats = py.allow_threads(|| {
            converter
                .convert_with_sharding(&input_path, &output_path)
                .into_py_result()
        })?;

        Ok(PyConversionStats::from(stats))
    }

    fn __repr__(&self) -> String {
        format!(
            "MzMLConverter(batch_size={}, preserve_precision={})",
            self.config.batch_size, self.config.preserve_precision
        )
    }
}

// ============================================================================
// TDF Converter (Bruker TimsTOF)
// ============================================================================

/// Statistics from TDF conversion
#[cfg(feature = "tdf")]
#[pyclass(name = "TdfConversionStats")]
#[derive(Clone)]
pub struct PyTdfConversionStats {
    spectra_read: usize,
    peaks_total: usize,
    ms1_count: usize,
    ms2_count: usize,
    imaging_frames: usize,
}

#[cfg(feature = "tdf")]
#[pymethods]
impl PyTdfConversionStats {
    /// Number of spectra converted
    #[getter]
    fn spectra_read(&self) -> usize {
        self.spectra_read
    }

    /// Total peak count processed
    #[getter]
    fn peaks_total(&self) -> usize {
        self.peaks_total
    }

    /// Count of MS1 spectra
    #[getter]
    fn ms1_count(&self) -> usize {
        self.ms1_count
    }

    /// Count of MS2 spectra
    #[getter]
    fn ms2_count(&self) -> usize {
        self.ms2_count
    }

    /// Number of frames with MALDI imaging metadata
    #[getter]
    fn imaging_frames(&self) -> usize {
        self.imaging_frames
    }

    fn __repr__(&self) -> String {
        format!(
            "TdfConversionStats(spectra={}, peaks={}, ms1={}, ms2={}, imaging={})",
            self.spectra_read, self.peaks_total, self.ms1_count, self.ms2_count, self.imaging_frames
        )
    }
}

#[cfg(feature = "tdf")]
impl From<TdfConversionStats> for PyTdfConversionStats {
    fn from(stats: TdfConversionStats) -> Self {
        Self {
            spectra_read: stats.spectra_read,
            peaks_total: stats.peaks_total,
            ms1_count: stats.ms1_count,
            ms2_count: stats.ms2_count,
            imaging_frames: stats.imaging_frames,
        }
    }
}

/// Converter for Bruker TDF (TimsTOF) datasets to mzPeak format
///
/// Converts Bruker .d directories containing TDF data to mzPeak v2 containers.
/// Supports LC-TIMS-MS, PASEF, diaPASEF, and MALDI-TIMS-MSI data.
///
/// Note: Requires the 'tdf' feature to be enabled at compile time.
///
/// Example:
///     >>> converter = mzpeak.TdfConverter()
///     >>> stats = converter.convert("sample.d", "output.mzpeak")
///     >>> print(f"Converted {stats.spectra_read} spectra")
#[cfg(feature = "tdf")]
#[pyclass(name = "TdfConverter")]
pub struct PyTdfConverter {
    include_extended_metadata: bool,
    batch_size: usize,
    compression_level: i32,
    row_group_size: usize,
}

#[cfg(feature = "tdf")]
#[pymethods]
impl PyTdfConverter {
    /// Create a new TDF converter
    ///
    /// Args:
    ///     include_extended_metadata: Include TIC/base peak prepopulation (default True)
    ///     batch_size: Batch size for streaming + parallel decode (default 256)
    ///     compression_level: ZSTD compression level 1-22 (default 9)
    ///     row_group_size: Rows per Parquet row group (default 100000)
    #[new]
    #[pyo3(signature = (include_extended_metadata=true, batch_size=256, compression_level=9, row_group_size=100000))]
    fn new(
        include_extended_metadata: bool,
        batch_size: usize,
        compression_level: i32,
        row_group_size: usize,
    ) -> Self {
        Self {
            include_extended_metadata,
            batch_size,
            compression_level,
            row_group_size,
        }
    }

    /// Convert a Bruker TDF dataset to mzPeak v2 container
    ///
    /// Args:
    ///     input_path: Path to Bruker .d directory
    ///     output_path: Path for output .mzpeak container
    ///
    /// Returns:
    ///     TdfConversionStats with details about the conversion
    fn convert(
        &self,
        py: Python<'_>,
        input_path: String,
        output_path: String,
    ) -> PyResult<PyTdfConversionStats> {
        let tdf_config = TdfConversionConfig {
            include_extended_metadata: self.include_extended_metadata,
            batch_size: self.batch_size,
        };
        let writer_config = WriterConfig {
            compression: CompressionType::Zstd(self.compression_level),
            row_group_size: self.row_group_size,
            ..Default::default()
        };

        let converter = TdfConverter::with_config(tdf_config);

        // Release GIL during conversion
        let stats = py.allow_threads(|| {
            converter
                .convert_to_v2_container(&input_path, &output_path, writer_config)
                .into_py_result()
        })?;

        Ok(PyTdfConversionStats::from(stats))
    }

    fn __repr__(&self) -> String {
        format!(
            "TdfConverter(batch_size={}, compression_level={})",
            self.batch_size, self.compression_level
        )
    }
}

// ============================================================================
// Thermo Converter
// ============================================================================

/// Statistics from Thermo RAW conversion
#[cfg(feature = "thermo")]
#[pyclass(name = "ThermoConversionStats")]
#[derive(Clone, Default)]
pub struct PyThermoConversionStats {
    spectra_count: usize,
    peak_count: usize,
    ms1_spectra: usize,
    ms2_spectra: usize,
    msn_spectra: usize,
    source_file_size: u64,
    output_file_size: u64,
    compression_ratio: f64,
}

#[cfg(feature = "thermo")]
#[pymethods]
impl PyThermoConversionStats {
    /// Total number of spectra converted
    #[getter]
    fn spectra_count(&self) -> usize {
        self.spectra_count
    }

    /// Total number of peaks converted
    #[getter]
    fn peak_count(&self) -> usize {
        self.peak_count
    }

    /// Number of MS1 spectra
    #[getter]
    fn ms1_spectra(&self) -> usize {
        self.ms1_spectra
    }

    /// Number of MS2 spectra
    #[getter]
    fn ms2_spectra(&self) -> usize {
        self.ms2_spectra
    }

    /// Number of MSn spectra (n > 2)
    #[getter]
    fn msn_spectra(&self) -> usize {
        self.msn_spectra
    }

    /// Source file size in bytes
    #[getter]
    fn source_file_size(&self) -> u64 {
        self.source_file_size
    }

    /// Output file size in bytes
    #[getter]
    fn output_file_size(&self) -> u64 {
        self.output_file_size
    }

    /// Compression ratio achieved
    #[getter]
    fn compression_ratio(&self) -> f64 {
        self.compression_ratio
    }

    fn __repr__(&self) -> String {
        format!(
            "ThermoConversionStats(spectra={}, peaks={}, compression_ratio={:.2}x)",
            self.spectra_count, self.peak_count, self.compression_ratio
        )
    }
}

/// Converter for Thermo RAW files to mzPeak format
///
/// Converts Thermo Fisher RAW files to mzPeak format. Supports both legacy
/// v1 Parquet output and v2 container format.
///
/// Note: Requires the 'thermo' feature and .NET 8 runtime.
///       Only supported on Windows x86_64, Linux x86_64, and macOS x86_64.
///
/// Example:
///     >>> converter = mzpeak.ThermoConverter()
///     >>> stats = converter.convert("sample.raw", "output.mzpeak")
///     >>> print(f"Converted {stats.spectra_count} spectra")
#[cfg(feature = "thermo")]
#[pyclass(name = "ThermoConverter")]
pub struct PyThermoConverter {
    batch_size: usize,
    compression_level: i32,
    row_group_size: usize,
    legacy_format: bool,
    centroid_spectra: bool,
}

#[cfg(feature = "thermo")]
#[pymethods]
impl PyThermoConverter {
    /// Create a new Thermo RAW converter
    ///
    /// Args:
    ///     batch_size: Number of spectra to process per batch (default 1000)
    ///     compression_level: ZSTD compression level 1-22 (default 9)
    ///     row_group_size: Rows per Parquet row group (default 100000)
    ///     legacy_format: Output legacy v1 Parquet instead of v2 container (default False)
    ///     centroid_spectra: Centroid profile spectra during conversion (default True)
    #[new]
    #[pyo3(signature = (batch_size=1000, compression_level=9, row_group_size=100000, legacy_format=false, centroid_spectra=true))]
    fn new(
        batch_size: usize,
        compression_level: i32,
        row_group_size: usize,
        legacy_format: bool,
        centroid_spectra: bool,
    ) -> Self {
        Self {
            batch_size,
            compression_level,
            row_group_size,
            legacy_format,
            centroid_spectra,
        }
    }

    /// Convert a Thermo RAW file to mzPeak format
    ///
    /// Args:
    ///     input_path: Path to Thermo RAW file
    ///     output_path: Path for output file (.mzpeak or .mzpeak.parquet)
    ///
    /// Returns:
    ///     ThermoConversionStats with details about the conversion
    fn convert(
        &self,
        py: Python<'_>,
        input_path: String,
        output_path: String,
    ) -> PyResult<PyThermoConversionStats> {
        let batch_size = self.batch_size;
        let compression_level = self.compression_level;
        let row_group_size = self.row_group_size;
        let legacy_format = self.legacy_format;

        // Release GIL during conversion
        py.allow_threads(|| {
            self.convert_inner(&input_path, &output_path, batch_size, compression_level, row_group_size, legacy_format)
                .into_py_result()
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "ThermoConverter(batch_size={}, compression_level={}, legacy={})",
            self.batch_size, self.compression_level, self.legacy_format
        )
    }
}

#[cfg(feature = "thermo")]
impl PyThermoConverter {
    fn convert_inner(
        &self,
        input_path: &str,
        output_path: &str,
        batch_size: usize,
        compression_level: i32,
        row_group_size: usize,
        legacy_format: bool,
    ) -> Result<PyThermoConversionStats, crate::thermo::ThermoError> {
        use std::path::Path;

        let input = Path::new(input_path);
        let output = Path::new(output_path);

        let writer_config = WriterConfig {
            compression: CompressionType::Zstd(compression_level),
            row_group_size,
            ..Default::default()
        };

        let mut streamer = ThermoStreamer::new(input, batch_size)?;
        let total_spectra = streamer.len();

        let metadata = MzPeakMetadata::new();
        let mut stats = PyThermoConversionStats {
            source_file_size: std::fs::metadata(input).map(|m| m.len()).unwrap_or(0),
            ..Default::default()
        };

        if legacy_format {
            let mut writer = MzPeakWriter::new_file(output, &metadata, writer_config)
                .map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))?;

            let mut ingest_converter = IngestSpectrumConverter::new();
            let converter = ThermoConverter::new();
            let mut spectrum_id: i64 = 0;

            while let Some(raw_batch) = streamer.next_batch()? {
                let mut batch = Vec::with_capacity(raw_batch.len());
                for raw_spectrum in raw_batch {
                    let ingest = converter.convert_spectrum(raw_spectrum, spectrum_id)?;
                    spectrum_id += 1;

                    let spectrum = ingest_converter.convert(ingest)
                        .map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e.to_string(),
                        )))?;

                    stats.spectra_count += 1;
                    stats.peak_count += spectrum.peak_count();
                    match spectrum.ms_level {
                        1 => stats.ms1_spectra += 1,
                        2 => stats.ms2_spectra += 1,
                        _ => stats.msn_spectra += 1,
                    }

                    batch.push(spectrum);
                }

                writer.write_spectra_owned(batch)
                    .map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    )))?;
            }

            writer.finish()
                .map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))?;
        } else {
            // V2 container format
            let vendor_hints = metadata.vendor_hints.clone();
            let dataset_config = DatasetWriterV2Config {
                spectra_config: SpectraWriterConfig {
                    compression: writer_config.compression,
                    ..Default::default()
                },
                peaks_config: PeaksWriterV2Config {
                    compression: writer_config.compression,
                    row_group_size: writer_config.row_group_size,
                    ..Default::default()
                },
            };

            let mut writer = MzPeakDatasetWriterV2::with_config(
                output,
                Modality::LcMs,
                vendor_hints,
                dataset_config,
            ).map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            )))?;
            writer.set_metadata(metadata);

            let mut ingest_converter = IngestSpectrumConverter::new();
            let converter = ThermoConverter::new();
            let mut spectrum_id: i64 = 0;

            while let Some(raw_batch) = streamer.next_batch()? {
                for raw_spectrum in raw_batch {
                    let ingest = converter.convert_spectrum(raw_spectrum, spectrum_id)?;
                    spectrum_id += 1;

                    let spectrum = ingest_converter.convert(ingest)
                        .map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e.to_string(),
                        )))?;

                    stats.spectra_count += 1;
                    stats.peak_count += spectrum.peak_count();
                    match spectrum.ms_level {
                        1 => stats.ms1_spectra += 1,
                        2 => stats.ms2_spectra += 1,
                        _ => stats.msn_spectra += 1,
                    }

                    let spectrum_v2 = SpectrumV2::try_from_spectrum_arrays(spectrum)
                        .map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            e.to_string(),
                        )))?;

                    writer.write_spectrum(&spectrum_v2)
                        .map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        )))?;
                }
            }

            writer.close()
                .map_err(|e| crate::thermo::ThermoError::IoError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))?;
        }

        stats.output_file_size = std::fs::metadata(output).map(|m| m.len()).unwrap_or(0);
        if stats.output_file_size > 0 {
            stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
        }

        Ok(stats)
    }
}

// ============================================================================
// Module-level convenience functions
// ============================================================================

/// Convert an mzML file to mzPeak format (convenience function)
///
/// This is a module-level function for simple one-shot conversions.
///
/// Args:
///     input_path: Path to input mzML file
///     output_path: Path for output mzPeak file/directory
///     config: Optional ConversionConfig
///
/// Returns:
///     ConversionStats with details about the conversion
///
/// Example:
///     >>> import mzpeak
///     >>> stats = mzpeak.convert("input.mzML", "output.mzpeak")
///     >>> print(f"Converted {stats.spectra_count} spectra")
#[pyfunction]
#[pyo3(signature = (input_path, output_path, config=None))]
pub fn convert(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    config: Option<PyConversionConfig>,
) -> PyResult<PyConversionStats> {
    let conversion_config = config.map(|c| c.inner).unwrap_or_default();
    let converter = MzMLConverter::with_config(conversion_config);

    // Release GIL during the potentially long conversion
    let stats = py.allow_threads(|| converter.convert(&input_path, &output_path).into_py_result())?;

    Ok(PyConversionStats::from(stats))
}

/// Convert an mzML file with automatic file sharding (convenience function)
///
/// Creates multiple output files when the data exceeds the specified
/// maximum peaks per file.
///
/// Args:
///     input_path: Path to input mzML file
///     output_path: Base path for output files
///     max_peaks_per_file: Maximum peaks per output file (default: 50 million)
///     config: Optional ConversionConfig
///
/// Returns:
///     ConversionStats with details about the conversion
///
/// Example:
///     >>> import mzpeak
///     >>> stats = mzpeak.convert_with_sharding("large.mzML", "output", max_peaks_per_file=10_000_000)
#[pyfunction]
#[pyo3(signature = (input_path, output_path, max_peaks_per_file=50_000_000, config=None))]
pub fn convert_with_sharding(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    max_peaks_per_file: usize,
    config: Option<PyConversionConfig>,
) -> PyResult<PyConversionStats> {
    let mut conversion_config = config.map(|c| c.inner).unwrap_or_default();
    conversion_config.writer_config.max_peaks_per_file = Some(max_peaks_per_file);
    let converter = MzMLConverter::with_config(conversion_config);

    // Release GIL during the potentially long conversion
    let stats = py.allow_threads(|| {
        converter
            .convert_with_sharding(&input_path, &output_path)
            .into_py_result()
    })?;

    Ok(PyConversionStats::from(stats))
}

/// Convert a Bruker TDF dataset to mzPeak format (convenience function)
///
/// Args:
///     input_path: Path to Bruker .d directory
///     output_path: Path for output .mzpeak container
///     batch_size: Batch size for streaming (default 256)
///     compression_level: ZSTD compression level 1-22 (default 9)
///
/// Returns:
///     TdfConversionStats with details about the conversion
///
/// Example:
///     >>> import mzpeak
///     >>> stats = mzpeak.convert_tdf("sample.d", "output.mzpeak")
///     >>> print(f"Converted {stats.spectra_read} spectra")
#[cfg(feature = "tdf")]
#[pyfunction]
#[pyo3(signature = (input_path, output_path, batch_size=256, compression_level=9))]
pub fn convert_tdf(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    batch_size: usize,
    compression_level: i32,
) -> PyResult<PyTdfConversionStats> {
    let tdf_config = TdfConversionConfig {
        include_extended_metadata: true,
        batch_size,
    };
    let writer_config = WriterConfig {
        compression: CompressionType::Zstd(compression_level),
        ..Default::default()
    };

    let converter = TdfConverter::with_config(tdf_config);

    let stats = py.allow_threads(|| {
        converter
            .convert_to_v2_container(&input_path, &output_path, writer_config)
            .into_py_result()
    })?;

    Ok(PyTdfConversionStats::from(stats))
}

/// Convert a Thermo RAW file to mzPeak format (convenience function)
///
/// Args:
///     input_path: Path to Thermo RAW file
///     output_path: Path for output .mzpeak container
///     batch_size: Number of spectra per batch (default 1000)
///     compression_level: ZSTD compression level 1-22 (default 9)
///
/// Returns:
///     ThermoConversionStats with details about the conversion
///
/// Example:
///     >>> import mzpeak
///     >>> stats = mzpeak.convert_thermo("sample.raw", "output.mzpeak")
///     >>> print(f"Converted {stats.spectra_count} spectra")
#[cfg(feature = "thermo")]
#[pyfunction]
#[pyo3(signature = (input_path, output_path, batch_size=1000, compression_level=9))]
pub fn convert_thermo(
    py: Python<'_>,
    input_path: String,
    output_path: String,
    batch_size: usize,
    compression_level: i32,
) -> PyResult<PyThermoConversionStats> {
    let converter = PyThermoConverter::new(batch_size, compression_level, 100000, false, true);
    converter.convert(py, input_path, output_path)
}
