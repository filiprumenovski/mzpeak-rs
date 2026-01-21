//! Python bindings for mzPeak metadata types
//!
//! This module exposes the structured metadata types used in mzPeak files,
//! including instrument configuration, LC settings, run parameters, SDRF
//! metadata, source file info, and processing history.

use pyo3::prelude::*;
use std::collections::HashMap;

use crate::metadata::{
    ColumnInfo, GradientProgram, GradientStep, ImagingMetadata, InstrumentConfig, LcConfig,
    MassAnalyzerConfig, MobilePhase, MzPeakMetadata, ProcessingHistory, ProcessingStep,
    RunParameters, SdrfMetadata, SourceFileInfo, VendorHints,
};

// ============================================================================
// VendorHints
// ============================================================================

/// Vendor hints for files converted via intermediate formats (e.g., mzML).
#[pyclass(name = "VendorHints")]
#[derive(Clone)]
pub struct PyVendorHints {
    inner: VendorHints,
}

#[pymethods]
impl PyVendorHints {
    /// Create a new VendorHints with optional vendor name.
    #[new]
    #[pyo3(signature = (original_vendor=None))]
    fn new(original_vendor: Option<String>) -> Self {
        let mut inner = VendorHints::default();
        inner.original_vendor = original_vendor;
        Self { inner }
    }

    /// Original vendor name (e.g., "Waters", "Sciex", "Agilent")
    #[getter]
    fn original_vendor(&self) -> Option<String> {
        self.inner.original_vendor.clone()
    }

    /// Original file format (e.g., "waters_raw", "wiff", "agilent_d")
    #[getter]
    fn original_format(&self) -> Option<String> {
        self.inner.original_format.clone()
    }

    /// Instrument model from original file
    #[getter]
    fn instrument_model(&self) -> Option<String> {
        self.inner.instrument_model.clone()
    }

    /// Conversion path taken (e.g., ["waters_raw", "mzML", "mzpeak"])
    #[getter]
    fn conversion_path(&self) -> Vec<String> {
        self.inner.conversion_path.clone()
    }

    /// Check if any vendor hints are present
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn __repr__(&self) -> String {
        format!(
            "VendorHints(vendor={:?}, format={:?})",
            self.inner.original_vendor, self.inner.original_format
        )
    }
}

impl From<VendorHints> for PyVendorHints {
    fn from(hints: VendorHints) -> Self {
        Self { inner: hints }
    }
}

// ============================================================================
// ImagingMetadata
// ============================================================================

/// MALDI/imaging grid metadata for spatial indexing.
#[pyclass(name = "ImagingMetadata")]
#[derive(Clone)]
pub struct PyImagingMetadata {
    inner: ImagingMetadata,
}

#[pymethods]
impl PyImagingMetadata {
    /// Create new imaging metadata.
    #[new]
    #[pyo3(signature = (grid_width=None, grid_height=None, pixel_size_x_um=None, pixel_size_y_um=None))]
    fn new(
        grid_width: Option<u32>,
        grid_height: Option<u32>,
        pixel_size_x_um: Option<f64>,
        pixel_size_y_um: Option<f64>,
    ) -> Self {
        Self {
            inner: ImagingMetadata {
                grid_width,
                grid_height,
                pixel_size_x_um,
                pixel_size_y_um,
            },
        }
    }

    /// Width of the pixel grid (X dimension)
    #[getter]
    fn grid_width(&self) -> Option<u32> {
        self.inner.grid_width
    }

    /// Height of the pixel grid (Y dimension)
    #[getter]
    fn grid_height(&self) -> Option<u32> {
        self.inner.grid_height
    }

    /// Pixel size along X in micrometers
    #[getter]
    fn pixel_size_x_um(&self) -> Option<f64> {
        self.inner.pixel_size_x_um
    }

    /// Pixel size along Y in micrometers
    #[getter]
    fn pixel_size_y_um(&self) -> Option<f64> {
        self.inner.pixel_size_y_um
    }

    fn __repr__(&self) -> String {
        format!(
            "ImagingMetadata(grid={}x{}, pixel_size={:?}x{:?} µm)",
            self.inner.grid_width.unwrap_or(0),
            self.inner.grid_height.unwrap_or(0),
            self.inner.pixel_size_x_um,
            self.inner.pixel_size_y_um
        )
    }
}

impl From<ImagingMetadata> for PyImagingMetadata {
    fn from(imaging: ImagingMetadata) -> Self {
        Self { inner: imaging }
    }
}

// ============================================================================
// SourceFileInfo
// ============================================================================

/// Source file information for provenance tracking.
#[pyclass(name = "SourceFileInfo")]
#[derive(Clone)]
pub struct PySourceFileInfo {
    inner: SourceFileInfo,
}

#[pymethods]
impl PySourceFileInfo {
    /// Create new source file info with the given filename.
    #[new]
    fn new(name: String) -> Self {
        Self {
            inner: SourceFileInfo::new(&name),
        }
    }

    /// Original file name
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Original file path
    #[getter]
    fn path(&self) -> Option<String> {
        self.inner.path.clone()
    }

    /// File format (e.g., "Thermo RAW", "Bruker .d")
    #[getter]
    fn format(&self) -> Option<String> {
        self.inner.format.clone()
    }

    /// File size in bytes
    #[getter]
    fn size_bytes(&self) -> Option<u64> {
        self.inner.size_bytes
    }

    /// SHA-256 checksum of the original file
    #[getter]
    fn sha256(&self) -> Option<String> {
        self.inner.sha256.clone()
    }

    /// MD5 checksum (for legacy compatibility)
    #[getter]
    fn md5(&self) -> Option<String> {
        self.inner.md5.clone()
    }

    /// Vendor file version/format version
    #[getter]
    fn format_version(&self) -> Option<String> {
        self.inner.format_version.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "SourceFileInfo(name='{}', format={:?})",
            self.inner.name, self.inner.format
        )
    }
}

impl From<SourceFileInfo> for PySourceFileInfo {
    fn from(info: SourceFileInfo) -> Self {
        Self { inner: info }
    }
}

// ============================================================================
// ProcessingStep and ProcessingHistory
// ============================================================================

/// A single data processing step in the processing history.
#[pyclass(name = "ProcessingStep")]
#[derive(Clone)]
pub struct PyProcessingStep {
    inner: ProcessingStep,
}

#[pymethods]
impl PyProcessingStep {
    /// Create a new processing step.
    #[new]
    fn new(order: i32, software: String, processing_type: String) -> Self {
        Self {
            inner: ProcessingStep {
                order,
                software,
                version: None,
                processing_type,
                timestamp: None,
                parameters: HashMap::new(),
                cv_params: Default::default(),
            },
        }
    }

    /// Step order (1-indexed)
    #[getter]
    fn order(&self) -> i32 {
        self.inner.order
    }

    /// Software name
    #[getter]
    fn software(&self) -> String {
        self.inner.software.clone()
    }

    /// Software version
    #[getter]
    fn version(&self) -> Option<String> {
        self.inner.version.clone()
    }

    /// Processing type (e.g., "conversion", "peak picking", "centroiding")
    #[getter]
    fn processing_type(&self) -> String {
        self.inner.processing_type.clone()
    }

    /// Timestamp when processing was performed
    #[getter]
    fn timestamp(&self) -> Option<String> {
        self.inner.timestamp.clone()
    }

    /// Processing parameters
    #[getter]
    fn parameters(&self) -> HashMap<String, String> {
        self.inner.parameters.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "ProcessingStep(order={}, software='{}', type='{}')",
            self.inner.order, self.inner.software, self.inner.processing_type
        )
    }
}

impl From<ProcessingStep> for PyProcessingStep {
    fn from(step: ProcessingStep) -> Self {
        Self { inner: step }
    }
}

/// Data processing history for audit trail.
#[pyclass(name = "ProcessingHistory")]
#[derive(Clone)]
pub struct PyProcessingHistory {
    inner: ProcessingHistory,
}

#[pymethods]
impl PyProcessingHistory {
    /// Create a new empty processing history.
    #[new]
    fn new() -> Self {
        Self {
            inner: ProcessingHistory::new(),
        }
    }

    /// List of processing steps applied
    #[getter]
    fn steps(&self) -> Vec<PyProcessingStep> {
        self.inner.steps.iter().cloned().map(PyProcessingStep::from).collect()
    }

    /// Number of processing steps
    fn __len__(&self) -> usize {
        self.inner.steps.len()
    }

    fn __repr__(&self) -> String {
        format!("ProcessingHistory(steps={})", self.inner.steps.len())
    }
}

impl From<ProcessingHistory> for PyProcessingHistory {
    fn from(history: ProcessingHistory) -> Self {
        Self { inner: history }
    }
}

// ============================================================================
// MassAnalyzerConfig
// ============================================================================

/// Mass analyzer configuration.
#[pyclass(name = "MassAnalyzerConfig")]
#[derive(Clone)]
pub struct PyMassAnalyzerConfig {
    inner: MassAnalyzerConfig,
}

#[pymethods]
impl PyMassAnalyzerConfig {
    /// Create a new mass analyzer config.
    #[new]
    fn new(analyzer_type: String, order: i32) -> Self {
        Self {
            inner: MassAnalyzerConfig {
                analyzer_type,
                order,
                resolution: None,
                resolution_mz: None,
                cv_params: Default::default(),
            },
        }
    }

    /// Analyzer type (e.g., "orbitrap", "quadrupole", "ion trap")
    #[getter]
    fn analyzer_type(&self) -> String {
        self.inner.analyzer_type.clone()
    }

    /// Analyzer order (1 = first analyzer, 2 = second, etc.)
    #[getter]
    fn order(&self) -> i32 {
        self.inner.order
    }

    /// Resolution at a given m/z (if applicable)
    #[getter]
    fn resolution(&self) -> Option<f64> {
        self.inner.resolution
    }

    /// Reference m/z for resolution
    #[getter]
    fn resolution_mz(&self) -> Option<f64> {
        self.inner.resolution_mz
    }

    fn __repr__(&self) -> String {
        format!(
            "MassAnalyzerConfig(type='{}', order={})",
            self.inner.analyzer_type, self.inner.order
        )
    }
}

impl From<MassAnalyzerConfig> for PyMassAnalyzerConfig {
    fn from(config: MassAnalyzerConfig) -> Self {
        Self { inner: config }
    }
}

// ============================================================================
// InstrumentConfig
// ============================================================================

/// Instrument configuration metadata.
#[pyclass(name = "InstrumentConfig")]
#[derive(Clone)]
pub struct PyInstrumentConfig {
    inner: InstrumentConfig,
}

#[pymethods]
impl PyInstrumentConfig {
    /// Create a new empty instrument configuration.
    #[new]
    fn new() -> Self {
        Self {
            inner: InstrumentConfig::new(),
        }
    }

    /// Instrument model name (CV: MS:1000031)
    #[getter]
    fn model(&self) -> Option<String> {
        self.inner.model.clone()
    }

    /// Instrument serial number (CV: MS:1000529)
    #[getter]
    fn serial_number(&self) -> Option<String> {
        self.inner.serial_number.clone()
    }

    /// Vendor name
    #[getter]
    fn vendor(&self) -> Option<String> {
        self.inner.vendor.clone()
    }

    /// Software version
    #[getter]
    fn software_version(&self) -> Option<String> {
        self.inner.software_version.clone()
    }

    /// Ion source type (e.g., ESI, MALDI)
    #[getter]
    fn ion_source(&self) -> Option<String> {
        self.inner.ion_source.clone()
    }

    /// Mass analyzer configurations
    #[getter]
    fn mass_analyzers(&self) -> Vec<PyMassAnalyzerConfig> {
        self.inner
            .mass_analyzers
            .iter()
            .cloned()
            .map(PyMassAnalyzerConfig::from)
            .collect()
    }

    /// Detector configuration
    #[getter]
    fn detector(&self) -> Option<String> {
        self.inner.detector.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "InstrumentConfig(model={:?}, vendor={:?})",
            self.inner.model, self.inner.vendor
        )
    }
}

impl From<InstrumentConfig> for PyInstrumentConfig {
    fn from(config: InstrumentConfig) -> Self {
        Self { inner: config }
    }
}

// ============================================================================
// GradientStep and GradientProgram
// ============================================================================

/// A single step in an LC gradient program.
#[pyclass(name = "GradientStep")]
#[derive(Clone)]
pub struct PyGradientStep {
    inner: GradientStep,
}

#[pymethods]
impl PyGradientStep {
    /// Create a new gradient step.
    #[new]
    #[pyo3(signature = (time_min, percent_b, flow_rate_ul_min=None))]
    fn new(time_min: f64, percent_b: f64, flow_rate_ul_min: Option<f64>) -> Self {
        Self {
            inner: GradientStep {
                time_min,
                percent_b,
                flow_rate_ul_min,
            },
        }
    }

    /// Time in minutes
    #[getter]
    fn time_min(&self) -> f64 {
        self.inner.time_min
    }

    /// Percentage of mobile phase B
    #[getter]
    fn percent_b(&self) -> f64 {
        self.inner.percent_b
    }

    /// Flow rate at this step (if variable)
    #[getter]
    fn flow_rate_ul_min(&self) -> Option<f64> {
        self.inner.flow_rate_ul_min
    }

    fn __repr__(&self) -> String {
        format!(
            "GradientStep(time={:.1} min, %B={:.1})",
            self.inner.time_min, self.inner.percent_b
        )
    }
}

impl From<GradientStep> for PyGradientStep {
    fn from(step: GradientStep) -> Self {
        Self { inner: step }
    }
}

/// LC gradient program definition.
#[pyclass(name = "GradientProgram")]
#[derive(Clone)]
pub struct PyGradientProgram {
    inner: GradientProgram,
}

#[pymethods]
impl PyGradientProgram {
    /// Create a new gradient program.
    #[new]
    fn new() -> Self {
        Self {
            inner: GradientProgram::default(),
        }
    }

    /// Gradient steps
    #[getter]
    fn steps(&self) -> Vec<PyGradientStep> {
        self.inner.steps.iter().cloned().map(PyGradientStep::from).collect()
    }

    fn __len__(&self) -> usize {
        self.inner.steps.len()
    }

    fn __repr__(&self) -> String {
        format!("GradientProgram(steps={})", self.inner.steps.len())
    }
}

impl From<GradientProgram> for PyGradientProgram {
    fn from(program: GradientProgram) -> Self {
        Self { inner: program }
    }
}

// ============================================================================
// MobilePhase and ColumnInfo
// ============================================================================

/// Mobile phase solvent configuration.
#[pyclass(name = "MobilePhase")]
#[derive(Clone)]
pub struct PyMobilePhase {
    inner: MobilePhase,
}

#[pymethods]
impl PyMobilePhase {
    /// Create a new mobile phase.
    #[new]
    #[pyo3(signature = (channel, composition, ph=None))]
    fn new(channel: String, composition: String, ph: Option<f64>) -> Self {
        Self {
            inner: MobilePhase {
                channel,
                composition,
                ph,
            },
        }
    }

    /// Channel identifier (A, B, C, D)
    #[getter]
    fn channel(&self) -> String {
        self.inner.channel.clone()
    }

    /// Composition description
    #[getter]
    fn composition(&self) -> String {
        self.inner.composition.clone()
    }

    /// pH (if applicable)
    #[getter]
    fn ph(&self) -> Option<f64> {
        self.inner.ph
    }

    fn __repr__(&self) -> String {
        format!(
            "MobilePhase(channel='{}', composition='{}')",
            self.inner.channel, self.inner.composition
        )
    }
}

impl From<MobilePhase> for PyMobilePhase {
    fn from(phase: MobilePhase) -> Self {
        Self { inner: phase }
    }
}

/// Information about an LC column.
#[pyclass(name = "ColumnInfo")]
#[derive(Clone)]
pub struct PyColumnInfo {
    inner: ColumnInfo,
}

#[pymethods]
impl PyColumnInfo {
    /// Create a new column info.
    #[new]
    fn new() -> Self {
        Self {
            inner: ColumnInfo::default(),
        }
    }

    /// Column name/model
    #[getter]
    fn name(&self) -> Option<String> {
        self.inner.name.clone()
    }

    /// Column manufacturer
    #[getter]
    fn manufacturer(&self) -> Option<String> {
        self.inner.manufacturer.clone()
    }

    /// Column length in mm
    #[getter]
    fn length_mm(&self) -> Option<f64> {
        self.inner.length_mm
    }

    /// Column inner diameter in um
    #[getter]
    fn inner_diameter_um(&self) -> Option<f64> {
        self.inner.inner_diameter_um
    }

    /// Particle size in um
    #[getter]
    fn particle_size_um(&self) -> Option<f64> {
        self.inner.particle_size_um
    }

    /// Pore size in Angstrom
    #[getter]
    fn pore_size_angstrom(&self) -> Option<f64> {
        self.inner.pore_size_angstrom
    }

    /// Stationary phase type
    #[getter]
    fn stationary_phase(&self) -> Option<String> {
        self.inner.stationary_phase.clone()
    }

    fn __repr__(&self) -> String {
        format!("ColumnInfo(name={:?})", self.inner.name)
    }
}

impl From<ColumnInfo> for PyColumnInfo {
    fn from(info: ColumnInfo) -> Self {
        Self { inner: info }
    }
}

// ============================================================================
// LcConfig
// ============================================================================

/// Liquid Chromatography configuration.
#[pyclass(name = "LcConfig")]
#[derive(Clone)]
pub struct PyLcConfig {
    inner: LcConfig,
}

#[pymethods]
impl PyLcConfig {
    /// Create a new empty LC configuration.
    #[new]
    fn new() -> Self {
        Self {
            inner: LcConfig::new(),
        }
    }

    /// LC system model
    #[getter]
    fn system_model(&self) -> Option<String> {
        self.inner.system_model.clone()
    }

    /// Column information
    #[getter]
    fn column(&self) -> Option<PyColumnInfo> {
        self.inner.column.clone().map(PyColumnInfo::from)
    }

    /// Mobile phases
    #[getter]
    fn mobile_phases(&self) -> Vec<PyMobilePhase> {
        self.inner
            .mobile_phases
            .iter()
            .cloned()
            .map(PyMobilePhase::from)
            .collect()
    }

    /// Gradient program
    #[getter]
    fn gradient(&self) -> Option<PyGradientProgram> {
        self.inner.gradient.clone().map(PyGradientProgram::from)
    }

    /// Flow rate in uL/min
    #[getter]
    fn flow_rate_ul_min(&self) -> Option<f64> {
        self.inner.flow_rate_ul_min
    }

    /// Column temperature in Celsius
    #[getter]
    fn column_temperature_celsius(&self) -> Option<f64> {
        self.inner.column_temperature_celsius
    }

    /// Injection volume in uL
    #[getter]
    fn injection_volume_ul(&self) -> Option<f64> {
        self.inner.injection_volume_ul
    }

    fn __repr__(&self) -> String {
        format!(
            "LcConfig(system={:?}, flow_rate={:?} uL/min)",
            self.inner.system_model, self.inner.flow_rate_ul_min
        )
    }
}

impl From<LcConfig> for PyLcConfig {
    fn from(config: LcConfig) -> Self {
        Self { inner: config }
    }
}

// ============================================================================
// RunParameters
// ============================================================================

/// Technical run parameters - lossless storage of vendor-specific data.
#[pyclass(name = "RunParameters")]
#[derive(Clone)]
pub struct PyRunParameters {
    inner: RunParameters,
}

#[pymethods]
impl PyRunParameters {
    /// Create a new empty run parameters instance.
    #[new]
    fn new() -> Self {
        Self {
            inner: RunParameters::new(),
        }
    }

    /// Run start timestamp (ISO 8601)
    #[getter]
    fn start_time(&self) -> Option<String> {
        self.inner.start_time.clone()
    }

    /// Run end timestamp (ISO 8601)
    #[getter]
    fn end_time(&self) -> Option<String> {
        self.inner.end_time.clone()
    }

    /// Operator name
    #[getter]
    fn operator(&self) -> Option<String> {
        self.inner.operator.clone()
    }

    /// Sample name as entered in instrument
    #[getter]
    fn sample_name(&self) -> Option<String> {
        self.inner.sample_name.clone()
    }

    /// Sample vial/position
    #[getter]
    fn sample_position(&self) -> Option<String> {
        self.inner.sample_position.clone()
    }

    /// Method file name
    #[getter]
    fn method_name(&self) -> Option<String> {
        self.inner.method_name.clone()
    }

    /// Tune file name
    #[getter]
    fn tune_file(&self) -> Option<String> {
        self.inner.tune_file.clone()
    }

    /// Calibration file or date
    #[getter]
    fn calibration_info(&self) -> Option<String> {
        self.inner.calibration_info.clone()
    }

    /// Spray voltage in kV (for ESI)
    #[getter]
    fn spray_voltage_kv(&self) -> Option<f64> {
        self.inner.spray_voltage_kv
    }

    /// Spray current in uA
    #[getter]
    fn spray_current_ua(&self) -> Option<f64> {
        self.inner.spray_current_ua
    }

    /// Capillary temperature in Celsius
    #[getter]
    fn capillary_temp_celsius(&self) -> Option<f64> {
        self.inner.capillary_temp_celsius
    }

    /// Source/desolvation temperature in Celsius
    #[getter]
    fn source_temp_celsius(&self) -> Option<f64> {
        self.inner.source_temp_celsius
    }

    /// Sheath gas flow
    #[getter]
    fn sheath_gas(&self) -> Option<f64> {
        self.inner.sheath_gas
    }

    /// Auxiliary gas flow
    #[getter]
    fn aux_gas(&self) -> Option<f64> {
        self.inner.aux_gas
    }

    /// Sweep gas flow
    #[getter]
    fn sweep_gas(&self) -> Option<f64> {
        self.inner.sweep_gas
    }

    /// S-lens/funnel RF level
    #[getter]
    fn funnel_rf_level(&self) -> Option<f64> {
        self.inner.funnel_rf_level
    }

    /// AGC (Automatic Gain Control) settings
    #[getter]
    fn agc_settings(&self) -> HashMap<String, String> {
        self.inner.agc_settings.clone()
    }

    /// Free-form vendor-specific parameters
    #[getter]
    fn vendor_params(&self) -> HashMap<String, String> {
        self.inner.vendor_params.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "RunParameters(sample={:?}, method={:?})",
            self.inner.sample_name, self.inner.method_name
        )
    }
}

impl From<RunParameters> for PyRunParameters {
    fn from(params: RunParameters) -> Self {
        Self { inner: params }
    }
}

// ============================================================================
// SdrfMetadata
// ============================================================================

/// SDRF-Proteomics metadata following the community standard.
#[pyclass(name = "SdrfMetadata")]
#[derive(Clone)]
pub struct PySdrfMetadata {
    inner: SdrfMetadata,
}

#[pymethods]
impl PySdrfMetadata {
    /// Create new SDRF metadata with the given source name.
    #[new]
    fn new(source_name: String) -> Self {
        Self {
            inner: SdrfMetadata::new(&source_name),
        }
    }

    /// Source file name (required)
    #[getter]
    fn source_name(&self) -> String {
        self.inner.source_name.clone()
    }

    /// Organism (NCBI taxonomy, e.g., "Homo sapiens")
    #[getter]
    fn organism(&self) -> Option<String> {
        self.inner.organism.clone()
    }

    /// Organism part / tissue
    #[getter]
    fn organism_part(&self) -> Option<String> {
        self.inner.organism_part.clone()
    }

    /// Cell type
    #[getter]
    fn cell_type(&self) -> Option<String> {
        self.inner.cell_type.clone()
    }

    /// Disease state
    #[getter]
    fn disease(&self) -> Option<String> {
        self.inner.disease.clone()
    }

    /// Instrument model
    #[getter]
    fn instrument(&self) -> Option<String> {
        self.inner.instrument.clone()
    }

    /// Cleavage agent (e.g., "Trypsin")
    #[getter]
    fn cleavage_agent(&self) -> Option<String> {
        self.inner.cleavage_agent.clone()
    }

    /// Modification parameters (e.g., "Carbamidomethyl")
    #[getter]
    fn modifications(&self) -> Vec<String> {
        self.inner.modifications.clone()
    }

    /// Label (e.g., "TMT126", "label free")
    #[getter]
    fn label(&self) -> Option<String> {
        self.inner.label.clone()
    }

    /// Fraction identifier
    #[getter]
    fn fraction(&self) -> Option<String> {
        self.inner.fraction.clone()
    }

    /// Technical replicate number
    #[getter]
    fn technical_replicate(&self) -> Option<i32> {
        self.inner.technical_replicate
    }

    /// Biological replicate number
    #[getter]
    fn biological_replicate(&self) -> Option<i32> {
        self.inner.biological_replicate
    }

    /// Factor values (experimental conditions)
    #[getter]
    fn factor_values(&self) -> HashMap<String, String> {
        self.inner.factor_values.clone()
    }

    /// Comment fields (free-form annotations)
    #[getter]
    fn comments(&self) -> HashMap<String, String> {
        self.inner.comments.clone()
    }

    /// Raw file name reference
    #[getter]
    fn raw_file(&self) -> Option<String> {
        self.inner.raw_file.clone()
    }

    /// Additional custom attributes
    #[getter]
    fn custom_attributes(&self) -> HashMap<String, String> {
        self.inner.custom_attributes.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "SdrfMetadata(source='{}', organism={:?})",
            self.inner.source_name, self.inner.organism
        )
    }
}

impl From<SdrfMetadata> for PySdrfMetadata {
    fn from(sdrf: SdrfMetadata) -> Self {
        Self { inner: sdrf }
    }
}

// ============================================================================
// MzPeakMetadata
// ============================================================================

/// Complete metadata container for an mzPeak file.
#[pyclass(name = "MzPeakMetadata")]
#[derive(Clone)]
pub struct PyMzPeakMetadata {
    inner: MzPeakMetadata,
}

#[pymethods]
impl PyMzPeakMetadata {
    /// Create a new empty metadata container.
    #[new]
    fn new() -> Self {
        Self {
            inner: MzPeakMetadata::new(),
        }
    }

    /// SDRF experimental metadata
    #[getter]
    fn sdrf(&self) -> Option<PySdrfMetadata> {
        self.inner.sdrf.clone().map(PySdrfMetadata::from)
    }

    /// Instrument configuration
    #[getter]
    fn instrument(&self) -> Option<PyInstrumentConfig> {
        self.inner.instrument.clone().map(PyInstrumentConfig::from)
    }

    /// LC configuration
    #[getter]
    fn lc_config(&self) -> Option<PyLcConfig> {
        self.inner.lc_config.clone().map(PyLcConfig::from)
    }

    /// Run-level technical parameters
    #[getter]
    fn run_parameters(&self) -> Option<PyRunParameters> {
        self.inner.run_parameters.clone().map(PyRunParameters::from)
    }

    /// Source file information
    #[getter]
    fn source_file(&self) -> Option<PySourceFileInfo> {
        self.inner.source_file.clone().map(PySourceFileInfo::from)
    }

    /// Processing history
    #[getter]
    fn processing_history(&self) -> Option<PyProcessingHistory> {
        self.inner
            .processing_history
            .clone()
            .map(PyProcessingHistory::from)
    }

    /// SHA-256 checksum of the original raw file
    #[getter]
    fn raw_file_checksum(&self) -> Option<String> {
        self.inner.raw_file_checksum.clone()
    }

    /// MALDI/imaging spatial metadata (if available)
    #[getter]
    fn imaging(&self) -> Option<PyImagingMetadata> {
        self.inner.imaging.clone().map(PyImagingMetadata::from)
    }

    /// Vendor hints for files converted via intermediate formats
    #[getter]
    fn vendor_hints(&self) -> Option<PyVendorHints> {
        self.inner.vendor_hints.clone().map(PyVendorHints::from)
    }

    /// Check if this metadata has SDRF information
    fn has_sdrf(&self) -> bool {
        self.inner.sdrf.is_some()
    }

    /// Check if this metadata has instrument configuration
    fn has_instrument(&self) -> bool {
        self.inner.instrument.is_some()
    }

    /// Check if this metadata has LC configuration
    fn has_lc_config(&self) -> bool {
        self.inner.lc_config.is_some()
    }

    /// Check if this metadata has run parameters
    fn has_run_parameters(&self) -> bool {
        self.inner.run_parameters.is_some()
    }

    /// Check if this metadata has imaging information
    fn has_imaging(&self) -> bool {
        self.inner.imaging.is_some()
    }

    fn __repr__(&self) -> String {
        let parts: Vec<&str> = [
            self.inner.sdrf.as_ref().map(|_| "sdrf"),
            self.inner.instrument.as_ref().map(|_| "instrument"),
            self.inner.lc_config.as_ref().map(|_| "lc"),
            self.inner.run_parameters.as_ref().map(|_| "run"),
            self.inner.source_file.as_ref().map(|_| "source"),
            self.inner.processing_history.as_ref().map(|_| "history"),
            self.inner.imaging.as_ref().map(|_| "imaging"),
        ]
        .into_iter()
        .flatten()
        .collect();

        if parts.is_empty() {
            "MzPeakMetadata(empty)".to_string()
        } else {
            format!("MzPeakMetadata(has=[{}])", parts.join(", "))
        }
    }
}

impl From<MzPeakMetadata> for PyMzPeakMetadata {
    fn from(metadata: MzPeakMetadata) -> Self {
        Self { inner: metadata }
    }
}

impl PyMzPeakMetadata {
    /// Get the inner MzPeakMetadata (for internal use)
    pub fn into_inner(self) -> MzPeakMetadata {
        self.inner
    }
}
