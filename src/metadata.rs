//! # Metadata Module for mzPeak
//!
//! This module handles the parsing and serialization of experimental metadata,
//! including SDRF-Proteomics metadata and lossless technical parameters from
//! vendor raw files.
//!
//! ## Design Goals
//!
//! As emphasized in the mzPeak whitepaper, comprehensive metadata is critical for:
//! - Regulatory compliance (precision medicine, chemical safety)
//! - Long-term data preservation and interpretability
//! - Multi-omics integration
//! - Reproducible science
//!
//! ## Metadata Categories
//!
//! 1. **SDRF Metadata**: Sample and experimental condition annotations following
//!    the SDRF-Proteomics standard (Dai et al., 2021)
//!
//! 2. **Instrument Configuration**: MS and LC settings from the instrument
//!
//! 3. **Run Parameters**: Technical details like pump pressures, temperatures,
//!    and other diagnostic data that vendors typically store but converters lose

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::controlled_vocabulary::{CvParamList, CvTerm};

/// Errors that can occur during metadata processing
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    /// I/O error reading metadata file
    #[error("Failed to read file: {0}")]
    IoError(#[from] std::io::Error),

    /// CSV/TSV parsing error
    #[error("CSV parsing error: {0}")]
    CsvError(#[from] csv::Error),

    /// Missing required column in SDRF file
    #[error("Missing required SDRF column: {0}")]
    MissingColumn(String),

    /// Invalid SDRF file format
    #[error("Invalid SDRF format: {0}")]
    InvalidFormat(String),

    /// JSON serialization/deserialization error
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// SDRF-Proteomics metadata following the community standard
/// Reference: https://github.com/bigbio/proteomics-sample-metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SdrfMetadata {
    /// Source file name (required)
    pub source_name: String,

    /// Organism (NCBI taxonomy, e.g., "Homo sapiens")
    pub organism: Option<String>,

    /// Organism part / tissue
    pub organism_part: Option<String>,

    /// Cell type
    pub cell_type: Option<String>,

    /// Disease state
    pub disease: Option<String>,

    /// Instrument model (e.g., "Orbitrap Exploris 480")
    pub instrument: Option<String>,

    /// Cleavage agent (e.g., "Trypsin")
    pub cleavage_agent: Option<String>,

    /// Modification parameters (e.g., "Carbamidomethyl")
    pub modifications: Vec<String>,

    /// Label (e.g., "TMT126", "label free")
    pub label: Option<String>,

    /// Fraction identifier
    pub fraction: Option<String>,

    /// Technical replicate number
    pub technical_replicate: Option<i32>,

    /// Biological replicate number
    pub biological_replicate: Option<i32>,

    /// Factor values (experimental conditions)
    pub factor_values: HashMap<String, String>,

    /// Comment fields (free-form annotations)
    pub comments: HashMap<String, String>,

    /// Raw file name reference
    pub raw_file: Option<String>,

    /// Additional custom attributes
    pub custom_attributes: HashMap<String, String>,
}

impl SdrfMetadata {
    pub fn new(source_name: &str) -> Self {
        Self {
            source_name: source_name.to_string(),
            ..Default::default()
        }
    }

    /// Parse SDRF metadata from a TSV file
    pub fn from_tsv_file<P: AsRef<Path>>(path: P) -> Result<Vec<Self>, MetadataError> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Self::from_reader(reader)
    }

    /// Parse SDRF metadata from a reader
    pub fn from_reader<R: BufRead>(reader: R) -> Result<Vec<Self>, MetadataError> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .flexible(true)
            .has_headers(true)
            .from_reader(reader);

        let headers: Vec<String> = csv_reader
            .headers()?
            .iter()
            .map(|s| s.to_lowercase().trim().to_string())
            .collect();

        // Validate required column
        if !headers.iter().any(|h| h.contains("source name")) {
            return Err(MetadataError::MissingColumn("source name".to_string()));
        }

        let mut results = Vec::new();

        for record in csv_reader.records() {
            let record = record?;
            let mut metadata = SdrfMetadata::default();

            for (i, value) in record.iter().enumerate() {
                if i >= headers.len() {
                    break;
                }

                let header = &headers[i];
                let value = value.trim();

                if value.is_empty() {
                    continue;
                }

                // Map SDRF column names to struct fields
                match header.as_str() {
                    h if h.contains("source name") => {
                        metadata.source_name = value.to_string();
                    }
                    h if h.contains("organism") && !h.contains("part") => {
                        metadata.organism = Some(value.to_string());
                    }
                    h if h.contains("organism part") || h.contains("tissue") => {
                        metadata.organism_part = Some(value.to_string());
                    }
                    h if h.contains("cell type") => {
                        metadata.cell_type = Some(value.to_string());
                    }
                    h if h.contains("disease") => {
                        metadata.disease = Some(value.to_string());
                    }
                    h if h.contains("instrument") => {
                        metadata.instrument = Some(value.to_string());
                    }
                    h if h.contains("cleavage agent") || h.contains("enzyme") => {
                        metadata.cleavage_agent = Some(value.to_string());
                    }
                    h if h.contains("modification") => {
                        metadata.modifications.push(value.to_string());
                    }
                    h if h.contains("label") => {
                        metadata.label = Some(value.to_string());
                    }
                    h if h.contains("fraction") => {
                        metadata.fraction = Some(value.to_string());
                    }
                    h if h.contains("technical replicate") => {
                        metadata.technical_replicate = value.parse().ok();
                    }
                    h if h.contains("biological replicate") => {
                        metadata.biological_replicate = value.parse().ok();
                    }
                    h if h.starts_with("factor value") => {
                        // Extract factor name from brackets: "factor value[treatment]"
                        if let Some(start) = h.find('[') {
                            if let Some(end) = h.find(']') {
                                let factor_name = &h[start + 1..end];
                                metadata
                                    .factor_values
                                    .insert(factor_name.to_string(), value.to_string());
                            }
                        }
                    }
                    h if h.starts_with("comment") => {
                        if let Some(start) = h.find('[') {
                            if let Some(end) = h.find(']') {
                                let comment_name = &h[start + 1..end];
                                metadata
                                    .comments
                                    .insert(comment_name.to_string(), value.to_string());
                            }
                        }
                    }
                    h if h.contains("file") || h.contains("data file") => {
                        metadata.raw_file = Some(value.to_string());
                    }
                    _ => {
                        // Store unknown columns as custom attributes
                        metadata
                            .custom_attributes
                            .insert(header.clone(), value.to_string());
                    }
                }
            }

            if !metadata.source_name.is_empty() {
                results.push(metadata);
            }
        }

        Ok(results)
    }

    /// Serialize to JSON for storage in Parquet footer
    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserialize from JSON stored in Parquet footer
    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}

/// Instrument configuration metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstrumentConfig {
    /// Instrument model name (CV: MS:1000031)
    pub model: Option<String>,

    /// Instrument serial number (CV: MS:1000529)
    pub serial_number: Option<String>,

    /// Vendor name
    pub vendor: Option<String>,

    /// Software version
    pub software_version: Option<String>,

    /// Ion source type (e.g., ESI, MALDI)
    pub ion_source: Option<String>,

    /// Mass analyzer configuration
    pub mass_analyzers: Vec<MassAnalyzerConfig>,

    /// Detector configuration
    pub detector: Option<String>,

    /// Additional CV parameters
    pub cv_params: CvParamList,
}

/// Mass analyzer configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MassAnalyzerConfig {
    /// Analyzer type (e.g., "orbitrap", "quadrupole", "ion trap")
    pub analyzer_type: String,

    /// Analyzer order (1 = first analyzer, 2 = second, etc.)
    pub order: i32,

    /// Resolution at a given m/z (if applicable)
    pub resolution: Option<f64>,

    /// Reference m/z for resolution
    pub resolution_mz: Option<f64>,

    /// CV parameters specific to this analyzer
    pub cv_params: CvParamList,
}

impl InstrumentConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialize to JSON for Parquet footer storage
    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}

/// Liquid Chromatography configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LcConfig {
    /// LC system model
    pub system_model: Option<String>,

    /// Column information
    pub column: Option<ColumnInfo>,

    /// Mobile phases
    pub mobile_phases: Vec<MobilePhase>,

    /// Gradient program
    pub gradient: Option<GradientProgram>,

    /// Flow rate in uL/min
    pub flow_rate_ul_min: Option<f64>,

    /// Column temperature in Celsius
    pub column_temperature_celsius: Option<f64>,

    /// Injection volume in uL
    pub injection_volume_ul: Option<f64>,

    /// Additional CV parameters
    pub cv_params: CvParamList,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name/model
    pub name: Option<String>,

    /// Column manufacturer
    pub manufacturer: Option<String>,

    /// Column length in mm
    pub length_mm: Option<f64>,

    /// Column inner diameter in um
    pub inner_diameter_um: Option<f64>,

    /// Particle size in um
    pub particle_size_um: Option<f64>,

    /// Pore size in Angstrom
    pub pore_size_angstrom: Option<f64>,

    /// Stationary phase type
    pub stationary_phase: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MobilePhase {
    /// Channel identifier (A, B, C, D)
    pub channel: String,

    /// Composition description
    pub composition: String,

    /// pH (if applicable)
    pub ph: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GradientProgram {
    /// Gradient steps as (time_min, %B)
    pub steps: Vec<GradientStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GradientStep {
    /// Time in minutes
    pub time_min: f64,

    /// Percentage of mobile phase B
    pub percent_b: f64,

    /// Flow rate at this step (if variable)
    pub flow_rate_ul_min: Option<f64>,
}

impl LcConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}

/// Technical run parameters - lossless storage of vendor-specific data
///
/// This is a critical differentiator for mzPeak. Unlike mzML converters that
/// discard technical metadata, mzPeak preserves all available vendor data.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunParameters {
    /// Run start timestamp (ISO 8601)
    pub start_time: Option<String>,

    /// Run end timestamp (ISO 8601)
    pub end_time: Option<String>,

    /// Operator name
    pub operator: Option<String>,

    /// Sample name as entered in instrument
    pub sample_name: Option<String>,

    /// Sample vial/position
    pub sample_position: Option<String>,

    /// Method file name
    pub method_name: Option<String>,

    /// Tune file name
    pub tune_file: Option<String>,

    /// Calibration file or date
    pub calibration_info: Option<String>,

    /// Pressure readings throughout the run (time-series)
    pub pressure_traces: Vec<PressureTrace>,

    /// Temperature readings
    pub temperature_traces: Vec<TemperatureTrace>,

    /// Spray current/voltage (for ESI)
    pub spray_voltage_kv: Option<f64>,

    /// Spray current in uA
    pub spray_current_ua: Option<f64>,

    /// Capillary temperature in Celsius
    pub capillary_temp_celsius: Option<f64>,

    /// Source/desolvation temperature
    pub source_temp_celsius: Option<f64>,

    /// Sheath gas flow
    pub sheath_gas: Option<f64>,

    /// Auxiliary gas flow
    pub aux_gas: Option<f64>,

    /// Sweep gas flow
    pub sweep_gas: Option<f64>,

    /// S-lens/funnel RF level
    pub funnel_rf_level: Option<f64>,

    /// AGC (Automatic Gain Control) settings
    pub agc_settings: HashMap<String, String>,

    /// Free-form vendor-specific parameters
    pub vendor_params: HashMap<String, String>,

    /// CV parameters
    pub cv_params: CvParamList,
}

/// Pressure trace over time (e.g., pump pressure during LC run)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PressureTrace {
    /// Name/identifier (e.g., "Pump A", "Column Pressure")
    pub name: String,

    /// Unit for pressure values
    pub unit: String,

    /// Time points in minutes
    pub times_min: Vec<f64>,

    /// Pressure values
    pub values: Vec<f64>,
}

/// Temperature trace over time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemperatureTrace {
    /// Name/identifier (e.g., "Column Oven", "Autosampler")
    pub name: String,

    /// Time points in minutes
    pub times_min: Vec<f64>,

    /// Temperature values in Celsius
    pub values_celsius: Vec<f64>,
}

impl RunParameters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a vendor-specific parameter
    pub fn add_vendor_param(&mut self, key: &str, value: &str) {
        self.vendor_params.insert(key.to_string(), value.to_string());
    }

    /// Add a CV parameter
    pub fn add_cv_param(&mut self, term: CvTerm) {
        self.cv_params.add(term);
    }

    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}

/// Source file information for provenance tracking
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceFileInfo {
    /// Original file name
    pub name: String,

    /// Original file path
    pub path: Option<String>,

    /// File format (e.g., "Thermo RAW", "Bruker .d")
    pub format: Option<String>,

    /// File size in bytes
    pub size_bytes: Option<u64>,

    /// SHA-256 checksum of the original file
    pub sha256: Option<String>,

    /// MD5 checksum (for legacy compatibility)
    pub md5: Option<String>,

    /// Vendor file version/format version
    pub format_version: Option<String>,
}

impl SourceFileInfo {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            ..Default::default()
        }
    }

    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}

/// Data processing history for audit trail
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessingHistory {
    /// List of processing steps applied
    pub steps: Vec<ProcessingStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingStep {
    /// Step order (1-indexed)
    pub order: i32,

    /// Software name
    pub software: String,

    /// Software version
    pub version: Option<String>,

    /// Processing type (e.g., "conversion", "peak picking", "centroiding")
    pub processing_type: String,

    /// Timestamp when processing was performed
    pub timestamp: Option<String>,

    /// Processing parameters
    pub parameters: HashMap<String, String>,

    /// CV parameters describing the processing
    pub cv_params: CvParamList,
}

impl ProcessingHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_step(&mut self, step: ProcessingStep) {
        self.steps.push(step);
    }

    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}

/// Complete metadata container for an mzPeak file
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MzPeakMetadata {
    /// SDRF experimental metadata
    pub sdrf: Option<SdrfMetadata>,

    /// Instrument configuration
    pub instrument: Option<InstrumentConfig>,

    /// LC configuration
    pub lc_config: Option<LcConfig>,

    /// Run-level technical parameters
    pub run_parameters: Option<RunParameters>,

    /// Source file information
    pub source_file: Option<SourceFileInfo>,

    /// Processing history
    pub processing_history: Option<ProcessingHistory>,
}

impl MzPeakMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert all metadata to a HashMap suitable for Parquet key_value_metadata
    pub fn to_parquet_metadata(&self) -> Result<HashMap<String, String>, MetadataError> {
        use crate::schema::*;

        let mut metadata = HashMap::new();

        metadata.insert(KEY_FORMAT_VERSION.to_string(), MZPEAK_FORMAT_VERSION.to_string());

        metadata.insert(
            KEY_CONVERSION_TIMESTAMP.to_string(),
            chrono::Utc::now().to_rfc3339(),
        );

        metadata.insert(
            KEY_CONVERTER_INFO.to_string(),
            format!("mzpeak-rs v{}", env!("CARGO_PKG_VERSION")),
        );

        if let Some(ref sdrf) = self.sdrf {
            metadata.insert(KEY_SDRF_METADATA.to_string(), sdrf.to_json()?);
        }

        if let Some(ref inst) = self.instrument {
            metadata.insert(KEY_INSTRUMENT_CONFIG.to_string(), inst.to_json()?);
        }

        if let Some(ref lc) = self.lc_config {
            metadata.insert(KEY_LC_CONFIG.to_string(), lc.to_json()?);
        }

        if let Some(ref run) = self.run_parameters {
            metadata.insert(KEY_RUN_PARAMETERS.to_string(), run.to_json()?);
        }

        if let Some(ref source) = self.source_file {
            metadata.insert(KEY_SOURCE_FILE.to_string(), source.to_json()?);
        }

        if let Some(ref history) = self.processing_history {
            metadata.insert(KEY_PROCESSING_HISTORY.to_string(), history.to_json()?);
        }

        Ok(metadata)
    }

    /// Reconstruct metadata from Parquet key_value_metadata
    pub fn from_parquet_metadata(
        metadata: &HashMap<String, String>,
    ) -> Result<Self, MetadataError> {
        use crate::schema::*;

        let mut result = Self::new();

        if let Some(json) = metadata.get(KEY_SDRF_METADATA) {
            result.sdrf = Some(SdrfMetadata::from_json(json)?);
        }

        if let Some(json) = metadata.get(KEY_INSTRUMENT_CONFIG) {
            result.instrument = Some(InstrumentConfig::from_json(json)?);
        }

        if let Some(json) = metadata.get(KEY_LC_CONFIG) {
            result.lc_config = Some(LcConfig::from_json(json)?);
        }

        if let Some(json) = metadata.get(KEY_RUN_PARAMETERS) {
            result.run_parameters = Some(RunParameters::from_json(json)?);
        }

        if let Some(json) = metadata.get(KEY_SOURCE_FILE) {
            result.source_file = Some(SourceFileInfo::from_json(json)?);
        }

        if let Some(json) = metadata.get(KEY_PROCESSING_HISTORY) {
            result.processing_history = Some(ProcessingHistory::from_json(json)?);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_SDRF: &str = r#"source name	characteristics[organism]	characteristics[organism part]	comment[data file]	comment[instrument]
Sample1	Homo sapiens	liver	sample1.raw	Orbitrap Exploris 480
Sample2	Homo sapiens	kidney	sample2.raw	Orbitrap Exploris 480"#;

    #[test]
    fn test_sdrf_parsing() {
        let reader = std::io::Cursor::new(SAMPLE_SDRF);
        let metadata = SdrfMetadata::from_reader(reader).unwrap();

        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].source_name, "Sample1");
        assert_eq!(metadata[0].organism, Some("Homo sapiens".to_string()));
        assert_eq!(metadata[0].organism_part, Some("liver".to_string()));
    }

    #[test]
    fn test_metadata_json_roundtrip() {
        let mut sdrf = SdrfMetadata::new("TestSample");
        sdrf.organism = Some("Mus musculus".to_string());
        sdrf.instrument = Some("Q Exactive HF".to_string());

        let json = sdrf.to_json().unwrap();
        let restored = SdrfMetadata::from_json(&json).unwrap();

        assert_eq!(restored.source_name, "TestSample");
        assert_eq!(restored.organism, Some("Mus musculus".to_string()));
    }

    #[test]
    fn test_run_parameters() {
        let mut params = RunParameters::new();
        params.spray_voltage_kv = Some(3.5);
        params.add_vendor_param("ThermoSpecific", "SomeValue");

        let json = params.to_json().unwrap();
        let restored = RunParameters::from_json(&json).unwrap();

        assert_eq!(restored.spray_voltage_kv, Some(3.5));
        assert_eq!(
            restored.vendor_params.get("ThermoSpecific"),
            Some(&"SomeValue".to_string())
        );
    }
}
