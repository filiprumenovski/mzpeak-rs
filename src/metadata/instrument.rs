use serde::{Deserialize, Serialize};

use crate::controlled_vocabulary::{CvParamList, CvTerm};

use super::MetadataError;

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
    /// Create a new empty instrument configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a CV parameter to the instrument configuration
    pub fn add_cv_param(&mut self, term: CvTerm) {
        self.cv_params.add(term);
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
