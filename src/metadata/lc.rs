use serde::{Deserialize, Serialize};

use crate::controlled_vocabulary::CvParamList;

use super::MetadataError;

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

/// Information about an LC column
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

/// Mobile phase solvent configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MobilePhase {
    /// Channel identifier (A, B, C, D)
    pub channel: String,

    /// Composition description
    pub composition: String,

    /// pH (if applicable)
    pub ph: Option<f64>,
}

/// LC gradient program definition
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GradientProgram {
    /// Gradient steps as (time_min, %B)
    pub steps: Vec<GradientStep>,
}

/// A single step in an LC gradient program
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
    /// Create a new empty LC configuration
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
