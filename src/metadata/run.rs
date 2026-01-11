use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::controlled_vocabulary::{CvParamList, CvTerm};

use super::traces::{PressureTrace, TemperatureTrace};
use super::MetadataError;

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

impl RunParameters {
    /// Create a new empty run parameters instance
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

    /// Serialize to JSON for Parquet footer storage
    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
}
