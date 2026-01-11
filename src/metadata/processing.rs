use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::controlled_vocabulary::CvParamList;

use super::MetadataError;

/// Data processing history for audit trail
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessingHistory {
    /// List of processing steps applied
    pub steps: Vec<ProcessingStep>,
}

/// A single data processing step in the processing history
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
    /// Create a new empty processing history
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a processing step to the history
    pub fn add_step(&mut self, step: ProcessingStep) {
        self.steps.push(step);
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
