use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::instrument::InstrumentConfig;
use super::lc::LcConfig;
use super::processing::ProcessingHistory;
use super::run::RunParameters;
use super::sdrf::SdrfMetadata;
use super::source::SourceFileInfo;
use super::MetadataError;

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
    /// Create a new empty metadata container
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
