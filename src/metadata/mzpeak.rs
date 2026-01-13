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

    /// SHA-256 checksum of the original raw file (top-level for quick access)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_file_checksum: Option<String>,

    /// MALDI/imaging spatial metadata (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imaging: Option<ImagingMetadata>,
}

/// MALDI/imaging grid metadata for spatial indexing.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImagingMetadata {
    /// Width of the pixel grid (X dimension, zero-indexed + 1)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grid_width: Option<u32>,
    /// Height of the pixel grid (Y dimension, zero-indexed + 1)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grid_height: Option<u32>,
    /// Pixel size along X in micrometers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pixel_size_x_um: Option<f64>,
    /// Pixel size along Y in micrometers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pixel_size_y_um: Option<f64>,
}

impl ImagingMetadata {
    /// Serialize imaging metadata to JSON for Parquet footer storage.
    pub fn to_json(&self) -> Result<String, MetadataError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserialize imaging metadata from JSON.
    pub fn from_json(json: &str) -> Result<Self, MetadataError> {
        Ok(serde_json::from_str(json)?)
    }
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

        if let Some(ref checksum) = self.raw_file_checksum {
            metadata.insert(KEY_RAW_FILE_CHECKSUM.to_string(), checksum.clone());
        }

        if let Some(ref imaging) = self.imaging {
            metadata.insert(KEY_IMAGING_METADATA.to_string(), imaging.to_json()?);
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

        if let Some(checksum) = metadata.get(KEY_RAW_FILE_CHECKSUM) {
            result.raw_file_checksum = Some(checksum.clone());
        }

        if let Some(json) = metadata.get(KEY_IMAGING_METADATA) {
            result.imaging = Some(ImagingMetadata::from_json(json)?);
        }

        Ok(result)
    }
}
