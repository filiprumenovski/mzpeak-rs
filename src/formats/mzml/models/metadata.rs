use serde::{Deserialize, Serialize};

use crate::mzml::cv_params::CvParam;

/// File-level metadata from mzML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MzMLFileMetadata {
    /// mzML version
    pub version: Option<String>,

    /// File content type descriptions
    pub file_content: Vec<CvParam>,

    /// Source files
    pub source_files: Vec<SourceFile>,

    /// Software used
    pub software_list: Vec<Software>,

    /// Instrument configurations
    pub instrument_configurations: Vec<InstrumentConfiguration>,

    /// Data processing steps
    pub data_processing: Vec<DataProcessing>,

    /// Run ID
    pub run_id: Option<String>,

    /// Run start time
    pub run_start_time: Option<String>,

    /// Default instrument configuration ref
    pub default_instrument_configuration_ref: Option<String>,

    /// Default source file ref
    pub default_source_file_ref: Option<String>,

    /// Sample information
    pub samples: Vec<Sample>,
}

/// Source file information from mzML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceFile {
    /// Unique identifier
    pub id: String,
    /// File name
    pub name: String,
    /// File location (path or URI)
    pub location: Option<String>,
    /// File checksum value
    pub checksum: Option<String>,
    /// Checksum algorithm (MD5, SHA-1, etc.)
    pub checksum_type: Option<String>,
    /// File format description
    pub file_format: Option<String>,
    /// CV parameters describing the file
    pub cv_params: Vec<CvParam>,
}

/// Software information from mzML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Software {
    /// Unique identifier
    pub id: String,
    /// Software version
    pub version: Option<String>,
    /// Software name
    pub name: Option<String>,
    /// CV parameters describing the software
    pub cv_params: Vec<CvParam>,
}

/// Instrument configuration from mzML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstrumentConfiguration {
    /// Unique identifier
    pub id: String,
    /// Instrument components (source, analyzer, detector)
    pub components: Vec<InstrumentComponent>,
    /// Reference to controlling software
    pub software_ref: Option<String>,
    /// CV parameters describing the instrument
    pub cv_params: Vec<CvParam>,
}

/// Instrument component (source, analyzer, detector)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstrumentComponent {
    /// Type of component
    pub component_type: ComponentType,
    /// Order in the instrument path
    pub order: i32,
    /// CV parameters describing the component
    pub cv_params: Vec<CvParam>,
}

/// Type of instrument component
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ComponentType {
    /// Unknown component type
    #[default]
    Unknown,
    /// Ion source
    Source,
    /// Mass analyzer
    Analyzer,
    /// Detector
    Detector,
}

/// Data processing information from mzML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataProcessing {
    /// Unique identifier
    pub id: String,
    /// Processing methods applied
    pub processing_methods: Vec<ProcessingMethod>,
}

/// Processing method from mzML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessingMethod {
    /// Order of the processing step
    pub order: i32,
    /// Reference to the software used
    pub software_ref: Option<String>,
    /// CV parameters describing the processing
    pub cv_params: Vec<CvParam>,
}

/// Sample information from mzML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Sample {
    /// Unique identifier
    pub id: String,
    /// Sample name
    pub name: Option<String>,
    /// CV parameters describing the sample
    pub cv_params: Vec<CvParam>,
}
