//! Data models for mzML structures
//!
//! These models represent the parsed mzML data in a Rust-native format,
//! ready for conversion to mzPeak Parquet format.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use super::cv_params::CvParam;

/// Represents a single spectrum from an mzML file
#[derive(Debug, Clone, Default)]
pub struct MzMLSpectrum {
    /// Spectrum index (0-based)
    pub index: i64,

    /// Native spectrum ID from the file
    pub id: String,

    /// Default array length (number of peaks)
    pub default_array_length: usize,

    /// MS level (1 for MS1, 2 for MS2, etc.)
    pub ms_level: i16,

    /// Whether this is a centroid (true) or profile (false) spectrum
    pub centroided: bool,

    /// Polarity: 1 for positive, -1 for negative, 0 for unknown
    pub polarity: i8,

    /// Retention time in seconds
    pub retention_time: Option<f64>,

    /// Total ion current
    pub total_ion_current: Option<f64>,

    /// Base peak m/z
    pub base_peak_mz: Option<f64>,

    /// Base peak intensity
    pub base_peak_intensity: Option<f64>,

    /// Lowest observed m/z
    pub lowest_mz: Option<f64>,

    /// Highest observed m/z
    pub highest_mz: Option<f64>,

    /// Scan window lower limit
    pub scan_window_lower: Option<f64>,

    /// Scan window upper limit
    pub scan_window_upper: Option<f64>,

    /// Ion injection time in milliseconds
    pub ion_injection_time: Option<f64>,

    /// Filter string (vendor-specific)
    pub filter_string: Option<String>,

    /// Preset scan configuration
    pub preset_scan_configuration: Option<i32>,

    /// Precursor information (for MS2+ spectra)
    pub precursors: Vec<Precursor>,

    /// m/z array (decoded)
    pub mz_array: Vec<f64>,

    /// Intensity array (decoded)
    pub intensity_array: Vec<f64>,

    /// Whether m/z was stored as 64-bit in source
    pub mz_precision_64bit: bool,

    /// Whether intensity was stored as 64-bit in source
    pub intensity_precision_64bit: bool,

    /// All CV parameters for this spectrum
    pub cv_params: Vec<CvParam>,

    /// User parameters
    pub user_params: HashMap<String, String>,
}

impl MzMLSpectrum {
    /// Get the scan number from the native ID
    pub fn scan_number(&self) -> Option<i64> {
        // Common formats:
        // "scan=12345"
        // "controllerType=0 controllerNumber=1 scan=12345"
        // "S12345"
        if let Some(pos) = self.id.find("scan=") {
            let start = pos + 5;
            let end = self.id[start..]
                .find(|c: char| !c.is_ascii_digit())
                .map(|i| start + i)
                .unwrap_or(self.id.len());
            self.id[start..end].parse().ok()
        } else if self.id.starts_with('S') {
            self.id[1..].parse().ok()
        } else {
            // Fall back to index + 1
            Some(self.index + 1)
        }
    }

    /// Get the number of peaks
    pub fn peak_count(&self) -> usize {
        self.mz_array.len()
    }
}

/// Precursor ion information for MS2+ spectra
#[derive(Debug, Clone, Default)]
pub struct Precursor {
    /// Reference to the precursor spectrum ID
    pub spectrum_ref: Option<String>,

    /// Isolation window target m/z
    pub isolation_window_target: Option<f64>,

    /// Isolation window lower offset
    pub isolation_window_lower: Option<f64>,

    /// Isolation window upper offset
    pub isolation_window_upper: Option<f64>,

    /// Selected ion m/z
    pub selected_ion_mz: Option<f64>,

    /// Selected ion intensity
    pub selected_ion_intensity: Option<f64>,

    /// Selected ion charge state
    pub selected_ion_charge: Option<i16>,

    /// Activation method (CID, HCD, ETD, etc.)
    pub activation_method: Option<String>,

    /// Collision energy
    pub collision_energy: Option<f64>,

    /// CV parameters for this precursor
    pub cv_params: Vec<CvParam>,
}

/// Represents a chromatogram from an mzML file
#[derive(Debug, Clone, Default)]
pub struct MzMLChromatogram {
    /// Chromatogram index (0-based)
    pub index: i64,

    /// Native chromatogram ID
    pub id: String,

    /// Default array length
    pub default_array_length: usize,

    /// Chromatogram type (TIC, BPC, SRM, etc.)
    pub chromatogram_type: ChromatogramType,

    /// Time array (in seconds)
    pub time_array: Vec<f64>,

    /// Intensity array
    pub intensity_array: Vec<f64>,

    /// Precursor isolation target (for SRM/MRM)
    pub precursor_mz: Option<f64>,

    /// Product isolation target (for SRM/MRM)
    pub product_mz: Option<f64>,

    /// CV parameters
    pub cv_params: Vec<CvParam>,
}

/// Types of chromatograms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ChromatogramType {
    #[default]
    Unknown,
    /// Total Ion Current
    TIC,
    /// Base Peak Chromatogram
    BPC,
    /// Selected Ion Monitoring
    SIM,
    /// Selected Reaction Monitoring / Multiple Reaction Monitoring
    SRM,
    /// Extracted Ion Chromatogram
    XIC,
    /// Absorption chromatogram
    Absorption,
    /// Emission chromatogram
    Emission,
}

impl ChromatogramType {
    pub fn from_cv_accession(accession: &str) -> Self {
        match accession {
            "MS:1000235" => ChromatogramType::TIC,
            "MS:1000628" => ChromatogramType::BPC,
            "MS:1001472" => ChromatogramType::SIM,
            "MS:1001473" | "MS:1000908" => ChromatogramType::SRM,
            "MS:1000627" => ChromatogramType::XIC,
            "MS:1000812" => ChromatogramType::Absorption,
            "MS:1000813" => ChromatogramType::Emission,
            _ => ChromatogramType::Unknown,
        }
    }
}

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

/// Source file information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceFile {
    pub id: String,
    pub name: String,
    pub location: Option<String>,
    pub checksum: Option<String>,
    pub checksum_type: Option<String>,
    pub file_format: Option<String>,
    pub cv_params: Vec<CvParam>,
}

/// Software information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Software {
    pub id: String,
    pub version: Option<String>,
    pub name: Option<String>,
    pub cv_params: Vec<CvParam>,
}

/// Instrument configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstrumentConfiguration {
    pub id: String,
    pub components: Vec<InstrumentComponent>,
    pub software_ref: Option<String>,
    pub cv_params: Vec<CvParam>,
}

/// Instrument component (source, analyzer, detector)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstrumentComponent {
    pub component_type: ComponentType,
    pub order: i32,
    pub cv_params: Vec<CvParam>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ComponentType {
    #[default]
    Unknown,
    Source,
    Analyzer,
    Detector,
}

/// Data processing information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataProcessing {
    pub id: String,
    pub processing_methods: Vec<ProcessingMethod>,
}

/// Processing method
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessingMethod {
    pub order: i32,
    pub software_ref: Option<String>,
    pub cv_params: Vec<CvParam>,
}

/// Sample information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Sample {
    pub id: String,
    pub name: Option<String>,
    pub cv_params: Vec<CvParam>,
}

/// Index entry for indexed mzML files
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub id: String,
    pub offset: u64,
}

/// Complete index from indexedmzML
#[derive(Debug, Clone, Default)]
pub struct MzMLIndex {
    pub spectrum_index: Vec<IndexEntry>,
    pub chromatogram_index: Vec<IndexEntry>,
    pub index_list_offset: Option<u64>,
}

impl MzMLIndex {
    /// Check if this is an indexed file
    pub fn is_indexed(&self) -> bool {
        self.index_list_offset.is_some()
    }

    /// Get spectrum count
    pub fn spectrum_count(&self) -> usize {
        self.spectrum_index.len()
    }

    /// Get chromatogram count
    pub fn chromatogram_count(&self) -> usize {
        self.chromatogram_index.len()
    }
}
