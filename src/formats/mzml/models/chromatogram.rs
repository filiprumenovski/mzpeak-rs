use serde::{Deserialize, Serialize};

use crate::mzml::cv_params::CvParam;

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
    /// Unknown or unspecified chromatogram type
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
    /// Determine chromatogram type from CV accession
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
