//! Controlled Vocabulary (CV) parameter handling for mzML
//!
//! mzML uses CV terms from the PSI-MS ontology to describe data semantically.
//! This module provides mappings and utilities for working with CV parameters.

use serde::{Deserialize, Serialize};

/// A controlled vocabulary parameter from mzML
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CvParam {
    /// CV reference (e.g., "MS" for PSI-MS)
    pub cv_ref: String,

    /// Accession number (e.g., "MS:1000511")
    pub accession: String,

    /// Human-readable name
    pub name: String,

    /// Optional value
    pub value: Option<String>,

    /// Unit CV reference
    pub unit_cv_ref: Option<String>,

    /// Unit accession
    pub unit_accession: Option<String>,

    /// Unit name
    pub unit_name: Option<String>,
}

impl CvParam {
    /// Get the value as f64 if possible
    pub fn value_as_f64(&self) -> Option<f64> {
        self.value.as_ref()?.parse().ok()
    }

    /// Get the value as i64 if possible
    pub fn value_as_i64(&self) -> Option<i64> {
        self.value.as_ref()?.parse().ok()
    }

    /// Get the value as i32 if possible
    pub fn value_as_i32(&self) -> Option<i32> {
        self.value.as_ref()?.parse().ok()
    }

    /// Check if this is a boolean CV param (no value means true)
    pub fn is_flag(&self) -> bool {
        self.value.is_none()
    }
}

/// Common MS CV accessions used in mzML
#[allow(non_snake_case)]
pub mod MS_CV_ACCESSIONS {
    // =========================================================================
    // Spectrum type
    // =========================================================================

    /// MS level
    pub const MS_LEVEL: &str = "MS:1000511";

    /// Centroid spectrum
    pub const CENTROID_SPECTRUM: &str = "MS:1000127";

    /// Profile spectrum
    pub const PROFILE_SPECTRUM: &str = "MS:1000128";

    /// Positive scan
    pub const POSITIVE_SCAN: &str = "MS:1000130";

    /// Negative scan
    pub const NEGATIVE_SCAN: &str = "MS:1000129";

    // =========================================================================
    // Scan/spectrum properties
    // =========================================================================

    /// Scan start time (retention time)
    pub const SCAN_START_TIME: &str = "MS:1000016";

    /// Total ion current
    pub const TOTAL_ION_CURRENT: &str = "MS:1000285";

    /// Base peak m/z
    pub const BASE_PEAK_MZ: &str = "MS:1000504";

    /// Base peak intensity
    pub const BASE_PEAK_INTENSITY: &str = "MS:1000505";

    /// Lowest observed m/z
    pub const LOWEST_OBSERVED_MZ: &str = "MS:1000528";

    /// Highest observed m/z
    pub const HIGHEST_OBSERVED_MZ: &str = "MS:1000527";

    /// Ion injection time
    pub const ION_INJECTION_TIME: &str = "MS:1000927";

    /// Filter string
    pub const FILTER_STRING: &str = "MS:1000512";

    /// Preset scan configuration
    pub const PRESET_SCAN_CONFIGURATION: &str = "MS:1000616";

    /// Scan window lower limit
    pub const SCAN_WINDOW_LOWER_LIMIT: &str = "MS:1000501";

    /// Scan window upper limit
    pub const SCAN_WINDOW_UPPER_LIMIT: &str = "MS:1000500";

    /// Ion mobility drift time
    pub const ION_MOBILITY_DRIFT_TIME: &str = "MS:1002476";

    // =========================================================================
    // Precursor/isolation
    // =========================================================================

    /// Selected ion m/z
    pub const SELECTED_ION_MZ: &str = "MS:1000744";

    /// Peak intensity (for selected ion)
    pub const PEAK_INTENSITY: &str = "MS:1000042";

    /// Charge state
    pub const CHARGE_STATE: &str = "MS:1000041";

    /// Isolation window target m/z
    pub const ISOLATION_WINDOW_TARGET_MZ: &str = "MS:1000827";

    /// Isolation window lower offset
    pub const ISOLATION_WINDOW_LOWER_OFFSET: &str = "MS:1000828";

    /// Isolation window upper offset
    pub const ISOLATION_WINDOW_UPPER_OFFSET: &str = "MS:1000829";

    // =========================================================================
    // Activation/fragmentation
    // =========================================================================

    /// Collision energy
    pub const COLLISION_ENERGY: &str = "MS:1000045";

    /// Collision-induced dissociation (CID)
    pub const CID: &str = "MS:1000133";

    /// Beam-type CID (HCD)
    pub const HCD: &str = "MS:1000422";

    /// Electron transfer dissociation (ETD)
    pub const ETD: &str = "MS:1000598";

    /// Electron capture dissociation (ECD)
    pub const ECD: &str = "MS:1000250";

    /// Infrared multiphoton dissociation (IRMPD)
    pub const IRMPD: &str = "MS:1000262";

    /// Photodissociation
    pub const PHOTODISSOCIATION: &str = "MS:1000435";

    // =========================================================================
    // Binary data encoding
    // =========================================================================

    /// 32-bit float
    pub const FLOAT_32_BIT: &str = "MS:1000521";

    /// 64-bit float
    pub const FLOAT_64_BIT: &str = "MS:1000523";

    /// zlib compression
    pub const ZLIB_COMPRESSION: &str = "MS:1000574";

    /// No compression
    pub const NO_COMPRESSION: &str = "MS:1000576";

    /// MS-Numpress linear prediction
    pub const NUMPRESS_LINEAR: &str = "MS:1002312";

    /// MS-Numpress positive integer compression
    pub const NUMPRESS_PIC: &str = "MS:1002313";

    /// MS-Numpress short logged float compression
    pub const NUMPRESS_SLOF: &str = "MS:1002314";

    // =========================================================================
    // Binary array types
    // =========================================================================

    /// m/z array
    pub const MZ_ARRAY: &str = "MS:1000514";

    /// Intensity array
    pub const INTENSITY_ARRAY: &str = "MS:1000515";

    /// Ion mobility array
    pub const ION_MOBILITY_ARRAY: &str = "MS:1002893";

    /// Time array
    pub const TIME_ARRAY: &str = "MS:1000595";

    // =========================================================================
    // Chromatogram types
    // =========================================================================

    /// Total ion current chromatogram
    pub const TIC_CHROMATOGRAM: &str = "MS:1000235";

    /// Base peak chromatogram
    pub const BPC_CHROMATOGRAM: &str = "MS:1000628";

    /// Selected ion monitoring chromatogram
    pub const SIM_CHROMATOGRAM: &str = "MS:1001472";

    /// Selected reaction monitoring chromatogram
    pub const SRM_CHROMATOGRAM: &str = "MS:1001473";

    /// Extracted ion chromatogram
    pub const XIC_CHROMATOGRAM: &str = "MS:1000627";

    // =========================================================================
    // File/source information
    // =========================================================================

    /// SHA-1 checksum
    pub const SHA1_CHECKSUM: &str = "MS:1000569";

    /// MD5 checksum
    pub const MD5_CHECKSUM: &str = "MS:1000568";

    /// Thermo RAW format
    pub const THERMO_RAW: &str = "MS:1000563";

    /// Waters RAW format
    pub const WATERS_RAW: &str = "MS:1000526";

    /// mzML format
    pub const MZML_FORMAT: &str = "MS:1000584";

    /// mzXML format
    pub const MZXML_FORMAT: &str = "MS:1000566";

    // =========================================================================
    // Instrument components
    // =========================================================================

    /// Electrospray ionization (ESI)
    pub const ESI: &str = "MS:1000073";

    /// Nanoelectrospray
    pub const NANOESI: &str = "MS:1000398";

    /// MALDI
    pub const MALDI: &str = "MS:1000075";

    /// Orbitrap
    pub const ORBITRAP: &str = "MS:1000484";

    /// Quadrupole
    pub const QUADRUPOLE: &str = "MS:1000081";

    /// Ion trap
    pub const ION_TRAP: &str = "MS:1000264";

    /// Time-of-flight
    pub const TOF: &str = "MS:1000084";

    /// Electron multiplier
    pub const ELECTRON_MULTIPLIER: &str = "MS:1000253";

    /// Inductive detector
    pub const INDUCTIVE_DETECTOR: &str = "MS:1000624";

    // =========================================================================
    // Time units
    // =========================================================================

    /// Second (UO)
    pub const UNIT_SECOND: &str = "UO:0000010";

    /// Minute (UO)
    pub const UNIT_MINUTE: &str = "UO:0000031";

    /// Millisecond (UO)
    pub const UNIT_MILLISECOND: &str = "UO:0000028";
}

/// Common IMS (imaging mass spectrometry) CV accessions used in imzML
#[allow(non_snake_case)]
pub mod IMS_CV_ACCESSIONS {
    /// Position x (pixel coordinate)
    pub const POSITION_X: &str = "IMS:1000050";

    /// Position y (pixel coordinate)
    pub const POSITION_Y: &str = "IMS:1000051";

    /// Position z (pixel coordinate)
    pub const POSITION_Z: &str = "IMS:1000052";

    /// External array length (imzML external binary data)
    pub const EXTERNAL_ARRAY_LENGTH: &str = "IMS:1000102";

    /// External data offset (imzML external binary data)
    pub const EXTERNAL_OFFSET: &str = "IMS:1000103";
}

/// Extract a CV parameter value from a list by accession
pub fn extract_cv_value(cv_params: &[CvParam], accession: &str) -> Option<String> {
    cv_params
        .iter()
        .find(|p| p.accession == accession)
        .and_then(|p| p.value.clone())
}

/// Extract a CV parameter as f64 from a list by accession
#[allow(dead_code)]
pub fn extract_cv_f64(cv_params: &[CvParam], accession: &str) -> Option<f64> {
    cv_params
        .iter()
        .find(|p| p.accession == accession)
        .and_then(|p| p.value_as_f64())
}

/// Extract a CV parameter as i64 from a list by accession
#[allow(dead_code)]
pub fn extract_cv_i64(cv_params: &[CvParam], accession: &str) -> Option<i64> {
    cv_params
        .iter()
        .find(|p| p.accession == accession)
        .and_then(|p| p.value_as_i64())
}

/// Check if a CV parameter flag is present
#[allow(dead_code)]
pub fn has_cv_param(cv_params: &[CvParam], accession: &str) -> bool {
    cv_params.iter().any(|p| p.accession == accession)
}

/// Get activation method name from CV params
#[allow(dead_code)]
pub fn get_activation_method(cv_params: &[CvParam]) -> Option<String> {
    let activation_methods = [
        (MS_CV_ACCESSIONS::CID, "CID"),
        (MS_CV_ACCESSIONS::HCD, "HCD"),
        (MS_CV_ACCESSIONS::ETD, "ETD"),
        (MS_CV_ACCESSIONS::ECD, "ECD"),
        (MS_CV_ACCESSIONS::IRMPD, "IRMPD"),
        (MS_CV_ACCESSIONS::PHOTODISSOCIATION, "Photodissociation"),
    ];

    for (accession, name) in activation_methods {
        if has_cv_param(cv_params, accession) {
            return Some(name.to_string());
        }
    }

    None
}

/// Convert retention time to seconds based on unit
pub fn normalize_retention_time(value: f64, unit_accession: Option<&str>) -> f64 {
    match unit_accession {
        Some(MS_CV_ACCESSIONS::UNIT_MINUTE) => value * 60.0,
        Some(MS_CV_ACCESSIONS::UNIT_MILLISECOND) => value / 1000.0,
        _ => value, // Default to seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cv_param_value_parsing() {
        let param = CvParam {
            accession: "MS:1000511".to_string(),
            name: "ms level".to_string(),
            value: Some("2".to_string()),
            ..Default::default()
        };

        assert_eq!(param.value_as_i64(), Some(2));
        assert_eq!(param.value_as_f64(), Some(2.0));
    }

    #[test]
    fn test_extract_cv_value() {
        let params = vec![
            CvParam {
                accession: MS_CV_ACCESSIONS::MS_LEVEL.to_string(),
                value: Some("2".to_string()),
                ..Default::default()
            },
            CvParam {
                accession: MS_CV_ACCESSIONS::SCAN_START_TIME.to_string(),
                value: Some("123.456".to_string()),
                ..Default::default()
            },
        ];

        assert_eq!(
            extract_cv_value(&params, MS_CV_ACCESSIONS::MS_LEVEL),
            Some("2".to_string())
        );
        assert_eq!(
            extract_cv_f64(&params, MS_CV_ACCESSIONS::SCAN_START_TIME),
            Some(123.456)
        );
    }

    #[test]
    fn test_has_cv_param() {
        let params = vec![CvParam {
            accession: MS_CV_ACCESSIONS::CENTROID_SPECTRUM.to_string(),
            ..Default::default()
        }];

        assert!(has_cv_param(&params, MS_CV_ACCESSIONS::CENTROID_SPECTRUM));
        assert!(!has_cv_param(&params, MS_CV_ACCESSIONS::PROFILE_SPECTRUM));
    }
}
