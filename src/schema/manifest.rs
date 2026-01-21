//! Manifest schema for mzPeak v2.0 container format.
//!
//! The manifest.json file declares the schema version and modality flags,
//! enabling readers to understand the data structure before parsing.

use serde::{Deserialize, Serialize};

// Re-export VendorHints from metadata module to avoid duplication
pub use crate::metadata::VendorHints;

/// Data modality determining which optional columns are present
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Modality {
    /// LC-MS: 3D data (RT, m/z, intensity)
    LcMs,
    /// LC-IMS-MS: 4D data (RT, m/z, intensity, ion_mobility)
    LcImsMs,
    /// MSI: Mass spectrometry imaging without ion mobility
    Msi,
    /// MSI-IMS: Mass spectrometry imaging with ion mobility
    MsiIms,
}

impl Modality {
    /// Returns true if this modality includes ion mobility data.
    #[inline]
    pub fn has_ion_mobility(&self) -> bool {
        matches!(self, Modality::LcImsMs | Modality::MsiIms)
    }

    /// Returns true if this modality includes imaging data.
    #[inline]
    pub fn has_imaging(&self) -> bool {
        matches!(self, Modality::Msi | Modality::MsiIms)
    }

    /// Determines the modality from boolean flags.
    ///
    /// # Arguments
    /// * `has_ion_mobility` - Whether the data includes ion mobility measurements
    /// * `has_imaging` - Whether the data includes spatial (imaging) coordinates
    ///
    /// # Returns
    /// The appropriate `Modality` variant based on the flags.
    pub fn from_flags(has_ion_mobility: bool, has_imaging: bool) -> Self {
        match (has_ion_mobility, has_imaging) {
            (false, false) => Modality::LcMs,
            (true, false) => Modality::LcImsMs,
            (false, true) => Modality::Msi,
            (true, true) => Modality::MsiIms,
        }
    }
}

/// Manifest for mzPeak v2.0 container format.
///
/// The manifest provides essential metadata about the mzPeak container,
/// including the data modality, counts, and provenance information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Format version (e.g., "2.0")
    pub format_version: String,
    /// Schema version for the Parquet tables (e.g., "2.0")
    pub schema_version: String,
    /// Data modality (LC-MS, LC-IMS-MS, MSI, or MSI-IMS)
    pub modality: Modality,
    /// Whether the data includes ion mobility measurements
    pub has_ion_mobility: bool,
    /// Whether the data includes imaging (spatial) coordinates
    pub has_imaging: bool,
    /// Whether precursor information is present (MS2+ data)
    pub has_precursor_info: bool,
    /// Total number of spectra in the container
    pub spectrum_count: u64,
    /// Total number of peaks across all spectra
    pub peak_count: u64,
    /// ISO 8601 timestamp of when the file was created
    pub created: String,
    /// Name and version of the converter that created the file
    pub converter: String,
    /// Vendor hints for files converted via intermediate formats
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vendor_hints: Option<VendorHints>,
    /// Optional hash of the schema for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
}

impl Manifest {
    /// Creates a new manifest with the specified parameters.
    ///
    /// # Arguments
    /// * `modality` - The data modality (LC-MS, LC-IMS-MS, MSI, or MSI-IMS)
    /// * `has_precursor_info` - Whether precursor information is present (for MS2+ data)
    /// * `spectrum_count` - Total number of spectra in the container
    /// * `peak_count` - Total number of peaks across all spectra
    /// * `created` - ISO 8601 timestamp of when the file was created
    /// * `converter` - Name and version of the converter that created the file
    ///
    /// # Returns
    /// A new `Manifest` instance with the v2.0 format and schema versions.
    pub fn new(
        modality: Modality,
        has_precursor_info: bool,
        spectrum_count: u64,
        peak_count: u64,
        created: String,
        converter: String,
    ) -> Self {
        Self {
            format_version: "2.0".to_string(),
            schema_version: "2.0".to_string(),
            modality,
            has_ion_mobility: modality.has_ion_mobility(),
            has_imaging: modality.has_imaging(),
            has_precursor_info,
            spectrum_count,
            peak_count,
            created,
            converter,
            vendor_hints: None,
            schema_hash: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_modality_has_ion_mobility() {
        assert!(!Modality::LcMs.has_ion_mobility());
        assert!(Modality::LcImsMs.has_ion_mobility());
        assert!(!Modality::Msi.has_ion_mobility());
        assert!(Modality::MsiIms.has_ion_mobility());
    }

    #[test]
    fn test_modality_has_imaging() {
        assert!(!Modality::LcMs.has_imaging());
        assert!(!Modality::LcImsMs.has_imaging());
        assert!(Modality::Msi.has_imaging());
        assert!(Modality::MsiIms.has_imaging());
    }

    #[test]
    fn test_modality_from_flags() {
        assert_eq!(Modality::from_flags(false, false), Modality::LcMs);
        assert_eq!(Modality::from_flags(true, false), Modality::LcImsMs);
        assert_eq!(Modality::from_flags(false, true), Modality::Msi);
        assert_eq!(Modality::from_flags(true, true), Modality::MsiIms);
    }

    #[test]
    fn test_manifest_new() {
        let manifest = Manifest::new(
            Modality::LcImsMs,
            true,
            1000,
            500000,
            "2024-01-01T00:00:00Z".to_string(),
            "mzpeak-rs v2.0.0".to_string(),
        );

        assert_eq!(manifest.format_version, "2.0");
        assert_eq!(manifest.schema_version, "2.0");
        assert_eq!(manifest.modality, Modality::LcImsMs);
        assert!(manifest.has_ion_mobility);
        assert!(!manifest.has_imaging);
        assert!(manifest.has_precursor_info);
        assert_eq!(manifest.spectrum_count, 1000);
        assert_eq!(manifest.peak_count, 500000);
        assert!(manifest.vendor_hints.is_none());
        assert!(manifest.schema_hash.is_none());
    }

    #[test]
    fn test_manifest_serialization() {
        let manifest = Manifest::new(
            Modality::LcMs,
            false,
            100,
            10000,
            "2024-01-01T00:00:00Z".to_string(),
            "mzpeak-rs".to_string(),
        );

        let json = serde_json::to_string(&manifest).unwrap();
        let deserialized: Manifest = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.modality, Modality::LcMs);
        assert_eq!(deserialized.spectrum_count, 100);
    }

    #[test]
    fn test_modality_kebab_case_serialization() {
        assert_eq!(
            serde_json::to_string(&Modality::LcMs).unwrap(),
            "\"lc-ms\""
        );
        assert_eq!(
            serde_json::to_string(&Modality::LcImsMs).unwrap(),
            "\"lc-ims-ms\""
        );
        assert_eq!(serde_json::to_string(&Modality::Msi).unwrap(), "\"msi\"");
        assert_eq!(
            serde_json::to_string(&Modality::MsiIms).unwrap(),
            "\"msi-ims\""
        );
    }
}
