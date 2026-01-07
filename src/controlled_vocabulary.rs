//! # HUPO-PSI Mass Spectrometry Controlled Vocabulary
//!
//! This module provides type-safe access to the HUPO-PSI Mass Spectrometry
//! Controlled Vocabulary (CV) terms. Using CV terms ensures global interoperability
//! as specified in the mzPeak whitepaper.
//!
//! ## Reference
//! - OBO file: https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo
//! - Documentation: https://github.com/HUPO-PSI/psi-ms-CV

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// A controlled vocabulary term with its accession and name
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CvTerm {
    /// CV accession (e.g., "MS:1000040")
    pub accession: String,
    /// Human-readable name
    pub name: String,
    /// Optional value associated with the term
    pub value: Option<String>,
    /// Optional unit accession for the value
    pub unit_accession: Option<String>,
    /// Optional unit name
    pub unit_name: Option<String>,
}

impl CvTerm {
    /// Create a new CV term with accession and name
    pub fn new(accession: &str, name: &str) -> Self {
        Self {
            accession: accession.to_string(),
            name: name.to_string(),
            value: None,
            unit_accession: None,
            unit_name: None,
        }
    }

    /// Add a value to the CV term
    pub fn with_value(mut self, value: impl ToString) -> Self {
        self.value = Some(value.to_string());
        self
    }

    /// Add a unit to the CV term value
    pub fn with_unit(mut self, unit_accession: &str, unit_name: &str) -> Self {
        self.unit_accession = Some(unit_accession.to_string());
        self.unit_name = Some(unit_name.to_string());
        self
    }
}

impl fmt::Display for CvTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            Some(v) => write!(f, "[{}: {}={}]", self.accession, self.name, v),
            None => write!(f, "[{}: {}]", self.accession, self.name),
        }
    }
}

/// Common MS CV terms used in mzPeak
pub mod ms_terms {
    use super::CvTerm;

    // =========================================================================
    // Spectrum-level terms
    // =========================================================================

    /// MS:1000511 - ms level
    pub fn ms_level(level: i16) -> CvTerm {
        CvTerm::new("MS:1000511", "ms level").with_value(level)
    }

    /// MS:1000016 - scan start time
    pub fn scan_start_time(time_seconds: f32) -> CvTerm {
        CvTerm::new("MS:1000016", "scan start time")
            .with_value(time_seconds)
            .with_unit("UO:0000010", "second")
    }

    /// MS:1000796 - spectrum title
    pub fn spectrum_title(title: &str) -> CvTerm {
        CvTerm::new("MS:1000796", "spectrum title").with_value(title)
    }

    /// MS:1000797 - peak list scans
    pub fn peak_list_scans(scans: i64) -> CvTerm {
        CvTerm::new("MS:1000797", "peak list scans").with_value(scans)
    }

    // =========================================================================
    // Polarity terms
    // =========================================================================

    /// MS:1000130 - positive scan
    pub fn positive_scan() -> CvTerm {
        CvTerm::new("MS:1000130", "positive scan")
    }

    /// MS:1000129 - negative scan
    pub fn negative_scan() -> CvTerm {
        CvTerm::new("MS:1000129", "negative scan")
    }

    /// MS:1000465 - scan polarity
    pub fn scan_polarity(is_positive: bool) -> CvTerm {
        if is_positive {
            positive_scan()
        } else {
            negative_scan()
        }
    }

    // =========================================================================
    // Peak data terms
    // =========================================================================

    /// MS:1000040 - m/z
    pub fn mz() -> CvTerm {
        CvTerm::new("MS:1000040", "m/z")
    }

    /// MS:1000042 - peak intensity
    pub fn peak_intensity() -> CvTerm {
        CvTerm::new("MS:1000042", "peak intensity")
    }

    // =========================================================================
    // Precursor terms
    // =========================================================================

    /// MS:1000744 - selected ion m/z
    pub fn selected_ion_mz(mz: f64) -> CvTerm {
        CvTerm::new("MS:1000744", "selected ion m/z")
            .with_value(mz)
            .with_unit("MS:1000040", "m/z")
    }

    /// MS:1000041 - charge state
    pub fn charge_state(charge: i16) -> CvTerm {
        CvTerm::new("MS:1000041", "charge state").with_value(charge)
    }

    /// MS:1000828 - isolation window lower offset
    pub fn isolation_window_lower_offset(offset: f32) -> CvTerm {
        CvTerm::new("MS:1000828", "isolation window lower offset")
            .with_value(offset)
            .with_unit("MS:1000040", "m/z")
    }

    /// MS:1000829 - isolation window upper offset
    pub fn isolation_window_upper_offset(offset: f32) -> CvTerm {
        CvTerm::new("MS:1000829", "isolation window upper offset")
            .with_value(offset)
            .with_unit("MS:1000040", "m/z")
    }

    // =========================================================================
    // Fragmentation terms
    // =========================================================================

    /// MS:1000045 - collision energy
    pub fn collision_energy(energy: f32) -> CvTerm {
        CvTerm::new("MS:1000045", "collision energy")
            .with_value(energy)
            .with_unit("UO:0000266", "electronvolt")
    }

    /// MS:1000133 - collision-induced dissociation
    pub fn cid() -> CvTerm {
        CvTerm::new("MS:1000133", "collision-induced dissociation")
    }

    /// MS:1000422 - beam-type collision-induced dissociation (HCD)
    pub fn hcd() -> CvTerm {
        CvTerm::new("MS:1000422", "beam-type collision-induced dissociation")
    }

    /// MS:1000598 - electron transfer dissociation
    pub fn etd() -> CvTerm {
        CvTerm::new("MS:1000598", "electron transfer dissociation")
    }

    // =========================================================================
    // Spectrum statistics
    // =========================================================================

    /// MS:1000285 - total ion current
    pub fn total_ion_current(tic: f64) -> CvTerm {
        CvTerm::new("MS:1000285", "total ion current").with_value(tic)
    }

    /// MS:1000504 - base peak m/z
    pub fn base_peak_mz(mz: f64) -> CvTerm {
        CvTerm::new("MS:1000504", "base peak m/z")
            .with_value(mz)
            .with_unit("MS:1000040", "m/z")
    }

    /// MS:1000505 - base peak intensity
    pub fn base_peak_intensity(intensity: f32) -> CvTerm {
        CvTerm::new("MS:1000505", "base peak intensity").with_value(intensity)
    }

    /// MS:1000927 - ion injection time
    pub fn ion_injection_time(time_ms: f32) -> CvTerm {
        CvTerm::new("MS:1000927", "ion injection time")
            .with_value(time_ms)
            .with_unit("UO:0000028", "millisecond")
    }

    // =========================================================================
    // Instrument terms
    // =========================================================================

    /// MS:1000031 - instrument model
    pub fn instrument_model(model: &str) -> CvTerm {
        CvTerm::new("MS:1000031", "instrument model").with_value(model)
    }

    /// MS:1000529 - instrument serial number
    pub fn instrument_serial_number(serial: &str) -> CvTerm {
        CvTerm::new("MS:1000529", "instrument serial number").with_value(serial)
    }

    /// MS:1000557 - Thermo Fisher Scientific instrument model
    pub fn thermo_instrument() -> CvTerm {
        CvTerm::new("MS:1000557", "Thermo Fisher Scientific instrument model")
    }

    /// MS:1000121 - SCIEX instrument model
    pub fn sciex_instrument() -> CvTerm {
        CvTerm::new("MS:1000121", "SCIEX instrument model")
    }

    /// MS:1000126 - Waters instrument model
    pub fn waters_instrument() -> CvTerm {
        CvTerm::new("MS:1000126", "Waters instrument model")
    }

    /// MS:1000122 - Bruker Daltonics instrument model
    pub fn bruker_instrument() -> CvTerm {
        CvTerm::new("MS:1000122", "Bruker Daltonics instrument model")
    }

    /// MS:1000123 - Agilent instrument model
    pub fn agilent_instrument() -> CvTerm {
        CvTerm::new("MS:1000123", "Agilent instrument model")
    }

    // =========================================================================
    // Mass analyzer terms
    // =========================================================================

    /// MS:1000484 - Orbitrap
    pub fn orbitrap() -> CvTerm {
        CvTerm::new("MS:1000484", "orbitrap")
    }

    /// MS:1000264 - ion trap
    pub fn ion_trap() -> CvTerm {
        CvTerm::new("MS:1000264", "ion trap")
    }

    /// MS:1000081 - quadrupole
    pub fn quadrupole() -> CvTerm {
        CvTerm::new("MS:1000081", "quadrupole")
    }

    /// MS:1000084 - time-of-flight
    pub fn tof() -> CvTerm {
        CvTerm::new("MS:1000084", "time-of-flight")
    }

    // =========================================================================
    // Data processing terms
    // =========================================================================

    /// MS:1000544 - Conversion to mzML
    pub fn conversion_to_mzml() -> CvTerm {
        CvTerm::new("MS:1000544", "Conversion to mzML")
    }

    /// MS:1000035 - peak picking
    pub fn peak_picking() -> CvTerm {
        CvTerm::new("MS:1000035", "peak picking")
    }

    /// MS:1000745 - retention time alignment
    pub fn retention_time_alignment() -> CvTerm {
        CvTerm::new("MS:1000745", "retention time alignment")
    }
}

/// Unit ontology terms commonly used with MS data
pub mod unit_terms {
    use super::CvTerm;

    /// UO:0000010 - second
    pub fn second() -> CvTerm {
        CvTerm::new("UO:0000010", "second")
    }

    /// UO:0000031 - minute
    pub fn minute() -> CvTerm {
        CvTerm::new("UO:0000031", "minute")
    }

    /// UO:0000028 - millisecond
    pub fn millisecond() -> CvTerm {
        CvTerm::new("UO:0000028", "millisecond")
    }

    /// UO:0000266 - electronvolt
    pub fn electronvolt() -> CvTerm {
        CvTerm::new("UO:0000266", "electronvolt")
    }

    /// UO:0000169 - parts per million
    pub fn ppm() -> CvTerm {
        CvTerm::new("UO:0000169", "parts per million")
    }

    /// UO:0000187 - percent
    pub fn percent() -> CvTerm {
        CvTerm::new("UO:0000187", "percent")
    }

    /// UO:0000175 - gram
    pub fn gram() -> CvTerm {
        CvTerm::new("UO:0000175", "gram")
    }

    /// UO:0000101 - bar (pressure)
    pub fn bar() -> CvTerm {
        CvTerm::new("UO:0000101", "bar")
    }

    /// UO:0000110 - pascal
    pub fn pascal() -> CvTerm {
        CvTerm::new("UO:0000110", "pascal")
    }
}

/// A parameter list containing multiple CV terms
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CvParamList {
    params: Vec<CvTerm>,
}

impl CvParamList {
    /// Create a new empty parameter list
    pub fn new() -> Self {
        Self { params: Vec::new() }
    }

    /// Add a CV term to the list
    pub fn add(&mut self, term: CvTerm) {
        self.params.push(term);
    }

    /// Add a CV term to the list (builder pattern)
    pub fn with(mut self, term: CvTerm) -> Self {
        self.add(term);
        self
    }

    /// Get a CV term by accession
    pub fn get(&self, accession: &str) -> Option<&CvTerm> {
        self.params.iter().find(|t| t.accession == accession)
    }

    /// Iterate over all CV terms
    pub fn iter(&self) -> impl Iterator<Item = &CvTerm> {
        self.params.iter()
    }

    /// Get the number of CV terms
    pub fn len(&self) -> usize {
        self.params.len()
    }

    /// Check if the list is empty
    pub fn is_empty(&self) -> bool {
        self.params.is_empty()
    }

    /// Convert to a HashMap for serialization to Parquet metadata
    pub fn to_metadata_map(&self) -> HashMap<String, String> {
        self.params
            .iter()
            .map(|t| {
                let value = t.value.clone().unwrap_or_default();
                (t.accession.clone(), format!("{}={}", t.name, value))
            })
            .collect()
    }
}

impl FromIterator<CvTerm> for CvParamList {
    fn from_iter<I: IntoIterator<Item = CvTerm>>(iter: I) -> Self {
        Self {
            params: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cv_term_creation() {
        let term = ms_terms::ms_level(2);
        assert_eq!(term.accession, "MS:1000511");
        assert_eq!(term.value, Some("2".to_string()));
    }

    #[test]
    fn test_cv_term_with_unit() {
        let term = ms_terms::scan_start_time(123.456);
        assert_eq!(term.unit_accession, Some("UO:0000010".to_string()));
        assert_eq!(term.unit_name, Some("second".to_string()));
    }

    #[test]
    fn test_cv_param_list() {
        let list = CvParamList::new()
            .with(ms_terms::ms_level(2))
            .with(ms_terms::scan_start_time(100.0));

        assert_eq!(list.len(), 2);
        assert!(list.get("MS:1000511").is_some());
    }
}
