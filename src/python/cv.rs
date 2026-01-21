//! Python bindings for Controlled Vocabulary utilities
//!
//! This module exposes the HUPO-PSI Mass Spectrometry Controlled Vocabulary (CV)
//! terms and utilities, enabling Python users to create standards-compliant
//! metadata annotations.

use pyo3::prelude::*;

use crate::controlled_vocabulary::{ms_terms, unit_terms, CvParamList, CvTerm};

/// A controlled vocabulary term with accession and name.
#[pyclass(name = "CvTerm")]
#[derive(Clone)]
pub struct PyCvTerm {
    inner: CvTerm,
}

#[pymethods]
impl PyCvTerm {
    /// Create a new CV term with accession and name.
    ///
    /// Args:
    ///     accession: CV accession (e.g., "MS:1000040")
    ///     name: Human-readable name
    #[new]
    fn new(accession: String, name: String) -> Self {
        Self {
            inner: CvTerm::new(&accession, &name),
        }
    }

    /// CV accession (e.g., "MS:1000040")
    #[getter]
    fn accession(&self) -> String {
        self.inner.accession.clone()
    }

    /// Human-readable name
    #[getter]
    fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// Optional value associated with the term
    #[getter]
    fn value(&self) -> Option<String> {
        self.inner.value.clone()
    }

    /// Optional unit accession for the value
    #[getter]
    fn unit_accession(&self) -> Option<String> {
        self.inner.unit_accession.clone()
    }

    /// Optional unit name
    #[getter]
    fn unit_name(&self) -> Option<String> {
        self.inner.unit_name.clone()
    }

    /// Add a value to the CV term, returning a new term.
    fn with_value(&self, value: String) -> Self {
        Self {
            inner: self.inner.clone().with_value(value),
        }
    }

    /// Add a unit to the CV term value, returning a new term.
    fn with_unit(&self, unit_accession: String, unit_name: String) -> Self {
        Self {
            inner: self.inner.clone().with_unit(&unit_accession, &unit_name),
        }
    }

    fn __repr__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }
}

impl From<CvTerm> for PyCvTerm {
    fn from(term: CvTerm) -> Self {
        Self { inner: term }
    }
}

impl From<PyCvTerm> for CvTerm {
    fn from(py_term: PyCvTerm) -> Self {
        py_term.inner
    }
}

/// A parameter list containing multiple CV terms.
#[pyclass(name = "CvParamList")]
#[derive(Clone)]
pub struct PyCvParamList {
    inner: CvParamList,
}

#[pymethods]
impl PyCvParamList {
    /// Create a new empty parameter list.
    #[new]
    fn new() -> Self {
        Self {
            inner: CvParamList::new(),
        }
    }

    /// Add a CV term to the list.
    fn add(&mut self, term: PyCvTerm) {
        self.inner.add(term.inner);
    }

    /// Get a CV term by accession.
    fn get(&self, accession: &str) -> Option<PyCvTerm> {
        self.inner.get(accession).cloned().map(PyCvTerm::from)
    }

    /// Get all CV terms as a list.
    fn terms(&self) -> Vec<PyCvTerm> {
        self.inner.iter().cloned().map(PyCvTerm::from).collect()
    }

    /// Check if the list is empty.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        format!("CvParamList(terms={})", self.inner.len())
    }
}

// ============================================================================
// MS CV Term factory functions
// ============================================================================

/// MS CV terms submodule
#[pyclass(name = "MsTerms")]
pub struct PyMsTerms;

#[pymethods]
impl PyMsTerms {
    /// MS:1000511 - ms level
    #[staticmethod]
    fn ms_level(level: i16) -> PyCvTerm {
        PyCvTerm::from(ms_terms::ms_level(level))
    }

    /// MS:1000016 - scan start time (in seconds)
    #[staticmethod]
    fn scan_start_time(time_seconds: f32) -> PyCvTerm {
        PyCvTerm::from(ms_terms::scan_start_time(time_seconds))
    }

    /// MS:1000796 - spectrum title
    #[staticmethod]
    fn spectrum_title(title: &str) -> PyCvTerm {
        PyCvTerm::from(ms_terms::spectrum_title(title))
    }

    /// MS:1000130 - positive scan
    #[staticmethod]
    fn positive_scan() -> PyCvTerm {
        PyCvTerm::from(ms_terms::positive_scan())
    }

    /// MS:1000129 - negative scan
    #[staticmethod]
    fn negative_scan() -> PyCvTerm {
        PyCvTerm::from(ms_terms::negative_scan())
    }

    /// MS:1000465 - scan polarity (returns positive or negative based on flag)
    #[staticmethod]
    fn scan_polarity(is_positive: bool) -> PyCvTerm {
        PyCvTerm::from(ms_terms::scan_polarity(is_positive))
    }

    /// MS:1000040 - m/z
    #[staticmethod]
    fn mz() -> PyCvTerm {
        PyCvTerm::from(ms_terms::mz())
    }

    /// MS:1000042 - peak intensity
    #[staticmethod]
    fn peak_intensity() -> PyCvTerm {
        PyCvTerm::from(ms_terms::peak_intensity())
    }

    /// MS:1000744 - selected ion m/z
    #[staticmethod]
    fn selected_ion_mz(mz: f64) -> PyCvTerm {
        PyCvTerm::from(ms_terms::selected_ion_mz(mz))
    }

    /// MS:1000041 - charge state
    #[staticmethod]
    fn charge_state(charge: i16) -> PyCvTerm {
        PyCvTerm::from(ms_terms::charge_state(charge))
    }

    /// MS:1000828 - isolation window lower offset
    #[staticmethod]
    fn isolation_window_lower_offset(offset: f32) -> PyCvTerm {
        PyCvTerm::from(ms_terms::isolation_window_lower_offset(offset))
    }

    /// MS:1000829 - isolation window upper offset
    #[staticmethod]
    fn isolation_window_upper_offset(offset: f32) -> PyCvTerm {
        PyCvTerm::from(ms_terms::isolation_window_upper_offset(offset))
    }

    /// MS:1000045 - collision energy (in eV)
    #[staticmethod]
    fn collision_energy(energy: f32) -> PyCvTerm {
        PyCvTerm::from(ms_terms::collision_energy(energy))
    }

    /// MS:1000133 - collision-induced dissociation (CID)
    #[staticmethod]
    fn cid() -> PyCvTerm {
        PyCvTerm::from(ms_terms::cid())
    }

    /// MS:1000422 - beam-type collision-induced dissociation (HCD)
    #[staticmethod]
    fn hcd() -> PyCvTerm {
        PyCvTerm::from(ms_terms::hcd())
    }

    /// MS:1000598 - electron transfer dissociation (ETD)
    #[staticmethod]
    fn etd() -> PyCvTerm {
        PyCvTerm::from(ms_terms::etd())
    }

    /// MS:1000285 - total ion current
    #[staticmethod]
    fn total_ion_current(tic: f64) -> PyCvTerm {
        PyCvTerm::from(ms_terms::total_ion_current(tic))
    }

    /// MS:1000504 - base peak m/z
    #[staticmethod]
    fn base_peak_mz(mz: f64) -> PyCvTerm {
        PyCvTerm::from(ms_terms::base_peak_mz(mz))
    }

    /// MS:1000505 - base peak intensity
    #[staticmethod]
    fn base_peak_intensity(intensity: f32) -> PyCvTerm {
        PyCvTerm::from(ms_terms::base_peak_intensity(intensity))
    }

    /// MS:1000927 - ion injection time (in ms)
    #[staticmethod]
    fn ion_injection_time(time_ms: f32) -> PyCvTerm {
        PyCvTerm::from(ms_terms::ion_injection_time(time_ms))
    }

    /// MS:1000031 - instrument model
    #[staticmethod]
    fn instrument_model(model: &str) -> PyCvTerm {
        PyCvTerm::from(ms_terms::instrument_model(model))
    }

    /// MS:1000529 - instrument serial number
    #[staticmethod]
    fn instrument_serial_number(serial: &str) -> PyCvTerm {
        PyCvTerm::from(ms_terms::instrument_serial_number(serial))
    }

    /// MS:1000557 - Thermo Fisher Scientific instrument model
    #[staticmethod]
    fn thermo_instrument() -> PyCvTerm {
        PyCvTerm::from(ms_terms::thermo_instrument())
    }

    /// MS:1000121 - SCIEX instrument model
    #[staticmethod]
    fn sciex_instrument() -> PyCvTerm {
        PyCvTerm::from(ms_terms::sciex_instrument())
    }

    /// MS:1000126 - Waters instrument model
    #[staticmethod]
    fn waters_instrument() -> PyCvTerm {
        PyCvTerm::from(ms_terms::waters_instrument())
    }

    /// MS:1000122 - Bruker Daltonics instrument model
    #[staticmethod]
    fn bruker_instrument() -> PyCvTerm {
        PyCvTerm::from(ms_terms::bruker_instrument())
    }

    /// MS:1000123 - Agilent instrument model
    #[staticmethod]
    fn agilent_instrument() -> PyCvTerm {
        PyCvTerm::from(ms_terms::agilent_instrument())
    }

    /// MS:1000484 - Orbitrap
    #[staticmethod]
    fn orbitrap() -> PyCvTerm {
        PyCvTerm::from(ms_terms::orbitrap())
    }

    /// MS:1000264 - ion trap
    #[staticmethod]
    fn ion_trap() -> PyCvTerm {
        PyCvTerm::from(ms_terms::ion_trap())
    }

    /// MS:1000081 - quadrupole
    #[staticmethod]
    fn quadrupole() -> PyCvTerm {
        PyCvTerm::from(ms_terms::quadrupole())
    }

    /// MS:1000084 - time-of-flight
    #[staticmethod]
    fn tof() -> PyCvTerm {
        PyCvTerm::from(ms_terms::tof())
    }

    /// MS:1000544 - Conversion to mzML
    #[staticmethod]
    fn conversion_to_mzml() -> PyCvTerm {
        PyCvTerm::from(ms_terms::conversion_to_mzml())
    }

    /// MS:1000035 - peak picking
    #[staticmethod]
    fn peak_picking() -> PyCvTerm {
        PyCvTerm::from(ms_terms::peak_picking())
    }

    /// MS:1000745 - retention time alignment
    #[staticmethod]
    fn retention_time_alignment() -> PyCvTerm {
        PyCvTerm::from(ms_terms::retention_time_alignment())
    }
}

/// Unit ontology terms submodule
#[pyclass(name = "UnitTerms")]
pub struct PyUnitTerms;

#[pymethods]
impl PyUnitTerms {
    /// UO:0000010 - second
    #[staticmethod]
    fn second() -> PyCvTerm {
        PyCvTerm::from(unit_terms::second())
    }

    /// UO:0000031 - minute
    #[staticmethod]
    fn minute() -> PyCvTerm {
        PyCvTerm::from(unit_terms::minute())
    }

    /// UO:0000028 - millisecond
    #[staticmethod]
    fn millisecond() -> PyCvTerm {
        PyCvTerm::from(unit_terms::millisecond())
    }

    /// UO:0000266 - electronvolt
    #[staticmethod]
    fn electronvolt() -> PyCvTerm {
        PyCvTerm::from(unit_terms::electronvolt())
    }

    /// UO:0000169 - parts per million
    #[staticmethod]
    fn ppm() -> PyCvTerm {
        PyCvTerm::from(unit_terms::ppm())
    }

    /// UO:0000187 - percent
    #[staticmethod]
    fn percent() -> PyCvTerm {
        PyCvTerm::from(unit_terms::percent())
    }

    /// UO:0000175 - gram
    #[staticmethod]
    fn gram() -> PyCvTerm {
        PyCvTerm::from(unit_terms::gram())
    }

    /// UO:0000101 - bar (pressure)
    #[staticmethod]
    fn bar() -> PyCvTerm {
        PyCvTerm::from(unit_terms::bar())
    }

    /// UO:0000110 - pascal
    #[staticmethod]
    fn pascal() -> PyCvTerm {
        PyCvTerm::from(unit_terms::pascal())
    }
}
