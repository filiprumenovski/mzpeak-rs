//! # Metadata Module for mzPeak
//!
//! This module handles the parsing and serialization of experimental metadata,
//! including SDRF-Proteomics metadata and lossless technical parameters from
//! vendor raw files.
//!
//! ## Design Goals
//!
//! As emphasized in the mzPeak whitepaper, comprehensive metadata is critical for:
//! - Regulatory compliance (precision medicine, chemical safety)
//! - Long-term data preservation and interpretability
//! - Multi-omics integration
//! - Reproducible science
//!
//! ## Metadata Categories
//!
//! 1. **SDRF Metadata**: Sample and experimental condition annotations following
//!    the SDRF-Proteomics standard (Dai et al., 2021)
//!
//! 2. **Instrument Configuration**: MS and LC settings from the instrument
//!
//! 3. **Run Parameters**: Technical details like pump pressures, temperatures,
//!    and other diagnostic data that vendors typically store but converters lose

mod error;
mod instrument;
mod lc;
mod mzpeak;
mod processing;
mod run;
mod sdrf;
mod source;
mod traces;

#[cfg(test)]
mod tests;

pub use error::MetadataError;
pub use instrument::{InstrumentConfig, MassAnalyzerConfig};
pub use lc::{ColumnInfo, GradientProgram, GradientStep, LcConfig, MobilePhase};
pub use mzpeak::MzPeakMetadata;
pub use processing::{ProcessingHistory, ProcessingStep};
pub use run::RunParameters;
pub use sdrf::SdrfMetadata;
pub use source::SourceFileInfo;
pub use traces::{PressureTrace, TemperatureTrace};
