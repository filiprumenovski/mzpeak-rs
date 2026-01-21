//! Format-specific parsers and converters.
//!
//! This module contains implementations for reading various mass spectrometry
//! file formats and converting them to the mzPeak format:
//!
//! - [`mzml`] - mzML/imzML XML format (HUPO-PSI standard)
//! - [`tdf`] - Bruker TimsTOF .d format
//! - [`thermo`] - Thermo RAW format (requires .NET 8 runtime)
//!
//! The [`ingest`] module provides a common interface for format-agnostic
//! spectrum ingestion.

/// Common spectrum ingestion interface.
pub mod ingest;

#[cfg(feature = "mzml")]
/// mzML/imzML format parser and converter.
pub mod mzml;

#[cfg(feature = "tdf")]
/// Bruker TimsTOF .d format reader.
pub mod tdf;

#[cfg(feature = "tdf")]
/// TDF reader utilities (raw frames + streamers).
pub mod readers;

#[cfg(feature = "thermo")]
/// Thermo RAW file reader (requires .NET 8 runtime).
pub mod thermo;
