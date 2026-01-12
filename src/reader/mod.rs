//! # mzPeak Reader Module
//!
//! This module provides functionality for reading mzPeak files and querying
//! mass spectrometry data efficiently.
//!
//! ## Features
//!
//! - **Random Access**: Query spectra by ID, retention time range, or m/z range
//! - **Streaming Iteration**: Memory-efficient iteration over large files
//! - **Container Support**: Read both ZIP container (`.mzpeak`) and directory formats
//! - **Metadata Access**: Retrieve embedded metadata from Parquet footer
//!
//! ## Example
//!
//! ```rust,no_run
//! use mzpeak::reader::MzPeakReader;
//!
//! // Open a file
//! let reader = MzPeakReader::open("data.mzpeak")?;
//!
//! // Get metadata
//! println!("Format version: {}", reader.metadata().format_version);
//!
//! // Query spectra by retention time range
//! for spectrum in reader.spectra_by_rt_range(60.0, 120.0)? {
//!     println!("Spectrum {}: {} peaks", spectrum.spectrum_id, spectrum.peaks.len());
//! }
//!
//! // Get a specific spectrum by ID
//! if let Some(spectrum) = reader.get_spectrum(42)? {
//!     println!("Found spectrum 42 with {} peaks", spectrum.peaks.len());
//! }
//! # Ok::<(), mzpeak::reader::ReaderError>(())
//! ```

mod batches;
mod config;
mod error;
mod metadata;
mod open;
mod spectra;
mod subfiles;
mod summary;
mod utils;
pub mod zip_chunk_reader;

#[cfg(test)]
mod tests;

pub use batches::RecordBatchIterator;
pub use config::ReaderConfig;
pub use error::ReaderError;
pub use metadata::FileMetadata;
pub use spectra::{
    SpectrumArraysIterator, SpectrumIterator, StreamingSpectrumArraysIterator,
    StreamingSpectrumIterator,
};
pub use summary::FileSummary;
pub use zip_chunk_reader::{SharedZipEntryReader, ZipEntryChunkReader};

use config::ReaderSource;

/// Reader for mzPeak files
///
/// Supports both ZIP container format (`.mzpeak`) and legacy directory/single-file formats.
pub struct MzPeakReader {
    source: ReaderSource,
    config: ReaderConfig,
    file_metadata: FileMetadata,
}
