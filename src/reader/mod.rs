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
//! - **v2 Format Support**: Read normalized two-table format (spectra + peaks)
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
//! // Query spectra by retention time range (SoA view)
//! for spectrum in reader.spectra_by_rt_range_arrays(60.0, 120.0)? {
//!     println!("Spectrum {}: {} peaks", spectrum.spectrum_id, spectrum.peak_count());
//! }
//!
//! // Get a specific spectrum by ID (SoA view)
//! if let Some(spectrum) = reader.get_spectrum_arrays(42)? {
//!     println!("Found spectrum 42 with {} peaks", spectrum.peak_count());
//! }
//!
//! // For v2 containers, access spectrum metadata separately
//! if reader.has_spectra_table() {
//!     for meta in reader.iter_spectra_metadata()? {
//!         let meta = meta?;
//!         println!("Spectrum {}: RT={:.2}s, {} peaks",
//!             meta.spectrum_id, meta.retention_time, meta.peak_count);
//!     }
//! }
//! # Ok::<(), mzpeak::reader::ReaderError>(())
//! ```

mod batches;
mod config;
mod error;
mod metadata;
mod open;
mod spectra;
mod spectra_v2;
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
pub use spectra::{SpectrumArraysView, StreamingSpectrumArraysViewIterator};
pub use spectra_v2::{SpectrumMetadataIterator, SpectrumMetadataView};
pub use summary::FileSummary;
pub use zip_chunk_reader::{SharedZipEntryReader, ZipEntryChunkReader};

use config::ReaderSource;

/// Reader for mzPeak files
///
/// Supports both ZIP container format (`.mzpeak`) and legacy directory/single-file formats.
/// Also supports v2 format with separate spectra and peaks tables.
pub struct MzPeakReader {
    source: ReaderSource,
    config: ReaderConfig,
    file_metadata: FileMetadata,
}
