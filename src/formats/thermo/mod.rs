//! Thermo RAW ingestion through the native Rust `rawr-ms` reader.
//!
//! Enable `thermo` to decode source-saved centroids without .NET or Rosetta.
//! Unsupported RAW generations or packets fail explicitly. This selects the
//! saved centroid representation; it does not perform vendor peak picking or
//! preserve the RAW file's profile arrays, auxiliary devices or every trailer.
//!
//! ```no_run
//! use mzpeak::thermo::ThermoStreamer;
//! use mzpeak::ingest::IngestSpectrumConverter;
//!
//! let mut streamer = ThermoStreamer::new("sample.raw", 1000)?;
//! let mut converter = IngestSpectrumConverter::new();
//! while let Some(batch) = streamer.next_batch()? {
//!     for ingest in batch {
//!         let spectrum = converter.convert(ingest)?;
//!         // Write spectrum through either mzPeak writer.
//!     }
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod convert;

pub mod error;
pub mod streamer;

pub use convert::{
    convert_thermo, ThermoConversionConfig, ThermoConversionStats, RAWR_MS_REVISION,
};
pub use error::ThermoError;
pub use streamer::ThermoStreamer;
