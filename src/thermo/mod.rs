//! Thermo RAW file format support for mzPeak.
//!
//! This module provides streaming access to Thermo Fisher RAW files using the
//! `thermorawfilereader` crate, which wraps the official .NET RawFileReader library.
//!
//! # Requirements
//!
//! - .NET 8 runtime must be installed on the system
//! - Thermo's RawFileReader license terms apply (bundled via thermorawfilereader)
//!
//! # Platform Support
//!
//! | Platform         | Support Status |
//! |------------------|----------------|
//! | Windows x86_64   | ✅ Full        |
//! | Linux x86_64     | ✅ Full        |
//! | macOS x86_64     | ✅ Full        |
//! | macOS ARM64      | ❌ Not supported - Thermo's RawFileReader .NET assemblies require x86 |
//! | Linux ARM64      | ❌ Not supported |
//!
//! On unsupported platforms, file opening will fail with a `PlatformNotSupported` error.
//!
//! # Example
//!
//! ```no_run
//! use mzpeak::thermo::{ThermoStreamer, ThermoConverter};
//! use mzpeak::ingest::IngestSpectrumConverter;
//!
//! let mut streamer = ThermoStreamer::new("sample.raw", 1000)?;
//! let converter = ThermoConverter::default();
//! let mut ingest_converter = IngestSpectrumConverter::new();
//!
//! while let Some(batch) = streamer.next_batch()? {
//!     for (spectrum_id, raw) in batch.into_iter().enumerate() {
//!         let ingest = converter.convert_spectrum(raw, spectrum_id as i64)?;
//!         let spectrum = ingest_converter.convert(ingest)?;
//!         // Process spectrum...
//!     }
//! }
//! # Ok::<(), mzpeak::thermo::ThermoError>(())
//! ```

pub mod error;
pub mod converter;
pub mod streamer;

pub use error::ThermoError;
pub use converter::ThermoConverter;
pub use streamer::ThermoStreamer;
