//! Streaming readers, writers and native RAW conversion for the experimental mzpeak v2 layout.
//! See the README for the supported format and the breaking v2-only API.
pub mod chromatogram_writer;
pub mod controlled_vocabulary;
pub mod convert;
pub mod dataset;
mod formats;
pub mod metadata;
pub mod reader;
pub mod schema;
pub mod validator;
pub mod writer;
pub use formats::ingest;
#[cfg(feature = "mzml")]
pub use formats::mzml;
#[cfg(feature = "thermo")]
pub use formats::thermo;
#[cfg(feature = "tdf")]
pub use formats::{readers, tdf};
#[cfg(feature = "python")]
mod python;
