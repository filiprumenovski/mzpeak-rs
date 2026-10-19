//! Bruker TDF calibration and streaming v2 conversion.
pub mod converter;
pub mod error;
pub use converter::{TdfConversionConfig, TdfConversionStats, TdfConverter};
pub use error::TdfError;
