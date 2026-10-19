//! Arrow field contracts for the normalized v2 container.
mod builders;
pub mod chromatogram_columns;
pub mod columns;
mod constants;
pub mod manifest;
pub mod spectra_columns;
pub use builders::{
    create_chromatogram_schema, create_chromatogram_schema_arc, create_peaks_schema_v2,
    create_peaks_schema_v2_arc,
};
pub use constants::*;
pub use manifest::{Manifest, Modality, VendorHints};
pub use spectra_columns::{create_spectra_schema, create_spectra_schema_arc};
