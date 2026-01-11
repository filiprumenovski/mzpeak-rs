//! # mzPeak Schema Definition
//!
//! This module defines the Apache Arrow schema for the mzPeak "Long" table format.
//!
//! ## Design Rationale
//!
//! The "Long" format stores every individual peak as its own row, enabling Parquet's
//! Run-Length Encoding (RLE) to efficiently compress metadata that repeats across peaks
//! within the same spectrum. This is in contrast to a "Wide" format where arrays would
//! be stored as nested lists.
//!
//! ## Schema Columns
//!
//! | Column | Type | Description | CV Term |
//! |--------|------|-------------|---------|
//! | spectrum_id | Int64 | Unique spectrum identifier | MS:1000796 |
//! | scan_number | Int64 | Native scan number from instrument | MS:1000797 |
//! | ms_level | Int16 | MS level (1 for MS1, 2 for MS2, etc.) | MS:1000511 |
//! | retention_time | Float32 | RT in seconds | MS:1000016 |
//! | polarity | Int8 | 1 for positive, -1 for negative | MS:1000465/MS:1000129 |
//! | mz | Float64 | Mass-to-charge ratio | MS:1000040 |
//! | intensity | Float32 | Signal intensity | MS:1000042 |
//! | ion_mobility | Float64 (nullable) | Ion mobility drift time | MS:1002476 |
//! | precursor_mz | Float64 (nullable) | Precursor m/z for MS2+ | MS:1000744 |
//! | precursor_charge | Int16 (nullable) | Precursor charge state | MS:1000041 |
//! | precursor_intensity | Float32 (nullable) | Precursor intensity | MS:1000042 |
//! | isolation_window_lower | Float32 (nullable) | Lower isolation window offset | MS:1000828 |
//! | isolation_window_upper | Float32 (nullable) | Upper isolation window offset | MS:1000829 |
//! | collision_energy | Float32 (nullable) | Collision energy in eV | MS:1000045 |
//! | total_ion_current | Float64 (nullable) | TIC for the spectrum | MS:1000285 |
//! | base_peak_mz | Float64 (nullable) | Base peak m/z | MS:1000504 |
//! | base_peak_intensity | Float32 (nullable) | Base peak intensity | MS:1000505 |
//! | injection_time | Float32 (nullable) | Ion injection time in ms | MS:1000927 |
//! | pixel_x | Int32 (nullable) | X coordinate for MSI data | IMS:1000050 |
//! | pixel_y | Int32 (nullable) | Y coordinate for MSI data | IMS:1000051 |
//! | pixel_z | Int32 (nullable) | Z coordinate for 3D MSI data | IMS:1000052 |
//!
//! ## Compression Strategy
//!
//! By sorting data by spectrum_id, all peaks from the same spectrum are grouped together.
//! Metadata columns (ms_level, retention_time, precursor_mz, etc.) will have identical
//! values within each spectrum group, allowing RLE to achieve excellent compression ratios.

mod builders;
/// Chromatogram column name constants.
pub mod chromatogram_columns;
/// Peak table column name constants.
pub mod columns;
mod constants;
mod validation;

#[cfg(test)]
mod tests;

pub use builders::{
    create_chromatogram_schema, create_chromatogram_schema_arc, create_mzpeak_schema,
    create_mzpeak_schema_arc,
};
pub use chromatogram_columns::*;
pub use columns::*;
pub use constants::*;
pub use validation::{validate_schema, SchemaValidationError};
