//! Bruker TimsTOF Data Format (TDF) parsing and conversion.
//!
//! # Features
//!
//! This module provides support for reading Bruker TimsTOF data files (`.d` directories)
//! and converting them to the mzpeak thin-waist `SpectrumArrays` format.
//!
//! Supported data types:
//! - **LC-TIMS-MS**: Liquid chromatography coupled TIMS (time-of-flight with ion mobility separation)
//! - **PASEF**: Parallel Acquisition Schemes For Enhanced multiplexing (DDA)
//! - **diaPASEF**: Data-independent PASEF (DIA)
//! - **MALDI-TIMS-MSI**: 4D MALDI imaging with spatial coordinates
//!
//! # Contract Compliance
//!
//! All TDF data is converted to [`crate::ingest::IngestSpectrum`] and validated through
//! [`crate::ingest::IngestSpectrumConverter`] to ensure:
//! - Contiguous `spectrum_id` from 0 (enforced by converter)
//! - Equal-length arrays for each spectrum (enforced by `PeakArrays`)
//! - Proper units: RT in seconds, m/z in Thomsons, ion mobility in milliseconds
//! - Optional fields clearly marked as `Option<T>`
//! - Explicit representation of missing data
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(feature = "tdf")]
//! # {
//! use mzpeak::tdf::TdfConverter;
//!
//! // Convert TDF dataset to spectrum arrays
//! let spectra = TdfConverter::try_convert("sample.d")?;
//!
//! println!("Spectra read: {}", spectra.spectrum_id.len());
//! println!("Total peaks: {}", spectra.peaks.mz.len());
//!
//! // Ion mobility is always present for TIMS data
//! match &spectra.peaks.ion_mobilities {
//!     mzpeak::writer::types::IonMobilityArrays::AllPresent(mobilities) => {
//!         println!("Ion mobility values: {}", mobilities.len());
//!     }
//!     _ => println!("No ion mobility data"),
//! }
//!
//! // Check for spatial coordinates (MALDI imaging)
//! if spectra.pixel_x.iter().any(|p| p.is_some()) {
//!     println!("This is imaging data with {} pixels", 
//!              spectra.pixel_x.iter().filter(|p| p.is_some()).count());
//! }
//! # }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Thin-Waist Mapping
//!
//! TDF fields are mapped to the thin-waist contract as follows:
//!
//! | TDF Source | IngestSpectrum Field | Notes |
//! |-----------|---------------------|-------|
//! | Frame.Id | spectrum_id | Contiguous from 0 |
//! | Frame.Id | scan_number | Native frame ID |
//! | Frame.MsMsType | ms_level | 1 for MS1, 2 for MS2 |
//! | Frame.Time | retention_time | In seconds |
//! | analysis.tdf_bin | mz_values, intensities | From peak data |
//! | TIMS domain converter | ion_mobility | 1/K₀ values, always present |
//! | Frames.SummedIntensities | tic | Total ion current |
//! | Frames.MaxIntensity | base_peak_intensity | Highest peak intensity |
//! | MaldiFrameInfo | pixel_x, pixel_y | For imaging data |
//! | Precursors | precursor_mz, charge | For MS2 spectra |
//! | PasefFrameMsMsInfo | isolation_mz, isolation_width | MS2 settings |
//!
//! # Feature Flag
//!
//! This module is only available when the `tdf` feature is enabled:
//!
//! ```toml
//! [dependencies]
//! mzpeak = { version = "0.1", features = ["tdf"] }
//! ```

pub mod converter;
pub mod error;

pub use converter::TdfConverter;
pub use error::TdfError;
