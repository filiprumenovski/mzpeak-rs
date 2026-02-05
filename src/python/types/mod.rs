//! Python-friendly data types for mzPeak
//!
//! Provides Python classes with property accessors for core mzPeak types.

mod chromatogram;
mod conversion;
mod file;
mod mobilogram;
mod peak;
mod spectrum;
mod spectrum_arrays;
mod spectrum_arrays_view;
mod v2;
mod writer;

pub use chromatogram::PyChromatogram;
pub use conversion::{PyConversionConfig, PyConversionStats, PyModality, PyOutputFormat, PyStreamingConfig};
pub use file::{PyFileMetadata, PyFileSummary};
pub use mobilogram::PyMobilogram;
pub use peak::PyPeak;
pub use spectrum::PySpectrum;
pub use spectrum_arrays::PySpectrumArrays;
pub use spectrum_arrays_view::PySpectrumArraysView;
pub(crate) use spectrum::build_peak_arrays;
pub use v2::{PyDatasetV2Stats, PyPeakArraysV2, PySpectrumMetadata, PySpectrumMetadataView, PySpectrumV2};
pub use writer::{PyWriterConfig, PyWriterStats};
