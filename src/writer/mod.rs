//! # mzPeak Writer Module
//!
//! This module provides the core functionality for writing mass spectrometry data
//! to the mzPeak Parquet format.
//!
//! ## Design Principles
//!
//! 1. **Streaming Architecture**: Data is written in batches to handle large files
//!    without loading everything into memory.
//!
//! 2. **RLE Optimization**: Data is sorted and grouped by spectrum_id to maximize
//!    Run-Length Encoding compression on repeated metadata.
//!
//! 3. **Self-Contained Files**: All metadata (SDRF, instrument config, etc.) is
//!    embedded in the Parquet footer's key_value_metadata.
//!
//! 4. **Configurable Compression**: Supports ZSTD (default), Snappy, and uncompressed.

mod config;
mod error;
mod rolling;
mod stats;
mod types;
mod writer_impl;

#[cfg(test)]
mod tests;

pub use config::{CompressionType, WriterConfig};
pub use error::WriterError;
pub use rolling::{RollingWriter, RollingWriterStats};
pub use stats::WriterStats;
pub use types::{
    ColumnarBatch, OptionalColumn, OptionalColumnBuf, OwnedColumnarBatch, PeakArrays, SpectrumArrays,
};
pub use writer_impl::MzPeakWriter;
