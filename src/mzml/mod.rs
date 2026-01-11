//! # mzML Parser Module
//!
//! This module provides streaming parsing of mzML files, the XML-based community
//! standard for mass spectrometry data defined by HUPO-PSI.
//!
//! ## Design Goals
//!
//! - **Streaming**: Process arbitrarily large files without loading into memory
//! - **Lossless**: Preserve numerical precision (32/64-bit) from source
//! - **Complete**: Extract all CV parameters and metadata
//! - **Efficient**: Use pull-parsing for minimal memory footprint
//!
//! ## mzML Structure
//!
//! ```text
//! indexedmzML (optional wrapper)
//! └── mzML
//!     ├── cvList (controlled vocabularies)
//!     ├── fileDescription
//!     │   ├── fileContent
//!     │   └── sourceFileList
//!     ├── softwareList
//!     ├── instrumentConfigurationList
//!     ├── dataProcessingList
//!     └── run
//!         ├── spectrumList
//!         │   └── spectrum* (many)
//!         │       ├── cvParam*
//!         │       ├── scanList
//!         │       ├── precursorList (for MS2+)
//!         │       └── binaryDataArrayList
//!         │           └── binaryDataArray*
//!         │               ├── cvParam* (encoding info)
//!         │               └── binary (base64 data)
//!         └── chromatogramList (optional)
//! ```

mod binary;
mod cv_params;
mod models;
mod streamer;
pub mod converter;

#[cfg(feature = "parallel-decode")]
pub mod simd;

pub use binary::{BinaryDecoder, BinaryEncoding, CompressionType as BinaryCompression};
pub use cv_params::{CvParam, extract_cv_value, MS_CV_ACCESSIONS};
pub use models::*;
pub use streamer::{MzMLStreamer, MzMLError, SpectrumIterator, RawSpectrumIterator};
pub use converter::{MzMLConverter, ConversionConfig, ConversionStats};
