//! Python exception types for mzPeak
//!
//! Maps Rust error types to appropriate Python exceptions with full error chain preservation.
//!
//! ## Error Chain Preservation (Issue 009 Fix)
//!
//! All error conversions now include the full error chain via `format_error_chain()`,
//! which walks the `std::error::Error::source()` chain and formats nested causes.
//! This ensures Python users see the complete diagnostic context.

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;

use crate::dataset::DatasetError;
use crate::formats::ingest::IngestError;
use crate::mzml::converter::ConversionError;
use crate::reader::ReaderError;
use crate::writer::WriterError;

// Define custom exception hierarchy
create_exception!(mzpeak, MzPeakException, PyException, "Base exception for all mzPeak errors.");
create_exception!(mzpeak, MzPeakIOError, MzPeakException, "I/O error during file operations.");
create_exception!(mzpeak, MzPeakFormatError, MzPeakException, "Invalid or corrupted file format.");
create_exception!(mzpeak, MzPeakValidationError, MzPeakException, "Data validation failed.");

/// Format an error with its full causal chain.
///
/// Walks the `std::error::Error::source()` chain and formats all nested causes.
/// This preserves diagnostic context that would otherwise be lost when converting
/// Rust errors to Python exceptions.
///
/// # Example Output
/// ```text
/// I/O error reading file: No such file or directory (os error 2)
///   caused by: No such file or directory (os error 2)
/// ```
fn format_error_chain<E: std::error::Error>(err: &E) -> String {
    let mut msg = err.to_string();
    let mut current: &dyn std::error::Error = err;

    while let Some(source) = current.source() {
        msg.push_str("\n  caused by: ");
        msg.push_str(&source.to_string());
        current = source;
    }

    msg
}

/// Convert ReaderError to Python exception with full error chain
impl From<ReaderError> for PyErr {
    fn from(err: ReaderError) -> Self {
        let msg = format_error_chain(&err);
        match &err {
            ReaderError::IoError(_) => MzPeakIOError::new_err(msg),
            ReaderError::ParquetError(_) => MzPeakFormatError::new_err(msg),
            ReaderError::ArrowError(_) => MzPeakFormatError::new_err(msg),
            ReaderError::InvalidFormat(_) => MzPeakFormatError::new_err(msg),
            ReaderError::ZipError(_) => MzPeakIOError::new_err(msg),
            ReaderError::MetadataError(_) => MzPeakFormatError::new_err(msg),
            ReaderError::ColumnNotFound(_) => MzPeakFormatError::new_err(msg),
            ReaderError::JsonError(_) => MzPeakFormatError::new_err(msg),
        }
    }
}

/// Convert WriterError to Python exception with full error chain
impl From<WriterError> for PyErr {
    fn from(err: WriterError) -> Self {
        let msg = format_error_chain(&err);
        match &err {
            WriterError::IoError(_) => MzPeakIOError::new_err(msg),
            WriterError::ArrowError(_) => MzPeakFormatError::new_err(msg),
            WriterError::ParquetError(_) => MzPeakFormatError::new_err(msg),
            WriterError::MetadataError(_) => MzPeakFormatError::new_err(msg),
            WriterError::InvalidData(_) => MzPeakValidationError::new_err(msg),
            WriterError::NotInitialized => MzPeakException::new_err(msg),
            WriterError::BackgroundWriterError(_) => MzPeakException::new_err(msg),
            WriterError::ThreadPanicked => MzPeakException::new_err(msg),
        }
    }
}

/// Convert DatasetError to Python exception with full error chain
impl From<DatasetError> for PyErr {
    fn from(err: DatasetError) -> Self {
        let msg = format_error_chain(&err);
        match &err {
            DatasetError::IoError(_) => MzPeakIOError::new_err(msg),
            DatasetError::WriterError(_) => MzPeakException::new_err(msg),
            DatasetError::MetadataError(_) => MzPeakFormatError::new_err(msg),
            DatasetError::SerdeJsonError(_) => MzPeakFormatError::new_err(msg),
            DatasetError::ZipError(_) => MzPeakIOError::new_err(msg),
            DatasetError::ChromatogramWriterError(_) => MzPeakException::new_err(msg),
            DatasetError::MobilogramWriterError(_) => MzPeakException::new_err(msg),
            DatasetError::InvalidPath(_) => PyValueError::new_err(msg),
            DatasetError::AlreadyExists(_) => MzPeakIOError::new_err(msg),
            DatasetError::NotInitialized => MzPeakException::new_err(msg),
        }
    }
}

/// Convert ConversionError to Python exception with full error chain
impl From<ConversionError> for PyErr {
    fn from(err: ConversionError) -> Self {
        let msg = format_error_chain(&err);
        match &err {
            ConversionError::MzMLError(_) => MzPeakFormatError::new_err(msg),
            ConversionError::WriterError(_) => MzPeakException::new_err(msg),
            ConversionError::DatasetError(_) => MzPeakException::new_err(msg),
            ConversionError::ChromatogramWriterError(_) => MzPeakException::new_err(msg),
            ConversionError::IoError(_) => MzPeakIOError::new_err(msg),
            ConversionError::MetadataError(_) => MzPeakFormatError::new_err(msg),
            ConversionError::BinaryDecodeError { .. } => MzPeakFormatError::new_err(msg),
        }
    }
}

/// Convert IngestError to Python exception
impl From<IngestError> for PyErr {
    fn from(err: IngestError) -> Self {
        MzPeakValidationError::new_err(format_error_chain(&err))
    }
}

/// Helper trait for converting Results to PyResult
pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T, E: Into<PyErr>> IntoPyResult<T> for Result<T, E> {
    fn into_py_result(self) -> PyResult<T> {
        self.map_err(Into::into)
    }
}
