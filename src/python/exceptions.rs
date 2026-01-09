//! Python exception types for mzPeak
//!
//! Maps Rust error types to appropriate Python exceptions.

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;

use crate::dataset::DatasetError;
use crate::mzml::converter::ConversionError;
use crate::reader::ReaderError;
use crate::writer::WriterError;

// Define custom exception hierarchy
create_exception!(mzpeak, MzPeakException, PyException, "Base exception for all mzPeak errors.");
create_exception!(mzpeak, MzPeakIOError, MzPeakException, "I/O error during file operations.");
create_exception!(mzpeak, MzPeakFormatError, MzPeakException, "Invalid or corrupted file format.");
create_exception!(mzpeak, MzPeakValidationError, MzPeakException, "Data validation failed.");

/// Convert ReaderError to Python exception
impl From<ReaderError> for PyErr {
    fn from(err: ReaderError) -> Self {
        match &err {
            ReaderError::IoError(_) => MzPeakIOError::new_err(err.to_string()),
            ReaderError::ParquetError(_) => MzPeakFormatError::new_err(err.to_string()),
            ReaderError::ArrowError(_) => MzPeakFormatError::new_err(err.to_string()),
            ReaderError::InvalidFormat(_) => MzPeakFormatError::new_err(err.to_string()),
            ReaderError::ZipError(_) => MzPeakIOError::new_err(err.to_string()),
            ReaderError::MetadataError(_) => MzPeakFormatError::new_err(err.to_string()),
            ReaderError::ColumnNotFound(_) => MzPeakFormatError::new_err(err.to_string()),
            ReaderError::JsonError(_) => MzPeakFormatError::new_err(err.to_string()),
        }
    }
}

/// Convert WriterError to Python exception
impl From<WriterError> for PyErr {
    fn from(err: WriterError) -> Self {
        match &err {
            WriterError::IoError(_) => MzPeakIOError::new_err(err.to_string()),
            WriterError::ArrowError(_) => MzPeakFormatError::new_err(err.to_string()),
            WriterError::ParquetError(_) => MzPeakFormatError::new_err(err.to_string()),
            WriterError::MetadataError(_) => MzPeakFormatError::new_err(err.to_string()),
            WriterError::InvalidData(_) => MzPeakValidationError::new_err(err.to_string()),
            WriterError::NotInitialized => MzPeakException::new_err(err.to_string()),
        }
    }
}

/// Convert DatasetError to Python exception
impl From<DatasetError> for PyErr {
    fn from(err: DatasetError) -> Self {
        match &err {
            DatasetError::IoError(_) => MzPeakIOError::new_err(err.to_string()),
            DatasetError::WriterError(_) => MzPeakException::new_err(err.to_string()),
            DatasetError::MetadataError(_) => MzPeakFormatError::new_err(err.to_string()),
            DatasetError::SerdeJsonError(_) => MzPeakFormatError::new_err(err.to_string()),
            DatasetError::ZipError(_) => MzPeakIOError::new_err(err.to_string()),
            DatasetError::ChromatogramWriterError(_) => MzPeakException::new_err(err.to_string()),
            DatasetError::MobilogramWriterError(_) => MzPeakException::new_err(err.to_string()),
            DatasetError::InvalidPath(_) => PyValueError::new_err(err.to_string()),
            DatasetError::AlreadyExists(_) => MzPeakIOError::new_err(err.to_string()),
            DatasetError::NotInitialized => MzPeakException::new_err(err.to_string()),
        }
    }
}

/// Convert ConversionError to Python exception
impl From<ConversionError> for PyErr {
    fn from(err: ConversionError) -> Self {
        match &err {
            ConversionError::MzMLError(_) => MzPeakFormatError::new_err(err.to_string()),
            ConversionError::WriterError(_) => MzPeakException::new_err(err.to_string()),
            ConversionError::DatasetError(_) => MzPeakException::new_err(err.to_string()),
            ConversionError::ChromatogramWriterError(_) => MzPeakException::new_err(err.to_string()),
            ConversionError::IoError(_) => MzPeakIOError::new_err(err.to_string()),
            ConversionError::MetadataError(_) => MzPeakFormatError::new_err(err.to_string()),
        }
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
