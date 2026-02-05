//! Python bindings for mzPeak validation functionality
//!
//! This module exposes the mzPeak file validator and validation report types,
//! enabling Python users to validate .mzpeak files for compliance with the
//! format specification.

use pyo3::prelude::*;
use std::path::Path;

use crate::validator::{validate_mzpeak_file, CheckStatus, ValidationCheck, ValidationReport};

/// Validation check status enum.
#[pyclass(name = "CheckStatus")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyCheckStatus {
    /// Check passed
    Ok = 0,
    /// Check passed with warnings
    Warning = 1,
    /// Check failed
    Failed = 2,
}

impl From<&CheckStatus> for PyCheckStatus {
    fn from(status: &CheckStatus) -> Self {
        match status {
            CheckStatus::Ok => PyCheckStatus::Ok,
            CheckStatus::Warning(_) => PyCheckStatus::Warning,
            CheckStatus::Failed(_) => PyCheckStatus::Failed,
        }
    }
}

#[pymethods]
impl PyCheckStatus {
    /// Check if the status represents a passing check
    fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Check if the status represents a warning
    fn is_warning(&self) -> bool {
        matches!(self, Self::Warning)
    }

    /// Check if the status represents a failure
    fn is_failed(&self) -> bool {
        matches!(self, Self::Failed)
    }

    fn __repr__(&self) -> String {
        match self {
            Self::Ok => "CheckStatus.Ok".to_string(),
            Self::Warning => "CheckStatus.Warning".to_string(),
            Self::Failed => "CheckStatus.Failed".to_string(),
        }
    }
}

/// Individual validation check result.
#[pyclass(name = "ValidationCheck")]
#[derive(Clone)]
pub struct PyValidationCheck {
    /// Name of the validation check
    name: String,
    /// Status of the check
    status: PyCheckStatus,
    /// Message (for warnings and failures)
    message: Option<String>,
}

#[pymethods]
impl PyValidationCheck {
    /// Name of the validation check
    #[getter]
    fn name(&self) -> String {
        self.name.clone()
    }

    /// Result status of the check
    #[getter]
    fn status(&self) -> PyCheckStatus {
        self.status
    }

    /// Message for warnings and failures (None if OK)
    #[getter]
    fn message(&self) -> Option<String> {
        self.message.clone()
    }

    /// Check if this check passed
    fn is_ok(&self) -> bool {
        self.status.is_ok()
    }

    /// Check if this check produced a warning
    fn is_warning(&self) -> bool {
        self.status.is_warning()
    }

    /// Check if this check failed
    fn is_failed(&self) -> bool {
        self.status.is_failed()
    }

    fn __repr__(&self) -> String {
        match &self.message {
            Some(msg) => format!(
                "ValidationCheck(name='{}', status={:?}, message='{}')",
                self.name, self.status, msg
            ),
            None => format!(
                "ValidationCheck(name='{}', status={:?})",
                self.name, self.status
            ),
        }
    }
}

impl From<&ValidationCheck> for PyValidationCheck {
    fn from(check: &ValidationCheck) -> Self {
        let (status, message) = match &check.status {
            CheckStatus::Ok => (PyCheckStatus::Ok, None),
            CheckStatus::Warning(msg) => (PyCheckStatus::Warning, Some(msg.clone())),
            CheckStatus::Failed(msg) => (PyCheckStatus::Failed, Some(msg.clone())),
        };
        Self {
            name: check.name.clone(),
            status,
            message,
        }
    }
}

/// Complete validation report for an mzPeak file.
#[pyclass(name = "ValidationReport")]
#[derive(Clone)]
pub struct PyValidationReport {
    /// List of individual validation check results
    checks: Vec<PyValidationCheck>,
    /// Path of the file that was validated
    file_path: String,
}

#[pymethods]
impl PyValidationReport {
    /// List of individual validation check results
    #[getter]
    fn checks(&self) -> Vec<PyValidationCheck> {
        self.checks.clone()
    }

    /// Path of the file that was validated
    #[getter]
    fn file_path(&self) -> String {
        self.file_path.clone()
    }

    /// Check if any validation checks failed
    fn has_failures(&self) -> bool {
        self.checks.iter().any(|c| c.is_failed())
    }

    /// Check if any validation checks produced warnings
    fn has_warnings(&self) -> bool {
        self.checks.iter().any(|c| c.is_warning())
    }

    /// Check if all validation checks passed (no failures)
    fn is_valid(&self) -> bool {
        !self.has_failures()
    }

    /// Count the number of successful checks
    fn success_count(&self) -> usize {
        self.checks.iter().filter(|c| c.is_ok()).count()
    }

    /// Count the number of warnings
    fn warning_count(&self) -> usize {
        self.checks.iter().filter(|c| c.is_warning()).count()
    }

    /// Count the number of failures
    fn failure_count(&self) -> usize {
        self.checks.iter().filter(|c| c.is_failed()).count()
    }

    /// Get all failed checks
    fn failed_checks(&self) -> Vec<PyValidationCheck> {
        self.checks.iter().filter(|c| c.is_failed()).cloned().collect()
    }

    /// Get all warnings
    fn warning_checks(&self) -> Vec<PyValidationCheck> {
        self.checks.iter().filter(|c| c.is_warning()).cloned().collect()
    }

    /// Get all passed checks
    fn passed_checks(&self) -> Vec<PyValidationCheck> {
        self.checks.iter().filter(|c| c.is_ok()).cloned().collect()
    }

    /// Get a summary string
    fn summary(&self) -> String {
        format!(
            "{} passed, {} warnings, {} failed",
            self.success_count(),
            self.warning_count(),
            self.failure_count()
        )
    }

    fn __len__(&self) -> usize {
        self.checks.len()
    }

    fn __repr__(&self) -> String {
        let status = if self.has_failures() {
            "FAILED"
        } else if self.has_warnings() {
            "PASSED with warnings"
        } else {
            "PASSED"
        };
        format!(
            "ValidationReport(file='{}', status={}, checks={})",
            self.file_path,
            status,
            self.checks.len()
        )
    }

    fn __str__(&self) -> String {
        let mut output = String::new();
        output.push_str("mzPeak Validation Report\n");
        output.push_str("========================\n");
        output.push_str(&format!("File: {}\n\n", self.file_path));

        for check in &self.checks {
            let symbol = if check.status.is_ok() {
                "✓"
            } else if check.status.is_warning() {
                "⚠"
            } else {
                "✗"
            };
            output.push_str(&format!("[{}] {}", symbol, check.name));
            if let Some(ref msg) = check.message {
                output.push_str(&format!(" - {}", msg));
            }
            output.push('\n');
        }

        output.push('\n');
        output.push_str(&format!("Summary: {}\n", self.summary()));

        if self.has_failures() {
            output.push_str("\nValidation FAILED\n");
        } else if self.has_warnings() {
            output.push_str("\nValidation PASSED with warnings\n");
        } else {
            output.push_str("\nValidation PASSED\n");
        }

        output
    }
}

impl From<ValidationReport> for PyValidationReport {
    fn from(report: ValidationReport) -> Self {
        Self {
            checks: report.checks.iter().map(PyValidationCheck::from).collect(),
            file_path: report.file_path,
        }
    }
}

/// Validate an mzPeak file for compliance with the format specification.
///
/// This function performs deep integrity validation including:
/// - Structure check: validates file/directory structure
/// - Metadata integrity: validates metadata.json against schema
/// - Schema contract: verifies Parquet schema matches specification
/// - Data sanity: performs semantic checks on data values
///
/// Args:
///     path: Path to the .mzpeak file or directory to validate
///
/// Returns:
///     ValidationReport with detailed results of all checks
///
/// Raises:
///     MzPeakIOError: If the file cannot be read
///     MzPeakValidationError: If validation encounters a critical error
///
/// Example:
///     >>> import mzpeak
///     >>> report = mzpeak.validate_mzpeak_file("data.mzpeak")
///     >>> if report.is_valid():
///     ...     print("File is valid!")
///     >>> else:
///     ...     for check in report.failed_checks():
///     ...         print(f"Failed: {check.name} - {check.message}")
#[pyfunction]
#[pyo3(name = "validate_mzpeak_file")]
pub fn py_validate_mzpeak_file(path: &str) -> PyResult<PyValidationReport> {
    let path = Path::new(path);

    // Release GIL during validation since it may involve I/O
    let report = Python::with_gil(|py| {
        py.allow_threads(|| validate_mzpeak_file(path))
    });

    match report {
        Ok(report) => Ok(PyValidationReport::from(report)),
        Err(e) => Err(crate::python::exceptions::MzPeakValidationError::new_err(
            format!("Validation error: {}", e),
        )),
    }
}
