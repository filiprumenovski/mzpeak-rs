use std::fmt;

#[cfg(feature = "colorized_output")]
use console::style;

/// Validation check result status
#[derive(Debug, Clone)]
pub enum CheckStatus {
    /// Check passed
    Ok,
    /// Check passed with warnings
    Warning(String),
    /// Check failed
    Failed(String),
}

impl CheckStatus {
    fn is_ok(&self) -> bool {
        matches!(self, CheckStatus::Ok)
    }

    fn is_failed(&self) -> bool {
        matches!(self, CheckStatus::Failed(_))
    }
}

/// Individual validation check result
#[derive(Debug, Clone)]
pub struct ValidationCheck {
    /// Name of the validation check
    pub name: String,
    /// Result status of the check
    pub status: CheckStatus,
}

impl ValidationCheck {
    pub(crate) fn ok(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Ok,
        }
    }

    pub(crate) fn warning(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Warning(message.into()),
        }
    }

    pub(crate) fn failed(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Failed(message.into()),
        }
    }
}

/// Complete validation report for an mzPeak file
#[derive(Debug)]
pub struct ValidationReport {
    /// List of individual validation check results
    pub checks: Vec<ValidationCheck>,
    /// Path of the file that was validated
    pub file_path: String,
}

impl ValidationReport {
    /// Create a new validation report for the given file path
    pub fn new(file_path: impl Into<String>) -> Self {
        Self {
            checks: Vec::new(),
            file_path: file_path.into(),
        }
    }

    /// Add a validation check result to the report
    pub fn add_check(&mut self, check: ValidationCheck) {
        self.checks.push(check);
    }

    /// Check if any validation checks failed
    pub fn has_failures(&self) -> bool {
        self.checks.iter().any(|c| c.status.is_failed())
    }

    /// Check if any validation checks produced warnings
    pub fn has_warnings(&self) -> bool {
        self.checks.iter().any(|c| matches!(c.status, CheckStatus::Warning(_)))
    }

    /// Count the number of successful checks
    pub fn success_count(&self) -> usize {
        self.checks.iter().filter(|c| c.status.is_ok()).count()
    }

    /// Count the number of warnings
    pub fn warning_count(&self) -> usize {
        self.checks.iter().filter(|c| matches!(c.status, CheckStatus::Warning(_))).count()
    }

    /// Count the number of failures
    pub fn failure_count(&self) -> usize {
        self.checks.iter().filter(|c| c.status.is_failed()).count()
    }

    /// Format the report with colors (requires console feature)
    pub fn format_colored(&self) -> String {
        #[cfg(feature = "colorized_output")]
        {
            use console::Emoji;

            static OK: Emoji<'_, '_> = Emoji("✓", "[OK]");
            static WARN: Emoji<'_, '_> = Emoji("⚠", "[WARN]");
            static FAIL: Emoji<'_, '_> = Emoji("✗", "[FAIL]");

            let mut output = String::new();

            output.push_str(&format!("{}\n", style("mzPeak Validation Report").bold().cyan()));
            output.push_str(&format!("{}\n", style("========================").cyan()));
            output.push_str(&format!("{}: {}\n\n", style("File").bold(), self.file_path));

            for check in &self.checks {
                let (symbol, color_fn): (_, fn(&str) -> console::StyledObject<&str>) = match &check.status {
                    CheckStatus::Ok => (OK, |s| style(s).green()),
                    CheckStatus::Warning(_) => (WARN, |s| style(s).yellow()),
                    CheckStatus::Failed(_) => (FAIL, |s| style(s).red()),
                };

                output.push_str(&format!("[{}] {}", symbol, color_fn(&check.name)));

                match &check.status {
                    CheckStatus::Ok => output.push('\n'),
                    CheckStatus::Warning(msg) => {
                        output.push_str(&format!(" - {}: {}\n", style("WARNING").yellow().bold(), msg));
                    }
                    CheckStatus::Failed(msg) => {
                        output.push_str(&format!(" - {}: {}\n", style("FAILED").red().bold(), msg));
                    }
                }
            }

            output.push('\n');
            output.push_str(&format!(
                "{}: {} passed, {} warnings, {} failed\n",
                style("Summary").bold(),
                style(self.success_count()).green(),
                style(self.warning_count()).yellow(),
                style(self.failure_count()).red()
            ));

            output.push('\n');
            if self.has_failures() {
                output.push_str(&format!("{}\n", style("Validation FAILED").red().bold()));
            } else if self.has_warnings() {
                output.push_str(&format!("{}\n", style("Validation PASSED with warnings").yellow().bold()));
            } else {
                output.push_str(&format!("{}\n", style("Validation PASSED").green().bold()));
            }

            output
        }

        #[cfg(not(feature = "colorized_output"))]
        {
            format!("{}", self)
        }
    }
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "mzPeak Validation Report")?;
        writeln!(f, "========================")?;
        writeln!(f, "File: {}", self.file_path)?;
        writeln!(f)?;

        for check in &self.checks {
            let symbol = match &check.status {
                CheckStatus::Ok => "✓",
                CheckStatus::Warning(_) => "⚠",
                CheckStatus::Failed(_) => "✗",
            };

            write!(f, "[{}] {}", symbol, check.name)?;

            match &check.status {
                CheckStatus::Ok => writeln!(f)?,
                CheckStatus::Warning(msg) => writeln!(f, " - WARNING: {}", msg)?,
                CheckStatus::Failed(msg) => writeln!(f, " - FAILED: {}", msg)?,
            }
        }

        writeln!(f)?;
        writeln!(
            f,
            "Summary: {} passed, {} warnings, {} failed",
            self.success_count(),
            self.warning_count(),
            self.failure_count()
        )?;

        if self.has_failures() {
            writeln!(f)?;
            writeln!(f, "Validation FAILED")?;
        } else if self.has_warnings() {
            writeln!(f)?;
            writeln!(f, "Validation PASSED with warnings")?;
        } else {
            writeln!(f)?;
            writeln!(f, "Validation PASSED")?;
        }

        Ok(())
    }
}
