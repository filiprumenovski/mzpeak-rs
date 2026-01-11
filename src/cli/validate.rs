use anyhow::Result;
use log::info;
use std::path::PathBuf;

/// Validate mzPeak file integrity
pub fn run(file: PathBuf) -> Result<()> {
    use mzpeak::validator::validate_mzpeak_file;

    info!("mzPeak Validator");
    info!("================");
    info!("File: {}", file.display());
    info!("");

    // Run validation
    match validate_mzpeak_file(&file) {
        Ok(report) => {
            // Use colorized output if available
            #[cfg(feature = "colorized_output")]
            {
                println!("{}", report.format_colored());
            }

            #[cfg(not(feature = "colorized_output"))]
            {
                println!("{}", report);
            }

            // Exit with error code if validation failed
            if report.has_failures() {
                std::process::exit(1);
            }

            Ok(())
        }
        Err(e) => {
            eprintln!("Validation error: {}", e);
            std::process::exit(1);
        }
    }
}
