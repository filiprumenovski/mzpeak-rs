//! Streaming access to Thermo RAW files with batch iteration.

use std::path::Path;

use thermorawfilereader::{RawFileReader, RawSpectrum};

use crate::thermo::ThermoError;

/// Check if the current platform supports Thermo RAW file reading.
///
/// Returns `Ok(())` if supported, `Err(ThermoError::PlatformNotSupported)` otherwise.
fn check_platform_support() -> Result<(), ThermoError> {
    // Thermo's RawFileReader .NET assemblies only support x86/x86_64 architectures
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        return Err(ThermoError::PlatformNotSupported(format!(
            "Current architecture '{}' is not supported. \
             Thermo RAW file reading requires Windows, Linux, or macOS on x86/x86_64. \
             ARM-based systems (including Apple Silicon Macs) are not supported \
             because Thermo's RawFileReader .NET libraries require x86 architecture.",
            std::env::consts::ARCH
        )));
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    Ok(())
}

/// Streaming access to Thermo RAW files with configurable batch sizes.
///
/// This streamer wraps `thermorawfilereader::RawFileReader` and provides
/// batch-based iteration for memory-efficient processing of large RAW files.
///
/// # Example
///
/// ```no_run
/// use mzpeak::thermo::ThermoStreamer;
///
/// let mut streamer = ThermoStreamer::new("sample.raw", 1000)?;
/// println!("Total spectra: {}", streamer.len());
///
/// while let Some(batch) = streamer.next_batch()? {
///     for spectrum in batch {
///         println!("Scan {}: {} peaks", spectrum.index() + 1, 
///             spectrum.data().map(|d| d.mz().len()).unwrap_or(0));
///     }
/// }
/// # Ok::<(), mzpeak::thermo::ThermoError>(())
/// ```
pub struct ThermoStreamer {
    reader: RawFileReader,
    next_index: usize,
    batch_size: usize,
    total_spectra: usize,
}

impl ThermoStreamer {
    /// Create a new streamer over a Thermo RAW file.
    ///
    /// # Arguments
    /// * `path` - Path to the .raw file
    /// * `batch_size` - Number of spectra to return per batch (minimum 1)
    ///
    /// # Errors
    /// - `ThermoError::PlatformNotSupported` if running on ARM architecture
    /// - `ThermoError::OpenError` if the file cannot be opened
    /// - `ThermoError::RuntimeError` if .NET initialization fails
    pub fn new<P: AsRef<Path>>(path: P, batch_size: usize) -> Result<Self, ThermoError> {
        // Check platform support early with a clear message
        check_platform_support()?;

        let path = path.as_ref();

        // Validate path exists and has .raw extension
        if !path.exists() {
            return Err(ThermoError::InvalidPath(format!(
                "File does not exist: {}",
                path.display()
            )));
        }

        if path.extension().map(|e| e.to_ascii_lowercase()) != Some("raw".into()) {
            return Err(ThermoError::InvalidPath(format!(
                "Expected .raw extension: {}",
                path.display()
            )));
        }

        let mut reader = RawFileReader::open(path).map_err(|e| {
            ThermoError::OpenError(format!("{}: {}", path.display(), e))
        })?;

        // Enable signal loading (peak data)
        reader.set_signal_loading(true);
        // Enable centroiding for profile spectra
        reader.set_centroid_spectra(true);

        let total_spectra = reader.len();
        let batch_size = batch_size.max(1);

        Ok(Self {
            reader,
            next_index: 0,
            batch_size,
            total_spectra,
        })
    }

    /// Total number of spectra in the RAW file.
    pub fn len(&self) -> usize {
        self.total_spectra
    }

    /// Whether the RAW file is empty.
    pub fn is_empty(&self) -> bool {
        self.total_spectra == 0
    }

    /// Current position in the file (0-based index of next spectrum to read).
    pub fn position(&self) -> usize {
        self.next_index
    }

    /// Reset the streamer to the beginning of the file.
    pub fn reset(&mut self) {
        self.next_index = 0;
    }

    /// Get a reference to the underlying RawFileReader for metadata access.
    pub fn reader(&self) -> &RawFileReader {
        &self.reader
    }

    /// Fetch the next batch of raw spectra.
    ///
    /// Returns `Ok(None)` when all spectra have been read.
    /// Returns `Ok(Some(Vec<RawSpectrum>))` with the next batch.
    ///
    /// # Errors
    /// Returns `ThermoError::ReadError` if spectrum reading fails.
    pub fn next_batch(&mut self) -> Result<Option<Vec<RawSpectrum>>, ThermoError> {
        if self.next_index >= self.total_spectra {
            return Ok(None);
        }

        let end = (self.next_index + self.batch_size).min(self.total_spectra);
        let mut batch = Vec::with_capacity(end - self.next_index);

        for idx in self.next_index..end {
            match self.reader.get(idx) {
                Some(spectrum) => batch.push(spectrum),
                None => {
                    // Log warning but continue - some scans may be missing
                    eprintln!(
                        "⚠️  Skipping spectrum {} (read returned None)",
                        idx + 1
                    );
                }
            }
        }

        self.next_index = end;
        Ok(Some(batch))
    }

    /// Get instrument model information.
    pub fn instrument_model(&self) -> String {
        let model = self.reader.instrument_model();
        model.model().unwrap_or("Unknown").to_string()
    }

    /// Get file description/metadata.
    pub fn file_description(&self) -> thermorawfilereader::FileDescription {
        self.reader.file_description()
    }
}

impl std::fmt::Debug for ThermoStreamer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThermoStreamer")
            .field("total_spectra", &self.total_spectra)
            .field("next_index", &self.next_index)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_path() {
        let result = ThermoStreamer::new("/nonexistent/file.raw", 100);
        assert!(matches!(result, Err(ThermoError::InvalidPath(_))));
    }

    #[test]
    fn test_wrong_extension() {
        // Create a temp file with wrong extension
        let temp_dir = std::env::temp_dir();
        let fake_file = temp_dir.join("test.mzML");
        std::fs::write(&fake_file, "fake").ok();

        let result = ThermoStreamer::new(&fake_file, 100);
        assert!(matches!(result, Err(ThermoError::InvalidPath(_))));

        std::fs::remove_file(&fake_file).ok();
    }
}
