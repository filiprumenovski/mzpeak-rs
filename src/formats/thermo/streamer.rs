//! Bounded RAW ingestion from the rawr executable's mzML stream.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};

use crate::ingest::IngestSpectrum;
use crate::mzml::{MzMLConverter, MzMLStreamer};
use crate::thermo::ThermoError;

/// Streams source-saved RAW centroids through rawr without a vendor runtime.
///
/// The mzML intermediate travels through a pipe; it is never stored on disk.
/// Process failures and incomplete streams are errors, including failures after
/// the last spectrum. Dropping this reader terminates and reaps its child.
pub struct ThermoStreamer {
    process: RawrProcess,
    xml: MzMLStreamer<BufReader<ChildStdout>>,
    converter: MzMLConverter,
    batch_size: usize,
    total_spectra: usize,
    next_index: usize,
    finished: bool,
    // Keeps the extracted executable alive until the child has exited.
    _executable: Option<tempfile::TempPath>,
}

struct RawrProcess {
    child: Child,
    stderr: File,
}

impl RawrProcess {
    fn check(&mut self) -> Result<(), ThermoError> {
        let status = self.child.wait()?;
        if status.success() {
            return Ok(());
        }
        self.stderr.seek(SeekFrom::Start(0))?;
        let mut detail = String::new();
        (&mut self.stderr).take(8192).read_to_string(&mut detail)?;
        Err(ThermoError::Process(format!(
            "exited with {status}: {}",
            detail.trim()
        )))
    }
}

impl Drop for RawrProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl ThermoStreamer {
    /// Starts rawr with a maximum number of spectra per returned batch.
    pub fn new(path: impl AsRef<Path>, batch_size: usize) -> Result<Self, ThermoError> {
        Self::with_binary(path, batch_size, None)
    }

    pub(crate) fn with_binary(
        path: impl AsRef<Path>,
        batch_size: usize,
        binary: Option<&Path>,
    ) -> Result<Self, ThermoError> {
        let path = path.as_ref();
        if !path.is_file()
            || !path
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("raw"))
        {
            return Err(ThermoError::InvalidPath(path.display().to_string()));
        }
        let (binary, executable) = resolve_binary(binary)?;
        let stderr = tempfile::tempfile()?;
        let mut child = Command::new(&binary)
            .args([
                "--color", "never", "convert", "--format", "mzml", "--output", "-", "--plain", "--",
            ])
            .arg(path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(stderr.try_clone()?)
            .spawn()
            .map_err(|error| {
                ThermoError::Process(format!(
                    "cannot start {}: {error}; set MZPEAK_RAWR to a compatible rawr executable",
                    binary.display()
                ))
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ThermoError::Process("stdout pipe unavailable".into()))?;
        let mut process = RawrProcess { child, stderr };
        let mut xml = MzMLStreamer::new(BufReader::with_capacity(1 << 20, stdout))?;
        xml.read_metadata()?;
        let total_spectra = match xml.spectrum_count() {
            Some(count) => count,
            None => {
                process.check()?;
                return Err(ThermoError::Process(
                    "mzML header has no spectrum count".into(),
                ));
            }
        };
        Ok(Self {
            process,
            xml,
            converter: MzMLConverter::new(),
            batch_size: batch_size.max(1),
            total_spectra,
            next_index: 0,
            finished: false,
            _executable: executable,
        })
    }

    /// Total mass spectra advertised by rawr's mzML header.
    pub fn len(&self) -> usize {
        self.total_spectra
    }

    /// Whether the advertised run is empty.
    pub fn is_empty(&self) -> bool {
        self.total_spectra == 0
    }

    /// Number of spectra returned so far.
    pub fn position(&self) -> usize {
        self.next_index
    }

    /// Reads the next batch. `None` means the stream and child both completed.
    pub fn next_batch(&mut self) -> Result<Option<Vec<IngestSpectrum>>, ThermoError> {
        if self.finished {
            return Ok(None);
        }
        let mut batch = Vec::new();
        while batch.len() < self.batch_size {
            let Some(raw) = self.xml.next_raw_spectrum()? else {
                // Drain the document trailer before waiting for a piped writer.
                while self.xml.next_chromatogram()?.is_some() {}
                self.process.check()?;
                if self.next_index + batch.len() != self.total_spectra {
                    return Err(ThermoError::Process(format!(
                        "expected {} spectra, received {}",
                        self.total_spectra,
                        self.next_index + batch.len()
                    )));
                }
                self.finished = true;
                break;
            };
            if self.next_index + batch.len() >= self.total_spectra {
                return Err(ThermoError::Process(
                    "more spectra than the header advertises".into(),
                ));
            }
            if !raw.centroided {
                return Err(ThermoError::UnsupportedRepresentation(
                    "expected source-saved centroids".into(),
                ));
            }
            let base_peak_mz = raw.base_peak_mz;
            let decoded = self.converter.build_ingest_spectrum_raw(raw)?;
            let mut ingest = decoded.ingest;
            ingest.total_ion_current = decoded.total_ion_current;
            ingest.base_peak_mz = base_peak_mz;
            ingest.base_peak_intensity = decoded.base_peak_intensity.map(|value| value as f32);
            ingest
                .validate_contract()
                .map_err(|error| ThermoError::PeakConversionError(error.to_string()))?;
            batch.push(ingest);
        }
        self.next_index += batch.len();
        Ok((!batch.is_empty()).then_some(batch))
    }
}

fn resolve_binary(
    override_path: Option<&Path>,
) -> Result<(PathBuf, Option<tempfile::TempPath>), ThermoError> {
    if let Some(path) = override_path {
        return Ok((path.to_path_buf(), None));
    }
    if let Some(path) = std::env::var_os("MZPEAK_RAWR") {
        return Ok((path.into(), None));
    }
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(include_bytes!(
            "../../../vendor/rawr-ms/bin/aarch64-apple-darwin/rawr"
        ))?;
        file.as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o700))?;
        let path = file.into_temp_path();
        return Ok((path.to_path_buf(), Some(path)));
    }
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    Ok((PathBuf::from("rawr"), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_paths_before_starting_rawr() {
        assert!(matches!(
            ThermoStreamer::new("/nonexistent/file.raw", 100),
            Err(ThermoError::InvalidPath(_))
        ));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.mzML");
        std::fs::write(&path, b"fake").unwrap();
        assert!(matches!(
            ThermoStreamer::new(path, 100),
            Err(ThermoError::InvalidPath(_))
        ));
    }
}
