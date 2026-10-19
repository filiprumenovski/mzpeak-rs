//! Full archive CRC, schema, row-link and numerical validation.
use crate::reader::MzPeakReader;
use std::path::Path;
#[derive(Debug, serde::Serialize)]
pub struct ValidationReport {
    pub spectra: u64,
    pub peaks: u64,
    pub chromatograms: usize,
}
pub fn validate_mzpeak_file(path: impl AsRef<Path>) -> anyhow::Result<ValidationReport> {
    let reader = MzPeakReader::open(path)?;
    reader.verify_archive_crc()?;
    let mut spectra = 0;
    let mut peaks = 0;
    for spectrum in reader.iter_spectra_arrays_streaming()? {
        let spectrum = spectrum?;
        let metadata = &spectrum.metadata;
        anyhow::ensure!(
            metadata.ms_level > 0
                && metadata.retention_time.is_finite()
                && metadata.retention_time >= 0.0
                && matches!(metadata.polarity, -1..=1),
            "invalid spectrum metadata for {}",
            metadata.spectrum_id
        );
        anyhow::ensure!(
            spectrum.mz().all(|v| v.is_finite() && v > 0.0),
            "invalid m/z in spectrum {}",
            metadata.spectrum_id
        );
        anyhow::ensure!(
            spectrum.intensity().all(|v| v.is_finite() && v >= 0.0),
            "invalid intensity in spectrum {}",
            metadata.spectrum_id
        );
        anyhow::ensure!(
            spectrum.ion_mobility().all(|v| v.is_finite() && v >= 0.0),
            "invalid mobility in spectrum {}",
            metadata.spectrum_id
        );
        anyhow::ensure!(
            reader.manifest().has_imaging
                == (metadata.pixel_x.is_some() && metadata.pixel_y.is_some())
                && metadata.pixel_x.is_some() == metadata.pixel_y.is_some()
                && (reader.manifest().has_imaging || metadata.pixel_z.is_none()),
            "imaging coordinates disagree with modality"
        );
        spectra += 1;
        peaks += spectrum.peak_count() as u64;
    }
    anyhow::ensure!(
        spectra == reader.total_spectra() && peaks == reader.total_peaks(),
        "manifest counts disagree with data"
    );
    let chromatograms = reader.read_chromatograms()?.unwrap_or_default();
    for c in &chromatograms {
        anyhow::ensure!(
            c.time_array.iter().all(|v| v.is_finite() && *v >= 0.0)
                && c.intensity_array.iter().all(|v| v.is_finite() && *v >= 0.0),
            "invalid chromatogram values"
        );
    }
    Ok(ValidationReport {
        spectra,
        peaks,
        chromatograms: chromatograms.len(),
    })
}
