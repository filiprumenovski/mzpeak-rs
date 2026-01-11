use std::fmt;

use super::{MzPeakReader, ReaderError};

/// Summary statistics about an mzPeak file
#[derive(Debug, Clone)]
pub struct FileSummary {
    /// Total number of peaks in the file
    pub total_peaks: i64,
    /// Number of unique spectra
    pub num_spectra: i64,
    /// Number of MS1 spectra
    pub num_ms1_spectra: i64,
    /// Number of MS2 spectra
    pub num_ms2_spectra: i64,
    /// Retention time range (min, max) in seconds
    pub rt_range: Option<(f32, f32)>,
    /// m/z range (min, max)
    pub mz_range: Option<(f64, f64)>,
    /// Format version
    pub format_version: String,
}

impl MzPeakReader {
    /// Get summary statistics about the file
    pub fn summary(&self) -> Result<FileSummary, ReaderError> {
        let spectra = self.iter_spectra()?;

        let num_spectra = spectra.len() as i64;
        let num_ms1 = spectra.iter().filter(|s| s.ms_level == 1).count() as i64;
        let num_ms2 = spectra.iter().filter(|s| s.ms_level == 2).count() as i64;

        let rt_range = if !spectra.is_empty() {
            let min_rt = spectra
                .iter()
                .map(|s| s.retention_time)
                .fold(f32::MAX, f32::min);
            let max_rt = spectra
                .iter()
                .map(|s| s.retention_time)
                .fold(f32::MIN, f32::max);
            Some((min_rt, max_rt))
        } else {
            None
        };

        let mz_range = if !spectra.is_empty() {
            let min_mz = spectra
                .iter()
                .flat_map(|s| s.peaks.iter())
                .map(|p| p.mz)
                .fold(f64::MAX, f64::min);
            let max_mz = spectra
                .iter()
                .flat_map(|s| s.peaks.iter())
                .map(|p| p.mz)
                .fold(f64::MIN, f64::max);
            if min_mz <= max_mz {
                Some((min_mz, max_mz))
            } else {
                None
            }
        } else {
            None
        };

        Ok(FileSummary {
            total_peaks: self.file_metadata.total_rows,
            num_spectra,
            num_ms1_spectra: num_ms1,
            num_ms2_spectra: num_ms2,
            rt_range,
            mz_range,
            format_version: self.file_metadata.format_version.clone(),
        })
    }
}

impl fmt::Display for FileSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "mzPeak File Summary")?;
        writeln!(f, "===================")?;
        writeln!(f, "Format version: {}", self.format_version)?;
        writeln!(f, "Total peaks: {}", self.total_peaks)?;
        writeln!(f, "Total spectra: {}", self.num_spectra)?;
        writeln!(f, "  MS1 spectra: {}", self.num_ms1_spectra)?;
        writeln!(f, "  MS2 spectra: {}", self.num_ms2_spectra)?;
        if let Some((min_rt, max_rt)) = self.rt_range {
            writeln!(f, "RT range: {:.2} - {:.2} sec", min_rt, max_rt)?;
        }
        if let Some((min_mz, max_mz)) = self.mz_range {
            writeln!(f, "m/z range: {:.4} - {:.4}", min_mz, max_mz)?;
        }
        Ok(())
    }
}
