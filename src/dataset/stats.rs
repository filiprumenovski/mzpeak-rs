use std::fmt;

use crate::chromatogram_writer::ChromatogramWriterStats;
use crate::mobilogram_writer::MobilogramWriterStats;
use crate::writer::WriterStats;

/// Statistics from a completed dataset write operation
#[derive(Debug, Clone)]
pub struct DatasetStats {
    /// Statistics from the peak writer
    pub peak_stats: WriterStats,

    /// Statistics from the chromatogram writer
    pub chromatogram_stats: Option<ChromatogramWriterStats>,

    /// Number of chromatograms written
    pub chromatograms_written: usize,

    /// Statistics from the mobilogram writer
    pub mobilogram_stats: Option<MobilogramWriterStats>,

    /// Number of mobilograms written
    pub mobilograms_written: usize,

    /// Total dataset size in bytes
    pub total_size_bytes: u64,
}

impl fmt::Display for DatasetStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Dataset: {} spectra, {} peaks, {} chromatograms, {} mobilograms, {} bytes",
            self.peak_stats.spectra_written,
            self.peak_stats.peaks_written,
            self.chromatograms_written,
            self.mobilograms_written,
            self.total_size_bytes
        )
    }
}
