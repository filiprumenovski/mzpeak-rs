use std::fmt;

/// Statistics from a completed write operation
#[derive(Debug, Clone)]
pub struct WriterStats {
    /// Number of spectra written to the file
    pub spectra_written: usize,
    /// Total number of peaks written
    pub peaks_written: usize,
    /// Number of Parquet row groups written
    pub row_groups_written: usize,
    /// Total file size in bytes
    pub file_size_bytes: u64,
}

impl fmt::Display for WriterStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Wrote {} spectra ({} peaks) in {} row groups",
            self.spectra_written, self.peaks_written, self.row_groups_written
        )
    }
}
