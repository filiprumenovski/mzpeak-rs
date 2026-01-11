use std::path::{Path, PathBuf};

use crate::metadata::MzPeakMetadata;

use super::config::WriterConfig;
use super::error::WriterError;
use super::stats::WriterStats;
use super::types::Spectrum;
use super::writer_impl::MzPeakWriter;

/// Rolling writer that automatically shards output into multiple files
pub struct RollingWriter {
    base_path: PathBuf,
    metadata: MzPeakMetadata,
    config: WriterConfig,
    current_writer: Option<MzPeakWriter<std::fs::File>>,
    current_part: usize,
    total_spectra_written: usize,
    total_peaks_written: usize,
    part_stats: Vec<WriterStats>,
}

impl RollingWriter {
    /// Create a new rolling writer
    pub fn new<P: AsRef<Path>>(
        base_path: P,
        metadata: MzPeakMetadata,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        let base_path = base_path.as_ref().to_path_buf();

        Ok(Self {
            base_path,
            metadata,
            config,
            current_writer: None,
            current_part: 0,
            total_spectra_written: 0,
            total_peaks_written: 0,
            part_stats: Vec::new(),
        })
    }

    /// Get the path for a specific part number
    fn part_path(&self, part: usize) -> PathBuf {
        if part == 0 {
            self.base_path.clone()
        } else {
            let stem = self.base_path.file_stem().unwrap_or_default().to_string_lossy();
            let extension = self.base_path.extension().unwrap_or_default().to_string_lossy();
            let parent = self.base_path.parent().unwrap_or_else(|| Path::new("."));

            if extension.is_empty() {
                parent.join(format!("{}-part-{:04}", stem, part))
            } else {
                parent.join(format!("{}-part-{:04}.{}", stem, part, extension))
            }
        }
    }

    /// Rotate to a new file
    fn rotate_file(&mut self) -> Result<(), WriterError> {
        // Finish current writer if exists
        if let Some(writer) = self.current_writer.take() {
            let stats = writer.finish()?;
            self.part_stats.push(stats);
        }

        // Create new writer for next part
        self.current_part += 1;
        let part_path = self.part_path(self.current_part - 1);

        let writer = MzPeakWriter::new_file(part_path, &self.metadata, self.config.clone())?;
        self.current_writer = Some(writer);

        Ok(())
    }

    /// Write a batch of spectra, automatically rotating files if needed
    pub fn write_spectra(&mut self, spectra: &[Spectrum]) -> Result<(), WriterError> {
        if spectra.is_empty() {
            return Ok(());
        }

        // Initialize first writer if needed
        if self.current_writer.is_none() {
            self.rotate_file()?;
        }

        let writer = self.current_writer.as_mut().unwrap();

        // Check if we need to rotate based on config
        if let Some(max_peaks) = self.config.max_peaks_per_file {
            let peaks_in_batch: usize = spectra.iter().map(|s| s.peaks.len()).sum();

            // If adding this batch would exceed limit, rotate first
            if writer.peaks_written() > 0 && writer.peaks_written() + peaks_in_batch > max_peaks {
                self.rotate_file()?;
                let writer = self.current_writer.as_mut().unwrap();
                writer.write_spectra(spectra)?;
            } else {
                writer.write_spectra(spectra)?;
            }
        } else {
            writer.write_spectra(spectra)?;
        }

        self.total_spectra_written += spectra.len();
        self.total_peaks_written += spectra.iter().map(|s| s.peaks.len()).sum::<usize>();

        Ok(())
    }

    /// Write a single spectrum
    pub fn write_spectrum(&mut self, spectrum: &Spectrum) -> Result<(), WriterError> {
        self.write_spectra(&[spectrum.clone()])
    }

    /// Finish writing and return combined statistics
    pub fn finish(mut self) -> Result<RollingWriterStats, WriterError> {
        // Finish current writer if exists
        if let Some(writer) = self.current_writer.take() {
            let stats = writer.finish()?;
            self.part_stats.push(stats);
        }

        Ok(RollingWriterStats {
            total_spectra_written: self.total_spectra_written,
            total_peaks_written: self.total_peaks_written,
            files_written: self.part_stats.len(),
            part_stats: self.part_stats,
        })
    }

    /// Get current statistics
    pub fn stats(&self) -> RollingWriterStats {
        RollingWriterStats {
            total_spectra_written: self.total_spectra_written,
            total_peaks_written: self.total_peaks_written,
            files_written: self.part_stats.len() + if self.current_writer.is_some() { 1 } else { 0 },
            part_stats: self.part_stats.clone(),
        }
    }
}

/// Statistics from a rolling writer operation
#[derive(Debug, Clone)]
pub struct RollingWriterStats {
    /// Total number of spectra written across all files
    pub total_spectra_written: usize,
    /// Total number of peaks written across all files
    pub total_peaks_written: usize,
    /// Number of output files created
    pub files_written: usize,
    /// Statistics for each individual file part
    pub part_stats: Vec<WriterStats>,
}

impl std::fmt::Display for RollingWriterStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Wrote {} spectra ({} peaks) across {} file(s)",
            self.total_spectra_written, self.total_peaks_written, self.files_written
        )
    }
}
