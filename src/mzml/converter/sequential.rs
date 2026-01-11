use std::path::Path;

use log::info;

use super::{ConversionError, ConversionStats, MzMLConverter};
use super::super::streamer::MzMLStreamer;
use crate::dataset::MzPeakDatasetWriter;
use crate::writer::{RollingWriter, Spectrum, WriterError};

impl MzMLConverter {
    /// Convert an mzML file to mzPeak format
    pub fn convert<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
    ) -> Result<ConversionStats, ConversionError> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();

        info!("Converting {} to {}", input_path.display(), output_path.display());

        // Get source file size
        let source_file_size = std::fs::metadata(input_path)?.len();

        // Open the mzML file
        let mut streamer = MzMLStreamer::open(input_path)?;

        // Read metadata first
        let mzml_metadata = streamer.read_metadata()?;
        info!("mzML version: {:?}", mzml_metadata.version);

        // Convert mzML metadata to mzPeak metadata
        let mzpeak_metadata = self.convert_metadata(mzml_metadata, input_path)?;

        // Create the dataset writer (auto-detects container vs directory mode)
        let mut writer = MzPeakDatasetWriter::new(
            output_path,
            &mzpeak_metadata,
            self.config.writer_config.clone(),
        )?;

        // Process spectra in batches
        let mut stats = ConversionStats {
            source_file_size,
            ..Default::default()
        };

        let mut batch: Vec<Spectrum> = Vec::with_capacity(self.config.batch_size);
        let expected_count = streamer.spectrum_count();

        // Accumulate TIC and BPC data during spectrum processing
        let mut tic_times: Vec<f64> = Vec::new();
        let mut tic_intensities: Vec<f32> = Vec::new();
        let mut bpc_times: Vec<f64> = Vec::new();
        let mut bpc_intensities: Vec<f32> = Vec::new();

        info!(
            "Converting {} spectra...",
            expected_count
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );

        while let Some(mzml_spectrum) = streamer.next_spectrum()? {
            let spectrum = self.convert_spectrum(&mzml_spectrum);

            // Update statistics
            stats.spectra_count += 1;
            stats.peak_count += spectrum.peak_count();

            match mzml_spectrum.ms_level {
                1 => stats.ms1_spectra += 1,
                2 => stats.ms2_spectra += 1,
                _ => stats.msn_spectra += 1,
            }

            // Accumulate TIC and BPC for MS1 spectra only
            if mzml_spectrum.ms_level == 1 {
                let rt = mzml_spectrum.retention_time.unwrap_or(0.0);

                // Calculate TIC from spectrum
                let tic = if let Some(tic_from_spectrum) = mzml_spectrum.total_ion_current {
                    tic_from_spectrum as f32
                } else {
                    mzml_spectrum
                        .intensity_array
                        .iter()
                        .map(|&i| i as f32)
                        .sum()
                };

                // Calculate BPC from spectrum
                let bpc = if let Some(bp_intensity) = mzml_spectrum.base_peak_intensity {
                    bp_intensity as f32
                } else {
                    mzml_spectrum
                        .intensity_array
                        .iter()
                        .map(|&i| i as f32)
                        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .unwrap_or(0.0)
                };

                tic_times.push(rt);
                tic_intensities.push(tic);
                bpc_times.push(rt);
                bpc_intensities.push(bpc);
            }

            batch.push(spectrum);

            // Write batch if full
            if batch.len() >= self.config.batch_size {
                writer.write_spectra(&batch)?;
                batch.clear();

                // Progress update
                if stats.spectra_count % self.config.progress_interval == 0 {
                    if let Some(total) = expected_count {
                        let pct = (stats.spectra_count as f64 / total as f64) * 100.0;
                        info!(
                            "Progress: {}/{} spectra ({:.1}%)",
                            stats.spectra_count, total, pct
                        );
                    } else {
                        info!("Progress: {} spectra", stats.spectra_count);
                    }
                }
            }
        }

        // Write remaining spectra
        if !batch.is_empty() {
            writer.write_spectra(&batch)?;
        }

        // Finalize spectrum writer first
        info!("Finalizing peak data...");

        // Process chromatograms if enabled
        if self.config.include_chromatograms {
            info!("Processing chromatograms...");

            // First, try to read chromatograms from mzML
            let chrom_count = self.stream_chromatograms(&mut streamer, &mut writer)?;
            stats.chromatograms_converted = chrom_count;

            // If no chromatograms were found in mzML and we have MS1 spectra, generate TIC/BPC
            if chrom_count == 0 && !tic_times.is_empty() {
                info!("Generating TIC and BPC from MS1 spectra...");

                // Create TIC chromatogram
                if let Ok(tic_chrom) = crate::chromatogram_writer::Chromatogram::new(
                    "TIC".to_string(),
                    "TIC".to_string(),
                    tic_times,
                    tic_intensities,
                ) {
                    writer
                        .write_chromatogram(&tic_chrom)
                        .map_err(|e| ConversionError::WriterError(WriterError::InvalidData(e.to_string())))?;
                    stats.chromatograms_converted += 1;
                }

                // Create BPC chromatogram
                if let Ok(bpc_chrom) = crate::chromatogram_writer::Chromatogram::new(
                    "BPC".to_string(),
                    "BPC".to_string(),
                    bpc_times,
                    bpc_intensities,
                ) {
                    writer
                        .write_chromatogram(&bpc_chrom)
                        .map_err(|e| ConversionError::WriterError(WriterError::InvalidData(e.to_string())))?;
                    stats.chromatograms_converted += 1;
                }

                info!("Generated TIC and BPC chromatograms");
            }

            info!("  Chromatograms: {}", stats.chromatograms_converted);
        }

        // Close dataset (finalizes both peaks and chromatograms)
        let dataset_stats = writer.close()?;
        info!("Dataset finalized: {}", dataset_stats);

        // Get output file size
        stats.output_file_size = std::fs::metadata(output_path)?.len();
        if stats.output_file_size > 0 {
            stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
        }

        info!("Conversion complete:");
        info!(
            "  Spectra: {} (MS1: {}, MS2: {}, MSn: {})",
            stats.spectra_count, stats.ms1_spectra, stats.ms2_spectra, stats.msn_spectra
        );
        info!("  Peaks: {}", stats.peak_count);
        info!("  Input size: {} bytes", stats.source_file_size);
        info!("  Output size: {} bytes", stats.output_file_size);
        info!("  Compression ratio: {:.2}x", stats.compression_ratio);

        Ok(stats)
    }

    /// Convert an mzML file to mzPeak format using rolling writer (for large datasets)
    pub fn convert_with_sharding<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
    ) -> Result<ConversionStats, ConversionError> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();

        info!(
            "Converting {} to {} (with sharding)",
            input_path.display(),
            output_path.display()
        );

        // Get source file size
        let source_file_size = std::fs::metadata(input_path)?.len();

        // Open the mzML file
        let mut streamer = MzMLStreamer::open(input_path)?;

        // Read metadata first
        let mzml_metadata = streamer.read_metadata()?;
        info!("mzML version: {:?}", mzml_metadata.version);

        // Convert mzML metadata to mzPeak metadata
        let mzpeak_metadata = self.convert_metadata(mzml_metadata, input_path)?;

        // Create the rolling writer
        let mut writer =
            RollingWriter::new(output_path, mzpeak_metadata, self.config.writer_config.clone())?;

        // Process spectra in batches
        let mut stats = ConversionStats {
            source_file_size,
            ..Default::default()
        };

        let mut batch: Vec<Spectrum> = Vec::with_capacity(self.config.batch_size);
        let expected_count = streamer.spectrum_count();

        info!(
            "Converting {} spectra...",
            expected_count
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );

        while let Some(mzml_spectrum) = streamer.next_spectrum()? {
            let spectrum = self.convert_spectrum(&mzml_spectrum);

            // Update statistics
            stats.spectra_count += 1;
            stats.peak_count += spectrum.peak_count();

            match mzml_spectrum.ms_level {
                1 => stats.ms1_spectra += 1,
                2 => stats.ms2_spectra += 1,
                _ => stats.msn_spectra += 1,
            }

            batch.push(spectrum);

            // Write batch if full
            if batch.len() >= self.config.batch_size {
                writer.write_spectra(&batch)?;
                batch.clear();

                // Progress update
                if stats.spectra_count % self.config.progress_interval == 0 {
                    if let Some(total) = expected_count {
                        let pct = (stats.spectra_count as f64 / total as f64) * 100.0;
                        info!(
                            "Progress: {}/{} spectra ({:.1}%)",
                            stats.spectra_count, total, pct
                        );
                    } else {
                        info!("Progress: {} spectra", stats.spectra_count);
                    }
                }
            }
        }

        // Write remaining spectra
        if !batch.is_empty() {
            writer.write_spectra(&batch)?;
        }

        // Finalize
        let writer_stats = writer.finish()?;
        info!("{}", writer_stats);

        // Calculate total output size from all parts
        stats.output_file_size = writer_stats
            .part_stats
            .iter()
            .map(|s| s.file_size_bytes)
            .sum();

        if stats.output_file_size > 0 {
            stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
        }

        info!("Conversion complete:");
        info!(
            "  Spectra: {} (MS1: {}, MS2: {}, MSn: {})",
            stats.spectra_count, stats.ms1_spectra, stats.ms2_spectra, stats.msn_spectra
        );
        info!("  Peaks: {}", stats.peak_count);
        info!("  Input size: {} bytes", stats.source_file_size);
        info!(
            "  Output size: {} bytes ({} files)",
            stats.output_file_size, writer_stats.files_written
        );
        info!("  Compression ratio: {:.2}x", stats.compression_ratio);

        Ok(stats)
    }
}
