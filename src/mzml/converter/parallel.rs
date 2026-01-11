use std::path::Path;

use log::info;
use rayon::prelude::*;

use super::{ConversionError, ConversionStats, MzMLConverter};
use super::super::models::{MzMLSpectrum, RawMzMLSpectrum};
use super::super::streamer::MzMLStreamer;
use crate::dataset::MzPeakDatasetWriter;
use crate::writer::WriterError;

impl MzMLConverter {
    /// Convert an mzML file to mzPeak format using parallel decoding
    ///
    /// This method implements a two-phase conversion pipeline:
    /// 1. **Phase 1 (Sequential)**: Parse XML and collect raw spectra without decoding
    /// 2. **Phase 2 (Parallel)**: Decode binary arrays in parallel using Rayon + SIMD
    ///
    /// # Performance
    /// - Expected 4-8x speedup over sequential conversion on compressed mzML files
    /// - Memory usage scales with `parallel_batch_size` (default ~8GB for 5000 spectra)
    /// - Uses SIMD-accelerated Base64 decoding and float conversion
    ///
    /// # Example
    /// ```ignore
    /// use mzpeak::mzml::{MzMLConverter, ConversionConfig};
    ///
    /// let mut config = ConversionConfig::default();
    /// config.parallel_batch_size = 10000; // Increase for more throughput
    ///
    /// let converter = MzMLConverter::with_config(config);
    /// let stats = converter.convert_parallel("input.mzML", "output.mzpeak")?;
    /// ```
    pub fn convert_parallel<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
    ) -> Result<ConversionStats, ConversionError> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();

        info!(
            "Converting {} to {} (parallel mode)",
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

        // Create the dataset writer
        let mut writer = MzPeakDatasetWriter::new(
            output_path,
            &mzpeak_metadata,
            self.config.writer_config.clone(),
        )?;

        // Process spectra in batches with parallel decoding
        let mut stats = ConversionStats {
            source_file_size,
            ..Default::default()
        };

        let parallel_batch_size = self.config.parallel_batch_size;
        let mut raw_batch: Vec<RawMzMLSpectrum> = Vec::with_capacity(parallel_batch_size);
        let expected_count = streamer.spectrum_count();

        // Accumulate TIC and BPC data during spectrum processing
        let mut tic_times: Vec<f64> = Vec::new();
        let mut tic_intensities: Vec<f32> = Vec::new();
        let mut bpc_times: Vec<f64> = Vec::new();
        let mut bpc_intensities: Vec<f32> = Vec::new();

        info!(
            "Converting {} spectra (parallel, batch_size={})...",
            expected_count
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            parallel_batch_size
        );

        // Phase 1: Collect raw spectra in batches
        while let Some(raw_spectrum) = streamer.next_raw_spectrum()? {
            raw_batch.push(raw_spectrum);

            if raw_batch.len() >= parallel_batch_size {
                // Phase 2: Parallel decode this batch
                let decoded_batch: Vec<_> = raw_batch
                    .par_drain(..)
                    .map(|raw| {
                        let index = raw.index;
                        let id = raw.id.clone();
                        raw.decode().map_err(|err| ConversionError::BinaryDecodeError {
                            index,
                            id,
                            source: err,
                        })
                    })
                    .collect::<Result<_, _>>()?;

                // Process decoded spectra
                let write_batch = self.process_decoded_batch(
                    decoded_batch,
                    &mut stats,
                    &mut tic_times,
                    &mut tic_intensities,
                    &mut bpc_times,
                    &mut bpc_intensities,
                );

                // Write to output
                writer.write_spectra(&write_batch)?;

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

        // Process remaining spectra
        if !raw_batch.is_empty() {
            let decoded_batch: Vec<_> = raw_batch
                .par_drain(..)
                .map(|raw| {
                    let index = raw.index;
                    let id = raw.id.clone();
                    raw.decode().map_err(|err| ConversionError::BinaryDecodeError {
                        index,
                        id,
                        source: err,
                    })
                })
                .collect::<Result<_, _>>()?;

            let write_batch = self.process_decoded_batch(
                decoded_batch,
                &mut stats,
                &mut tic_times,
                &mut tic_intensities,
                &mut bpc_times,
                &mut bpc_intensities,
            );

            writer.write_spectra(&write_batch)?;
        }

        // Finalize spectrum writer
        info!("Finalizing peak data...");

        // Process chromatograms if enabled
        if self.config.include_chromatograms {
            info!("Processing chromatograms...");

            // First, try to read chromatograms from mzML
            let chrom_count = self.stream_chromatograms(&mut streamer, &mut writer)?;
            stats.chromatograms_converted = chrom_count;

            // If no chromatograms were found and we have MS1 spectra, generate TIC/BPC
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

        info!("Conversion complete (parallel):");
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

    /// Process a batch of decoded spectra, updating stats and accumulating TIC/BPC
    fn process_decoded_batch(
        &self,
        decoded_batch: Vec<MzMLSpectrum>,
        stats: &mut ConversionStats,
        tic_times: &mut Vec<f64>,
        tic_intensities: &mut Vec<f32>,
        bpc_times: &mut Vec<f64>,
        bpc_intensities: &mut Vec<f32>,
    ) -> Vec<crate::writer::Spectrum> {
        let mut write_batch = Vec::with_capacity(decoded_batch.len());

        for mzml_spectrum in decoded_batch {
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

            write_batch.push(spectrum);
        }

        write_batch
    }
}
