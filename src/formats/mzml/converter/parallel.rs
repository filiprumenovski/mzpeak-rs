use std::path::Path;

use log::info;
use rayon::prelude::*;

use super::{ConversionError, ConversionStats, MzMLConverter, OutputFormat};
use super::super::models::RawMzMLSpectrum;
use super::super::streamer::MzMLStreamer;
use crate::dataset::{DatasetWriterV2Config, MzPeakDatasetWriter, MzPeakDatasetWriterV2};
use crate::ingest::IngestSpectrumConverter;
use crate::schema::manifest::Modality;
use crate::writer::{
    PeaksWriterV2Config, SpectraWriterConfig, SpectrumArrays, SpectrumV2, WriterError,
};
use super::spectrum::DecodedRawSpectrum;

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
        match self.config.output_format {
            OutputFormat::V1Parquet => self.convert_parallel_v1_legacy(input_path, output_path),
            OutputFormat::V2Container => self.convert_parallel_v2_container(input_path, output_path),
        }
    }

    fn convert_parallel_v1_legacy<P: AsRef<Path>, Q: AsRef<Path>>(
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

        // Open the mzML/imzML file with configured buffer size
        let buffer_size = self.config.streaming_config.input_buffer_size;
        let mut streamer = if is_imzml_path(input_path) {
            MzMLStreamer::open_imzml_with_buffer_size(input_path, buffer_size)?
        } else {
            MzMLStreamer::open_with_buffer_size(input_path, buffer_size)?
        };

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
        let mut ingest_converter = IngestSpectrumConverter::new();

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
                    .map(|raw| self.build_ingest_spectrum_raw(raw))
                    .collect::<Result<_, _>>()?;

                // Process decoded spectra
                let write_batch = self.process_decoded_batch(
                    decoded_batch,
                    &mut stats,
                    &mut tic_times,
                    &mut tic_intensities,
                    &mut bpc_times,
                    &mut bpc_intensities,
                    &mut ingest_converter,
                )?;

                // Write to output
                writer.write_spectra_owned(write_batch)?;

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
                .map(|raw| self.build_ingest_spectrum_raw(raw))
                .collect::<Result<_, _>>()?;

            let write_batch = self.process_decoded_batch(
                decoded_batch,
                &mut stats,
                &mut tic_times,
                &mut tic_intensities,
                &mut bpc_times,
                &mut bpc_intensities,
                &mut ingest_converter,
            )?;

            writer.write_spectra_owned(write_batch)?;
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

    fn convert_parallel_v2_container<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
    ) -> Result<ConversionStats, ConversionError> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();

        info!(
            "Converting {} to {} (parallel v2 container)",
            input_path.display(),
            output_path.display()
        );

        let source_file_size = std::fs::metadata(input_path)?.len();
        let buffer_size = self.config.streaming_config.input_buffer_size;
        let mut streamer = if is_imzml_path(input_path) {
            MzMLStreamer::open_imzml_with_buffer_size(input_path, buffer_size)?
        } else {
            MzMLStreamer::open_with_buffer_size(input_path, buffer_size)?
        };

        let mzml_metadata = streamer.read_metadata()?;
        info!("mzML version: {:?}", mzml_metadata.version);

        let mzpeak_metadata = self.convert_metadata(mzml_metadata, input_path)?;

        let mut pending_raw = streamer.next_raw_spectrum()?;
        let mut has_imaging = is_imzml_path(input_path);
        let mut has_ion_mobility = false;
        if let Some(ref raw) = pending_raw {
            if raw.pixel_x.is_some() && raw.pixel_y.is_some() {
                has_imaging = true;
            }
            has_ion_mobility = raw.ion_mobility_data.is_some();
        }

        let modality = self
            .config
            .modality
            .unwrap_or_else(|| Modality::from_flags(has_ion_mobility, has_imaging));

        let dataset_config = DatasetWriterV2Config {
            spectra_config: SpectraWriterConfig {
                compression: self.config.writer_config.compression,
                ..Default::default()
            },
            peaks_config: PeaksWriterV2Config {
                compression: self.config.writer_config.compression,
                row_group_size: self.config.writer_config.row_group_size,
                ..Default::default()
            },
        };

        let vendor_hints = mzpeak_metadata.vendor_hints.clone();
        let mut writer =
            MzPeakDatasetWriterV2::with_config(output_path, modality, vendor_hints, dataset_config)?;
        writer.set_metadata(mzpeak_metadata);

        let mut stats = ConversionStats {
            source_file_size,
            ..Default::default()
        };

        let parallel_batch_size = self.config.parallel_batch_size;
        let mut raw_batch: Vec<RawMzMLSpectrum> = Vec::with_capacity(parallel_batch_size);
        let expected_count = streamer.spectrum_count();
        let mut ingest_converter = IngestSpectrumConverter::new();

        info!(
            "Converting {} spectra (parallel, batch_size={})...",
            expected_count
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            parallel_batch_size
        );

        if let Some(raw) = pending_raw.take() {
            raw_batch.push(raw);
        }

        while let Some(raw_spectrum) = streamer.next_raw_spectrum()? {
            raw_batch.push(raw_spectrum);

            if raw_batch.len() >= parallel_batch_size {
                let decoded_batch: Vec<_> = raw_batch
                    .par_drain(..)
                    .map(|raw| self.build_ingest_spectrum_raw(raw))
                    .collect::<Result<_, _>>()?;

                let write_batch = self.process_decoded_batch_v2(
                    decoded_batch,
                    &mut stats,
                    &mut ingest_converter,
                    modality,
                )?;

                writer.write_spectra(&write_batch)?;
                log_progress(&stats, expected_count, self.config.progress_interval);
            }
        }

        if !raw_batch.is_empty() {
            let decoded_batch: Vec<_> = raw_batch
                .par_drain(..)
                .map(|raw| self.build_ingest_spectrum_raw(raw))
                .collect::<Result<_, _>>()?;

            let write_batch = self.process_decoded_batch_v2(
                decoded_batch,
                &mut stats,
                &mut ingest_converter,
                modality,
            )?;

            writer.write_spectra(&write_batch)?;
        }

        let dataset_stats = writer.close()?;
        info!("Dataset finalized: {}", dataset_stats);

        stats.output_file_size = std::fs::metadata(output_path)?.len();
        if stats.output_file_size > 0 {
            stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
        }

        info!("Conversion complete (parallel v2):");
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
        decoded_batch: Vec<DecodedRawSpectrum>,
        stats: &mut ConversionStats,
        tic_times: &mut Vec<f64>,
        tic_intensities: &mut Vec<f32>,
        bpc_times: &mut Vec<f64>,
        bpc_intensities: &mut Vec<f32>,
        ingest_converter: &mut IngestSpectrumConverter,
    ) -> Result<Vec<SpectrumArrays>, ConversionError> {
        let mut write_batch = Vec::with_capacity(decoded_batch.len());

        for decoded in decoded_batch {
            let DecodedRawSpectrum {
                ingest,
                retention_time,
                total_ion_current,
                base_peak_intensity,
            } = decoded;
            let spectrum = ingest_converter
                .convert(ingest)
                .map_err(WriterError::from)?;

            // Update statistics
            stats.spectra_count += 1;
            stats.peak_count += spectrum.peak_count();

            match spectrum.ms_level {
                1 => stats.ms1_spectra += 1,
                2 => stats.ms2_spectra += 1,
                _ => stats.msn_spectra += 1,
            }

            // Accumulate TIC and BPC for MS1 spectra only
            if spectrum.ms_level == 1 {
                let rt = retention_time.unwrap_or(0.0);
                let tic = total_ion_current
                    .map(|value| value as f32)
                    .unwrap_or_else(|| spectrum.total_ion_current.unwrap_or(0.0) as f32);
                let bpc = base_peak_intensity
                    .map(|value| value as f32)
                    .unwrap_or_else(|| spectrum.base_peak_intensity.unwrap_or(0.0));

                tic_times.push(rt);
                tic_intensities.push(tic);
                bpc_times.push(rt);
                bpc_intensities.push(bpc);
            }

            write_batch.push(spectrum);
        }

        Ok(write_batch)
    }

    fn process_decoded_batch_v2(
        &self,
        decoded_batch: Vec<DecodedRawSpectrum>,
        stats: &mut ConversionStats,
        ingest_converter: &mut IngestSpectrumConverter,
        modality: Modality,
    ) -> Result<Vec<SpectrumV2>, ConversionError> {
        let mut write_batch = Vec::with_capacity(decoded_batch.len());

        for decoded in decoded_batch {
            let DecodedRawSpectrum { ingest, .. } = decoded;
            let spectrum = ingest_converter
                .convert(ingest)
                .map_err(WriterError::from)?;

            let spectrum_v2 = SpectrumV2::try_from_spectrum_arrays(spectrum)
                .map_err(ConversionError::WriterError)?;

            if modality.has_ion_mobility() {
                if spectrum_v2.peaks.ion_mobility.is_none() {
                    return Err(ConversionError::WriterError(WriterError::InvalidData(
                        "ion_mobility missing for modality requiring it".to_string(),
                    )));
                }
            } else if spectrum_v2.peaks.ion_mobility.is_some() {
                return Err(ConversionError::WriterError(WriterError::InvalidData(
                    "ion_mobility present for modality without it".to_string(),
                )));
            }

            if modality.has_imaging() {
                if spectrum_v2.metadata.pixel_x.is_none()
                    || spectrum_v2.metadata.pixel_y.is_none()
                {
                    return Err(ConversionError::WriterError(WriterError::InvalidData(
                        "pixel coordinates missing for imaging modality".to_string(),
                    )));
                }
            } else if spectrum_v2.metadata.pixel_x.is_some()
                || spectrum_v2.metadata.pixel_y.is_some()
                || spectrum_v2.metadata.pixel_z.is_some()
            {
                return Err(ConversionError::WriterError(WriterError::InvalidData(
                    "imaging coordinates present for non-imaging modality".to_string(),
                )));
            }

            update_v2_stats(stats, &spectrum_v2);
            write_batch.push(spectrum_v2);
        }

        Ok(write_batch)
    }
}

fn update_v2_stats(stats: &mut ConversionStats, spectrum: &SpectrumV2) {
    stats.spectra_count += 1;
    stats.peak_count += spectrum.peaks.len();

    match spectrum.metadata.ms_level {
        1 => stats.ms1_spectra += 1,
        2 => stats.ms2_spectra += 1,
        _ => stats.msn_spectra += 1,
    }
}

fn log_progress(stats: &ConversionStats, expected_count: Option<usize>, interval: usize) {
    if stats.spectra_count % interval == 0 {
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

fn is_imzml_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("imzml"))
        .unwrap_or(false)
}
