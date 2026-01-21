use std::path::Path;

use log::info;

use super::{ConversionError, ConversionStats, MzMLConverter, OutputFormat};
use super::spectrum::DecodedRawSpectrum;
use super::super::models::RawMzMLSpectrum;
use super::super::streamer::MzMLStreamer;
use crate::dataset::{DatasetWriterV2Config, MzPeakDatasetWriter, MzPeakDatasetWriterV2};
use crate::ingest::IngestSpectrumConverter;
use crate::schema::manifest::Modality;
use crate::writer::{
    PeaksWriterV2Config, RollingWriter, SpectraWriterConfig, SpectrumArrays, SpectrumV2,
    WriterError,
};

impl MzMLConverter {
    /// Convert an mzML file to mzPeak format
    pub fn convert<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
    ) -> Result<ConversionStats, ConversionError> {
        match self.config.output_format {
            OutputFormat::V1Parquet => self.convert_v1_legacy(input_path, output_path),
            OutputFormat::V2Container => self.convert_v2_container(input_path, output_path),
        }
    }

    fn convert_v1_legacy<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
    ) -> Result<ConversionStats, ConversionError> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();

        info!("Converting {} to {}", input_path.display(), output_path.display());

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

        let mut batch: Vec<SpectrumArrays> = Vec::with_capacity(self.config.batch_size);
        let mut ingest_converter = IngestSpectrumConverter::new();
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

        while let Some(raw_spectrum) = streamer.next_raw_spectrum()? {
            let DecodedRawSpectrum {
                ingest,
                retention_time,
                total_ion_current,
                base_peak_intensity,
            } = self.build_ingest_spectrum_raw(raw_spectrum)?;
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

            batch.push(spectrum);

            // Write batch if full
            if batch.len() >= self.config.batch_size {
                writer.write_spectra_owned(batch)?;
                batch = Vec::with_capacity(self.config.batch_size);

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
            writer.write_spectra_owned(batch)?;
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

    fn convert_v2_container<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
    ) -> Result<ConversionStats, ConversionError> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();

        info!(
            "Converting {} to {} (v2 container)",
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

        let mut ingest_converter = IngestSpectrumConverter::new();
        let expected_count = streamer.spectrum_count();

        info!(
            "Converting {} spectra...",
            expected_count
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );

        if let Some(raw) = pending_raw.take() {
            let spectrum_v2 =
                self.build_spectrum_v2_from_raw(raw, &mut ingest_converter, modality)?;
            writer.write_spectrum(&spectrum_v2)?;
            update_v2_stats(&mut stats, &spectrum_v2);
            log_progress(&stats, expected_count, self.config.progress_interval);
        }

        while let Some(raw_spectrum) = streamer.next_raw_spectrum()? {
            let spectrum_v2 =
                self.build_spectrum_v2_from_raw(raw_spectrum, &mut ingest_converter, modality)?;
            writer.write_spectrum(&spectrum_v2)?;
            update_v2_stats(&mut stats, &spectrum_v2);
            log_progress(&stats, expected_count, self.config.progress_interval);
        }

        let dataset_stats = writer.close()?;
        info!("Dataset finalized: {}", dataset_stats);

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

    fn build_spectrum_v2_from_raw(
        &self,
        raw_spectrum: RawMzMLSpectrum,
        ingest_converter: &mut IngestSpectrumConverter,
        modality: Modality,
    ) -> Result<SpectrumV2, ConversionError> {
        let DecodedRawSpectrum {
            ingest,
            retention_time: _,
            total_ion_current: _,
            base_peak_intensity: _,
        } = self.build_ingest_spectrum_raw(raw_spectrum)?;

        let spectrum = ingest_converter
            .convert(ingest)
            .map_err(WriterError::from)?;

        let spectrum_v2 =
            SpectrumV2::try_from_spectrum_arrays(spectrum).map_err(ConversionError::WriterError)?;

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
            if spectrum_v2.metadata.pixel_x.is_none() || spectrum_v2.metadata.pixel_y.is_none() {
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

        Ok(spectrum_v2)
    }

    /// Convert an mzML file to mzPeak format using rolling writer (for large datasets)
    pub fn convert_with_sharding<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        input_path: P,
        output_path: Q,
    ) -> Result<ConversionStats, ConversionError> {
        if self.config.output_format != OutputFormat::V1Parquet {
            return Err(ConversionError::WriterError(WriterError::InvalidData(
                "sharded output is only supported for legacy v1 parquet".to_string(),
            )));
        }

        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();

        info!(
            "Converting {} to {} (with sharding)",
            input_path.display(),
            output_path.display()
        );

        // Get source file size
        let source_file_size = std::fs::metadata(input_path)?.len();

        // Open the mzML file with configured buffer size
        let buffer_size = self.config.streaming_config.input_buffer_size;
        let mut streamer = MzMLStreamer::open_with_buffer_size(input_path, buffer_size)?;

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

        let mut batch: Vec<SpectrumArrays> = Vec::with_capacity(self.config.batch_size);
        let mut ingest_converter = IngestSpectrumConverter::new();
        let expected_count = streamer.spectrum_count();

        info!(
            "Converting {} spectra...",
            expected_count
                .map(|c| c.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );

        while let Some(raw_spectrum) = streamer.next_raw_spectrum()? {
            let DecodedRawSpectrum { ingest, .. } = self.build_ingest_spectrum_raw(raw_spectrum)?;
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

            batch.push(spectrum);

            // Write batch if full
            if batch.len() >= self.config.batch_size {
                writer.write_spectra_owned(batch)?;
                batch = Vec::with_capacity(self.config.batch_size);

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
            writer.write_spectra_owned(batch)?;
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
