//! mzML to mzPeak converter
//!
//! This module provides the high-level conversion pipeline from mzML files
//! to the mzPeak Parquet format, preserving all metadata and numerical precision.

use std::path::Path;

use log::{info, warn};

use super::cv_params::MS_CV_ACCESSIONS;
use super::models::*;
use super::streamer::{MzMLError, MzMLStreamer};
use crate::metadata::{
    InstrumentConfig, MassAnalyzerConfig, MzPeakMetadata, ProcessingHistory,
    ProcessingStep, RunParameters, SdrfMetadata, SourceFileInfo,
};
use crate::dataset::MzPeakDatasetWriter;
use crate::writer::{Peak, RollingWriter, Spectrum, SpectrumBuilder, WriterConfig, WriterError};

/// Errors that can occur during conversion
#[derive(Debug, thiserror::Error)]
pub enum ConversionError {
    #[error("mzML parsing error: {0}")]
    MzMLError(#[from] MzMLError),

    #[error("Writer error: {0}")]
    WriterError(#[from] WriterError),

    #[error("Dataset error: {0}")]
    DatasetError(#[from] crate::dataset::DatasetError),

    #[error("Chromatogram writer error: {0}")]
    ChromatogramWriterError(#[from] crate::chromatogram_writer::ChromatogramWriterError),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Metadata error: {0}")]
    MetadataError(#[from] crate::metadata::MetadataError),
}

/// Configuration for the mzML to mzPeak conversion
#[derive(Debug, Clone)]
pub struct ConversionConfig {
    /// Writer configuration
    pub writer_config: WriterConfig,

    /// Batch size for writing spectra
    pub batch_size: usize,

    /// Whether to preserve original precision (32/64 bit)
    /// If false, all data is stored as the schema default
    pub preserve_precision: bool,

    /// Whether to include chromatograms
    pub include_chromatograms: bool,

    /// Optional SDRF file path
    pub sdrf_path: Option<String>,

    /// Progress callback interval (spectra count)
    pub progress_interval: usize,
}

impl Default for ConversionConfig {
    fn default() -> Self {
        Self {
            writer_config: WriterConfig::default(),
            batch_size: 100,
            preserve_precision: true,
            include_chromatograms: true, // Enable chromatograms for wide-schema
            sdrf_path: None,
            progress_interval: 1000,
        }
    }
}

impl ConversionConfig {
    /// Configuration optimized for maximum compression (slower conversion)
    /// Best for archival storage or when file size is critical
    /// Expected: 2-3x better compression than default
    pub fn max_compression() -> Self {
        Self {
            writer_config: WriterConfig::max_compression(),
            batch_size: 500, // Larger batches for better compression
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
        }
    }

    /// Configuration optimized for fast conversion (larger files)
    /// Best for quick prototyping or when write speed is critical
    pub fn fast_write() -> Self {
        Self {
            writer_config: WriterConfig::fast_write(),
            batch_size: 50, // Smaller batches for faster throughput
            preserve_precision: true,
            include_chromatograms: true,
            sdrf_path: None,
            progress_interval: 1000,
        }
    }

    /// Balanced configuration (default)
    /// Good balance between compression ratio and conversion speed
    pub fn balanced() -> Self {
        Self::default()
    }
}

/// Statistics from a conversion
#[derive(Debug, Clone, Default)]
pub struct ConversionStats {
    /// Total spectra converted
    pub spectra_count: usize,
    /// Total peaks converted
    pub peak_count: usize,
    pub ms1_spectra: usize,
    pub ms2_spectra: usize,
    pub msn_spectra: usize,
    pub chromatograms_converted: usize,
    pub source_file_size: u64,
    pub output_file_size: u64,
    pub compression_ratio: f64,
}

/// Converter from mzML to mzPeak format
pub struct MzMLConverter {
    config: ConversionConfig,
}

impl MzMLConverter {
    /// Create a new converter with default configuration
    pub fn new() -> Self {
        Self {
            config: ConversionConfig::default(),
        }
    }

    /// Create a new converter with custom configuration
    pub fn with_config(config: ConversionConfig) -> Self {
        Self { config }
    }

    /// Set batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

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
        let mut writer =
            MzPeakDatasetWriter::new(output_path, &mzpeak_metadata, self.config.writer_config.clone())?;

        // Process spectra in batches
        let mut stats = ConversionStats {
            source_file_size,
            ..Default::default()
        };

        let mut batch: Vec<Spectrum> = Vec::with_capacity(self.config.batch_size);
        let expected_count = streamer.spectrum_count();

        info!(
            "Converting {} spectra...",
            expected_count.map(|c| c.to_string()).unwrap_or_else(|| "unknown".to_string())
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

        // Finalize spectrum writer first
        info!("Finalizing peak data...");

        // Process chromatograms if enabled and integrate into same dataset
        if self.config.include_chromatograms {
            info!("Processing chromatograms...");
            let chrom_count = self.stream_chromatograms(&mut streamer, &mut writer)?;
            stats.chromatograms_converted = chrom_count;
            info!("  Chromatograms: {}", chrom_count);
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
        info!("  Spectra: {} (MS1: {}, MS2: {}, MSn: {})",
            stats.spectra_count, stats.ms1_spectra, stats.ms2_spectra, stats.msn_spectra);
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

        info!("Converting {} to {} (with sharding)", input_path.display(), output_path.display());

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
            expected_count.map(|c| c.to_string()).unwrap_or_else(|| "unknown".to_string())
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
        stats.output_file_size = writer_stats.part_stats.iter()
            .map(|s| s.file_size_bytes)
            .sum();
        
        if stats.output_file_size > 0 {
            stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
        }

        info!("Conversion complete:");
        info!("  Spectra: {} (MS1: {}, MS2: {}, MSn: {})",
            stats.spectra_count, stats.ms1_spectra, stats.ms2_spectra, stats.msn_spectra);
        info!("  Peaks: {}", stats.peak_count);
        info!("  Input size: {} bytes", stats.source_file_size);
        info!("  Output size: {} bytes ({} files)", stats.output_file_size, writer_stats.files_written);
        info!("  Compression ratio: {:.2}x", stats.compression_ratio);

        Ok(stats)
    }

    /// Stream chromatograms directly to the dataset writer
    fn stream_chromatograms<R: std::io::BufRead>(
        &self,
        streamer: &mut MzMLStreamer<R>,
        writer: &mut MzPeakDatasetWriter,
    ) -> Result<usize, ConversionError> {
        let mut count = 0;
        while let Some(mzml_chrom) = streamer.next_chromatogram()? {
            let chromatogram = self.convert_chromatogram(&mzml_chrom)?;
            writer.write_chromatogram(&chromatogram)
                .map_err(|e| ConversionError::WriterError(WriterError::InvalidData(e.to_string())))?;
            count += 1;
        }
        Ok(count)
    }

    /// Convert chromatograms from mzML to mzPeak format (deprecated, kept for backward compatibility)
    #[allow(dead_code)]
    fn convert_chromatograms<R: std::io::BufRead>(
        &self,
        streamer: &mut MzMLStreamer<R>,
        output_path: &Path,
        metadata: &MzPeakMetadata,
    ) -> Result<usize, ConversionError> {
        use crate::chromatogram_writer::{ChromatogramWriter, ChromatogramWriterConfig};

        // Create output path for chromatograms subdirectory
        let chromatogram_dir = output_path.parent().unwrap_or(Path::new(".")).join("chromatograms");
        std::fs::create_dir_all(&chromatogram_dir)?;

        let chromatogram_file = chromatogram_dir.join(
            output_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("chromatograms")
        ).with_extension("chromatograms.parquet");

        let config = ChromatogramWriterConfig::default();
        let mut writer = ChromatogramWriter::new_file(&chromatogram_file, metadata, config)?;

        let mut count = 0;
        while let Some(mzml_chrom) = streamer.next_chromatogram()? {
            let chromatogram = self.convert_chromatogram(&mzml_chrom)?;
            writer.write_chromatogram(&chromatogram)?;
            count += 1;
        }

        let stats = writer.finish()?;
        info!("Wrote {} chromatograms to {}", stats.chromatograms_written, chromatogram_file.display());

        Ok(count)
    }

    /// Convert a single mzML chromatogram to mzPeak format
    fn convert_chromatogram(&self, mzml_chrom: &MzMLChromatogram) -> Result<crate::chromatogram_writer::Chromatogram, ConversionError> {
        use crate::chromatogram_writer::Chromatogram;

        // Convert chromatogram type to string
        let chrom_type = match mzml_chrom.chromatogram_type {
            ChromatogramType::TIC => "TIC",
            ChromatogramType::BPC => "BPC",
            ChromatogramType::SIM => "SIM",
            ChromatogramType::SRM => "SRM",
            ChromatogramType::XIC => "XIC",
            ChromatogramType::Absorption => "Absorption",
            ChromatogramType::Emission => "Emission",
            ChromatogramType::Unknown => "Unknown",
        };

        // Convert intensity array from f64 to f32
        let intensity_array: Vec<f32> = mzml_chrom.intensity_array.iter().map(|&x| x as f32).collect();

        Chromatogram::new(
            mzml_chrom.id.clone(),
            chrom_type.to_string(),
            mzml_chrom.time_array.clone(),
            intensity_array,
        ).map_err(|e| ConversionError::WriterError(crate::writer::WriterError::InvalidData(e.to_string())))
    }

    /// Convert mzML file metadata to mzPeak metadata
    fn convert_metadata(
        &self,
        mzml: &MzMLFileMetadata,
        input_path: &Path,
    ) -> Result<MzPeakMetadata, ConversionError> {
        let mut metadata = MzPeakMetadata::new();

        // Source file information
        let mut source = SourceFileInfo::new(
            input_path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown"),
        );
        source.path = input_path.to_str().map(String::from);
        source.format = Some("mzML".to_string());
        source.size_bytes = std::fs::metadata(input_path).ok().map(|m| m.len());

        // Extract checksum from mzML source files
        if let Some(mzml_source) = mzml.source_files.first() {
            source.sha256 = mzml_source.checksum.clone();
            if let Some(ref format) = mzml_source.file_format {
                source.format = Some(format.clone());
            }
        }
        metadata.source_file = Some(source);

        // Instrument configuration
        if let Some(ic) = mzml.instrument_configurations.first() {
            let mut instrument = InstrumentConfig::new();

            // Extract instrument info from CV params
            for cv in &ic.cv_params {
                if cv.name.contains("instrument model") || cv.accession.starts_with("MS:10005") {
                    instrument.model = Some(cv.name.clone());
                }
            }

            // Extract software info
            if let Some(ref sw_ref) = ic.software_ref {
                if let Some(sw) = mzml.software_list.iter().find(|s| &s.id == sw_ref) {
                    instrument.software_version = sw.version.clone();
                }
            }

            // Process components
            for component in &ic.components {
                match component.component_type {
                    ComponentType::Source => {
                        for cv in &component.cv_params {
                            if cv.accession == MS_CV_ACCESSIONS::ESI
                                || cv.accession == MS_CV_ACCESSIONS::NANOESI
                            {
                                instrument.ion_source = Some("electrospray ionization".to_string());
                            } else if cv.accession == MS_CV_ACCESSIONS::MALDI {
                                instrument.ion_source = Some("MALDI".to_string());
                            }
                        }
                    }
                    ComponentType::Analyzer => {
                        let mut analyzer = MassAnalyzerConfig {
                            order: component.order,
                            ..Default::default()
                        };
                        for cv in &component.cv_params {
                            match cv.accession.as_str() {
                                MS_CV_ACCESSIONS::ORBITRAP => {
                                    analyzer.analyzer_type = "orbitrap".to_string();
                                }
                                MS_CV_ACCESSIONS::QUADRUPOLE => {
                                    analyzer.analyzer_type = "quadrupole".to_string();
                                }
                                MS_CV_ACCESSIONS::ION_TRAP => {
                                    analyzer.analyzer_type = "ion trap".to_string();
                                }
                                MS_CV_ACCESSIONS::TOF => {
                                    analyzer.analyzer_type = "time-of-flight".to_string();
                                }
                                _ => {}
                            }
                        }
                        instrument.mass_analyzers.push(analyzer);
                    }
                    ComponentType::Detector => {
                        for cv in &component.cv_params {
                            if cv.accession == MS_CV_ACCESSIONS::ELECTRON_MULTIPLIER {
                                instrument.detector = Some("electron multiplier".to_string());
                            } else if cv.accession == MS_CV_ACCESSIONS::INDUCTIVE_DETECTOR {
                                instrument.detector = Some("inductive detector".to_string());
                            }
                        }
                    }
                    _ => {}
                }
            }

            metadata.instrument = Some(instrument);
        }

        // Run parameters
        let mut run_params = RunParameters::new();
        run_params.start_time = mzml.run_start_time.clone();
        run_params.method_name = mzml.run_id.clone();

        // Extract software info
        for sw in &mzml.software_list {
            if let Some(ref name) = sw.name {
                run_params.add_vendor_param(
                    &format!("software_{}", sw.id),
                    &format!("{} v{}", name, sw.version.as_deref().unwrap_or("unknown")),
                );
            }
        }

        metadata.run_parameters = Some(run_params);

        // Processing history
        let mut history = ProcessingHistory::new();

        // Add original processing from mzML
        for dp in &mzml.data_processing {
            for pm in &dp.processing_methods {
                let mut params = std::collections::HashMap::new();
                for cv in &pm.cv_params {
                    params.insert(cv.accession.clone(), cv.name.clone());
                }

                history.add_step(ProcessingStep {
                    order: pm.order,
                    software: pm.software_ref.clone().unwrap_or_default(),
                    version: None,
                    processing_type: pm
                        .cv_params
                        .first()
                        .map(|cv| cv.name.clone())
                        .unwrap_or_else(|| "unknown".to_string()),
                    timestamp: None,
                    parameters: params,
                    cv_params: Default::default(),
                });
            }
        }

        // Add this conversion step
        history.add_step(ProcessingStep {
            order: history.steps.len() as i32 + 1,
            software: "mzpeak-rs".to_string(),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            processing_type: "Conversion to mzPeak".to_string(),
            timestamp: Some(chrono::Utc::now().to_rfc3339()),
            parameters: std::collections::HashMap::new(),
            cv_params: Default::default(),
        });

        metadata.processing_history = Some(history);

        // Load SDRF if provided
        if let Some(ref sdrf_path) = self.config.sdrf_path {
            match SdrfMetadata::from_tsv_file(sdrf_path) {
                Ok(sdrf_list) => {
                    if let Some(sdrf) = sdrf_list.into_iter().next() {
                        metadata.sdrf = Some(sdrf);
                    }
                }
                Err(e) => {
                    warn!("Failed to load SDRF file: {}", e);
                }
            }
        }

        Ok(metadata)
    }

    /// Convert a single mzML spectrum to mzPeak format
    fn convert_spectrum(&self, mzml: &MzMLSpectrum) -> Spectrum {
        let scan_number = mzml.scan_number().unwrap_or(mzml.index + 1);

        let mut builder = SpectrumBuilder::new(mzml.index, scan_number)
            .ms_level(mzml.ms_level)
            .retention_time(mzml.retention_time.unwrap_or(0.0) as f32)
            .polarity(mzml.polarity);

        // Add injection time if available
        if let Some(it) = mzml.ion_injection_time {
            builder = builder.injection_time(it as f32);
        }

        // Add precursor information for MS2+
        if mzml.ms_level >= 2 {
            if let Some(precursor) = mzml.precursors.first() {
                let precursor_mz = precursor
                    .selected_ion_mz
                    .or(precursor.isolation_window_target)
                    .unwrap_or(0.0);

                builder = builder.precursor(
                    precursor_mz,
                    precursor.selected_ion_charge,
                    precursor.selected_ion_intensity.map(|i| i as f32),
                );

                // Isolation window
                if let (Some(lower), Some(upper)) =
                    (precursor.isolation_window_lower, precursor.isolation_window_upper)
                {
                    builder = builder.isolation_window(lower as f32, upper as f32);
                }

                // Collision energy
                if let Some(ce) = precursor.collision_energy {
                    builder = builder.collision_energy(ce as f32);
                }
            }
        }

        // Convert peaks with ion mobility if available
        let peaks: Vec<Peak> = if !mzml.ion_mobility_array.is_empty() 
            && mzml.ion_mobility_array.len() == mzml.mz_array.len() {
            // With ion mobility data
            mzml.mz_array
                .iter()
                .zip(mzml.intensity_array.iter())
                .zip(mzml.ion_mobility_array.iter())
                .map(|((&mz, &intensity), &ion_mobility)| Peak {
                    mz,
                    intensity: intensity as f32,
                    ion_mobility: Some(ion_mobility),
                })
                .collect()
        } else {
            // Without ion mobility data
            mzml.mz_array
                .iter()
                .zip(mzml.intensity_array.iter())
                .map(|(&mz, &intensity)| Peak {
                    mz,
                    intensity: intensity as f32,
                    ion_mobility: None,
                })
                .collect()
        };

        builder = builder.peaks(peaks);

        builder.build()
    }
}

impl Default for MzMLConverter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spectrum_conversion() {
        let mzml_spectrum = MzMLSpectrum {
            index: 0,
            id: "scan=1".to_string(),
            ms_level: 1,
            polarity: 1,
            retention_time: Some(60.0),
            mz_array: vec![100.0, 200.0, 300.0],
            intensity_array: vec![1000.0, 2000.0, 500.0],
            ..Default::default()
        };

        let converter = MzMLConverter::new();
        let spectrum = converter.convert_spectrum(&mzml_spectrum);

        assert_eq!(spectrum.spectrum_id, 0);
        assert_eq!(spectrum.ms_level, 1);
        assert_eq!(spectrum.polarity, 1);
        assert_eq!(spectrum.retention_time, 60.0);
        assert_eq!(spectrum.peak_count(), 3);
    }

    #[test]
    fn test_ms2_spectrum_conversion() {
        let mzml_spectrum = MzMLSpectrum {
            index: 1,
            id: "scan=2".to_string(),
            ms_level: 2,
            polarity: 1,
            retention_time: Some(61.0),
            precursors: vec![Precursor {
                selected_ion_mz: Some(500.25),
                selected_ion_charge: Some(2),
                isolation_window_lower: Some(0.8),
                isolation_window_upper: Some(0.8),
                collision_energy: Some(30.0),
                ..Default::default()
            }],
            mz_array: vec![150.0, 250.0],
            intensity_array: vec![500.0, 1000.0],
            ..Default::default()
        };

        let converter = MzMLConverter::new();
        let spectrum = converter.convert_spectrum(&mzml_spectrum);

        assert_eq!(spectrum.ms_level, 2);
        assert_eq!(spectrum.precursor_mz, Some(500.25));
        assert_eq!(spectrum.precursor_charge, Some(2));
        assert_eq!(spectrum.collision_energy, Some(30.0));
    }
}
