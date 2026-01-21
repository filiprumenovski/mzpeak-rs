use std::path::Path;

use log::{info, warn};

use super::{ConversionError, MzMLConverter};
use super::super::cv_params::MS_CV_ACCESSIONS;
use super::super::models::{ChromatogramType, ComponentType, MzMLChromatogram, MzMLFileMetadata};
use super::super::streamer::MzMLStreamer;
use crate::dataset::MzPeakDatasetWriter;
use crate::metadata::{
    InstrumentConfig, MassAnalyzerConfig, MzPeakMetadata, ProcessingHistory, ProcessingStep,
    RunParameters, SdrfMetadata, SourceFileInfo,
};
use crate::writer::WriterError;

impl MzMLConverter {
    /// Stream chromatograms directly to the dataset writer
    pub(crate) fn stream_chromatograms<R: std::io::BufRead>(
        &self,
        streamer: &mut MzMLStreamer<R>,
        writer: &mut MzPeakDatasetWriter,
    ) -> Result<usize, ConversionError> {
        let mut count = 0;
        while let Some(mzml_chrom) = streamer.next_chromatogram()? {
            let chromatogram = self.convert_chromatogram(&mzml_chrom)?;
            writer
                .write_chromatogram(&chromatogram)
                .map_err(|e| ConversionError::WriterError(WriterError::InvalidData(e.to_string())))?;
            count += 1;
        }
        Ok(count)
    }

    /// Convert chromatograms from mzML to mzPeak format (deprecated, kept for backward compatibility)
    #[allow(dead_code)]
    pub(crate) fn convert_chromatograms<R: std::io::BufRead>(
        &self,
        streamer: &mut MzMLStreamer<R>,
        output_path: &Path,
        metadata: &MzPeakMetadata,
    ) -> Result<usize, ConversionError> {
        use crate::chromatogram_writer::{ChromatogramWriter, ChromatogramWriterConfig};

        // Create output path for chromatograms subdirectory
        let chromatogram_dir = output_path
            .parent()
            .unwrap_or(Path::new("."))
            .join("chromatograms");
        std::fs::create_dir_all(&chromatogram_dir)?;

        let chromatogram_file = chromatogram_dir
            .join(
                output_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("chromatograms"),
            )
            .with_extension("chromatograms.parquet");

        let config = ChromatogramWriterConfig::default();
        let mut writer = ChromatogramWriter::new_file(&chromatogram_file, metadata, config)?;

        let mut count = 0;
        while let Some(mzml_chrom) = streamer.next_chromatogram()? {
            let chromatogram = self.convert_chromatogram(&mzml_chrom)?;
            writer.write_chromatogram(&chromatogram)?;
            count += 1;
        }

        let stats = writer.finish()?;
        info!(
            "Wrote {} chromatograms to {}",
            stats.chromatograms_written,
            chromatogram_file.display()
        );

        Ok(count)
    }

    /// Convert a single mzML chromatogram to mzPeak format
    pub(crate) fn convert_chromatogram(
        &self,
        mzml_chrom: &MzMLChromatogram,
    ) -> Result<crate::chromatogram_writer::Chromatogram, ConversionError> {
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
        let intensity_array: Vec<f32> = mzml_chrom
            .intensity_array
            .iter()
            .map(|&x| x as f32)
            .collect();

        Chromatogram::new(
            mzml_chrom.id.clone(),
            chrom_type.to_string(),
            mzml_chrom.time_array.clone(),
            intensity_array,
        )
        .map_err(|e| ConversionError::WriterError(WriterError::InvalidData(e.to_string())))
    }

    /// Convert mzML file metadata to mzPeak metadata
    pub(crate) fn convert_metadata(
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
                                instrument.ion_source =
                                    Some("electrospray ionization".to_string());
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
}
