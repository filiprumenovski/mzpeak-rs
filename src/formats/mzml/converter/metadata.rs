use super::{ConversionError, MzMLConverter};
use crate::metadata::{
    InstrumentConfig, MassAnalyzerConfig, MzPeakMetadata, ProcessingHistory, ProcessingStep,
    RunParameters, SourceFileInfo,
};
use crate::mzml::{ComponentType, MzMLFileMetadata, MS_CV_ACCESSIONS};
use std::path::Path;
impl MzMLConverter {
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
            match mzml_source.checksum_type.as_deref() {
                Some("SHA-256") => source.sha256 = mzml_source.checksum.clone(),
                Some("MD5") => source.md5 = mzml_source.checksum.clone(),
                _ => {}
            }
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

        metadata.extensions.insert(
            "mzml".into(),
            serde_json::to_value(mzml).map_err(|e| std::io::Error::other(e.to_string()))?,
        );
        Ok(metadata)
    }
}
