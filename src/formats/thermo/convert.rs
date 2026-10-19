//! Shared RAW conversion for the CLI, Python bindings and Rust callers.

use std::path::Path;

use crate::dataset::{DatasetWriterV2Config, MzPeakDatasetWriterV2};
use crate::ingest::IngestSpectrumConverter;
use crate::metadata::{
    MzPeakMetadata, ProcessingHistory, ProcessingStep, SourceFileInfo, VendorHints,
};
use crate::schema::manifest::Modality;
use crate::writer::{SpectrumV2, WriterConfig};

use super::{ThermoError, ThermoStreamer};

/// Pinned rawr-ms revision used to decode the source.
pub const RAWR_MS_REVISION: &str = "4c353b70f82b4edb01abbe1a433762541c2676ef";

/// Native RAW conversion settings.
#[derive(Debug, Clone)]
pub struct ThermoConversionConfig {
    /// Maximum scans per batch (zero is treated as one).
    pub batch_size: usize,
    /// Optional rawr executable override; otherwise use MZPEAK_RAWR or the bundled binary.
    pub rawr_binary: Option<std::path::PathBuf>,
    /// Parquet compression and row group settings.
    pub writer_config: WriterConfig,
}

impl Default for ThermoConversionConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            rawr_binary: None,
            writer_config: WriterConfig::default(),
        }
    }
}

pub type ThermoConversionStats = crate::mzml::ConversionStats;

/// Converts saved RAW centroids into mzPeak with source provenance.
///
/// Writes to a temporary sibling directory and publishes only a completed
/// output. Existing destinations, including the RAW input, are never replaced.
/// A decoding or writing failure removes the temporary output.
pub fn convert_thermo(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    config: &ThermoConversionConfig,
) -> Result<ThermoConversionStats, ThermoError> {
    let input = input.as_ref();
    let output = output.as_ref();
    if output.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("output exists: {}", output.display()),
        )
        .into());
    }
    let mut streamer =
        ThermoStreamer::with_binary(input, config.batch_size, config.rawr_binary.as_deref())?;
    let bundled = cfg!(all(target_os = "macos", target_arch = "aarch64"))
        && config.rawr_binary.is_none()
        && std::env::var_os("MZPEAK_RAWR").is_none();
    let metadata = build_metadata(input, bundled);
    let mut writer = MzPeakDatasetWriterV2::with_config(
        output,
        Modality::LcMs,
        metadata.vendor_hints.clone(),
        DatasetWriterV2Config::from(&config.writer_config),
    )?;
    writer.set_metadata(metadata);
    let mut stats = ThermoConversionStats {
        source_file_size: std::fs::metadata(input)?.len(),
        ..Default::default()
    };
    let mut converter = IngestSpectrumConverter::new();
    while let Some(batch) = streamer.next_batch()? {
        for ingest in batch {
            let spectrum = converter
                .convert(ingest)
                .map_err(crate::writer::WriterError::from)?;
            stats.spectra_count += 1;
            stats.peak_count += spectrum.peak_count();
            match spectrum.ms_level {
                1 => stats.ms1_spectra += 1,
                2 => stats.ms2_spectra += 1,
                _ => stats.msn_spectra += 1,
            }
            writer.write_spectrum(&SpectrumV2::try_from_spectrum_arrays(spectrum)?)?;
        }
    }
    stats.output_file_size = writer.close()?.output_file_size;
    stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
    Ok(stats)
}

fn build_metadata(input: &Path, bundled: bool) -> MzPeakMetadata {
    let mut metadata = MzPeakMetadata::new();
    let mut source = SourceFileInfo::new(&input.file_name().unwrap_or_default().to_string_lossy());
    source.path = Some(input.to_string_lossy().into_owned());
    source.format = Some("Thermo RAW".into());
    source.size_bytes = std::fs::metadata(input).ok().map(|m| m.len());
    metadata.source_file = Some(source);
    metadata.vendor_hints = Some(
        VendorHints::new("Thermo Fisher Scientific")
            .with_format("thermo_raw")
            .with_conversion_path(vec![
                "thermo_raw".into(),
                "rawr-ms".into(),
                "mzML stream".into(),
                "mzpeak".into(),
            ]),
    );
    metadata.processing_history = Some(ProcessingHistory {
        steps: vec![ProcessingStep {
            order: 1,
            software: "rawr-ms".into(),
            version: bundled.then(|| RAWR_MS_REVISION.into()),
            processing_type: "conversion".into(),
            timestamp: None,
            parameters: [
                ("representation".into(), "source-saved centroids".into()),
                (
                    "collision_energy_unit".into(),
                    "normalized collision energy (percent)".into(),
                ),
            ]
            .into(),
            cv_params: Default::default(),
        }],
    });
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existing_destination_is_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("existing.raw");
        std::fs::write(&output, b"original").unwrap();
        assert!(convert_thermo(&output, &output, &ThermoConversionConfig::default()).is_err());
        assert_eq!(std::fs::read(output).unwrap(), b"original");
    }

    #[test]
    fn records_decoder_revision_and_representation() {
        let metadata = build_metadata(Path::new("source.raw"), true);
        let history = metadata.processing_history.unwrap();
        assert_eq!(history.steps[0].version.as_deref(), Some(RAWR_MS_REVISION));
        assert_eq!(
            history.steps[0].parameters["representation"],
            "source-saved centroids"
        );
    }
}
