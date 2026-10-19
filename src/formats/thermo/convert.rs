//! Shared RAW conversion for the CLI, Python bindings and Rust callers.

use std::path::Path;

use crate::dataset::{DatasetWriterV2Config, MzPeakDatasetWriterV2};
use crate::ingest::IngestSpectrumConverter;
use crate::metadata::{
    MzPeakMetadata, ProcessingHistory, ProcessingStep, SourceFileInfo, VendorHints,
};
use crate::schema::manifest::Modality;
use crate::writer::{
    MzPeakWriter, PeaksWriterV2Config, SpectraWriterConfig, SpectrumV2, WriterConfig,
};

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
    /// Write legacy v1 Parquet instead of the default v2 container.
    pub legacy_format: bool,
}

impl Default for ThermoConversionConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            rawr_binary: None,
            writer_config: WriterConfig::default(),
            legacy_format: false,
        }
    }
}

/// Counts and sizes from a completed conversion.
#[derive(Debug, Clone, Default)]
pub struct ThermoConversionStats {
    /// Spectra written, including empty spectra in v2.
    pub spectra_count: usize,
    /// Source-saved centroids written.
    pub peak_count: usize,
    /// MS1 spectra written.
    pub ms1_spectra: usize,
    /// MS2 spectra written.
    pub ms2_spectra: usize,
    /// Higher-order spectra written.
    pub msn_spectra: usize,
    /// Input bytes.
    pub source_file_size: u64,
    /// Output bytes.
    pub output_file_size: u64,
    /// Input bytes divided by output bytes.
    pub compression_ratio: f64,
}

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
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    let staging = tempfile::tempdir_in(parent)?;
    let staged_output = staging.path().join("output");
    let mut legacy_writer = if config.legacy_format {
        Some(MzPeakWriter::new_file(
            &staged_output,
            &metadata,
            config.writer_config.clone(),
        )?)
    } else {
        None
    };
    let mut dataset_writer = if config.legacy_format {
        None
    } else {
        let mut writer = MzPeakDatasetWriterV2::with_config(
            &staged_output,
            Modality::LcMs,
            metadata.vendor_hints.clone(),
            DatasetWriterV2Config {
                spectra_config: SpectraWriterConfig {
                    compression: config.writer_config.compression,
                    data_page_size: config.writer_config.data_page_size,
                    write_statistics: config.writer_config.write_statistics,
                    dictionary_page_size_limit: config.writer_config.dictionary_page_size_limit,
                    ..Default::default()
                },
                peaks_config: PeaksWriterV2Config {
                    compression: config.writer_config.compression,
                    row_group_size: config.writer_config.row_group_size,
                    data_page_size: config.writer_config.data_page_size,
                    write_statistics: config.writer_config.write_statistics,
                    use_byte_stream_split: config.writer_config.use_byte_stream_split,
                    ..Default::default()
                },
            },
        )?;
        writer.set_metadata(metadata);
        Some(writer)
    };
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
            if let Some(writer) = legacy_writer.as_mut() {
                if spectrum.peak_count() == 0 {
                    return Err(ThermoError::UnsupportedRepresentation(format!(
                        "legacy v1 cannot preserve empty scan {}; use v2",
                        spectrum.scan_number
                    )));
                }
                writer.write_spectra_owned(vec![spectrum])?;
            } else if let Some(writer) = dataset_writer.as_mut() {
                writer.write_spectrum(&SpectrumV2::try_from_spectrum_arrays(spectrum)?)?;
            }
        }
    }
    if let Some(writer) = legacy_writer {
        writer.finish()?;
    }
    if let Some(writer) = dataset_writer {
        writer.close()?;
    }
    stats.output_file_size = std::fs::metadata(&staged_output)?.len();
    if stats.output_file_size > 0 {
        stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
    }
    tempfile::TempPath::try_from_path(staged_output)?
        .persist_noclobber(output)
        .map_err(|error| error.error)?;
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
