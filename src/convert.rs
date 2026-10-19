//! Shared format selection for Rust, Python and the CLI.
use crate::{schema::manifest::Modality, writer::WriterConfig};
use std::path::{Path, PathBuf};
#[derive(Debug, Clone)]
pub struct ConvertOptions {
    pub writer_config: WriterConfig,
    pub batch_size: usize,
    pub rawr_binary: Option<PathBuf>,
    pub modality: Option<Modality>,
}
impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            writer_config: WriterConfig::default(),
            batch_size: 256,
            rawr_binary: None,
            modality: None,
        }
    }
}
#[derive(Debug, Default, serde::Serialize)]
pub struct ConversionStats {
    pub spectra_count: usize,
    pub peak_count: usize,
    pub ms1_spectra: usize,
    pub ms2_spectra: usize,
    pub msn_spectra: usize,
    pub chromatograms_converted: usize,
    pub source_file_size: u64,
    pub output_file_size: u64,
    pub compression_ratio: f64,
}
pub fn convert(
    input: impl AsRef<Path>,
    output: impl AsRef<Path>,
    options: &ConvertOptions,
) -> anyhow::Result<ConversionStats> {
    let input = input.as_ref();
    let output = output.as_ref();
    anyhow::ensure!(input.exists(), "input does not exist: {}", input.display());
    let extension = input
        .extension()
        .unwrap_or_default()
        .to_string_lossy()
        .to_ascii_lowercase();
    #[cfg(feature = "thermo")]
    if extension == "raw" {
        return Ok(crate::thermo::convert_thermo(
            input,
            output,
            &crate::thermo::ThermoConversionConfig {
                batch_size: options.batch_size,
                rawr_binary: options.rawr_binary.clone(),
                writer_config: options.writer_config.clone(),
            },
        )?);
    }
    #[cfg(feature = "mzml")]
    if extension == "mzml" || extension == "imzml" {
        return Ok(
            crate::mzml::MzMLConverter::with_config(crate::mzml::ConversionConfig {
                writer_config: options.writer_config.clone(),
                batch_size: options.batch_size,
                modality: options.modality,
                ..Default::default()
            })
            .convert(input, output)?,
        );
    }
    #[cfg(feature = "tdf")]
    if input.is_dir() {
        let converter = crate::tdf::TdfConverter::with_config(crate::tdf::TdfConversionConfig {
            batch_size: options.batch_size,
            ..Default::default()
        });
        let stats =
            converter.convert_to_v2_container(input, output, options.writer_config.clone())?;
        let source_file_size = ["analysis.tdf", "analysis.tdf_bin"]
            .iter()
            .filter_map(|name| std::fs::metadata(input.join(name)).ok())
            .map(|m| m.len())
            .sum();
        let output_file_size = std::fs::metadata(output)?.len();
        return Ok(ConversionStats {
            spectra_count: stats.spectra_read,
            peak_count: stats.peaks_total,
            ms1_spectra: stats.ms1_count,
            ms2_spectra: stats.ms2_count,
            source_file_size,
            output_file_size,
            compression_ratio: source_file_size as f64 / output_file_size as f64,
            ..Default::default()
        });
    }
    let _ = (output, options);
    anyhow::bail!("unsupported input {extension:?}, or its reader feature is disabled")
}
