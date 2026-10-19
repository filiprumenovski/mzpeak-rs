use anyhow::{Context, Result};
use log::{info, warn};
use std::path::PathBuf;

use super::config::Config;
use super::profile::Profile;
use mzpeak::writer::{CompressionType, WriterConfig};

/// Convert Thermo RAW file to mzPeak format.
#[allow(clippy::too_many_arguments)]
pub fn run(
    input: PathBuf,
    output: Option<PathBuf>,
    profile: Profile,
    config_path: Option<PathBuf>,
    legacy: bool,
    cli_compression_level: Option<i32>,
    cli_row_group_size: Option<usize>,
    cli_batch_size: Option<usize>,
) -> Result<()> {
    if !input.exists() {
        anyhow::bail!("Input file does not exist: {}", input.display());
    }

    let file_config = if let Some(ref path) = config_path {
        Some(Config::from_file(path)?)
    } else {
        None
    };

    let compression_level = cli_compression_level
        .or(file_config
            .as_ref()
            .and_then(|c| c.conversion.compression_level))
        .unwrap_or_else(|| profile.compression_level());

    let row_group_size = cli_row_group_size
        .or(file_config
            .as_ref()
            .and_then(|c| c.conversion.row_group_size))
        .unwrap_or_else(|| profile.row_group_size());

    let batch_size = cli_batch_size
        .or(file_config.as_ref().and_then(|c| c.conversion.batch_size))
        .unwrap_or_else(|| profile.batch_size())
        .max(1);

    let use_legacy = legacy
        || file_config
            .as_ref()
            .and_then(|c| c.conversion.legacy)
            .unwrap_or(false);

    let parallel_requested = file_config
        .as_ref()
        .and_then(|c| c.conversion.parallel)
        .unwrap_or(false);
    if parallel_requested {
        warn!("Parallel decoding is not supported for Thermo RAW conversion.");
    }

    let output = output.unwrap_or_else(|| {
        let stem = input.file_stem().unwrap_or_default().to_string_lossy();
        if use_legacy {
            input.with_file_name(format!("{}.mzpeak.parquet", stem))
        } else {
            input.with_file_name(format!("{}.mzpeak", stem))
        }
    });

    info!("mzPeak Converter - Thermo RAW to mzPeak");
    info!("=======================================");
    info!("Input:  {}", input.display());
    info!("Output: {}", output.display());
    info!("Profile: {}", profile);
    if config_path.is_some() {
        info!("Config file: {}", config_path.as_ref().unwrap().display());
    }
    if use_legacy {
        info!("Format: Legacy single-file .mzpeak.parquet (v1)");
    } else {
        info!("Format: Container .mzpeak (v2)");
    }
    info!("Compression level: {}", compression_level);
    info!("Row group size: {}", row_group_size);
    info!("Batch size: {}", batch_size);

    let stats = mzpeak::thermo::convert_thermo(
        &input,
        &output,
        &mzpeak::thermo::ThermoConversionConfig {
            batch_size,
            rawr_binary: None,
            writer_config: WriterConfig {
                compression: CompressionType::Zstd(compression_level),
                row_group_size,
                ..Default::default()
            },
            legacy_format: use_legacy,
        },
    )
    .context("Thermo RAW conversion failed")?;
    info!(
        "Converted {} spectra ({} peaks) into {} bytes",
        stats.spectra_count, stats.peak_count, stats.output_file_size
    );
    Ok(())
}
