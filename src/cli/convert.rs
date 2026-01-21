use anyhow::{Context, Result};
use log::info;
#[cfg(not(feature = "mzml-parallel"))]
use log::warn;
use std::path::PathBuf;

use super::config::Config;
use super::profile::Profile;
use mzpeak::mzml::{ConversionConfig, MzMLConverter, OutputFormat};
use mzpeak::schema::manifest::Modality;
use mzpeak::writer::{CompressionType, WriterConfig};

/// Convert mzML file to mzPeak format
#[allow(clippy::too_many_arguments)]
pub fn run(
    input: PathBuf,
    output: Option<PathBuf>,
    profile: Profile,
    config_path: Option<PathBuf>,
    legacy: bool,
    parallel: bool,
    modality: Option<Modality>,
    cli_compression_level: Option<i32>,
    cli_row_group_size: Option<usize>,
    cli_batch_size: Option<usize>,
) -> Result<()> {
    // Validate input file exists
    if !input.exists() {
        anyhow::bail!("Input file does not exist: {}", input.display());
    }

    // Load config file if specified
    let file_config = if let Some(ref path) = config_path {
        Some(Config::from_file(path)?)
    } else {
        None
    };

    // Resolve settings with priority: CLI > config file > profile defaults
    let compression_level = cli_compression_level
        .or(file_config.as_ref().and_then(|c| c.conversion.compression_level))
        .unwrap_or_else(|| profile.compression_level());

    let row_group_size = cli_row_group_size
        .or(file_config.as_ref().and_then(|c| c.conversion.row_group_size))
        .unwrap_or_else(|| profile.row_group_size());

    let batch_size = cli_batch_size
        .or(file_config.as_ref().and_then(|c| c.conversion.batch_size))
        .unwrap_or_else(|| profile.batch_size());

    let use_parallel = parallel
        || file_config
            .as_ref()
            .and_then(|c| c.conversion.parallel)
            .unwrap_or(false);

    let use_legacy = legacy
        || file_config
            .as_ref()
            .and_then(|c| c.conversion.legacy)
            .unwrap_or(false);

    // Determine output path (default to .mzpeak container format or .mzpeak.parquet if legacy)
    let output = output.unwrap_or_else(|| {
        let stem = input.file_stem().unwrap_or_default().to_string_lossy();
        let stem = stem
            .trim_end_matches(".mzML")
            .trim_end_matches(".mzml")
            .trim_end_matches(".imzML")
            .trim_end_matches(".imzml");
        if use_legacy {
            input.with_file_name(format!("{}.mzpeak.parquet", stem))
        } else {
            input.with_file_name(format!("{}.mzpeak", stem))
        }
    });

    info!("mzPeak Converter - mzML/imzML to mzPeak");
    info!("==================================");
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
    if use_parallel {
        info!("Parallel decode: enabled");
    }

    // Create converter with configuration
    let writer_config = WriterConfig {
        compression: CompressionType::Zstd(compression_level),
        row_group_size,
        ..Default::default()
    };

    let mut config = ConversionConfig::default();
    config.writer_config = writer_config;
    config.batch_size = batch_size;

    config.output_format = if use_legacy {
        OutputFormat::V1Parquet
    } else {
        OutputFormat::V2Container
    };
    config.modality = modality;

    let converter = MzMLConverter::with_config(config);

    // Run conversion
    info!("Starting conversion...");
    let stats = {
        #[cfg(feature = "mzml-parallel")]
        {
            if use_parallel {
                converter
                    .convert_parallel(&input, &output)
                    .context("Parallel conversion failed")?
            } else {
                converter.convert(&input, &output).context("Conversion failed")?
            }
        }
        #[cfg(not(feature = "mzml-parallel"))]
        {
            if use_parallel {
                warn!("Parallel decoding requested but binary was built without mzml-parallel feature; falling back to sequential conversion.");
            }
            converter.convert(&input, &output).context("Conversion failed")?
        }
    };

    // Print results
    info!("Conversion complete!");
    info!("  Spectra converted: {}", stats.spectra_count);
    info!("  Total peaks: {}", stats.peak_count);

    let file_size = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
    info!(
        "  Output file size: {} bytes ({:.2} MB)",
        file_size,
        file_size as f64 / 1024.0 / 1024.0
    );

    if stats.compression_ratio > 0.0 {
        info!("  Compression ratio: {:.1}x", stats.compression_ratio);
    }

    info!("\nFile can be read with any Parquet-compatible tool:");
    info!(
        "  - Python: pyarrow.parquet.read_table('{}').to_pandas()",
        output.display()
    );
    info!("  - R: arrow::read_parquet('{}')", output.display());
    info!(
        "  - DuckDB: SELECT * FROM read_parquet('{}')",
        output.display()
    );

    Ok(())
}
