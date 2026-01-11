use anyhow::{Context, Result};
use log::info;
#[cfg(not(feature = "parallel-decode"))]
use log::warn;
use std::path::PathBuf;

use mzpeak::mzml::{ConversionConfig, MzMLConverter};
use mzpeak::writer::{CompressionType, WriterConfig};

/// Convert mzML file to mzPeak format
pub fn run(
    input: PathBuf,
    output: Option<PathBuf>,
    legacy: bool,
    compression_level: i32,
    row_group_size: usize,
    batch_size: usize,
    parallel: bool,
) -> Result<()> {
    // Validate input file exists
    if !input.exists() {
        anyhow::bail!("Input file does not exist: {}", input.display());
    }

    // Determine output path (default to .mzpeak container format or .mzpeak.parquet if legacy)
    let output = output.unwrap_or_else(|| {
        let stem = input.file_stem().unwrap_or_default().to_string_lossy();
        let stem = stem.trim_end_matches(".mzML").trim_end_matches(".mzml");
        if legacy {
            input.with_file_name(format!("{}.mzpeak.parquet", stem))
        } else {
            input.with_file_name(format!("{}.mzpeak", stem))
        }
    });

    info!("mzPeak Converter - mzML to mzPeak");
    info!("==================================");
    info!("Input:  {}", input.display());
    info!("Output: {}", output.display());
    if legacy {
        info!("Format: Legacy single-file .mzpeak.parquet");
    } else {
        info!("Format: Container .mzpeak (standard)");
    }
    info!("Compression level: {}", compression_level);
    info!("Row group size: {}", row_group_size);
    info!("Batch size: {}", batch_size);
    if parallel {
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

    let converter = MzMLConverter::with_config(config);

    // Run conversion
    info!("Starting conversion...");
    let stats = {
        #[cfg(feature = "parallel-decode")]
        {
            if parallel {
                converter
                    .convert_parallel(&input, &output)
                    .context("Parallel conversion failed")?
            } else {
                converter.convert(&input, &output).context("Conversion failed")?
            }
        }
        #[cfg(not(feature = "parallel-decode"))]
        {
            if parallel {
                warn!("Parallel decoding requested but binary was built without parallel-decode; falling back to sequential conversion.");
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
