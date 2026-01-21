use anyhow::{Context, Result};
use log::{info, warn};
use std::path::{Path, PathBuf};

use super::config::Config;
use super::profile::Profile;
use mzpeak::controlled_vocabulary::ms_terms;
use mzpeak::dataset::{DatasetWriterV2Config, MzPeakDatasetWriterV2};
use mzpeak::ingest::IngestSpectrumConverter;
use mzpeak::metadata::{InstrumentConfig, MzPeakMetadata, SourceFileInfo, VendorHints};
use mzpeak::thermo::{ThermoConverter, ThermoStreamer};
use mzpeak::schema::manifest::Modality;
use mzpeak::writer::{
    CompressionType, MzPeakWriter, PeaksWriterV2Config, SpectraWriterConfig, SpectrumArrays,
    SpectrumV2, WriterConfig,
};

#[derive(Default)]
struct ThermoConversionStats {
    spectra_count: usize,
    peak_count: usize,
    ms1_spectra: usize,
    ms2_spectra: usize,
    msn_spectra: usize,
    chromatograms_converted: usize,
    source_file_size: u64,
    output_file_size: u64,
    compression_ratio: f64,
}

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
        .or(file_config.as_ref().and_then(|c| c.conversion.compression_level))
        .unwrap_or_else(|| profile.compression_level());

    let row_group_size = cli_row_group_size
        .or(file_config.as_ref().and_then(|c| c.conversion.row_group_size))
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

    let writer_config = WriterConfig {
        compression: CompressionType::Zstd(compression_level),
        row_group_size,
        ..Default::default()
    };

    let mut streamer = ThermoStreamer::new(&input, batch_size)
        .context("Failed to open Thermo RAW file")?;
    let instrument_model_raw = streamer.instrument_model();
    let instrument_model = normalize_instrument_model(&instrument_model_raw);
    let total_spectra = streamer.len();

    if total_spectra > 0 {
        info!("Total spectra: {}", total_spectra);
    }
    if let Some(model) = instrument_model.as_deref() {
        info!("Instrument: {}", model);
    }

    let metadata = build_metadata(&input, instrument_model.as_deref());

    if use_legacy {
        let mut writer = MzPeakWriter::new_file(&output, &metadata, writer_config)
            .context("Failed to create legacy mzPeak writer")?;

        let mut stats = ThermoConversionStats {
            source_file_size: std::fs::metadata(&input).map(|m| m.len()).unwrap_or(0),
            ..Default::default()
        };

        let mut batch: Vec<SpectrumArrays> = Vec::with_capacity(batch_size);
        let mut ingest_converter = IngestSpectrumConverter::new();
        let converter = ThermoConverter::new();
        let mut spectrum_id: i64 = 0;

        info!("Starting conversion...");

        while let Some(raw_batch) = streamer
            .next_batch()
            .context("Failed to read Thermo RAW spectra batch")?
        {
            for raw_spectrum in raw_batch {
                let scan_number = raw_spectrum.index() + 1;
                let ingest = converter
                    .convert_spectrum(raw_spectrum, spectrum_id)
                    .with_context(|| format!("Failed to convert scan {}", scan_number))?;
                spectrum_id += 1;

                let spectrum = ingest_converter
                    .convert(ingest)
                    .with_context(|| format!("Ingest contract failed at scan {}", scan_number))?;

                stats.spectra_count += 1;
                stats.peak_count += spectrum.peak_count();
                match spectrum.ms_level {
                    1 => stats.ms1_spectra += 1,
                    2 => stats.ms2_spectra += 1,
                    _ => stats.msn_spectra += 1,
                }

                batch.push(spectrum);

                if batch.len() >= batch_size {
                    writer
                        .write_spectra_owned(batch)
                        .context("Failed to write spectra batch")?;
                    batch = Vec::with_capacity(batch_size);
                }

                if stats.spectra_count % PROGRESS_INTERVAL == 0 && total_spectra > 0 {
                    let processed = streamer.position();
                    let pct = (processed as f64 / total_spectra as f64) * 100.0;
                    info!(
                        "Progress: {}/{} spectra ({:.1}%)",
                        processed, total_spectra, pct
                    );
                }
            }
        }

        if !batch.is_empty() {
            writer
                .write_spectra_owned(batch)
                .context("Failed to write final spectra batch")?;
        }

        let writer_stats = writer.finish().context("Failed to finalize mzPeak file")?;
        info!("Writer finalized: {}", writer_stats);

        stats.output_file_size = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
        if stats.output_file_size > 0 {
            stats.compression_ratio =
                stats.source_file_size as f64 / stats.output_file_size as f64;
        }

        info!("Conversion complete!");
        info!(
            "  Spectra: {} (MS1: {}, MS2: {}, MSn: {})",
            stats.spectra_count, stats.ms1_spectra, stats.ms2_spectra, stats.msn_spectra
        );
        info!("  Peaks: {}", stats.peak_count);
        info!("  Input size: {} bytes", stats.source_file_size);
        info!(
            "  Output size: {} bytes ({:.2} MB)",
            stats.output_file_size,
            stats.output_file_size as f64 / 1024.0 / 1024.0
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

        return Ok(());
    }

    let vendor_hints = metadata.vendor_hints.clone();
    let dataset_config = DatasetWriterV2Config {
        spectra_config: SpectraWriterConfig {
            compression: writer_config.compression,
            ..Default::default()
        },
        peaks_config: PeaksWriterV2Config {
            compression: writer_config.compression,
            row_group_size: writer_config.row_group_size,
            ..Default::default()
        },
    };
    let mut writer = MzPeakDatasetWriterV2::with_config(
        &output,
        Modality::LcMs,
        vendor_hints,
        dataset_config,
    )
    .context("Failed to create mzPeak v2 dataset writer")?;
    writer.set_metadata(metadata);

    let mut stats = ThermoConversionStats {
        source_file_size: std::fs::metadata(&input).map(|m| m.len()).unwrap_or(0),
        ..Default::default()
    };

    let mut ingest_converter = IngestSpectrumConverter::new();
    let converter = ThermoConverter::new();
    let mut spectrum_id: i64 = 0;

    const PROGRESS_INTERVAL: usize = 1000;

    info!("Starting conversion...");

    while let Some(raw_batch) = streamer
        .next_batch()
        .context("Failed to read Thermo RAW spectra batch")?
    {
        for raw_spectrum in raw_batch {
            let scan_number = raw_spectrum.index() + 1;
            let ingest = converter
                .convert_spectrum(raw_spectrum, spectrum_id)
                .with_context(|| format!("Failed to convert scan {}", scan_number))?;
            spectrum_id += 1;

            let spectrum = ingest_converter
                .convert(ingest)
                .with_context(|| format!("Ingest contract failed at scan {}", scan_number))?;

            let spectrum_v2 = SpectrumV2::try_from_spectrum_arrays(spectrum)
                .with_context(|| format!("v2 conversion failed at scan {}", scan_number))?;

            stats.spectra_count += 1;
            stats.peak_count += spectrum_v2.peaks.len();
            match spectrum_v2.metadata.ms_level {
                1 => stats.ms1_spectra += 1,
                2 => stats.ms2_spectra += 1,
                _ => stats.msn_spectra += 1,
            }

            writer
                .write_spectrum(&spectrum_v2)
                .context("Failed to write spectrum")?;

            if stats.spectra_count % PROGRESS_INTERVAL == 0 && total_spectra > 0 {
                let processed = streamer.position();
                let pct = (processed as f64 / total_spectra as f64) * 100.0;
                info!(
                    "Progress: {}/{} spectra ({:.1}%)",
                    processed, total_spectra, pct
                );
            }
        }
    }

    let dataset_stats = writer.close().context("Failed to finalize dataset")?;
    info!("Dataset finalized: {}", dataset_stats);

    stats.output_file_size = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
    if stats.output_file_size > 0 {
        stats.compression_ratio = stats.source_file_size as f64 / stats.output_file_size as f64;
    }

    info!("Conversion complete!");
    info!(
        "  Spectra: {} (MS1: {}, MS2: {}, MSn: {})",
        stats.spectra_count, stats.ms1_spectra, stats.ms2_spectra, stats.msn_spectra
    );
    info!("  Peaks: {}", stats.peak_count);
    info!("  Chromatograms: {}", stats.chromatograms_converted);
    info!("  Input size: {} bytes", stats.source_file_size);
    info!(
        "  Output size: {} bytes ({:.2} MB)",
        stats.output_file_size,
        stats.output_file_size as f64 / 1024.0 / 1024.0
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

fn build_metadata(input: &Path, instrument_model: Option<&str>) -> MzPeakMetadata {
    let mut metadata = MzPeakMetadata::new();

    let mut source = SourceFileInfo::new(
        input
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown"),
    );
    source.path = input.to_str().map(String::from);
    source.format = Some("Thermo RAW".to_string());
    source.size_bytes = std::fs::metadata(input).ok().map(|m| m.len());
    metadata.source_file = Some(source);

    let mut vendor_hints = VendorHints::new("Thermo Fisher Scientific")
        .with_format("thermo_raw")
        .with_conversion_path(vec!["thermo_raw".to_string(), "mzpeak".to_string()]);
    if let Some(model) = instrument_model {
        vendor_hints = vendor_hints.with_instrument_model(model);
    }
    metadata.vendor_hints = Some(vendor_hints);

    if let Some(model) = instrument_model {
        let mut instrument = InstrumentConfig::new();
        instrument.model = Some(model.to_string());
        instrument.vendor = Some("Thermo Fisher Scientific".to_string());
        instrument.cv_params.add(ms_terms::thermo_instrument());
        metadata.instrument = Some(instrument);
    }

    metadata
}

fn normalize_instrument_model(model: &str) -> Option<String> {
    let trimmed = model.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        None
    } else {
        Some(trimmed.to_string())
    }
}
