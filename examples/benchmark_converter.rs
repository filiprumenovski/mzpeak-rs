//! Benchmark for converting real mzML files to mzPeak format
//!
//! Usage:
//!   cargo run --release --example benchmark_converter -- <input.mzML> <output.mzpeak>

use mzpeak::mzml::converter::{ConversionConfig, MzMLConverter};
use std::env;
use std::time::Instant;

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn format_duration(secs: f64) -> String {
    if secs < 1.0 {
        format!("{:.2} ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.2} s", secs)
    } else {
        let mins = (secs / 60.0) as u64;
        let secs = secs % 60.0;
        format!("{}m {:.2}s", mins, secs)
    }
}

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input.mzML> <output.mzpeak>", args[0]);
        std::process::exit(1);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    println!("═══════════════════════════════════════════════════════════");
    println!("  mzPeak mzML Converter Benchmark");
    println!("═══════════════════════════════════════════════════════════");
    println!();
    println!("Input:  {}", input_path);
    println!("Output: {}", output_path);
    println!();

    // Configuration
    let config = ConversionConfig {
        include_chromatograms: true,
        batch_size: 1000,
        preserve_precision: true,
        progress_interval: 5000,
        ..Default::default()
    };

    println!("Configuration:");
    println!("  Batch size:              {}", config.batch_size);
    println!("  Include chromatograms:   {}", config.include_chromatograms);
    println!("  Preserve precision:      {}", config.preserve_precision);
    println!("  Progress interval:       {} spectra", config.progress_interval);
    println!();

    // Start conversion
    println!("Starting conversion...");
    let start = Instant::now();

    let converter = MzMLConverter::with_config(config);
    let stats = match converter.convert(input_path, output_path) {
        Ok(stats) => stats,
        Err(e) => {
            eprintln!("❌ Conversion failed: {}", e);
            std::process::exit(1);
        }
    };

    let duration = start.elapsed();
    let duration_secs = duration.as_secs_f64();

    println!();
    println!("═══════════════════════════════════════════════════════════");
    println!("  Conversion Complete!");
    println!("═══════════════════════════════════════════════════════════");
    println!();
    println!("Statistics:");
    println!("  Total time:              {}", format_duration(duration_secs));
    println!("  Total spectra:           {}", stats.spectra_count);
    println!("  ├─ MS1:                  {}", stats.ms1_spectra);
    println!("  ├─ MS2:                  {}", stats.ms2_spectra);
    println!("  └─ MSn:                  {}", stats.msn_spectra);
    println!("  Total peaks:             {}", stats.peak_count);
    println!("  Chromatograms:           {}", stats.chromatograms_converted);
    println!();
    println!("File sizes:");
    println!("  Input (mzML):            {}", format_size(stats.source_file_size));
    println!("  Output (.mzpeak):        {}", format_size(stats.output_file_size));
    println!("  Compression ratio:       {:.2}x", stats.compression_ratio);
    println!();
    println!("Performance:");
    println!("  Spectra/second:          {:.0}", stats.spectra_count as f64 / duration_secs);
    println!("  Peaks/second:            {:.0}", stats.peak_count as f64 / duration_secs);
    println!("  Throughput:              {}/s", format_size((stats.source_file_size as f64 / duration_secs) as u64));
    println!();
    println!("✅ Success!");
}
