//! Compare different compression configurations on a real mzML file
//!
//! Usage:
//!   cargo run --release --example compare_compression -- <input.mzML>

#[cfg(feature = "mzml")]
use mzpeak::mzml::converter::{ConversionConfig, MzMLConverter};
#[cfg(feature = "mzml")]
use std::env;
#[cfg(feature = "mzml")]
use std::fs;
#[cfg(feature = "mzml")]
use std::time::Instant;

#[cfg(feature = "mzml")]
fn format_size(bytes: u64) -> String {
    if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

#[cfg(feature = "mzml")]
fn format_duration(secs: f64) -> String {
    if secs < 60.0 {
        format!("{:.2}s", secs)
    } else {
        let mins = (secs / 60.0) as u64;
        let secs = secs % 60.0;
        format!("{}m {:.2}s", mins, secs)
    }
}

#[cfg(feature = "mzml")]
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <input.mzML>", args[0]);
        std::process::exit(1);
    }

    let input_path = &args[1];
    let input_size = fs::metadata(input_path).unwrap().len();

    println!("═══════════════════════════════════════════════════════════");
    println!("  mzPeak Compression Comparison");
    println!("═══════════════════════════════════════════════════════════");
    println!();
    println!("Input file: {}", input_path);
    println!("Input size: {}", format_size(input_size));
    println!();

    let configs = vec![
        ("Fast (Snappy)", ConversionConfig::fast_write()),
        ("Balanced (ZSTD-9)", ConversionConfig::balanced()),
        ("Max (ZSTD-22)", ConversionConfig::max_compression()),
    ];

    let mut results = Vec::new();

    for (name, config) in configs {
        println!("───────────────────────────────────────────────────────────");
        println!("Testing: {}", name);
        println!("───────────────────────────────────────────────────────────");

        let output_path = format!("data/test_compression_{}.mzpeak", 
            name.to_lowercase().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_"));

        let start = Instant::now();
        let converter = MzMLConverter::with_config(config);
        let stats = converter.convert(input_path, &output_path).unwrap();
        let duration = start.elapsed().as_secs_f64();

        let output_size = fs::metadata(&output_path).unwrap().len();
        let compression_ratio = input_size as f64 / output_size as f64;
        let throughput = stats.spectra_count as f64 / duration;

        println!();
        println!("  Time: {}", format_duration(duration));
        println!("  Output size: {}", format_size(output_size));
        println!("  Compression ratio: {:.2}x", compression_ratio);
        println!("  Throughput: {:.0} spectra/s", throughput);
        println!();

        results.push((name, duration, output_size, compression_ratio, throughput));

        // Clean up
        fs::remove_file(&output_path).ok();
    }

    println!("═══════════════════════════════════════════════════════════");
    println!("  Summary");
    println!("═══════════════════════════════════════════════════════════");
    println!();
    println!("{:<20} {:>12} {:>12} {:>10} {:>12}", "Config", "Time", "Size", "Ratio", "Speed");
    println!("{}", "─".repeat(70));

    for (name, duration, size, ratio, throughput) in &results {
        println!("{:<20} {:>12} {:>12} {:>9.2}x {:>10.0}/s", 
            name, 
            format_duration(*duration),
            format_size(*size),
            ratio,
            throughput);
    }

    println!();
    println!("Recommendation:");
    println!("  - Use 'Fast' for quick prototyping or streaming pipelines");
    println!("  - Use 'Balanced' for general-purpose archival (default)");
    println!("  - Use 'Max' for long-term storage when size is critical");
    println!();
}

#[cfg(not(feature = "mzml"))]
fn main() {
    eprintln!("This example requires the `mzml` feature.");
}
