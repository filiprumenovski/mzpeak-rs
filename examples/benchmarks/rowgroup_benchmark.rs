//! Benchmark row group size impact on write performance.
//!
//! Tests how row group size affects:
//! 1. Write throughput
//! 2. File size
//! 3. Metadata overhead

use mzpeak::tdf::TdfConverter;
use mzpeak::tdf::converter::TdfConversionConfig;
use mzpeak::writer::{MzPeakWriter, WriterConfig, CompressionType};
use mzpeak::metadata::MzPeakMetadata;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_path = std::path::PathBuf::from(
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d"
    );

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║              Row Group Size Benchmark                            ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // Load data once
    println!("Loading TDF data...");
    let config = TdfConversionConfig::default();
    let converter = TdfConverter::with_config(config);
    let spectra = converter.convert(&data_path)?;
    
    let total_peaks: usize = spectra.iter().map(|s| s.peak_count()).sum();
    println!("Loaded {} spectra, {} peaks ({:.1}M)\n", 
        spectra.len(), total_peaks, total_peaks as f64 / 1_000_000.0);

    // Test configurations: row_group_size
    let configs = [
        ("100K (default)", 100_000),
        ("500K", 500_000),
        ("1M", 1_000_000),
        ("5M", 5_000_000),
        ("10M", 10_000_000),
        ("50M (single)", 50_000_000),
    ];

    println!("{:<18} {:>12} {:>12} {:>10} {:>12}", 
        "Row Group Size", "Write Time", "File Size", "RG Count", "Speed");
    println!("{}", "-".repeat(68));

    for (name, rg_size) in configs {
        let output_path = format!("/tmp/benchmark_rg_{}.mzpeak", rg_size);
        
        let mut writer_config = WriterConfig::default();
        writer_config.row_group_size = rg_size;
        writer_config.compression = CompressionType::Zstd(3); // Faster for benchmark
        
        let metadata = MzPeakMetadata::default();
        let mut writer = MzPeakWriter::new_file(&output_path, &metadata, writer_config)?;
        
        let start = Instant::now();
        
        // Clone spectra for fair comparison (each test gets fresh data)
        for spectrum in spectra.clone() {
            writer.write_spectrum_owned(spectrum)?;
        }
        let stats = writer.finish()?;
        
        let elapsed = start.elapsed();
        let file_size = std::fs::metadata(&output_path)?.len();
        let speed_mbps = (file_size as f64 / 1_000_000.0) / elapsed.as_secs_f64();
        
        // Get row group count
        let rg_count = stats.row_groups_written;
        
        println!("{:<18} {:>10.2}s {:>10.1} MB {:>10} {:>10.1} MB/s", 
            name,
            elapsed.as_secs_f64(),
            file_size as f64 / 1_000_000.0,
            rg_count,
            speed_mbps);
        
        // Clean up
        std::fs::remove_file(&output_path)?;
    }

    println!("\n✅ Benchmark complete");
    Ok(())
}
