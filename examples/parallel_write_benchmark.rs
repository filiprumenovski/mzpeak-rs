//! Benchmark ParallelTdfConverter vs batch write approach
//!
//! Compare zero-copy streaming writes vs materialized batch writes

use std::time::Instant;
use std::path::PathBuf;

use mzpeak::tdf::{ParallelTdfConverter, ParallelConversionConfig};
use mzpeak::writer::WriterConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_path = PathBuf::from(
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d"
    );
    let output_dir = PathBuf::from("/tmp/parallel_output");

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║     ParallelTdfConverter Zero-Copy Streaming Benchmark           ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // Clean up previous output
    if output_dir.exists() {
        std::fs::remove_dir_all(&output_dir)?;
    }
    std::fs::create_dir_all(&output_dir)?;

    // Configure parallel converter
    let mut config = ParallelConversionConfig::default();
    config.merge_shards = false; // Keep shards separate for now
    config.writer_config = WriterConfig::default();
    
    println!("Configuration:");
    println!("  Workers: {}", config.num_workers);
    println!("  Merge shards: {}", config.merge_shards);
    println!("  Output: {}\n", output_dir.display());

    // Run conversion
    let start = Instant::now();
    let converter = ParallelTdfConverter::with_config(config);
    let stats = converter.convert(&data_path, &output_dir)?;
    let elapsed = start.elapsed();

    // Calculate metrics
    let peaks_per_sec = stats.total_peaks as f64 / elapsed.as_secs_f64();
    
    println!("\n═══════════════════════════════════════════════════════════════════");
    println!("Results:");
    println!("  Total spectra: {}", stats.total_spectra);
    println!("  Total peaks: {} ({:.1}M)", stats.total_peaks, stats.total_peaks as f64 / 1_000_000.0);
    println!("  Total time: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.1}M peaks/sec", peaks_per_sec / 1_000_000.0);
    println!("  Shards written: {}", stats.shard_stats.len());
    
    // Calculate total file size
    let total_size: u64 = stats.shard_stats.iter()
        .filter_map(|s| std::fs::metadata(&s.path).ok())
        .map(|m: std::fs::Metadata| m.len())
        .sum();
    let write_speed = total_size as f64 / 1_000_000.0 / elapsed.as_secs_f64();
    
    println!("  Total file size: {:.1} MB", total_size as f64 / 1_000_000.0);
    println!("  Write speed: {:.1} MB/s", write_speed);
    println!("═══════════════════════════════════════════════════════════════════");

    println!("\n✅ Benchmark complete");
    Ok(())
}
