//! Benchmark parallel vs sequential TDF conversion.
//!
//! This example compares the performance of:
//! - Sequential conversion (existing TdfConverter)
//! - Parallel sharded conversion (new ParallelTdfConverter)

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use mzpeak::tdf::{ParallelConversionConfig, ParallelTdfConverter, TdfConverter};
use mzpeak::writer::{MzPeakWriter, WriterConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = PathBuf::from("/Volumes/NVMe 2TB/Test/parallel_benchmark");
    
    // Create output directories
    let sequential_dir = output_dir.join("sequential");
    let parallel_dir = output_dir.join("parallel");
    fs::create_dir_all(&sequential_dir)?;
    fs::create_dir_all(&parallel_dir)?;

    let data_path = PathBuf::from(
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d"
    );

    if !data_path.exists() {
        eprintln!("❌ Data file not found: {}", data_path.display());
        return Ok(());
    }

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║          Parallel vs Sequential TDF Conversion Benchmark         ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!("\nInput: {}", data_path.display());

    // =========================================================================
    // Parallel Conversion
    // =========================================================================
    println!("\n┌─────────────────────────────────────────────────────────────────┐");
    println!("│                    PARALLEL CONVERSION                          │");
    println!("└─────────────────────────────────────────────────────────────────┘");

    let num_workers = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);
    println!("Workers: {}", num_workers);

    let config = ParallelConversionConfig {
        num_workers,
        include_extended_metadata: true,
        merge_shards: false,
        ..Default::default()
    };

    let converter = ParallelTdfConverter::with_config(config);

    let start = Instant::now();
    let stats = converter.convert(&data_path, &parallel_dir)?;
    let parallel_time = start.elapsed();

    println!("\n📊 Parallel Results:");
    println!("   Total spectra: {}", stats.total_spectra);
    println!("   Total peaks:   {}", stats.total_peaks);
    println!("   Wall time:     {:.2}s", parallel_time.as_secs_f64());
    println!("   Throughput:    {:.0} spectra/sec", stats.total_spectra as f64 / parallel_time.as_secs_f64());
    println!("   Peak rate:     {:.1}M peaks/sec", stats.total_peaks as f64 / parallel_time.as_secs_f64() / 1_000_000.0);

    println!("\n   Shard breakdown:");
    for shard in &stats.shard_stats {
        println!("     Shard {}: {} spectra, {} peaks", 
            shard.shard_id, shard.spectra_written, shard.peaks_written);
    }

    // Calculate load balance
    if stats.shard_stats.len() > 1 {
        let spectra_counts: Vec<f64> = stats.shard_stats.iter()
            .map(|s| s.spectra_written as f64)
            .collect();
        let mean = spectra_counts.iter().sum::<f64>() / spectra_counts.len() as f64;
        let variance = spectra_counts.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / spectra_counts.len() as f64;
        let std_dev = variance.sqrt();
        let cv = std_dev / mean * 100.0;
        println!("\n   Load balance CV: {:.1}% (lower is better)", cv);
    }

    // =========================================================================
    // Sequential Conversion (for comparison)
    // =========================================================================
    println!("\n┌─────────────────────────────────────────────────────────────────┐");
    println!("│                   SEQUENTIAL CONVERSION                         │");
    println!("└─────────────────────────────────────────────────────────────────┘");

    let mut config = mzpeak::tdf::converter::TdfConversionConfig::default();
    config.include_extended_metadata = true;
    let converter = TdfConverter::with_config(config);

    let start = Instant::now();
    let spectra = converter.convert(&data_path)?;
    let sequential_convert_time = start.elapsed();

    println!("\n   Conversion time: {:.2}s", sequential_convert_time.as_secs_f64());

    // Write to file
    let output_path = sequential_dir.join("output.mzpeak");
    let metadata = mzpeak::metadata::MzPeakMetadata::default();
    let writer_config = WriterConfig::default();

    let start = Instant::now();
    let mut writer = MzPeakWriter::new_file(&output_path, &metadata, writer_config)?;
    for spectrum in spectra {
        writer.write_spectrum_owned(spectrum)?;
    }
    let write_stats = writer.finish()?;
    let sequential_write_time = start.elapsed();

    let sequential_total = sequential_convert_time + sequential_write_time;

    println!("   Write time:      {:.2}s", sequential_write_time.as_secs_f64());
    println!("\n📊 Sequential Results:");
    println!("   Total spectra: {}", write_stats.spectra_written);
    println!("   Total peaks:   {}", write_stats.peaks_written);
    println!("   Wall time:     {:.2}s (convert) + {:.2}s (write) = {:.2}s total",
        sequential_convert_time.as_secs_f64(),
        sequential_write_time.as_secs_f64(),
        sequential_total.as_secs_f64()
    );
    println!("   Throughput:    {:.0} spectra/sec", write_stats.spectra_written as f64 / sequential_total.as_secs_f64());

    // =========================================================================
    // Comparison
    // =========================================================================
    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║                         COMPARISON                               ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    
    let speedup = sequential_total.as_secs_f64() / parallel_time.as_secs_f64();
    println!("\n   Sequential: {:.2}s", sequential_total.as_secs_f64());
    println!("   Parallel:   {:.2}s", parallel_time.as_secs_f64());
    println!("   Speedup:    {:.2}× faster", speedup);
    println!("   Efficiency: {:.0}% (speedup / workers)", speedup / num_workers as f64 * 100.0);

    Ok(())
}
