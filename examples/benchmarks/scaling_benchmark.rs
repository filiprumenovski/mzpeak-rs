//! Benchmark worker scaling for ParallelTdfConverter
//!
//! Test how throughput scales with number of parallel workers

use std::time::Instant;
use std::path::PathBuf;

use mzpeak::tdf::{ParallelTdfConverter, ParallelConversionConfig};
use mzpeak::writer::WriterConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_path = PathBuf::from(
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d"
    );
    let base_output_dir = PathBuf::from("/tmp/scaling_test");

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║         Worker Scaling Benchmark                                 ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // Test different worker counts
    let worker_counts = [1, 2, 4, 8, 16];
    
    println!("{:<10} {:>12} {:>15} {:>12}", 
        "Workers", "Time (s)", "Peaks/sec (M)", "Speedup");
    println!("{}", "-".repeat(52));

    let mut baseline_time = 0.0;

    for &num_workers in &worker_counts {
        let output_dir = base_output_dir.join(format!("workers_{}", num_workers));
        
        // Clean up
        if output_dir.exists() {
            std::fs::remove_dir_all(&output_dir)?;
        }
        std::fs::create_dir_all(&output_dir)?;

        // Configure
        let mut config = ParallelConversionConfig::default();
        config.num_workers = num_workers;
        config.merge_shards = false;
        config.writer_config = WriterConfig::default();

        // Run
        let start = Instant::now();
        let converter = ParallelTdfConverter::with_config(config);
        let stats = converter.convert(&data_path, &output_dir)?;
        let elapsed = start.elapsed().as_secs_f64();

        if num_workers == 1 {
            baseline_time = elapsed;
        }

        let peaks_per_sec = stats.total_peaks as f64 / elapsed / 1_000_000.0;
        let speedup = baseline_time / elapsed;

        println!("{:<10} {:>10.2}s {:>13.1}M {:>10.2}x", 
            num_workers, elapsed, peaks_per_sec, speedup);

        // Cleanup output
        std::fs::remove_dir_all(&output_dir)?;
    }

    println!("\n✅ Scaling benchmark complete");
    Ok(())
}
