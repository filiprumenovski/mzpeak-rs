//! Writer Throughput and Speed Benchmark
//!
//! Benchmarks the async writer pipeline with synthetic data.

use std::fs::{self, File};
use std::path::PathBuf;
use std::time::Instant;

use mzpeak::metadata::MzPeakMetadata;
use mzpeak::writer::{AsyncMzPeakWriter, MzPeakWriter, OptionalColumnBuf, OwnedColumnarBatch, WriterConfig};

/// Create a realistic batch simulating mass spectrometry data
fn create_realistic_batch(num_peaks: usize, spectrum_id: i64, base_rt: f32) -> OwnedColumnarBatch {
    let mut mz = Vec::with_capacity(num_peaks);
    let mut intensity = Vec::with_capacity(num_peaks);
    
    // Simulate realistic m/z distribution (100-2000 Da)
    for i in 0..num_peaks {
        mz.push(100.0 + (i as f64 * 1900.0 / num_peaks as f64));
        intensity.push(1000.0 + (i as f32 % 1000.0) * 10.0);
    }
    
    OwnedColumnarBatch {
        mz,
        intensity,
        spectrum_id: vec![spectrum_id; num_peaks],
        scan_number: vec![spectrum_id; num_peaks],
        ms_level: vec![1; num_peaks],
        retention_time: vec![base_rt; num_peaks],
        polarity: vec![1; num_peaks],
        ion_mobility: OptionalColumnBuf::AllPresent(vec![1.0; num_peaks]),
        precursor_mz: OptionalColumnBuf::AllNull { len: num_peaks },
        precursor_charge: OptionalColumnBuf::AllNull { len: num_peaks },
        precursor_intensity: OptionalColumnBuf::AllNull { len: num_peaks },
        isolation_window_lower: OptionalColumnBuf::AllNull { len: num_peaks },
        isolation_window_upper: OptionalColumnBuf::AllNull { len: num_peaks },
        collision_energy: OptionalColumnBuf::AllNull { len: num_peaks },
        total_ion_current: OptionalColumnBuf::AllNull { len: num_peaks },
        base_peak_mz: OptionalColumnBuf::AllNull { len: num_peaks },
        base_peak_intensity: OptionalColumnBuf::AllNull { len: num_peaks },
        injection_time: OptionalColumnBuf::AllNull { len: num_peaks },
        pixel_x: OptionalColumnBuf::AllNull { len: num_peaks },
        pixel_y: OptionalColumnBuf::AllNull { len: num_peaks },
        pixel_z: OptionalColumnBuf::AllNull { len: num_peaks },
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Output directory from user request
    let output_dir = PathBuf::from("/Volumes/NVMe 2TB/Test/writer_throughput_benchmark");
    fs::create_dir_all(&output_dir)?;

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║           Writer Throughput & Speed Benchmark                    ║");
    println!("║           Testing Sync vs Async MzPeakWriter                     ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!("\nOutput: {}", output_dir.display());

    let num_cores = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4);
    
    println!("\n┌─────────────────────────────────────────────────────────────────┐");
    println!("│                    SYSTEM CONFIGURATION                         │");
    println!("└─────────────────────────────────────────────────────────────────┘");
    println!("   CPU cores:     {}", num_cores);
    println!("   Compression:   ZSTD level 3 (default)");
    println!("   Buffer cap:    32 batches (async pipeline)");

    // Test parameters
    let peaks_per_batch = 100_000;  // 100K peaks per batch (realistic)
    let num_batches = 100;          // 100 batches = 10M total peaks
    let total_peaks = peaks_per_batch * num_batches;

    println!("\n┌─────────────────────────────────────────────────────────────────┐");
    println!("│                    TEST PARAMETERS                              │");
    println!("└─────────────────────────────────────────────────────────────────┘");
    println!("   Peaks per batch: {:>10}", peaks_per_batch);
    println!("   Number of batches: {:>8}", num_batches);
    println!("   Total peaks:     {:>10} ({:.1}M)", total_peaks, total_peaks as f64 / 1_000_000.0);

    // =========================================================================
    // Synchronous Writer Benchmark
    // =========================================================================
    println!("\n┌─────────────────────────────────────────────────────────────────┐");
    println!("│                SYNCHRONOUS WRITER BENCHMARK                     │");
    println!("└─────────────────────────────────────────────────────────────────┘");

    let sync_output = output_dir.join("sync_output.mzpeak");
    let metadata = MzPeakMetadata::default();
    let config = WriterConfig::default();

    let start = Instant::now();
    let mut writer = MzPeakWriter::new_file(&sync_output, &metadata, config.clone())?;
    
    for i in 0..num_batches {
        let batch = create_realistic_batch(peaks_per_batch, i as i64, i as f32 * 0.5);
        writer.write_owned_batch(batch)?;
    }
    
    let stats = writer.finish()?;
    let sync_time = start.elapsed();
    let sync_file_size = fs::metadata(&sync_output)?.len();

    println!("   Time:          {:.3}s", sync_time.as_secs_f64());
    println!("   Peaks written: {}", stats.peaks_written);
    println!("   File size:     {:.2} MB", sync_file_size as f64 / 1_000_000.0);
    println!("   Throughput:    {:.2}M peaks/sec", stats.peaks_written as f64 / sync_time.as_secs_f64() / 1_000_000.0);
    println!("   Write speed:   {:.2} MB/s (compressed)", sync_file_size as f64 / sync_time.as_secs_f64() / 1_000_000.0);

    // =========================================================================
    // Asynchronous Writer Benchmark
    // =========================================================================
    println!("\n┌─────────────────────────────────────────────────────────────────┐");
    println!("│                ASYNCHRONOUS WRITER BENCHMARK                    │");
    println!("└─────────────────────────────────────────────────────────────────┘");

    let async_output = output_dir.join("async_output.mzpeak");
    let file = File::create(&async_output)?;
    let metadata = MzPeakMetadata::default();

    let start = Instant::now();
    let writer = AsyncMzPeakWriter::new(file, metadata, config.clone())?;
    
    for i in 0..num_batches {
        let batch = create_realistic_batch(peaks_per_batch, i as i64, i as f32 * 0.5);
        writer.write_owned_batch(batch)?;
    }
    
    let stats = writer.finish()?;
    let async_time = start.elapsed();
    let async_file_size = fs::metadata(&async_output)?.len();

    println!("   Time:          {:.3}s", async_time.as_secs_f64());
    println!("   Peaks written: {}", stats.peaks_written);
    println!("   File size:     {:.2} MB", async_file_size as f64 / 1_000_000.0);
    println!("   Throughput:    {:.2}M peaks/sec", stats.peaks_written as f64 / async_time.as_secs_f64() / 1_000_000.0);
    println!("   Write speed:   {:.2} MB/s (compressed)", async_file_size as f64 / async_time.as_secs_f64() / 1_000_000.0);

    // =========================================================================
    // Comparison
    // =========================================================================
    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║                         COMPARISON                               ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    
    let speedup = sync_time.as_secs_f64() / async_time.as_secs_f64();
    println!("\n   Synchronous:   {:.3}s @ {:.2}M peaks/sec", 
        sync_time.as_secs_f64(),
        total_peaks as f64 / sync_time.as_secs_f64() / 1_000_000.0);
    println!("   Asynchronous:  {:.3}s @ {:.2}M peaks/sec", 
        async_time.as_secs_f64(),
        total_peaks as f64 / async_time.as_secs_f64() / 1_000_000.0);
    
    if speedup > 1.0 {
        println!("\n   ✅ Async writer is {:.2}× FASTER", speedup);
    } else {
        println!("\n   ⚠️  Sync writer is {:.2}× faster (async overhead)", 1.0 / speedup);
        println!("       (This is expected for small workloads where pipeline startup dominates)");
    }

    // =========================================================================
    // Multi-iteration benchmark for timing stability
    // =========================================================================
    println!("\n┌─────────────────────────────────────────────────────────────────┐");
    println!("│              MULTI-ITERATION TIMING (5 runs)                    │");
    println!("└─────────────────────────────────────────────────────────────────┘");

    let iterations = 5;
    let mut sync_times = Vec::with_capacity(iterations);
    let mut async_times = Vec::with_capacity(iterations);

    for i in 0..iterations {
        // Sync
        let sync_path = output_dir.join(format!("iter_{}_sync.mzpeak", i));
        let start = Instant::now();
        let mut writer = MzPeakWriter::new_file(&sync_path, &MzPeakMetadata::default(), config.clone())?;
        for j in 0..num_batches {
            writer.write_owned_batch(create_realistic_batch(peaks_per_batch, j as i64, j as f32 * 0.5))?;
        }
        writer.finish()?;
        sync_times.push(start.elapsed().as_secs_f64());
        fs::remove_file(&sync_path)?;

        // Async
        let async_path = output_dir.join(format!("iter_{}_async.mzpeak", i));
        let file = File::create(&async_path)?;
        let start = Instant::now();
        let writer = AsyncMzPeakWriter::new(file, MzPeakMetadata::default(), config.clone())?;
        for j in 0..num_batches {
            writer.write_owned_batch(create_realistic_batch(peaks_per_batch, j as i64, j as f32 * 0.5))?;
        }
        writer.finish()?;
        async_times.push(start.elapsed().as_secs_f64());
        fs::remove_file(&async_path)?;

        println!("   Iteration {}: Sync={:.3}s, Async={:.3}s", i + 1, sync_times[i], async_times[i]);
    }

    let sync_mean = sync_times.iter().sum::<f64>() / iterations as f64;
    let async_mean = async_times.iter().sum::<f64>() / iterations as f64;
    let sync_std = (sync_times.iter().map(|t| (t - sync_mean).powi(2)).sum::<f64>() / iterations as f64).sqrt();
    let async_std = (async_times.iter().map(|t| (t - async_mean).powi(2)).sum::<f64>() / iterations as f64).sqrt();

    println!("\n   Sync mean:     {:.3}s ± {:.3}s", sync_mean, sync_std);
    println!("   Async mean:    {:.3}s ± {:.3}s", async_mean, async_std);
    println!("   Mean speedup:  {:.2}×", sync_mean / async_mean);

    println!("\n╔══════════════════════════════════════════════════════════════════╗");
    println!("║                    BENCHMARK COMPLETE                            ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!("\nOutput files saved to: {}", output_dir.display());

    Ok(())
}
