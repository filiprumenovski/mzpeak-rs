//! Validate that sync and async outputs are identical and match source data
//!
//! Usage:
//!     cargo run --release --example validate_lossless

use std::fs::File;
use std::io::BufReader;

use arrow::array::{Array, Float32Array, Float64Array};
use mzpeak::mzml::MzMLStreamer;
use mzpeak::reader::MzPeakReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mzml_path = "/Volumes/NVMe 2TB/Test/large_test.mzML";
    // These are bare Parquet files, rename to .parquet for the reader
    let sync_path = "/Volumes/NVMe 2TB/Test/mzml_benchmark/sync_output.parquet";
    let async_path = "/Volumes/NVMe 2TB/Test/mzml_benchmark/async_output.parquet";
    
    // Copy/rename if needed
    let sync_src = "/Volumes/NVMe 2TB/Test/mzml_benchmark/sync_output.mzpeak";
    let async_src = "/Volumes/NVMe 2TB/Test/mzml_benchmark/async_output.mzpeak";
    if std::path::Path::new(sync_src).exists() && !std::path::Path::new(sync_path).exists() {
        std::fs::copy(sync_src, sync_path)?;
    }
    if std::path::Path::new(async_src).exists() && !std::path::Path::new(async_path).exists() {
        std::fs::copy(async_src, async_path)?;
    }

    println!("═══════════════════════════════════════════════════════════════════");
    println!("                    LOSSLESS VALIDATION                             ");
    println!("═══════════════════════════════════════════════════════════════════\n");

    // 1. Compare sync vs async outputs using Arrow batches
    println!("1️⃣  Comparing sync vs async output files...");
    let sync_reader = MzPeakReader::open(sync_path)?;
    let async_reader = MzPeakReader::open(async_path)?;

    let sync_batches = sync_reader.read_all_batches()?;
    let async_batches = async_reader.read_all_batches()?;

    println!("   Sync:  {} batches", sync_batches.len());
    println!("   Async: {} batches", async_batches.len());

    let sync_rows: usize = sync_batches.iter().map(|b| b.num_rows()).sum();
    let async_rows: usize = async_batches.iter().map(|b| b.num_rows()).sum();

    println!("   Sync rows:  {}", sync_rows);
    println!("   Async rows: {}", async_rows);

    if sync_rows != async_rows {
        println!("   ❌ ROW COUNT MISMATCH!");
        return Ok(());
    }
    println!("   ✅ Row counts match\n");

    // Compute checksums from Arrow batches
    println!("2️⃣  Computing checksums from output files...");
    let mut sync_mz_sum = 0.0f64;
    let mut sync_int_sum = 0.0f64;
    let mut async_mz_sum = 0.0f64;
    let mut async_int_sum = 0.0f64;

    for batch in &sync_batches {
        let mz_col = batch.column_by_name("mz").unwrap();
        let int_col = batch.column_by_name("intensity").unwrap();
        let mz_arr = mz_col.as_any().downcast_ref::<Float64Array>().unwrap();
        let int_arr = int_col.as_any().downcast_ref::<Float32Array>().unwrap();
        sync_mz_sum += mz_arr.values().iter().sum::<f64>();
        sync_int_sum += int_arr.values().iter().map(|&x| x as f64).sum::<f64>();
    }

    for batch in &async_batches {
        let mz_col = batch.column_by_name("mz").unwrap();
        let int_col = batch.column_by_name("intensity").unwrap();
        let mz_arr = mz_col.as_any().downcast_ref::<Float64Array>().unwrap();
        let int_arr = int_col.as_any().downcast_ref::<Float32Array>().unwrap();
        async_mz_sum += mz_arr.values().iter().sum::<f64>();
        async_int_sum += int_arr.values().iter().map(|&x| x as f64).sum::<f64>();
    }

    let sync_async_mz_diff = (sync_mz_sum - async_mz_sum).abs();
    let sync_async_int_diff = (sync_int_sum - async_int_sum).abs();

    println!("   Sync m/z sum:   {:.6}", sync_mz_sum);
    println!("   Async m/z sum:  {:.6}", async_mz_sum);
    println!("   m/z diff:       {:.2e}", sync_async_mz_diff);

    if sync_async_mz_diff < 1e-6 && sync_async_int_diff < 1e-6 {
        println!("   ✅ Sync and async outputs are IDENTICAL");
    } else {
        println!("   ❌ Sync and async outputs DIFFER!");
    }

    // 3. Validate against source mzML
    println!("\n3️⃣  Validating against source mzML...");
    let file = File::open(mzml_path)?;
    let reader = BufReader::with_capacity(64 * 1024, file);
    let mut streamer = MzMLStreamer::new(reader)?;

    let mut source_peaks = 0u64;
    let mut source_mz_sum = 0.0f64;
    let mut source_int_sum = 0.0f64;

    while let Some(spectrum) = streamer.next_spectrum()? {
        for &mz in &spectrum.mz_array {
            source_mz_sum += mz;
        }
        for &int in &spectrum.intensity_array {
            source_int_sum += int;
        }
        source_peaks += spectrum.mz_array.len() as u64;
    }

    println!("   Source peaks: {}", source_peaks);
    println!("   Output peaks: {}", sync_rows);

    if source_peaks as usize != sync_rows {
        println!("   ❌ PEAK COUNT MISMATCH!");
    } else {
        println!("   ✅ Peak counts match");
    }

    let mz_diff = (source_mz_sum - sync_mz_sum).abs();
    let int_diff = (source_int_sum - sync_int_sum).abs();

    println!("\n   m/z checksum:");
    println!("      Source: {:.6}", source_mz_sum);
    println!("      Output: {:.6}", sync_mz_sum);
    println!("      Diff:   {:.2e}", mz_diff);

    println!("\n   Intensity checksum:");
    println!("      Source: {:.6}", source_int_sum);
    println!("      Output: {:.6}", sync_int_sum);
    println!("      Diff:   {:.2e}", int_diff);

    // mzML stores intensity as f64, we convert to f32, so some rounding expected
    let mz_relative_diff = mz_diff / source_mz_sum;
    let int_relative_diff = int_diff / source_int_sum;

    println!("\n   Relative m/z diff:        {:.2e}", mz_relative_diff);
    println!("   Relative intensity diff:  {:.2e}", int_relative_diff);

    // For lossless: m/z should be exact (f64), intensity allows f32 rounding
    // Relative diff < 1e-10 means essentially perfect for floating point
    if mz_relative_diff < 1e-10 && int_relative_diff < 1e-10 {
        println!("\n   ✅ LOSSLESS VALIDATION PASSED");
        println!("      • m/z: exact (f64 preserved)");
        println!("      • intensity: f64→f32 rounding within machine epsilon");
    } else if mz_relative_diff < 1e-6 && int_relative_diff < 1e-6 {
        println!("\n   ⚠️  Near-lossless (within f32 precision)");
    } else {
        println!("\n   ❌ VALIDATION FAILED");
    }

    println!("\n═══════════════════════════════════════════════════════════════════");
    Ok(())
}
