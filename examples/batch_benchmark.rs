//! Test aggressive batching for Parquet writes
//!
//! Compare per-spectrum writes vs batched writes

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use mzpeak::tdf::TdfConverter;
use mzpeak::writer::{MzPeakWriter, WriterConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = PathBuf::from("./bench_output");
    fs::create_dir_all(&output_dir)?;

    let data_path = PathBuf::from(
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d"
    );

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║              Batching Strategy Benchmark                         ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // Load data once
    println!("Loading TDF data...");
    let load_start = Instant::now();
    let config = mzpeak::tdf::converter::TdfConversionConfig::default();
    let converter = TdfConverter::with_config(config);
    let spectra = converter.convert(&data_path)?;
    let load_time = load_start.elapsed();
    
    println!("Loaded {} spectra in {:.2}s\n", spectra.len(), load_time.as_secs_f64());

    let total_peaks: usize = spectra.iter().map(|s| s.peaks.mz.len()).sum();
    println!("Total peaks: {} ({:.1}M)\n", total_peaks, total_peaks as f64 / 1_000_000.0);

    // Test different batch sizes
    let batch_sizes = vec![500];

    println!("{:<15} {:>12} {:>12} {:>15}", "Batch Size", "Write Time", "File Size", "Peaks/sec");
    println!("{}", "-".repeat(58));

    for batch_size in batch_sizes {
        let output_path = output_dir.join(format!("batch_{}.mzpeak", batch_size));
        
        let writer_config = WriterConfig::default();
        let metadata = mzpeak::metadata::MzPeakMetadata::default();
        let mut writer = MzPeakWriter::new_file(&output_path, &metadata, writer_config)?;
        
        let start = Instant::now();
        
        // Write in batches
        for chunk in spectra.chunks(batch_size) {
            writer.write_spectra_arrays(chunk)?;
        }
        
        let _stats = writer.finish()?;
        let elapsed = start.elapsed();
        
        let file_size = fs::metadata(&output_path)?.len();
        let peaks_per_sec = total_peaks as f64 / elapsed.as_secs_f64();
        
        println!("{:<15} {:>10.2}s {:>10.1} MB {:>12.1}M/s", 
            if batch_size == 11811 { "ALL".to_string() } else { batch_size.to_string() },
            elapsed.as_secs_f64(),
            file_size as f64 / 1_000_000.0,
            peaks_per_sec / 1_000_000.0
        );
        
        // Clean up
        fs::remove_file(&output_path)?;
    }

    println!("\n✅ Benchmark complete");

    Ok(())
}
