//! Isolate the Parquet write bottleneck
//!
//! Test different compression levels to see CPU vs I/O split

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use mzpeak::tdf::TdfConverter;
use mzpeak::writer::{CompressionType, MzPeakWriter, WriterConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = PathBuf::from("/Volumes/NVMe 2TB/Test/compression_test");
    fs::create_dir_all(&output_dir)?;

    let data_path = PathBuf::from(
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d"
    );

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║              Compression Level Benchmark                         ║");
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

    // Test different compression levels
    let configs = vec![
        ("Uncompressed", CompressionType::Uncompressed),
        ("Snappy", CompressionType::Snappy),
        ("ZSTD-1", CompressionType::Zstd(1)),
        ("ZSTD-3 (default)", CompressionType::Zstd(3)),
        ("ZSTD-9", CompressionType::Zstd(9)),
    ];

    println!("{:<20} {:>12} {:>12} {:>12}", "Compression", "Write Time", "File Size", "Speed");
    println!("{}", "-".repeat(60));

    for (name, compression) in configs {
        let output_path = output_dir.join(format!("{}.mzpeak", name.replace(" ", "_")));
        
        let mut writer_config = WriterConfig::default();
        writer_config.compression = compression;
        
        let metadata = mzpeak::metadata::MzPeakMetadata::default();
        let mut writer = MzPeakWriter::new_file(&output_path, &metadata, writer_config)?;
        
        let start = Instant::now();
        for spectrum in spectra.iter() {
            writer.write_spectrum_arrays(spectrum)?;
        }
        let stats = writer.finish()?;
        let elapsed = start.elapsed();
        
        let file_size = fs::metadata(&output_path)?.len();
        let speed_mb = file_size as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        
        println!("{:<20} {:>10.2}s {:>10.1} MB {:>10.1} MB/s", 
            name, 
            elapsed.as_secs_f64(),
            file_size as f64 / 1_000_000.0,
            speed_mb
        );
        
        // Clean up
        fs::remove_file(&output_path)?;
    }

    println!("\n✅ Benchmark complete");

    Ok(())
}
