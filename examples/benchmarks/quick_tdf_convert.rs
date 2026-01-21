//! Quick TDF to mzPeak conversion for specific dataset

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use mzpeak::tdf::TdfConverter;
use mzpeak::writer::WriterConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_path = PathBuf::from(
        "/Volumes/NVMe 2TB/Test/Sara_Ligandome_Ovarian_20201008_S4_Slot2-44_20-10-16_2943.d"
    );

    let output_dir = PathBuf::from("/Volumes/NVMe 2TB/mz-peak output");

    if !input_path.exists() {
        eprintln!("❌ Input file not found: {}", input_path.display());
        return Ok(());
    }

    fs::create_dir_all(&output_dir)?;

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                Bruker TDF to mzPeak v2 Converter                 ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!("\nInput:  {}", input_path.display());
    println!("Output: {}", output_dir.display());
    let output_file = output_dir.join(format!(
        "{}.mzpeak",
        input_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("output")
    ));

    let converter = TdfConverter::new();
    let writer_config = WriterConfig::default();

    let start = Instant::now();
    let stats = converter.convert_to_v2_container(&input_path, &output_file, writer_config)?;
    let elapsed = start.elapsed();

    println!("\n📊 Conversion Results:");
    println!("   Total spectra: {}", stats.spectra_read);
    println!("   Total peaks:   {}", stats.peaks_total);
    println!("   Wall time:     {:.2}s", elapsed.as_secs_f64());
    println!(
        "   Throughput:    {:.0} spectra/sec",
        stats.spectra_read as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Peak rate:     {:.1}M peaks/sec",
        stats.peaks_total as f64 / elapsed.as_secs_f64() / 1_000_000.0
    );

    let output_size = fs::metadata(&output_file).map(|m| m.len()).unwrap_or(0);
    println!(
        "\n   Output file: {}",
        output_file.display()
    );
    println!("   Total output size: {:.2} MB", output_size as f64 / 1_000_000.0);

    println!("\n✅ Conversion complete!");

    Ok(())
}
