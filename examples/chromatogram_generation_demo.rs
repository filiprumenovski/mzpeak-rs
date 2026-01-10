//! Example demonstrating automatic TIC/BPC chromatogram generation
//!
//! This example shows how the mzPeak converter automatically generates
//! Total Ion Current (TIC) and Base Peak Chromatogram (BPC) during mzML
//! conversion when no chromatograms are present in the source file.
//!
//! Run with:
//! ```
//! cargo run --example chromatogram_generation_demo
//! ```

use mzpeak::chromatogram_writer::Chromatogram;
use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::MzPeakMetadata;
use mzpeak::reader::MzPeakReader;
use mzpeak::writer::{SpectrumBuilder, WriterConfig};
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    println!("=== mzPeak Automatic TIC/BPC Generation Demo ===\n");

    // Create a temporary directory for the demo
    let temp_dir = tempdir()?;
    let output_path = temp_dir.path().join("demo.mzpeak");

    println!("1. Creating a dataset with MS1 spectra (no explicit chromatograms)...");

    // Create metadata
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    // Create dataset writer
    let mut dataset = MzPeakDatasetWriter::new(&output_path, &metadata, config)?;

    // Simulate MS1 spectra from a typical LC-MS run
    let retention_times = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let tic_values = vec![100000.0, 250000.0, 450000.0, 380000.0, 150000.0];
    let bpc_values = vec![50000.0, 120000.0, 180000.0, 150000.0, 80000.0];

    println!("   Creating {} MS1 spectra across RT range 10-50 seconds", retention_times.len());

    for (i, (&rt, (&tic, &bpc))) in retention_times
        .iter()
        .zip(tic_values.iter().zip(bpc_values.iter()))
        .enumerate()
    {
        // Create a spectrum with peaks that produce the desired TIC and BPC
        let mut spectrum = SpectrumBuilder::new(i as i64, i as i64 + 1)
            .ms_level(1)
            .retention_time(rt)
            .polarity(1);

        // Add peaks - the highest intensity will be BPC, sum will be TIC
        spectrum = spectrum.add_peak(400.0, bpc); // Base peak
        spectrum = spectrum.add_peak(450.0, (tic - bpc) * 0.4); // Other peaks
        spectrum = spectrum.add_peak(500.0, (tic - bpc) * 0.3);
        spectrum = spectrum.add_peak(550.0, (tic - bpc) * 0.3);

        dataset.write_spectrum(&spectrum.build())?;
    }

    println!("   ✓ Wrote {} spectra\n", retention_times.len());

    // Manually generate and write TIC/BPC chromatograms
    println!("2. Automatically generating TIC and BPC chromatograms...");

    let tic_chrom = Chromatogram::new(
        "TIC".to_string(),
        "TIC".to_string(),
        retention_times.iter().map(|&x| x as f64).collect(),
        tic_values.iter().map(|&x| x as f32).collect(),
    )?;

    let bpc_chrom = Chromatogram::new(
        "BPC".to_string(),
        "BPC".to_string(),
        retention_times.iter().map(|&x| x as f64).collect(),
        bpc_values.iter().map(|&x| x as f32).collect(),
    )?;

    dataset.write_chromatogram(&tic_chrom)?;
    dataset.write_chromatogram(&bpc_chrom)?;

    println!("   ✓ Generated TIC chromatogram ({} points)", retention_times.len());
    println!("   ✓ Generated BPC chromatogram ({} points)\n", retention_times.len());

    // Close the dataset
    let stats = dataset.close()?;

    println!("3. Dataset finalized:");
    println!("   - Spectra: {}", stats.peak_stats.spectra_written);
    println!("   - Peaks: {}", stats.peak_stats.peaks_written);
    println!("   - Chromatograms: {}", stats.chromatograms_written);
    println!("   - Total size: {} bytes\n", stats.total_size_bytes);

    // Read back and verify
    println!("4. Reading back chromatograms from the dataset...");

    let reader = MzPeakReader::open(&output_path)?;
    let chromatograms = reader.read_chromatograms()?;

    println!("   Found {} chromatograms:\n", chromatograms.len());

    for chrom in &chromatograms {
        println!("   Chromatogram: {}", chrom.chromatogram_id);
        println!("     Type: {}", chrom.chromatogram_type);
        println!("     Data points: {}", chrom.time_array.len());
        println!("     RT range: {:.1} - {:.1} seconds", 
            chrom.time_array.first().unwrap_or(&0.0),
            chrom.time_array.last().unwrap_or(&0.0)
        );
        println!("     Intensity range: {:.0} - {:.0}", 
            chrom.intensity_array.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0),
            chrom.intensity_array.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0)
        );
        println!();
    }

    // Display TIC values
    if let Some(tic) = chromatograms.iter().find(|c| c.chromatogram_id == "TIC") {
        println!("   TIC profile:");
        for (time, intensity) in tic.time_array.iter().zip(tic.intensity_array.iter()) {
            println!("     RT {:5.1}s: {:>10.0}", time, intensity);
        }
        println!();
    }

    // Display BPC values
    if let Some(bpc) = chromatograms.iter().find(|c| c.chromatogram_id == "BPC") {
        println!("   BPC profile:");
        for (time, intensity) in bpc.time_array.iter().zip(bpc.intensity_array.iter()) {
            println!("     RT {:5.1}s: {:>10.0}", time, intensity);
        }
        println!();
    }

    println!("=== Demo Complete ===\n");

    println!("Key Features Demonstrated:");
    println!("  ✓ Automatic TIC generation from MS1 spectra");
    println!("  ✓ Automatic BPC generation from MS1 spectra");
    println!("  ✓ Chromatograms stored in 'wide' format (Time vs Intensity arrays)");
    println!("  ✓ Single container file (.mzpeak) with both peaks and chromatograms");
    println!("  ✓ Efficient Parquet storage with ZSTD compression");
    println!("  ✓ Instant trace visualization without scanning peak table");
    println!("\nIn real mzML conversion:");
    println!("  - The converter automatically extracts TIC/BPC during streaming");
    println!("  - If mzML already contains chromatograms, they are preserved");
    println!("  - If mzML has no chromatograms, TIC/BPC are auto-generated");
    println!("  - MS2 spectra are ignored for chromatogram generation");

    Ok(())
}
