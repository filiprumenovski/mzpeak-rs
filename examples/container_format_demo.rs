//! Example: Complete workflow using .mzpeak container format
//!
//! This example demonstrates:
//! 1. Creating a .mzpeak container with spectra and chromatograms
//! 2. Zero-extraction reading
//! 3. Validation
//! 4. Comparison with directory format

use mzpeak::chromatogram_writer::Chromatogram;
use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::{MzPeakMetadata, SdrfMetadata, SourceFileInfo};
use mzpeak::reader::MzPeakReader;
use mzpeak::validator::validate_mzpeak_file;
use mzpeak::writer::{SpectrumBuilder, WriterConfig};
use std::fs;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== mzPeak Container Format Example ===\n");

    let temp = tempdir()?;
    let container_path = temp.path().join("example.mzpeak");

    // 1. Create metadata
    println!("1. Creating metadata...");
    let mut metadata = MzPeakMetadata::new();
    metadata.sdrf = Some(SdrfMetadata::new("QC_Sample_001"));
    metadata.source_file = Some(SourceFileInfo::new("QC_Sample_001.raw"));

    // 2. Write container
    println!("2. Writing .mzpeak container...");
    let config = WriterConfig::default();
    let mut dataset = MzPeakDatasetWriter::new(&container_path, &metadata, config)?;

    // Generate synthetic MS1 and MS2 spectra
    println!("   - Writing 100 MS1 spectra...");
    println!("   - Writing 900 MS2 spectra...");
    
    for i in 0..1000 {
        let is_ms1 = i % 10 == 0;
        let mut builder = SpectrumBuilder::new(i as i64, (i + 1) as i64)
            .ms_level(if is_ms1 { 1 } else { 2 })
            .retention_time((i as f32) * 0.1)
            .polarity(1);

        // Add precursor info for MS2
        if !is_ms1 {
            let precursor_mz = 400.0 + ((i % 100) as f64) * 5.0;
            builder = builder.precursor(precursor_mz, Some(2), Some(1e6));
        }

        // Add peaks
        let num_peaks = if is_ms1 { 500 } else { 50 };
        for j in 0..num_peaks {
            let mz = 100.0 + (j as f64) * 2.0;
            let intensity = 1000.0 + (j as f32) * 100.0;
            builder = builder.add_peak(mz, intensity);
        }

        dataset.write_spectrum(&builder.build())?;
    }

    // Write chromatograms
    println!("   - Writing TIC and BPC chromatograms...");
    let tic = Chromatogram {
        chromatogram_id: "TIC".to_string(),
        chromatogram_type: "TIC".to_string(),
        time_array: (0..1000).map(|i| i as f64 * 0.1).collect(),
        intensity_array: (0..1000).map(|i| 1e6 + (i as f32) * 1000.0).collect(),
    };
    
    let bpc = Chromatogram {
        chromatogram_id: "BPC".to_string(),
        chromatogram_type: "BPC".to_string(),
        time_array: (0..1000).map(|i| i as f64 * 0.1).collect(),
        intensity_array: (0..1000).map(|i| 5e5 + (i as f32) * 500.0).collect(),
    };

    dataset.write_chromatogram(&tic)?;
    dataset.write_chromatogram(&bpc)?;

    let stats = dataset.close()?;
    
    let file_size_mb = stats.total_size_bytes as f64 / 1_048_576.0;
    println!("   ✓ Container created: {:.2} MB", file_size_mb);
    println!("   ✓ {} spectra, {} peaks", stats.peak_stats.spectra_written, stats.peak_stats.peaks_written);
    println!("   ✓ {} chromatograms", stats.chromatograms_written);

    // 3. Validate container
    println!("\n3. Validating container...");
    let report = validate_mzpeak_file(&container_path)?;
    
    if report.has_failures() {
        println!("   ✗ Validation FAILED");
        println!("{}", report);
        return Ok(());
    }
    
    println!("   ✓ Validation passed: {} checks", report.success_count());
    
    // Show some validation details
    for check in report.checks.iter().take(10) {
        match &check.status {
            mzpeak::validator::CheckStatus::Ok => {
                println!("     [✓] {}", check.name);
            }
            mzpeak::validator::CheckStatus::Warning(msg) => {
                println!("     [⚠] {} - {}", check.name, msg);
            }
            mzpeak::validator::CheckStatus::Failed(msg) => {
                println!("     [✗] {} - {}", check.name, msg);
            }
        }
    }

    // 4. Read without extraction
    println!("\n4. Reading container (zero-extraction)...");
    let reader = MzPeakReader::open(&container_path)?;
    
    let file_metadata = reader.metadata();
    println!("   Format version: {}", file_metadata.format_version);
    println!("   Total peaks: {}", file_metadata.total_rows);
    println!("   Row groups: {}", file_metadata.num_row_groups);

    // 5. Query operations
    println!("\n5. Running queries...");
    
    // Get file summary
    let summary = reader.summary()?;
    println!("   Total spectra: {}", summary.num_spectra);
    println!("   MS1 spectra: {}", summary.num_ms1_spectra);
    println!("   MS2 spectra: {}", summary.num_ms2_spectra);
    
    if let Some((min_rt, max_rt)) = summary.rt_range {
        println!("   RT range: {:.2} - {:.2} seconds", min_rt, max_rt);
    }

    // Query by retention time
    let rt_spectra = reader.spectra_by_rt_range(50.0, 60.0)?;
    println!("   Spectra in RT 50-60s: {}", rt_spectra.len());

    // Query by MS level
    let ms1_spectra = reader.spectra_by_ms_level(1)?;
    println!("   MS1 spectra retrieved: {}", ms1_spectra.len());

    // Get specific spectrum
    let spectrum = reader.get_spectrum(500)?.unwrap();
    println!("   Spectrum 500: {} peaks, RT={:.2}s", 
             spectrum.peaks.len(), spectrum.retention_time);

    // Read chromatograms
    let chromatograms = reader.read_chromatograms()?;
    println!("   Chromatograms: {}", chromatograms.len());
    for chrom in &chromatograms {
        println!("     - {}: {} points", chrom.chromatogram_id, chrom.time_array.len());
    }

    // 6. Performance comparison
    println!("\n6. Performance comparison...");
    
    use std::time::Instant;
    
    // Measure open time
    let start = Instant::now();
    let _reader = MzPeakReader::open(&container_path)?;
    println!("   Open time: {:?}", start.elapsed());

    // Measure full read time
    let reader = MzPeakReader::open(&container_path)?;
    let start = Instant::now();
    let all_spectra = reader.iter_spectra()?;
    let read_duration = start.elapsed();
    println!("   Read all spectra: {:?} ({} spectra)", read_duration, all_spectra.len());

    // Measure random access
    let reader = MzPeakReader::open(&container_path)?;
    let start = Instant::now();
    let _spectrum = reader.get_spectrum(750)?;
    println!("   Random access (single spectrum): {:?}", start.elapsed());

    // 7. Inspect container structure
    println!("\n7. Container structure:");
    use std::fs::File;
    use zip::ZipArchive;
    
    let file = File::open(&container_path)?;
    let mut archive = ZipArchive::new(file)?;
    
    println!("   Total entries: {}", archive.len());
    for i in 0..archive.len() {
        let entry = archive.by_index(i)?;
        let compression = match entry.compression() {
            zip::CompressionMethod::Stored => "Stored (uncompressed)",
            zip::CompressionMethod::Deflated => "Deflated",
            _ => "Other",
        };
        println!("   [{:2}] {} - {} bytes ({})", 
                 i, entry.name(), entry.size(), compression);
    }

    // 8. Compare with directory format
    println!("\n8. Creating directory format for comparison...");
    let dir_path = temp.path().join("example_dir");
    
    let mut dir_dataset = MzPeakDatasetWriter::new_directory(
        &dir_path, &metadata, WriterConfig::default()
    )?;
    
    // Write same data
    for i in 0..1000 {
        let is_ms1 = i % 10 == 0;
        let mut builder = SpectrumBuilder::new(i as i64, (i + 1) as i64)
            .ms_level(if is_ms1 { 1 } else { 2 })
            .retention_time((i as f32) * 0.1)
            .polarity(1);

        if !is_ms1 {
            builder = builder.precursor(400.0 + ((i % 100) as f64) * 5.0, Some(2), Some(1e6));
        }

        let num_peaks = if is_ms1 { 500 } else { 50 };
        for j in 0..num_peaks {
            builder = builder.add_peak(100.0 + (j as f64) * 2.0, 1000.0 + (j as f32) * 100.0);
        }

        dir_dataset.write_spectrum(&builder.build())?;
    }
    
    dir_dataset.close()?;

    // Calculate directory size
    let dir_size = calculate_dir_size(&dir_path)?;
    let container_size = fs::metadata(&container_path)?.len();
    
    println!("   Container: {:.2} MB", container_size as f64 / 1_048_576.0);
    println!("   Directory: {:.2} MB", dir_size as f64 / 1_048_576.0);
    println!("   Size difference: {:.1}%", 
             ((container_size as f64 - dir_size as f64) / dir_size as f64) * 100.0);

    println!("\n=== Example Complete ===");
    println!("\nKey Takeaways:");
    println!("✓ Container format is single file (easy distribution)");
    println!("✓ Zero-extraction reading (no temp files)");
    println!("✓ Same size as directory format (Parquet already compressed)");
    println!("✓ Fast random access and queries");
    println!("✓ Validates successfully");
    println!("\nContainer saved to: {}", container_path.display());

    Ok(())
}

/// Calculate total size of a directory recursively
fn calculate_dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0;
    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                total += calculate_dir_size(&entry.path())?;
            } else {
                total += metadata.len();
            }
        }
    } else {
        total = fs::metadata(path)?.len();
    }
    Ok(total)
}
