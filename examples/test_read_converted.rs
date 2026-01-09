//! Test reading back the converted mzPeak file

use mzpeak::reader::MzPeakReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "data/A4_El_etdOT.mzpeak";
    
    println!("Opening: {}", path);
    let reader = MzPeakReader::open(path)?;
    
    println!("\n=== METADATA ===");
    let metadata = reader.metadata();
    println!("Format version: {:?}", metadata.format_version);
    
    println!("\n=== SUMMARY ===");
    let summary = reader.summary()?;
    println!("Total spectra: {}", summary.num_spectra);
    println!("Total peaks: {}", summary.total_peaks);
    println!("MS1 spectra: {}", summary.num_ms1_spectra);
    println!("MS2 spectra: {}", summary.num_ms2_spectra);
    if let Some((min_rt, max_rt)) = summary.rt_range {
        println!("RT range: {:.2} - {:.2} seconds", min_rt, max_rt);
    }
    if let Some((min_mz, max_mz)) = summary.mz_range {
        println!("m/z range: {:.2} - {:.2}", min_mz, max_mz);
    }
    
    println!("\n=== CHROMATOGRAMS ===");
    let chromatograms = reader.read_chromatograms()?;
    println!("Found {} chromatograms:", chromatograms.len());
    for chrom in &chromatograms {
        println!("  {} ({}): {} points", 
            chrom.chromatogram_id, 
            chrom.chromatogram_type,
            chrom.time_array.len());
    }
    
    println!("\n=== SAMPLE SPECTRA ===");
    let first_10 = reader.get_spectra(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])?;
    println!("Reading first 10 spectra...");
    for spec in &first_10 {
        println!("  Spectrum {}: MS{}, RT={:.2}s, {} peaks",
            spec.spectrum_id,
            spec.ms_level,
            spec.retention_time,
            spec.peak_count());
    }
    
    println!("\n✅ SUCCESS! File is readable and intact.");
    
    Ok(())
}
