//! Test reading back the converted real mzML file

use mzpeak::reader::MzPeakReader;

fn main() {
    let path = "/Users/filiprumenovski/Code/mzpeak-rs/data/A4_El_etdOT.mzpeak";
    
    println!("Opening {}...", path);
    let reader = MzPeakReader::open(path).unwrap();
    
    println!("\n📊 File Summary:");
    let summary = reader.summary().unwrap();
    println!("  Total spectra: {}", summary.num_spectra);
    if let Some((min_rt, max_rt)) = summary.rt_range {
        println!("  RT range: {:.2} - {:.2} seconds", min_rt, max_rt);
    }
    
    println!("\n🔬 Reading chromatograms...");
    let chromatograms = reader.read_chromatograms().unwrap();
    println!("  Found {} chromatograms:", chromatograms.len());
    for chrom in &chromatograms {
        println!("    - {} ({}): {} points", chrom.chromatogram_id, chrom.chromatogram_type, chrom.time_array.len());
    }
    
    println!("\n📈 Sample spectra:");
    let spectra = reader.spectra_by_ms_level(1).unwrap();
    println!("  MS1 spectra: {}", spectra.len());
    if let Some(first) = spectra.first() {
        println!("    First MS1: ID={}, RT={:.2}s, peaks={}", 
            first.spectrum_id, first.retention_time, first.peaks.len());
    }
    
    let ms2_spectra = reader.spectra_by_ms_level(2).unwrap();
    println!("  MS2 spectra: {}", ms2_spectra.len());
    if let Some(first) = ms2_spectra.first() {
        println!("    First MS2: ID={}, RT={:.2}s, precursor={:.4}, peaks={}", 
            first.spectrum_id, first.retention_time, 
            first.precursor_mz.unwrap_or(0.0), first.peaks.len());
    }
    
    println!("\n✅ Successfully read back all data!");
}
