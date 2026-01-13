/// Test the TDF converter against real Bruker data
use mzpeak::tdf::converter::TdfConverter;
use mzpeak::writer::MzPeakWriter;
use std::path::PathBuf;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = PathBuf::from("/Volumes/NVMe 2TB/Test");
    
    // Create output directory if it doesn't exist
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)?;
        println!("📁 Created output directory: {}", output_dir.display());
    }

    // Try the large AlphaTims file first; fall back to smaller test data if needed
    let data_paths = vec![
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d",
        "/Users/filiprumenovski/Code/timsrust-4d/tests/test.d",
    ];

    for data_path_str in data_paths {
        let data_path = PathBuf::from(data_path_str);
        if !data_path.exists() {
            println!("⏭️  Skipping (not found): {}", data_path_str);
            continue;
        }

        println!("\n{}", "=".repeat(80));
        println!("Testing TDF converter...");
        println!("Path: {}", data_path.display());
        println!("{}", "=".repeat(80));

        let mut config = mzpeak::tdf::converter::TdfConversionConfig::default();
        config.include_extended_metadata = true;
        let converter = TdfConverter::with_config(config);
        println!("Initialized converter with TDF feature");

        match converter.convert(&data_path) {
            Ok(spectra) => {
                println!("\n✅ Successfully parsed TDF file!");
                println!("Total spectra: {}", spectra.len());

                if !spectra.is_empty() {
                    let first = &spectra[0];
                    println!("\nFirst spectrum:");
                    println!("  Peaks: {}", first.peaks.mz.len());
                    if !first.peaks.mz.is_empty() {
                        println!("  m/z range: {:.2} - {:.2}", first.peaks.mz[0], first.peaks.mz[first.peaks.mz.len() - 1]);
                        println!("  Intensity range: {:.0} - {:.0}",
                            first.peaks.intensity.iter().copied().fold(f32::INFINITY, f32::min),
                            first.peaks.intensity.iter().copied().fold(0.0, f32::max)
                        );
                    }
                    println!("  MS Level: {}", first.ms_level);
                    println!("  Retention time: {:.2}s", first.retention_time);
                    if let Some(prec_mz) = first.precursor_mz {
                        println!("  Precursor m/z: {:.4}", prec_mz);
                    }
                }

                // Sample some stats across the dataset
                let mut total_peaks = 0usize;
                let mut rt_values = vec![];
                for spectrum in &spectra {
                    total_peaks += spectrum.peaks.mz.len();
                    rt_values.push(spectrum.retention_time);
                }

                println!("\nDataset stats:");
                println!("  Total peaks: {}", total_peaks);
                println!("  Average peaks/spectrum: {:.1}", total_peaks as f64 / spectra.len() as f64);
                if !rt_values.is_empty() {
                    let min_rt = rt_values.iter().copied().fold(f32::INFINITY, f32::min);
                    let max_rt = rt_values.iter().copied().fold(0.0, f32::max);
                    println!("  RT range: {:.2} - {:.2} minutes", min_rt / 60.0, max_rt / 60.0);
                }

                // Write to mzpeak format
                let file_stem = data_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("output");
                let output_path = output_dir.join(format!("{}.mzpeak", file_stem));
                
                println!("\n💾 Writing to: {}", output_path.display());
                
                let metadata = mzpeak::metadata::MzPeakMetadata::default();
                let config = mzpeak::writer::WriterConfig::default();
                let mut writer = MzPeakWriter::new_file(&output_path, &metadata, config)?;
                
                for spectrum in spectra {
                    writer.write_spectrum_owned(spectrum)?;
                }
                let stats = writer.finish()?;
                
                println!("✅ Successfully wrote {} spectra, {} peaks", stats.spectra_written, stats.peaks_written);

                return Ok(());
            }
            Err(e) => {
                eprintln!("❌ Error parsing TDF: {}", e);
                continue;
            }
        }
    }

    println!("\n⚠️  No valid TDF files could be parsed.");
    Err("All test files failed to parse".into())
}
