/// Diagnostic tool to inspect frame 91 decompression issue
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_path = PathBuf::from(
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d",
    );

    println!("🔍 Diagnosing TDF frame structure at: {}", data_path.display());
    println!("{}", "=".repeat(80));

    // Open frame reader
    use timsrust::readers::FrameReader;
    
    match FrameReader::new(&data_path) {
        Ok(reader) => {
            println!("\n✅ FrameReader opened successfully");
            println!("  Total frames available: {}", reader.len());
            
            // Try to read frames incrementally, focusing on frame 91
            println!("\n📊 Frame-by-frame inspection (80-100):");
            
            let start = 80;
            let end = std::cmp::min(100, reader.len());
            let mut last_ok = start;
            
            for frame_idx in start..end {
                match reader.get(frame_idx) {
                    Ok(frame) => {
                        last_ok = frame_idx;
                        let num_peaks = frame.intensities.len();
                        println!("  ✅ Frame {:3}: {} peaks, MS{:?}, {:?}", 
                            frame_idx,
                            num_peaks,
                            frame.ms_level,
                            frame.acquisition_type
                        );
                    }
                    Err(e) => {
                        println!("\n  ❌ Frame {:3}: FAILED - {}", frame_idx, e);
                        println!("\n💡 Diagnostic Summary:");
                        println!("     Last successful frame: {}", last_ok);
                        println!("     First failed frame: {}", frame_idx);
                        println!("     Error: {}", e);
                        
                        // Try reading a few frames after to see if it's localized
                        println!("\n📋 Checking frames after failure:");
                        for test_idx in (frame_idx + 1)..std::cmp::min(frame_idx + 5, reader.len()) {
                            match reader.get(test_idx) {
                                Ok(frame) => {
                                    println!("  ✅ Frame {:3}: {} peaks (recovered!)", test_idx, frame.intensities.len());
                                }
                                Err(e2) => {
                                    println!("  ❌ Frame {:3}: {} (still failing)", test_idx, e2);
                                }
                            }
                        }
                        
                        break;
                    }
                }
            }
            
            println!("\n{}", "=".repeat(80));
        }
        Err(e) => {
            eprintln!("❌ Failed to open FrameReader: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
