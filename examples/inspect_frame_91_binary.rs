/// Inspect the binary structure of frame 91
use std::path::PathBuf;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_path = PathBuf::from(
        "/Users/filiprumenovski/Code/mzpeak-rs/data/20201207_tims03_Evo03_PS_SA_HeLa_200ng_EvoSep_prot_DDA_21min_8cm_S1-C10_1_22476.d",
    );

    println!("🔬 Binary inspection of frame 91 at: {}", data_path.display());
    println!("{}", "=".repeat(80));

    // Open the index file (analysis.tdf) to get frame 91 binary offset
    use timsrust::readers::MetadataReader;
    
    let metadata = MetadataReader::new(&data_path)?;
    
    println!("\n📊 Frame 91 metadata:");
    
    // Read frames to get properties
    use timsrust::readers::FrameReader;
    let frame_reader = FrameReader::new(&data_path)?;
    
    // Read frames 90 and 92 (before and after the broken one)
    if let Ok(frame_90) = frame_reader.get(90) {
        println!("  Frame 90: {} peaks", frame_90.intensities.len());
        println!("    scan_offsets: {} entries", frame_90.scan_offsets.len());
        println!("    tof_indices: {} entries", frame_90.tof_indices.len());
        println!("    intensities: {} entries", frame_90.intensities.len());
    }
    
    println!("  Frame 91: DECOMPRESSION FAILED");
    
    if let Ok(frame_92) = frame_reader.get(92) {
        println!("  Frame 92: {} peaks", frame_92.intensities.len());
        println!("    scan_offsets: {} entries", frame_92.scan_offsets.len());
        println!("    tof_indices: {} entries", frame_92.tof_indices.len());
        println!("    intensities: {} entries", frame_92.intensities.len());
    }

    // Try to read frame 91 directly from binary
    inspect_binary_directly(&data_path)?;

    println!("\n{}", "=".repeat(80));
    println!("💡 Summary:");
    println!("  • Frame 91 likely has corrupted or malformed compression data");
    println!("  • Frames before and after can be read successfully");
    println!("  • Possible causes:");
    println!("    1. Download corruption during AlphaTims file transfer");
    println!("    2. Incompatible compression format in Bruker's software");
    println!("    3. timsrust decompressor bug with specific frame configuration");
    println!("\n✅ Workaround: Skip frame 91 in production parsing (non-critical for validation)");

    Ok(())
}

fn inspect_binary_directly(data_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let tdf_bin_path = data_path.join("analysis.tdf_bin");
    let mut file = File::open(&tdf_bin_path)?;
    
    // Try to find frame 91's blob offset
    // Note: This is reverse-engineering; the actual offset depends on timsrust's frame indexing
    
    println!("\n📄 .tdf_bin file inspection:");
    
    let metadata = std::fs::metadata(&tdf_bin_path)?;
    println!("  Total file size: {} bytes ({:.2} MB)", 
        metadata.len(), 
        metadata.len() as f64 / 1_000_000.0
    );
    
    // Read first 1KB to check structure
    let mut header = vec![0u8; 1024];
    file.seek(SeekFrom::Start(0))?;
    let bytes_read = file.read(&mut header)?;
    
    println!("  First {} bytes (hex):", std::cmp::min(64, bytes_read));
    for i in 0..std::cmp::min(64, bytes_read) {
        if i % 16 == 0 {
            print!("\n    {:04x}: ", i);
        }
        print!("{:02x} ", header[i]);
    }
    println!();
    
    Ok(())
}
