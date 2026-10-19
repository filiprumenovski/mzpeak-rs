//! Benchmark Thermo RAW file reading performance.
//!
//! Usage:
//!     cargo run --release --features thermo --example thermo_benchmark -- /path/to/file.raw
//!
//! This benchmark measures:
//! - File open time
//! - Streaming throughput (spectra/second)
//! - Peak data extraction rate
//! - Memory efficiency with batch processing

use std::env;
use std::time::Instant;

#[cfg(feature = "thermo")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use mzpeak::thermo::ThermoStreamer;

    // Get file path from command line
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path/to/file.raw>", args[0]);
        eprintln!("\nExample:");
        eprintln!(
            "  cargo run --release --features thermo --example thermo_benchmark -- sample.raw"
        );
        std::process::exit(1);
    }
    let raw_path = &args[1];

    println!("═══════════════════════════════════════════════════════════════════════════");
    println!("                    Thermo RAW File Benchmark");
    println!("═══════════════════════════════════════════════════════════════════════════");
    println!("File: {}", raw_path);
    println!();

    // ─── Phase 1: File Open ───────────────────────────────────────────────────────
    println!("📂 Opening RAW file...");
    let open_start = Instant::now();
    let mut streamer = ThermoStreamer::new(raw_path, 1000)?;
    let open_time = open_start.elapsed();

    let total_spectra = streamer.len();

    println!("   ✓ Opened in {:.2?}", open_time);
    println!("   • Total spectra: {}", total_spectra);

    println!();

    // ─── Phase 2: Streaming Throughput ────────────────────────────────────────────
    println!("🚀 Streaming spectra (batch size = 1000)...");

    let stream_start = Instant::now();
    let mut ms1_count = 0u64;
    let mut ms2_count = 0u64;
    let mut total_peaks = 0u64;
    let mut max_peaks = 0usize;
    let mut min_peaks = usize::MAX;

    let mut batch_count = 0;

    while let Some(batch) = streamer.next_batch()? {
        batch_count += 1;

        for raw_spectrum in batch {
            // Convert to IngestSpectrum to test the full pipeline
            let ingest = raw_spectrum;

            let n_peaks = ingest.peaks.mz.len();
            total_peaks += n_peaks as u64;
            max_peaks = max_peaks.max(n_peaks);
            if n_peaks > 0 {
                min_peaks = min_peaks.min(n_peaks);
            }

            match ingest.ms_level {
                1 => ms1_count += 1,
                2 => ms2_count += 1,
                _ => {}
            }
        }

        // Progress indicator
        if batch_count % 10 == 0 {
            let processed = streamer.position();
            let pct = (processed as f64 / total_spectra as f64) * 100.0;
            print!(
                "\r   Progress: {:.1}% ({}/{})",
                pct, processed, total_spectra
            );
        }
    }
    println!();

    let stream_time = stream_start.elapsed();
    let spectra_per_sec = total_spectra as f64 / stream_time.as_secs_f64();
    let peaks_per_sec = total_peaks as f64 / stream_time.as_secs_f64();

    // ─── Results ──────────────────────────────────────────────────────────────────
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════");
    println!("                              Results");
    println!("═══════════════════════════════════════════════════════════════════════════");
    println!();
    println!("📊 Spectrum Statistics:");
    println!("   • MS1 spectra: {}", ms1_count);
    println!("   • MS2 spectra: {}", ms2_count);
    println!(
        "   • Other:       {}",
        total_spectra as u64 - ms1_count - ms2_count
    );
    println!();
    println!("📈 Peak Statistics:");
    println!("   • Total peaks: {}", total_peaks);
    println!(
        "   • Avg peaks/spectrum: {:.1}",
        total_peaks as f64 / total_spectra as f64
    );
    println!("   • Max peaks: {}", max_peaks);
    println!(
        "   • Min peaks: {}",
        if min_peaks == usize::MAX {
            0
        } else {
            min_peaks
        }
    );
    println!();
    println!("⏱️  Performance:");
    println!("   • Open time: {:.2?}", open_time);
    println!("   • Stream time: {:.2?}", stream_time);
    println!("   • Total time: {:.2?}", open_time + stream_time);
    println!();
    println!("🚀 Throughput:");
    println!("   • {:.0} spectra/second", spectra_per_sec);
    println!("   • {:.0} peaks/second", peaks_per_sec);
    println!(
        "   • {:.2} MB/second (peaks only, f64+f32)",
        (total_peaks as f64 * 12.0) / (stream_time.as_secs_f64() * 1_000_000.0)
    );
    println!();
    println!("═══════════════════════════════════════════════════════════════════════════");

    Ok(())
}

#[cfg(not(feature = "thermo"))]
fn main() {
    eprintln!("Error: This example requires the 'thermo' feature.");
    eprintln!(
        "Run with: cargo run --release --features thermo --example thermo_benchmark -- <file.raw>"
    );
    std::process::exit(1);
}
