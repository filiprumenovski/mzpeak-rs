//! Benchmark mzML file reading and writing performance.
//!
//! Usage:
//!     cargo run --release --example mzml_benchmark -- /path/to/file.mzML
//!
//! This benchmark measures:
//! - Streaming throughput (spectra/second)
//! - Peak data extraction rate
//! - Sync vs Async writer comparison

use std::env;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::PathBuf;
use std::time::Instant;

use mzpeak::ingest::{IngestSpectrum, IngestSpectrumConverter};
use mzpeak::metadata::MzPeakMetadata;
use mzpeak::mzml::{MzMLSpectrum, MzMLStreamer};
use mzpeak::writer::{AsyncMzPeakWriter, MzPeakWriter, OwnedColumnarBatch, OptionalColumnBuf, PeakArrays, WriterConfig};

/// Convert MzMLSpectrum to IngestSpectrum
fn mzml_to_ingest(mzml: MzMLSpectrum, spectrum_id: i64) -> IngestSpectrum {
    let scan_number = mzml.scan_number().unwrap_or(mzml.index + 1);
    let peak_count = mzml.mz_array.len();
    
    let ion_mobility = if !mzml.ion_mobility_array.is_empty() && mzml.ion_mobility_array.len() == peak_count {
        OptionalColumnBuf::AllPresent(mzml.ion_mobility_array)
    } else {
        OptionalColumnBuf::all_null(peak_count)
    };
    
    let peaks = PeakArrays {
        mz: mzml.mz_array,
        intensity: mzml.intensity_array.into_iter().map(|v| v as f32).collect(),
        ion_mobility,
    };
    
    let precursor = mzml.precursors.first();
    
    IngestSpectrum {
        spectrum_id,
        scan_number,
        ms_level: mzml.ms_level,
        retention_time: mzml.retention_time.unwrap_or(0.0) as f32,
        polarity: mzml.polarity,
        precursor_mz: precursor.and_then(|p| p.selected_ion_mz.or(p.isolation_window_target)),
        precursor_charge: precursor.and_then(|p| p.selected_ion_charge),
        precursor_intensity: precursor.and_then(|p| p.selected_ion_intensity.map(|v| v as f32)),
        isolation_window_lower: precursor.and_then(|p| p.isolation_window_lower.map(|v| v as f32)),
        isolation_window_upper: precursor.and_then(|p| p.isolation_window_upper.map(|v| v as f32)),
        collision_energy: precursor.and_then(|p| p.collision_energy.map(|v| v as f32)),
        total_ion_current: mzml.total_ion_current,
        base_peak_mz: mzml.base_peak_mz,
        base_peak_intensity: mzml.base_peak_intensity.map(|v| v as f32),
        injection_time: mzml.ion_injection_time.map(|v| v as f32),
        pixel_x: mzml.pixel_x,
        pixel_y: mzml.pixel_y,
        pixel_z: mzml.pixel_z,
        peaks,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get file path from command line
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path/to/file.mzML>", args[0]);
        eprintln!("\nExample:");
        eprintln!("  cargo run --release --example mzml_benchmark -- sample.mzML");
        std::process::exit(1);
    }
    let mzml_path = &args[1];
    
    // Output directory
    let output_dir = PathBuf::from("/Volumes/NVMe 2TB/Test/mzml_benchmark");
    fs::create_dir_all(&output_dir)?;

    let input_size = fs::metadata(mzml_path)?.len();

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║           mzML File Benchmark + Writer Comparison                ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!("\nInput:  {} ({:.2} MB)", mzml_path, input_size as f64 / 1_000_000.0);
    println!("Output: {}", output_dir.display());
    println!();

    // ─── Phase 1: Read-Only Streaming ─────────────────────────────────────────────
    println!("┌─────────────────────────────────────────────────────────────────┐");
    println!("│                READ-ONLY STREAMING BENCHMARK                    │");
    println!("└─────────────────────────────────────────────────────────────────┘");
    
    let file = File::open(mzml_path)?;
    let reader = BufReader::with_capacity(64 * 1024, file);
    let mut streamer = MzMLStreamer::new(reader)?;
    
    let stream_start = Instant::now();
    let mut ms1_count = 0u64;
    let mut ms2_count = 0u64;
    let mut total_peaks = 0u64;
    let mut total_spectra = 0u64;
    let mut max_peaks = 0usize;
    
    while let Some(spectrum) = streamer.next_spectrum()? {
        let n_peaks = spectrum.mz_array.len();
        total_peaks += n_peaks as u64;
        max_peaks = max_peaks.max(n_peaks);
        total_spectra += 1;
        
        match spectrum.ms_level {
            1 => ms1_count += 1,
            2 => ms2_count += 1,
            _ => {}
        }
    }
    
    let stream_time = stream_start.elapsed();
    let read_spectra_per_sec = total_spectra as f64 / stream_time.as_secs_f64();
    let read_peaks_per_sec = total_peaks as f64 / stream_time.as_secs_f64();
    
    println!("   MS1: {}, MS2: {}, Other: {}", ms1_count, ms2_count, total_spectra - ms1_count - ms2_count);
    println!("   Total spectra: {}, Total peaks: {} ({:.1}M)", total_spectra, total_peaks, total_peaks as f64 / 1_000_000.0);
    println!("   Time: {:.2?}", stream_time);
    println!("   Read throughput: {:.0} spectra/sec, {:.2}M peaks/sec", 
        read_spectra_per_sec, read_peaks_per_sec / 1_000_000.0);
    println!();

    // ─── Phase 2: Synchronous Writer Benchmark ────────────────────────────────────
    println!("┌─────────────────────────────────────────────────────────────────┐");
    println!("│                SYNCHRONOUS WRITER BENCHMARK                     │");
    println!("└─────────────────────────────────────────────────────────────────┘");
    
    // Re-open streamer
    let file = File::open(mzml_path)?;
    let reader = BufReader::with_capacity(64 * 1024, file);
    let mut streamer = MzMLStreamer::new(reader)?;
    
    let sync_output = output_dir.join("sync_output.mzpeak");
    let metadata = MzPeakMetadata::default();
    let config = WriterConfig::default();
    
    let sync_start = Instant::now();
    let mut writer = MzPeakWriter::new_file(&sync_output, &metadata, config.clone())?;
    let mut ingest_converter = IngestSpectrumConverter::new();
    
    let mut spectrum_id = 0i64;
    let mut sync_peaks = 0u64;
    while let Some(mzml_spectrum) = streamer.next_spectrum()? {
        let ingest = mzml_to_ingest(mzml_spectrum, spectrum_id);
        spectrum_id += 1;
        sync_peaks += ingest.peaks.mz.len() as u64;
        let spectrum_arrays = ingest_converter.convert(ingest)?;
        writer.write_spectrum_owned(spectrum_arrays)?;
    }
    
    let sync_stats = writer.finish()?;
    let sync_time = sync_start.elapsed();
    let sync_file_size = fs::metadata(&sync_output)?.len();
    
    println!("   Time:          {:.3}s", sync_time.as_secs_f64());
    println!("   Peaks written: {}", sync_peaks);
    println!("   Row groups:    {}", sync_stats.row_groups_written);
    println!("   File size:     {:.2} MB", sync_file_size as f64 / 1_000_000.0);
    println!("   Throughput:    {:.2}M peaks/sec", sync_peaks as f64 / sync_time.as_secs_f64() / 1_000_000.0);
    println!();

    // ─── Phase 3: Asynchronous Writer Benchmark ───────────────────────────────────
    println!("┌─────────────────────────────────────────────────────────────────┐");
    println!("│                ASYNCHRONOUS WRITER BENCHMARK                    │");
    println!("└─────────────────────────────────────────────────────────────────┘");
    
    // Re-open streamer
    let file = File::open(mzml_path)?;
    let reader = BufReader::with_capacity(64 * 1024, file);
    let mut streamer = MzMLStreamer::new(reader)?;
    
    let async_output = output_dir.join("async_output.mzpeak");
    let file = File::create(&async_output)?;
    let metadata = MzPeakMetadata::default();
    
    let async_start = Instant::now();
    let writer = AsyncMzPeakWriter::new(file, metadata, config.clone())?;
    let mut ingest_converter = IngestSpectrumConverter::new();
    
    let mut spectrum_id = 0i64;
    let mut async_peaks = 0u64;
    while let Some(mzml_spectrum) = streamer.next_spectrum()? {
        let ingest = mzml_to_ingest(mzml_spectrum, spectrum_id);
        spectrum_id += 1;
        async_peaks += ingest.peaks.mz.len() as u64;
        
        // Convert IngestSpectrum → SpectrumArrays → OwnedColumnarBatch
        let spectrum_arrays = ingest_converter.convert(ingest)?;
        let owned_batch = OwnedColumnarBatch::from_spectrum_arrays(spectrum_arrays);
        writer.write_owned_batch(owned_batch)?;
    }
    
    let async_stats = writer.finish()?;
    let async_time = async_start.elapsed();
    let async_file_size = fs::metadata(&async_output)?.len();
    
    println!("   Time:          {:.3}s", async_time.as_secs_f64());
    println!("   Peaks written: {}", async_peaks);
    println!("   Row groups:    {}", async_stats.row_groups_written);
    println!("   File size:     {:.2} MB", async_file_size as f64 / 1_000_000.0);
    println!("   Throughput:    {:.2}M peaks/sec", async_peaks as f64 / async_time.as_secs_f64() / 1_000_000.0);
    println!();

    // ─── Results Summary ──────────────────────────────────────────────────────────
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                         RESULTS SUMMARY                          ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("📊 Data Statistics:");
    println!("   • Total spectra: {}", total_spectra);
    println!("   • Total peaks:   {} ({:.1}M)", total_peaks, total_peaks as f64 / 1_000_000.0);
    println!("   • MS1: {}, MS2: {}", ms1_count, ms2_count);
    println!("   • Avg peaks/spectrum: {:.1}", total_peaks as f64 / total_spectra as f64);
    println!("   • Max peaks in spectrum: {}", max_peaks);
    println!();
    println!("💾 Compression:");
    println!("   • Input mzML:   {:.2} MB", input_size as f64 / 1_000_000.0);
    println!("   • Output sync:  {:.2} MB ({:.1}× compression)", 
        sync_file_size as f64 / 1_000_000.0, input_size as f64 / sync_file_size as f64);
    println!("   • Output async: {:.2} MB ({:.1}× compression)", 
        async_file_size as f64 / 1_000_000.0, input_size as f64 / async_file_size as f64);
    println!();
    println!("⏱️  Timing:");
    println!("   • Read-only:    {:.3}s @ {:.2}M peaks/sec", 
        stream_time.as_secs_f64(), read_peaks_per_sec / 1_000_000.0);
    println!("   • Sync write:   {:.3}s @ {:.2}M peaks/sec", 
        sync_time.as_secs_f64(), sync_peaks as f64 / sync_time.as_secs_f64() / 1_000_000.0);
    println!("   • Async write:  {:.3}s @ {:.2}M peaks/sec", 
        async_time.as_secs_f64(), async_peaks as f64 / async_time.as_secs_f64() / 1_000_000.0);
    println!();
    
    let speedup = sync_time.as_secs_f64() / async_time.as_secs_f64();
    if speedup > 1.0 {
        println!("🚀 Async writer is {:.2}× FASTER than sync", speedup);
    } else {
        println!("⚠️  Sync writer is {:.2}× faster (async overhead)", 1.0 / speedup);
    }
    
    println!();
    println!("📁 Output files:");
    println!("   • Sync:  {} ({:.2} MB)", sync_output.display(), sync_file_size as f64 / 1_000_000.0);
    println!("   • Async: {} ({:.2} MB)", async_output.display(), async_file_size as f64 / 1_000_000.0);
    println!();
    println!("═══════════════════════════════════════════════════════════════════");
    
    Ok(())
}
