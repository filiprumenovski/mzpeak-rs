use std::env;
use std::time::Instant;

use mzpeak::reader::MzPeakReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let path = match args.next() {
        Some(path) => path,
        None => {
            eprintln!("usage: lookup_speed <mzpeak_path> [spectrum_id] [iterations]");
            std::process::exit(1);
        }
    };

    let spectrum_id: i64 = args
        .next()
        .unwrap_or_else(|| "0".to_string())
        .parse()?;
    let iterations: usize = args
        .next()
        .unwrap_or_else(|| "1".to_string())
        .parse()?;

    let start = Instant::now();
    let reader = MzPeakReader::open(&path)?;
    let open_time = start.elapsed();
    println!("File: {}", path);
    println!("Open time: {:?}", open_time);
    println!("Target spectrum_id: {}", spectrum_id);
    println!("Iterations: {}", iterations);

    let start = Instant::now();
    let mut hits = 0usize;
    for _ in 0..iterations {
        let spectrum = reader.get_spectrum_arrays(spectrum_id)?;
        if spectrum.is_some() {
            hits += 1;
        }
    }
    let elapsed = start.elapsed();
    let avg_secs = elapsed.as_secs_f64() / iterations.max(1) as f64;
    println!("get_spectrum_arrays: {:?} total ({:.4}s avg), hits={}", elapsed, avg_secs, hits);

    let start = Instant::now();
    let mut streaming_hit = None;
    let iter = reader.iter_spectra_arrays_streaming()?;
    for spectrum in iter {
        let spectrum = spectrum?;
        if spectrum.spectrum_id == spectrum_id {
            streaming_hit = Some(spectrum);
            break;
        }
    }
    let elapsed = start.elapsed();
    println!(
        "streaming search: {:?} ({})",
        elapsed,
        if streaming_hit.is_some() { "hit" } else { "miss" }
    );

    Ok(())
}
