#[cfg(feature = "mzml")]
use std::path::Path;
#[cfg(feature = "mzml")]
use std::time::Instant;

#[cfg(all(feature = "mzml", feature = "parallel-decode"))]
use rayon::prelude::*;

#[cfg(all(feature = "mzml", not(feature = "parallel-decode")))]
use mzpeak::mzml::BinaryDecoder;
#[cfg(feature = "mzml")]
use mzpeak::mzml::{MzMLStreamer, RawBinaryData, RawMzMLSpectrum};
#[cfg(all(feature = "mzml", feature = "parallel-decode"))]
use mzpeak::mzml::simd::{decode_binary_array_simd, decode_binary_array_simd_f32};

#[cfg(feature = "mzml")]
fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let input = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: decode_only <input.mzML> [--batch-size N]"))?;
    let mut batch_size = 5000usize;
    while let Some(arg) = args.next() {
        if arg == "--batch-size" {
            let value = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--batch-size requires a value"))?
                .parse::<usize>()?;
            batch_size = value;
        }
    }

    let input_path = Path::new(&input);
    let file_size = std::fs::metadata(input_path)?.len();

    let mut streamer = if is_imzml_path(input_path) {
        MzMLStreamer::open_imzml(input_path)?
    } else {
        MzMLStreamer::open(input_path)?
    };

    let _metadata = streamer.read_metadata()?;

    let start = Instant::now();
    let mut raw_batch: Vec<RawMzMLSpectrum> = Vec::with_capacity(batch_size);
    let mut spectra = 0usize;
    let mut peaks = 0usize;

    while let Some(raw) = streamer.next_raw_spectrum()? {
        raw_batch.push(raw);
        if raw_batch.len() >= batch_size {
            let batch_peaks = decode_batch(&mut raw_batch)?;
            spectra += batch_peaks.0;
            peaks += batch_peaks.1;
        }
    }

    if !raw_batch.is_empty() {
        let batch_peaks = decode_batch(&mut raw_batch)?;
        spectra += batch_peaks.0;
        peaks += batch_peaks.1;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let mbps = file_size as f64 / 1_000_000.0 / elapsed;
    let mibps = file_size as f64 / (1024.0 * 1024.0) / elapsed;

    println!("decode_only");
    println!("  spectra: {}", spectra);
    println!("  peaks: {}", peaks);
    println!("  elapsed_seconds: {:.2}", elapsed);
    println!("  throughput_MBps: {:.2}", mbps);
    println!("  throughput_MiBps: {:.2}", mibps);

    Ok(())
}

#[cfg(not(feature = "mzml"))]
fn main() {
    eprintln!("This example requires the `mzml` feature.");
}

#[cfg(all(feature = "mzml", feature = "parallel-decode"))]
fn decode_batch(batch: &mut Vec<RawMzMLSpectrum>) -> anyhow::Result<(usize, usize)> {
    let decoded: Vec<usize> = batch
        .par_drain(..)
        .map(decode_raw_spectrum)
        .collect::<Result<Vec<_>, _>>()?;
    let spectra = decoded.len();
    let peaks = decoded.iter().sum();
    Ok((spectra, peaks))
}

#[cfg(all(feature = "mzml", not(feature = "parallel-decode")))]
fn decode_batch(batch: &mut Vec<RawMzMLSpectrum>) -> anyhow::Result<(usize, usize)> {
    let decoded: Vec<usize> = batch
        .drain(..)
        .map(decode_raw_spectrum)
        .collect::<Result<Vec<_>, _>>()?;
    let spectra = decoded.len();
    let peaks = decoded.iter().sum();
    Ok((spectra, peaks))
}

#[cfg(feature = "mzml")]
fn decode_raw_spectrum(raw: RawMzMLSpectrum) -> anyhow::Result<usize> {
    let mz = decode_f64(&raw.mz_data, raw.default_array_length)?;
    let _intensity = decode_f32(&raw.intensity_data, raw.default_array_length)?;

    if let Some(im_data) = raw.ion_mobility_data {
        let _ion_mobility = decode_f64(&im_data, raw.default_array_length)?;
    }

    Ok(mz.len())
}

#[cfg(feature = "mzml")]
fn decode_f64(data: &RawBinaryData, expected_len: usize) -> anyhow::Result<Vec<f64>> {
    if data.base64.trim().is_empty() {
        return Ok(Vec::new());
    }

    #[cfg(feature = "parallel-decode")]
    {
        Ok(decode_binary_array_simd(
            &data.base64,
            data.encoding,
            data.compression,
            Some(expected_len),
        )?)
    }

    #[cfg(not(feature = "parallel-decode"))]
    {
        Ok(BinaryDecoder::decode(
            &data.base64,
            data.encoding,
            data.compression,
            Some(expected_len),
        )?)
    }
}

#[cfg(feature = "mzml")]
fn decode_f32(data: &RawBinaryData, expected_len: usize) -> anyhow::Result<Vec<f32>> {
    if data.base64.trim().is_empty() {
        return Ok(Vec::new());
    }

    #[cfg(feature = "parallel-decode")]
    {
        Ok(decode_binary_array_simd_f32(
            &data.base64,
            data.encoding,
            data.compression,
            Some(expected_len),
        )?)
    }

    #[cfg(not(feature = "parallel-decode"))]
    {
        Ok(BinaryDecoder::decode_f32(
            &data.base64,
            data.encoding,
            data.compression,
            Some(expected_len),
        )?)
    }
}

#[cfg(feature = "mzml")]
fn is_imzml_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("imzml"))
        .unwrap_or(false)
}
