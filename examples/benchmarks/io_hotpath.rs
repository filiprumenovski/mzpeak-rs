//! Reproducible decode and Parquet serialization measurements.
use base64::{engine::general_purpose::STANDARD, Engine};
use mzpeak::mzml::BinaryCompression;
use mzpeak::mzml::{BinaryDecoder, BinaryEncoding};
use mzpeak::writer::{CompressionType, PeakArraysV2, PeaksWriterV2, PeaksWriterV2Config};
use std::{
    hint::black_box,
    io::{Cursor, Read, Write},
    time::Instant,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let values: Vec<f64> = (0..65_536)
        .map(|i| 100.0 + f64::from(i) * 0.123456789)
        .collect();
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let mut compressed = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    compressed.write_all(&bytes)?;
    let compressed = compressed.finish()?;
    let mut results = Vec::new();
    for (name, encoded, compression) in [
        (
            "decode_plain",
            STANDARD.encode(&bytes),
            BinaryCompression::None,
        ),
        (
            "decode_zlib",
            STANDARD.encode(&compressed),
            BinaryCompression::Zlib,
        ),
    ] {
        for (backend, decode) in [
            (
                "reference",
                scalar_reference
                    as fn(
                        &str,
                        BinaryCompression,
                        usize,
                    ) -> Result<Vec<f64>, Box<dyn std::error::Error>>,
            ),
            (
                "native",
                native_decode
                    as fn(
                        &str,
                        BinaryCompression,
                        usize,
                    ) -> Result<Vec<f64>, Box<dyn std::error::Error>>,
            ),
        ] {
            let mut timings = Vec::new();
            for _ in 0..5 {
                let start = Instant::now();
                for _ in 0..100 {
                    let decoded = decode(black_box(&encoded), compression, values.len())?;
                    assert_eq!(decoded, values);
                    black_box(decoded);
                }
                timings.push(start.elapsed().as_secs_f64());
            }
            timings.sort_by(f64::total_cmp);
            results.push(serde_json::json!({"phase": name, "backend": backend, "median_seconds": timings[2], "values": values.len() * 100}));
        }
    }
    let peaks = PeakArraysV2::new(
        values[..4096].to_vec(),
        (0..4096).map(|i| (i * 17 % 10000) as f32).collect(),
    );
    for (name, compression) in [
        ("write_plain", CompressionType::Uncompressed),
        ("write_zstd3", CompressionType::Zstd(3)),
        ("write_zstd9", CompressionType::Zstd(9)),
    ] {
        let mut timings = Vec::new();
        let mut bytes = 0;
        for _ in 0..5 {
            let start = Instant::now();
            let config = PeaksWriterV2Config {
                compression,
                row_group_size: 65_536,
                ..Default::default()
            };
            let mut writer = PeaksWriterV2::new(Cursor::new(Vec::new()), &config, false)?;
            for id in 0..256 {
                writer.write_peaks(id, black_box(&peaks))?;
            }
            let output = writer.finish_into_inner()?.into_inner();
            bytes = output.len();
            black_box(output);
            timings.push(start.elapsed().as_secs_f64());
        }
        timings.sort_by(f64::total_cmp);
        results.push(serde_json::json!({"phase": name, "median_seconds": timings[2], "peaks": 256 * 4096, "output_bytes": bytes}));
    }
    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}

fn native_decode(
    input: &str,
    compression: BinaryCompression,
    count: usize,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    Ok(BinaryDecoder::decode(
        input,
        BinaryEncoding::Float64,
        compression,
        Some(count),
    )?)
}

// Measurement reference: the previous scalar Base64 -> inflate -> float-copy path.
fn scalar_reference(
    input: &str,
    compression: BinaryCompression,
    count: usize,
) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
    let bytes = STANDARD.decode(input)?;
    let bytes = if compression == BinaryCompression::Zlib {
        let mut output = Vec::new();
        flate2::read::ZlibDecoder::new(bytes.as_slice()).read_to_end(&mut output)?;
        output
    } else {
        bytes
    };
    let values: Vec<f64> = bytes
        .chunks_exact(8)
        .map(|v| f64::from_le_bytes(v.try_into().expect("exact chunk")))
        .collect();
    assert_eq!(values.len(), count);
    Ok(values)
}
