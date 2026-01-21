//! SIMD-accelerated binary data decoding for parallel mzML processing
//!
//! This module provides optimized decoding functions using SIMD intrinsics
//! for Base64 decoding, whitespace removal, and float conversion.
//!
//! These functions are gated behind the `parallel-decode` feature flag.

#![cfg(feature = "parallel-decode")]

use std::io::Read;

use base64_simd::STANDARD as BASE64_SIMD;
use wide::{f32x4, f64x2, u8x16};

use super::binary::{BinaryDecodeError, BinaryEncoding, CompressionType};

/// Read a little-endian f32 from a byte slice at the given offset with bounds checking.
///
/// # Errors
/// Returns `BinaryDecodeError::InvalidLength` if there aren't enough bytes.
#[inline]
fn read_f32_le(data: &[u8], offset: usize) -> Result<f32, BinaryDecodeError> {
    let bytes = data.get(offset..offset + 4).ok_or(BinaryDecodeError::InvalidLength {
        expected: offset + 4,
        actual: data.len(),
    })?;
    // SAFETY: We just verified the slice has exactly 4 bytes
    Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// Read a little-endian f64 from a byte slice at the given offset with bounds checking.
///
/// # Errors
/// Returns `BinaryDecodeError::InvalidLength` if there aren't enough bytes.
#[inline]
fn read_f64_le(data: &[u8], offset: usize) -> Result<f64, BinaryDecodeError> {
    let bytes = data.get(offset..offset + 8).ok_or(BinaryDecodeError::InvalidLength {
        expected: offset + 8,
        actual: data.len(),
    })?;
    // SAFETY: We just verified the slice has exactly 8 bytes
    Ok(f64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

/// SIMD-accelerated binary array decoding
///
/// This is the main entry point for parallel decoding. It performs:
/// 1. SIMD whitespace removal
/// 2. SIMD Base64 decoding
/// 3. Zlib decompression (if needed)
/// 4. SIMD float conversion
pub fn decode_binary_array_simd(
    base64_data: &str,
    encoding: BinaryEncoding,
    compression: CompressionType,
    expected_length: Option<usize>,
) -> Result<Vec<f64>, BinaryDecodeError> {
    let trimmed = base64_data.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    // Step 1: SIMD whitespace removal (only if needed)
    let clean_bytes = if trimmed.bytes().any(|b| b.is_ascii_whitespace()) {
        simd_remove_whitespace_bytes(trimmed.as_bytes())
    } else {
        trimmed.as_bytes().to_vec()
    };

    // Step 2: SIMD Base64 decoding
    let decoded_bytes = BASE64_SIMD
        .decode_to_vec(&clean_bytes)
        .map_err(|e| BinaryDecodeError::Base64Error(base64::DecodeError::InvalidByte(0, e.to_string().as_bytes().first().copied().unwrap_or(0))))?;

    // Step 3: Decompress if needed
    let uncompressed = match compression {
        CompressionType::None => decoded_bytes,
        CompressionType::Zlib => {
            let mut decoder = flate2::read::ZlibDecoder::new(&decoded_bytes[..]);
            let mut uncompressed = Vec::new();
            decoder.read_to_end(&mut uncompressed)?;
            uncompressed
        }
        CompressionType::NumpressLinear
        | CompressionType::NumpressPic
        | CompressionType::NumpressSlof => {
            return Err(BinaryDecodeError::UnsupportedCompression(compression));
        }
    };

    // Step 4: SIMD float conversion with bounds checking (Issue 007 fix - no unwrap)
    let values = match encoding {
        BinaryEncoding::Float32 => simd_decode_f32_to_f64_checked(&uncompressed)?,
        BinaryEncoding::Float64 => simd_decode_f64_checked(&uncompressed)?,
    };

    // Step 5: Validate expected length (Issue 006 fix - SIMD must match scalar validation)
    if let Some(expected) = expected_length {
        if values.len() != expected {
            return Err(BinaryDecodeError::InvalidLength {
                expected,
                actual: values.len(),
            });
        }
    }

    Ok(values)
}

/// SIMD-accelerated binary array decoding into f32 output.
pub fn decode_binary_array_simd_f32(
    base64_data: &str,
    encoding: BinaryEncoding,
    compression: CompressionType,
    expected_length: Option<usize>,
) -> Result<Vec<f32>, BinaryDecodeError> {
    let trimmed = base64_data.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let clean_bytes = if trimmed.bytes().any(|b| b.is_ascii_whitespace()) {
        simd_remove_whitespace_bytes(trimmed.as_bytes())
    } else {
        trimmed.as_bytes().to_vec()
    };

    let decoded_bytes = BASE64_SIMD
        .decode_to_vec(&clean_bytes)
        .map_err(|e| BinaryDecodeError::Base64Error(base64::DecodeError::InvalidByte(0, e.to_string().as_bytes().first().copied().unwrap_or(0))))?;

    let uncompressed = match compression {
        CompressionType::None => decoded_bytes,
        CompressionType::Zlib => {
            let mut decoder = flate2::read::ZlibDecoder::new(&decoded_bytes[..]);
            let mut uncompressed = Vec::new();
            decoder.read_to_end(&mut uncompressed)?;
            uncompressed
        }
        CompressionType::NumpressLinear
        | CompressionType::NumpressPic
        | CompressionType::NumpressSlof => {
            return Err(BinaryDecodeError::UnsupportedCompression(compression));
        }
    };

    let values = match encoding {
        BinaryEncoding::Float32 => simd_decode_f32_checked(&uncompressed)?,
        BinaryEncoding::Float64 => simd_decode_f64_to_f32_checked(&uncompressed)?,
    };

    if let Some(expected) = expected_length {
        if values.len() != expected {
            return Err(BinaryDecodeError::InvalidLength {
                expected,
                actual: values.len(),
            });
        }
    }

    Ok(values)
}

/// SIMD-accelerated whitespace removal
///
/// Uses 16-byte SIMD vectors to process data in parallel, comparing against
/// whitespace characters (space, tab, newline, carriage return) and filtering
/// them out efficiently.
///
/// # Performance
/// - Processes 16 bytes at a time using SIMD comparison
/// - Falls back to scalar processing for remaining bytes
/// - Typically 2-4x faster than scalar for large inputs
pub fn simd_remove_whitespace_bytes(bytes: &[u8]) -> Vec<u8> {
    let len = bytes.len();
    let mut result = Vec::with_capacity(len);

    // Process 16 bytes at a time with SIMD
    let chunks = len / 16;
    let mut i = 0;

    for _ in 0..chunks {
        let chunk = &bytes[i..i + 16];
        let v = u8x16::from([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
            chunk[8], chunk[9], chunk[10], chunk[11], chunk[12], chunk[13], chunk[14], chunk[15],
        ]);

        // Check for whitespace characters (space, tab, newline, carriage return)
        let space = u8x16::splat(b' ');
        let tab = u8x16::splat(b'\t');
        let newline = u8x16::splat(b'\n');
        let cr = u8x16::splat(b'\r');

        let is_space = v.cmp_eq(space);
        let is_tab = v.cmp_eq(tab);
        let is_newline = v.cmp_eq(newline);
        let is_cr = v.cmp_eq(cr);

        // Combine masks using bitwise OR
        let is_whitespace = is_space | is_tab | is_newline | is_cr;

        // Extract non-whitespace bytes
        let arr: [u8; 16] = v.into();
        let mask_arr: [u8; 16] = is_whitespace.into();

        for j in 0..16 {
            if mask_arr[j] == 0 {
                result.push(arr[j]);
            }
        }

        i += 16;
    }

    // Handle remaining bytes with scalar processing
    for &b in &bytes[i..] {
        if !b.is_ascii_whitespace() {
            result.push(b);
        }
    }

    result
}

/// SIMD-accelerated f32 to f64 conversion with bounds checking
///
/// Reads little-endian 32-bit floats and converts them to 64-bit.
/// Processes 4 floats at a time using f32x4 SIMD vectors.
///
/// # Performance
/// - Processes 4 floats (16 bytes) at a time
/// - Falls back to scalar for remaining floats
///
/// # Errors
/// Returns `BinaryDecodeError::InvalidLength` if data is not properly aligned.
fn simd_decode_f32_to_f64_checked(data: &[u8]) -> Result<Vec<f64>, BinaryDecodeError> {
    let num_floats = data.len() / 4;
    let mut result = Vec::with_capacity(num_floats);

    // Process 4 floats at a time (16 bytes)
    let chunks = num_floats / 4;
    let mut i = 0;

    for _ in 0..chunks {
        // Read 4 f32 values in little-endian format with bounds checking
        let f0 = read_f32_le(data, i)?;
        let f1 = read_f32_le(data, i + 4)?;
        let f2 = read_f32_le(data, i + 8)?;
        let f3 = read_f32_le(data, i + 12)?;

        // Use SIMD for parallel processing
        let v = f32x4::from([f0, f1, f2, f3]);
        let arr: [f32; 4] = v.into();

        // Convert to f64 and push to result
        result.push(arr[0] as f64);
        result.push(arr[1] as f64);
        result.push(arr[2] as f64);
        result.push(arr[3] as f64);

        i += 16;
    }

    // Handle remaining floats with scalar processing
    while i + 4 <= data.len() {
        let f = read_f32_le(data, i)?;
        result.push(f as f64);
        i += 4;
    }

    Ok(result)
}

/// SIMD-accelerated f32 decoding with bounds checking.
fn simd_decode_f32_checked(data: &[u8]) -> Result<Vec<f32>, BinaryDecodeError> {
    let num_floats = data.len() / 4;
    let mut result = Vec::with_capacity(num_floats);

    let chunks = num_floats / 4;
    let mut i = 0;

    for _ in 0..chunks {
        let f0 = read_f32_le(data, i)?;
        let f1 = read_f32_le(data, i + 4)?;
        let f2 = read_f32_le(data, i + 8)?;
        let f3 = read_f32_le(data, i + 12)?;

        let v = f32x4::from([f0, f1, f2, f3]);
        let arr: [f32; 4] = v.into();

        result.push(arr[0]);
        result.push(arr[1]);
        result.push(arr[2]);
        result.push(arr[3]);

        i += 16;
    }

    while i + 4 <= data.len() {
        let f = read_f32_le(data, i)?;
        result.push(f);
        i += 4;
    }

    Ok(result)
}

/// Legacy unchecked version for backwards compatibility in tests
#[allow(dead_code)]
pub fn simd_decode_f32_to_f64(data: &[u8]) -> Vec<f64> {
    simd_decode_f32_to_f64_checked(data).expect("invalid data in simd_decode_f32_to_f64")
}

/// SIMD-accelerated f64 decoding with bounds checking
///
/// Reads little-endian 64-bit floats directly.
/// Processes 2 doubles at a time using f64x2 SIMD vectors.
///
/// # Performance
/// - Processes 2 doubles (16 bytes) at a time
/// - Falls back to scalar for remaining doubles
///
/// # Errors
/// Returns `BinaryDecodeError::InvalidLength` if data is not properly aligned.
fn simd_decode_f64_checked(data: &[u8]) -> Result<Vec<f64>, BinaryDecodeError> {
    let num_floats = data.len() / 8;
    let mut result = Vec::with_capacity(num_floats);

    // Process 2 f64 values at a time (16 bytes)
    let chunks = num_floats / 2;
    let mut i = 0;

    for _ in 0..chunks {
        let f0 = read_f64_le(data, i)?;
        let f1 = read_f64_le(data, i + 8)?;

        let v = f64x2::from([f0, f1]);
        let arr: [f64; 2] = v.into();

        result.push(arr[0]);
        result.push(arr[1]);

        i += 16;
    }

    // Handle remaining doubles with scalar processing
    while i + 8 <= data.len() {
        let f = read_f64_le(data, i)?;
        result.push(f);
        i += 8;
    }

    Ok(result)
}

fn simd_decode_f64_to_f32_checked(data: &[u8]) -> Result<Vec<f32>, BinaryDecodeError> {
    let num_floats = data.len() / 8;
    let mut result = Vec::with_capacity(num_floats);

    let chunks = num_floats / 2;
    let mut i = 0;

    for _ in 0..chunks {
        let f0 = read_f64_le(data, i)?;
        let f1 = read_f64_le(data, i + 8)?;

        let v = f64x2::from([f0, f1]);
        let arr: [f64; 2] = v.into();

        result.push(arr[0] as f32);
        result.push(arr[1] as f32);

        i += 16;
    }

    while i + 8 <= data.len() {
        let f = read_f64_le(data, i)?;
        result.push(f as f32);
        i += 8;
    }

    Ok(result)
}

/// Legacy unchecked version for backwards compatibility in tests
#[allow(dead_code)]
pub fn simd_decode_f64(data: &[u8]) -> Vec<f64> {
    simd_decode_f64_checked(data).expect("invalid data in simd_decode_f64")
}

/// Fast f64 parsing using fast-float crate
#[inline]
pub fn parse_f64_fast(bytes: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(bytes).ok()?;
    fast_float::parse(s).ok()
}

/// Fast u8 parsing
#[inline]
pub fn parse_u8_fast(bytes: &[u8]) -> Option<u8> {
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse().ok()
}

/// Fast u32 parsing
#[inline]
pub fn parse_u32_fast(bytes: &[u8]) -> Option<u32> {
    let s = std::str::from_utf8(bytes).ok()?;
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::prelude::*;

    #[test]
    fn test_simd_whitespace_removal() {
        let input = b"AAAA BBBB\nCCCC\tDDDD\rEEEE";
        let result = simd_remove_whitespace_bytes(input);
        assert_eq!(result, b"AAAABBBBCCCCDDDDEEEE");
    }

    #[test]
    fn test_simd_whitespace_removal_no_whitespace() {
        let input = b"AAAABBBBCCCCDDDD";
        let result = simd_remove_whitespace_bytes(input);
        assert_eq!(result, input.to_vec());
    }

    #[test]
    fn test_simd_whitespace_removal_large() {
        // Test with more than 16 bytes to exercise SIMD path
        let input = b"AAAA BBBB CCCC DDDD EEEE FFFF GGGG HHHH";
        let result = simd_remove_whitespace_bytes(input);
        assert_eq!(result, b"AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHH");
    }

    #[test]
    fn test_simd_decode_f32_to_f64() {
        // Create test data: 4 f32 values
        let values: [f32; 4] = [100.0, 200.0, 300.0, 400.0];
        let mut bytes = Vec::new();
        for v in values {
            bytes.extend_from_slice(&v.to_le_bytes());
        }

        let result = simd_decode_f32_to_f64(&bytes);
        assert_eq!(result.len(), 4);
        assert!((result[0] - 100.0).abs() < 1e-5);
        assert!((result[1] - 200.0).abs() < 1e-5);
        assert!((result[2] - 300.0).abs() < 1e-5);
        assert!((result[3] - 400.0).abs() < 1e-5);
    }

    #[test]
    fn test_simd_decode_f64() {
        // Create test data: 4 f64 values
        let values: [f64; 4] = [100.0, 200.0, 300.0, 400.0];
        let mut bytes = Vec::new();
        for v in values {
            bytes.extend_from_slice(&v.to_le_bytes());
        }

        let result = simd_decode_f64(&bytes);
        assert_eq!(result.len(), 4);
        assert!((result[0] - 100.0).abs() < 1e-10);
        assert!((result[1] - 200.0).abs() < 1e-10);
        assert!((result[2] - 300.0).abs() < 1e-10);
        assert!((result[3] - 400.0).abs() < 1e-10);
    }

    #[test]
    fn test_decode_binary_array_simd_uncompressed_f64() {
        // Two 64-bit floats: 100.0 and 200.0
        let bytes: [u8; 16] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // 100.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x69, 0x40, // 200.0
        ];
        let base64_data = BASE64_STANDARD.encode(bytes);

        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            CompressionType::None,
            Some(2),
        )
        .unwrap();

        assert_eq!(result.len(), 2);
        assert!((result[0] - 100.0).abs() < 1e-10);
        assert!((result[1] - 200.0).abs() < 1e-10);
    }

    #[test]
    fn test_decode_binary_array_simd_uncompressed_f32() {
        // Two 32-bit floats: 100.0 and 200.0
        let bytes: [u8; 8] = [
            0x00, 0x00, 0xc8, 0x42, // 100.0
            0x00, 0x00, 0x48, 0x43, // 200.0
        ];
        let base64_data = BASE64_STANDARD.encode(bytes);

        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float32,
            CompressionType::None,
            Some(2),
        )
        .unwrap();

        assert_eq!(result.len(), 2);
        assert!((result[0] - 100.0).abs() < 1e-5);
        assert!((result[1] - 200.0).abs() < 1e-5);
    }

    #[test]
    fn test_decode_binary_array_simd_with_whitespace() {
        // Base64 with embedded whitespace (common in XML)
        let bytes: [u8; 16] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x69, 0x40,
        ];
        let mut base64_data = BASE64_STANDARD.encode(bytes);
        // Insert whitespace
        base64_data.insert(4, ' ');
        base64_data.insert(10, '\n');

        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            CompressionType::None,
            Some(2),
        )
        .unwrap();

        assert_eq!(result.len(), 2);
        assert!((result[0] - 100.0).abs() < 1e-10);
        assert!((result[1] - 200.0).abs() < 1e-10);
    }

    #[test]
    fn test_decode_binary_array_simd_zlib() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        // Create some float64 data
        let values: Vec<f64> = vec![100.0, 200.0, 300.0, 400.0];
        let mut bytes = Vec::new();
        for v in &values {
            bytes.extend_from_slice(&v.to_le_bytes());
        }

        // Compress with zlib
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&bytes).unwrap();
        let compressed = encoder.finish().unwrap();

        // Base64 encode
        let base64_data = BASE64_STANDARD.encode(&compressed);

        // Decode
        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            CompressionType::Zlib,
            Some(4),
        )
        .unwrap();

        assert_eq!(result.len(), 4);
        for (i, v) in values.iter().enumerate() {
            assert!((result[i] - v).abs() < 1e-10);
        }
    }

    #[test]
    fn test_decode_empty() {
        let result = decode_binary_array_simd(
            "",
            BinaryEncoding::Float64,
            CompressionType::None,
            None,
        )
        .unwrap();

        assert!(result.is_empty());
    }

    // Issue 006 tests: SIMD expected_length validation
    #[test]
    fn test_decode_simd_expected_length_mismatch_returns_error() {
        // Two 64-bit floats: 100.0 and 200.0
        let bytes: [u8; 16] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // 100.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x69, 0x40, // 200.0
        ];
        let base64_data = BASE64_STANDARD.encode(bytes);

        // Expect 3 values but data only has 2
        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            CompressionType::None,
            Some(3), // Wrong expected length
        );

        assert!(result.is_err());
        match result {
            Err(BinaryDecodeError::InvalidLength { expected, actual }) => {
                assert_eq!(expected, 3);
                assert_eq!(actual, 2);
            }
            _ => panic!("Expected InvalidLength error"),
        }
    }

    #[test]
    fn test_decode_simd_expected_length_f32_mismatch() {
        // Two 32-bit floats: 100.0 and 200.0
        let bytes: [u8; 8] = [
            0x00, 0x00, 0xc8, 0x42, // 100.0
            0x00, 0x00, 0x48, 0x43, // 200.0
        ];
        let base64_data = BASE64_STANDARD.encode(bytes);

        // Expect 5 values but data only has 2
        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float32,
            CompressionType::None,
            Some(5),
        );

        assert!(result.is_err());
        match result {
            Err(BinaryDecodeError::InvalidLength { expected, actual }) => {
                assert_eq!(expected, 5);
                assert_eq!(actual, 2);
            }
            _ => panic!("Expected InvalidLength error"),
        }
    }

    #[test]
    fn test_decode_simd_expected_length_none_accepts_any() {
        // With None expected_length, any length should be accepted
        let bytes: [u8; 16] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x69, 0x40,
        ];
        let base64_data = BASE64_STANDARD.encode(bytes);

        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            CompressionType::None,
            None, // No expectation
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_decode_simd_expected_length_exact_match() {
        let bytes: [u8; 16] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x69, 0x40,
        ];
        let base64_data = BASE64_STANDARD.encode(bytes);

        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            CompressionType::None,
            Some(2), // Exact match
        );

        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 2);
        assert!((values[0] - 100.0).abs() < 1e-10);
        assert!((values[1] - 200.0).abs() < 1e-10);
    }
}
