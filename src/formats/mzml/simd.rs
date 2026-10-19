//! Historical entry points, delegated to the shared checked decoder.
use super::binary::{BinaryDecodeError, BinaryDecoder, BinaryEncoding, CompressionType};

/// Decodes a checked numeric array into f64 values.
pub fn decode_binary_array_simd(
    input: &str,
    encoding: BinaryEncoding,
    compression: CompressionType,
    expected: Option<usize>,
) -> Result<Vec<f64>, BinaryDecodeError> {
    BinaryDecoder::decode(input, encoding, compression, expected)
}

/// Decodes a checked numeric array directly into f32 values.
pub fn decode_binary_array_simd_f32(
    input: &str,
    encoding: BinaryEncoding,
    compression: CompressionType,
    expected: Option<usize>,
) -> Result<Vec<f32>, BinaryDecodeError> {
    BinaryDecoder::decode_f32(input, encoding, compression, expected)
}

/// Removes ASCII whitespace from a Base64 payload.
pub fn simd_remove_whitespace_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .copied()
        .filter(|b| !b.is_ascii_whitespace())
        .collect()
}

/// Compatibility conversion; malformed byte lengths return an empty array.
pub fn simd_decode_f32_to_f64(bytes: &[u8]) -> Vec<f64> {
    BinaryDecoder::decode_bytes(bytes, BinaryEncoding::Float32, CompressionType::None, None)
        .unwrap_or_default()
}

/// Compatibility conversion; malformed byte lengths return an empty array.
pub fn simd_decode_f64(bytes: &[u8]) -> Vec<f64> {
    BinaryDecoder::decode_bytes(bytes, BinaryEncoding::Float64, CompressionType::None, None)
        .unwrap_or_default()
}

/// Parses an ASCII floating point value.
pub fn parse_f64_fast(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}
/// Parses an ASCII unsigned byte.
pub fn parse_u8_fast(bytes: &[u8]) -> Option<u8> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}
/// Parses an ASCII unsigned integer.
pub fn parse_u32_fast(bytes: &[u8]) -> Option<u32> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x69, 0x40,
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
        let result =
            decode_binary_array_simd("", BinaryEncoding::Float64, CompressionType::None, None)
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x69, 0x40,
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x69, 0x40,
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
