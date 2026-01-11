//! Binary data decoding for mzML
//!
//! mzML stores numerical arrays (m/z, intensity) as Base64-encoded binary data,
//! optionally compressed with zlib. This module handles the decoding pipeline:
//!
//! 1. Base64 decode the text
//! 2. Decompress if needed (zlib)
//! 3. Interpret bytes as float32 or float64 (little-endian)

use std::io::Read;

use base64::prelude::*;
use byteorder::{LittleEndian, ReadBytesExt};
use flate2::read::ZlibDecoder;

/// Compression types used in mzML binary data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionType {
    /// No compression (raw binary)
    #[default]
    None,
    /// zlib compression (most common)
    Zlib,
    /// MS-Numpress linear prediction
    NumpressLinear,
    /// MS-Numpress positive integer compression
    NumpressPic,
    /// MS-Numpress short logged float compression
    NumpressSlof,
}

impl CompressionType {
    /// Determine compression type from CV accession
    pub fn from_cv_accession(accession: &str) -> Option<Self> {
        match accession {
            "MS:1000574" => Some(CompressionType::Zlib),
            "MS:1000576" => Some(CompressionType::None),
            "MS:1002312" => Some(CompressionType::NumpressLinear),
            "MS:1002313" => Some(CompressionType::NumpressPic),
            "MS:1002314" => Some(CompressionType::NumpressSlof),
            _ => None,
        }
    }
}

/// Binary encoding precision
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BinaryEncoding {
    /// 32-bit floating point (CV: MS:1000521)
    Float32,
    /// 64-bit floating point (CV: MS:1000523)
    #[default]
    Float64,
}

impl BinaryEncoding {
    /// Determine encoding from CV accession
    pub fn from_cv_accession(accession: &str) -> Option<Self> {
        match accession {
            "MS:1000521" => Some(BinaryEncoding::Float32),
            "MS:1000523" => Some(BinaryEncoding::Float64),
            _ => None,
        }
    }

    /// Get the byte size per value
    pub fn byte_size(&self) -> usize {
        match self {
            BinaryEncoding::Float32 => 4,
            BinaryEncoding::Float64 => 8,
        }
    }
}

/// Errors that can occur during binary decoding
#[derive(Debug, thiserror::Error)]
pub enum BinaryDecodeError {
    #[error("Base64 decode error: {0}")]
    Base64Error(#[from] base64::DecodeError),

    #[error("Decompression error: {0}")]
    DecompressionError(#[from] std::io::Error),

    #[error("Invalid data length: expected {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },

    #[error("Unsupported compression: {0:?}")]
    UnsupportedCompression(CompressionType),
}

/// Decoder for mzML binary data arrays
pub struct BinaryDecoder;

impl BinaryDecoder {
    /// Decode a Base64-encoded binary array from mzML
    ///
    /// # Arguments
    /// * `base64_data` - The Base64-encoded string from the `<binary>` element
    /// * `encoding` - The numerical precision (32 or 64 bit)
    /// * `compression` - The compression type (none, zlib, etc.)
    /// * `expected_length` - Expected number of values (from defaultArrayLength)
    ///
    /// # Returns
    /// A `Vec<f64>` containing the decoded values
    pub fn decode(
        base64_data: &str,
        encoding: BinaryEncoding,
        compression: CompressionType,
        expected_length: Option<usize>,
    ) -> Result<Vec<f64>, BinaryDecodeError> {
        // Skip if empty
        let trimmed = base64_data.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        // Step 1: Base64 decode
        let decoded_bytes = BASE64_STANDARD.decode(trimmed)?;

        // Step 2: Decompress if needed
        let uncompressed = match compression {
            CompressionType::None => decoded_bytes,
            CompressionType::Zlib => {
                let mut decoder = ZlibDecoder::new(&decoded_bytes[..]);
                let mut uncompressed = Vec::new();
                decoder.read_to_end(&mut uncompressed)?;
                uncompressed
            }
            CompressionType::NumpressLinear
            | CompressionType::NumpressPic
            | CompressionType::NumpressSlof => {
                // MS-Numpress is not commonly used; implement if needed
                return Err(BinaryDecodeError::UnsupportedCompression(compression));
            }
        };

        // Step 3: Convert bytes to floats
        let values = Self::bytes_to_floats(&uncompressed, encoding)?;

        // Validate length if expected
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

    /// Convert raw bytes to f64 values based on encoding
    fn bytes_to_floats(
        bytes: &[u8],
        encoding: BinaryEncoding,
    ) -> Result<Vec<f64>, BinaryDecodeError> {
        let byte_size = encoding.byte_size();

        if bytes.len() % byte_size != 0 {
            return Err(BinaryDecodeError::InvalidLength {
                expected: bytes.len() / byte_size * byte_size,
                actual: bytes.len(),
            });
        }

        let count = bytes.len() / byte_size;
        let mut values = Vec::with_capacity(count);
        let mut cursor = std::io::Cursor::new(bytes);

        match encoding {
            BinaryEncoding::Float32 => {
                for _ in 0..count {
                    let val = cursor.read_f32::<LittleEndian>()?;
                    values.push(val as f64);
                }
            }
            BinaryEncoding::Float64 => {
                for _ in 0..count {
                    let val = cursor.read_f64::<LittleEndian>()?;
                    values.push(val);
                }
            }
        }

        Ok(values)
    }

    /// Decode with automatic detection of encoding from CV params
    ///
    /// This is a convenience method that extracts encoding info from CV params
    pub fn decode_with_cv_params(
        base64_data: &str,
        cv_params: &[(String, Option<String>)],
        expected_length: Option<usize>,
    ) -> Result<(Vec<f64>, BinaryEncoding), BinaryDecodeError> {
        let mut encoding = BinaryEncoding::Float64;
        let mut compression = CompressionType::None;

        for (accession, _value) in cv_params {
            if let Some(enc) = BinaryEncoding::from_cv_accession(accession) {
                encoding = enc;
            }
            if let Some(comp) = CompressionType::from_cv_accession(accession) {
                compression = comp;
            }
        }

        let values = Self::decode(base64_data, encoding, compression, expected_length)?;
        Ok((values, encoding))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_float64_uncompressed() {
        // Example: two 64-bit floats: 100.0 and 200.0
        // In little-endian:
        // 100.0 = 0x4059000000000000
        // 200.0 = 0x4069000000000000
        let bytes: [u8; 16] = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, // 100.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x69, 0x40, // 200.0
        ];
        let base64_data = BASE64_STANDARD.encode(bytes);

        let result = BinaryDecoder::decode(
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
    fn test_decode_float32_uncompressed() {
        // Two 32-bit floats: 100.0 and 200.0
        let bytes: [u8; 8] = [
            0x00, 0x00, 0xc8, 0x42, // 100.0
            0x00, 0x00, 0x48, 0x43, // 200.0
        ];
        let base64_data = BASE64_STANDARD.encode(bytes);

        let result = BinaryDecoder::decode(
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
    fn test_decode_empty() {
        let result = BinaryDecoder::decode(
            "",
            BinaryEncoding::Float64,
            CompressionType::None,
            None,
        )
        .unwrap();

        assert!(result.is_empty());
    }

    #[test]
    fn test_decode_zlib_compressed() {
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
        let result = BinaryDecoder::decode(
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
}
