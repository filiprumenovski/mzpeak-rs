//! Binary data decoding for mzML
//!
//! mzML stores numerical arrays (m/z, intensity) as Base64-encoded binary data,
//! optionally compressed with zlib. This module handles the decoding pipeline:
//!
//! 1. Base64 decode the text
//! 2. Decompress if needed (zlib)
//! 3. Interpret bytes as float32 or float64 (little-endian)

use std::io::Read;

#[cfg(test)]
use base64::prelude::*;

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

/// Decoder for checked mzML numeric arrays.
pub struct BinaryDecoder;

thread_local! {
    static INFLATER: std::cell::RefCell<libdeflater::Decompressor> =
        std::cell::RefCell::new(libdeflater::Decompressor::new());
}

impl BinaryDecoder {
    /// Decodes standard Base64, optional zlib, and little-endian f32/f64 values.
    pub fn decode(
        base64_data: &str,
        encoding: BinaryEncoding,
        compression: CompressionType,
        expected_length: Option<usize>,
    ) -> Result<Vec<f64>, BinaryDecodeError> {
        let bytes = decode_base64(base64_data)?;
        Self::decode_bytes(&bytes, encoding, compression, expected_length)
    }

    /// Decodes directly into f32 output without an intermediate f64 column.
    pub fn decode_f32(
        base64_data: &str,
        encoding: BinaryEncoding,
        compression: CompressionType,
        expected_length: Option<usize>,
    ) -> Result<Vec<f32>, BinaryDecodeError> {
        let bytes = decode_base64(base64_data)?;
        let bytes = inflate(&bytes, encoding, compression, expected_length)?;
        validate_length(&bytes, encoding, expected_length)?;
        Ok(match encoding {
            BinaryEncoding::Float32 => bytes
                .chunks_exact(4)
                .map(|v| f32::from_le_bytes([v[0], v[1], v[2], v[3]]))
                .collect(),
            BinaryEncoding::Float64 => bytes
                .chunks_exact(8)
                .map(|v| {
                    f64::from_le_bytes([v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]]) as f32
                })
                .collect(),
        })
    }

    /// Decodes an external imzML array, borrowing uncompressed input bytes.
    pub fn decode_bytes(
        bytes: &[u8],
        encoding: BinaryEncoding,
        compression: CompressionType,
        expected_length: Option<usize>,
    ) -> Result<Vec<f64>, BinaryDecodeError> {
        let bytes = inflate(bytes, encoding, compression, expected_length)?;
        validate_length(&bytes, encoding, expected_length)?;
        Ok(match encoding {
            BinaryEncoding::Float32 => bytes
                .chunks_exact(4)
                .map(|v| f64::from(f32::from_le_bytes([v[0], v[1], v[2], v[3]])))
                .collect(),
            BinaryEncoding::Float64 => bytes
                .chunks_exact(8)
                .map(|v| f64::from_le_bytes([v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]]))
                .collect(),
        })
    }

    /// Resolves precision and compression from PSI-MS accessions, then decodes.
    pub fn decode_with_cv_params(
        base64_data: &str,
        cv_params: &[(String, Option<String>)],
        expected_length: Option<usize>,
    ) -> Result<(Vec<f64>, BinaryEncoding), BinaryDecodeError> {
        let mut encoding = BinaryEncoding::Float64;
        let mut compression = CompressionType::None;
        for (accession, _) in cv_params {
            if let Some(value) = BinaryEncoding::from_cv_accession(accession) {
                encoding = value;
            }
            if let Some(value) = CompressionType::from_cv_accession(accession) {
                compression = value;
            }
        }
        Ok((
            Self::decode(base64_data, encoding, compression, expected_length)?,
            encoding,
        ))
    }
}

fn decode_base64(input: &str) -> Result<Vec<u8>, BinaryDecodeError> {
    use base64::Engine;
    let input = input.trim().as_bytes();
    match base64::engine::general_purpose::STANDARD.decode(input) {
        Ok(bytes) => Ok(bytes),
        Err(error) => {
            // Compact payloads avoid an extra full scan. Normalize XML
            // whitespace only on the cold decode-failure path.
            let cleaned: Vec<_> = input
                .iter()
                .copied()
                .filter(|b| !b.is_ascii_whitespace())
                .collect();
            if cleaned.len() == input.len() {
                return Err(error.into());
            }
            Ok(base64::engine::general_purpose::STANDARD.decode(cleaned)?)
        }
    }
}

fn validate_length(
    bytes: &[u8],
    encoding: BinaryEncoding,
    expected: Option<usize>,
) -> Result<(), BinaryDecodeError> {
    let width = encoding.byte_size();
    if bytes.len() % width != 0 {
        return Err(BinaryDecodeError::InvalidLength {
            expected: bytes.len() / width * width,
            actual: bytes.len(),
        });
    }
    if let Some(expected) = expected {
        if bytes.len() / width != expected {
            return Err(BinaryDecodeError::InvalidLength {
                expected,
                actual: bytes.len() / width,
            });
        }
    }
    Ok(())
}

// One decoded array is bounded independently of the number of scans in a run.
const MAX_ARRAY_BYTES: usize = 1 << 30;

fn inflate<'a>(
    bytes: &'a [u8],
    encoding: BinaryEncoding,
    compression: CompressionType,
    expected: Option<usize>,
) -> Result<std::borrow::Cow<'a, [u8]>, BinaryDecodeError> {
    use std::borrow::Cow;
    match compression {
        CompressionType::None => Ok(Cow::Borrowed(bytes)),
        CompressionType::Zlib => {
            if let Some(count) = expected {
                let len = count
                    .checked_mul(encoding.byte_size())
                    .filter(|n| *n <= MAX_ARRAY_BYTES)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "decoded array exceeds 1 GiB",
                        )
                    })?;
                let mut output = vec![0; len];
                let written = INFLATER
                    .with(|decoder| decoder.borrow_mut().zlib_decompress(bytes, &mut output))
                    .map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                    })?;
                output.truncate(written);
                Ok(Cow::Owned(output))
            } else {
                // Streams lacking a declared count retain the bounded streaming
                // decoder; the normal indexed-array path uses libdeflate.
                let mut output = Vec::new();
                flate2::read::ZlibDecoder::new(bytes)
                    .take(MAX_ARRAY_BYTES as u64 + 1)
                    .read_to_end(&mut output)?;
                if output.len() > MAX_ARRAY_BYTES {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "decoded array exceeds 1 GiB",
                    )
                    .into());
                }
                Ok(Cow::Owned(output))
            }
        }
        _ => Err(BinaryDecodeError::UnsupportedCompression(compression)),
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
        let result =
            BinaryDecoder::decode("", BinaryEncoding::Float64, CompressionType::None, None)
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

#[cfg(test)]
mod contract_tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine};

    #[test]
    fn declared_empty_and_nonempty_lengths_are_checked() {
        assert!(
            BinaryDecoder::decode("", BinaryEncoding::Float64, CompressionType::None, Some(1))
                .is_err()
        );
        assert!(
            BinaryDecoder::decode("", BinaryEncoding::Float64, CompressionType::None, Some(0))
                .unwrap()
                .is_empty()
        );
        assert!(BinaryDecoder::decode(
            &STANDARD.encode([1, 2, 3]),
            BinaryEncoding::Float32,
            CompressionType::None,
            None
        )
        .is_err());
    }

    #[test]
    fn compressed_arrays_reject_truncation_corruption_and_unbounded_counts() {
        use std::io::Write;
        let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&123.123456789_f64.to_le_bytes()).unwrap();
        let mut bytes = encoder.finish().unwrap();
        let encoded = STANDARD.encode(&bytes);
        assert_eq!(
            BinaryDecoder::decode(
                &encoded,
                BinaryEncoding::Float64,
                CompressionType::Zlib,
                Some(1)
            )
            .unwrap(),
            [123.123456789]
        );
        assert!(BinaryDecoder::decode(
            &encoded,
            BinaryEncoding::Float64,
            CompressionType::Zlib,
            Some(0)
        )
        .is_err());
        assert!(BinaryDecoder::decode(
            &encoded,
            BinaryEncoding::Float64,
            CompressionType::Zlib,
            Some(usize::MAX)
        )
        .is_err());
        bytes.pop();
        assert!(BinaryDecoder::decode(
            &STANDARD.encode(bytes),
            BinaryEncoding::Float64,
            CompressionType::Zlib,
            Some(1)
        )
        .is_err());
    }

    #[test]
    fn whitespace_is_accepted_but_invalid_padding_is_not() {
        let encoded = STANDARD.encode(42.5_f64.to_le_bytes());
        let wrapped = encoded
            .chars()
            .map(|c| format!("{c}\n"))
            .collect::<String>();
        assert_eq!(
            BinaryDecoder::decode(
                &wrapped,
                BinaryEncoding::Float64,
                CompressionType::None,
                Some(1)
            )
            .unwrap(),
            [42.5]
        );
        assert!(BinaryDecoder::decode(
            "AA=A",
            BinaryEncoding::Float64,
            CompressionType::None,
            None
        )
        .is_err());
    }
}
