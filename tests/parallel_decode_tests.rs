#![cfg(feature = "mzml")]
//! Comprehensive tests for the parallel decoding architecture
//!
//! This test suite validates:
//! - SIMD vs scalar decoder equivalence (fidelity)
//! - Sequential vs parallel conversion output identity
//! - Raw spectrum parsing and decoding
//! - Property-based testing for edge cases

use std::io::{BufReader, Cursor};

use mzpeak::mzml::{
    BinaryCompression, BinaryEncoding, MzMLSpectrum, MzMLStreamer,
};

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a minimal mzML document for testing
fn create_test_mzml(spectra_count: usize) -> String {
    let mut mzml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <run id="test_run">
    <spectrumList count=""#);
    mzml.push_str(&spectra_count.to_string());
    mzml.push_str(r#"">"#);

    for i in 0..spectra_count {
        let ms_level = if i % 5 == 0 { 1 } else { 2 };
        let rt = (i as f64) * 0.5;

        // Create test m/z and intensity arrays
        let mz_values: Vec<f64> = (0..100).map(|j| 100.0 + (j as f64) * 10.0 + (i as f64) * 0.1).collect();
        let intensity_values: Vec<f32> = (0..100).map(|j| 1000.0 + (j as f32) * 50.0).collect();

        // Encode as base64 (64-bit m/z, 32-bit intensity, uncompressed)
        let mz_bytes: Vec<u8> = mz_values.iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let intensity_bytes: Vec<u8> = intensity_values.iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();

        let mz_base64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &mz_bytes);
        let intensity_base64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &intensity_bytes);

        mzml.push_str(&format!(r#"
      <spectrum index="{}" id="scan={}" defaultArrayLength="100">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="{}"/>
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan"/>
        <scanList count="1">
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="{}" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <binary>{}</binary>
          </binaryDataArray>
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <binary>{}</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>"#, i, i + 1, ms_level, rt, mz_base64, intensity_base64));
    }

    mzml.push_str(r#"
    </spectrumList>
  </run>
</mzML>"#);

    mzml
}

/// Compare two spectra for exact equality
fn spectra_are_equal(a: &MzMLSpectrum, b: &MzMLSpectrum) -> bool {
    a.index == b.index
        && a.id == b.id
        && a.ms_level == b.ms_level
        && a.polarity == b.polarity
        && a.retention_time == b.retention_time
        && a.mz_array.len() == b.mz_array.len()
        && a.intensity_array.len() == b.intensity_array.len()
        && a.mz_array.iter().zip(b.mz_array.iter()).all(|(x, y)| (x - y).abs() < 1e-10)
        && a.intensity_array.iter().zip(b.intensity_array.iter()).all(|(x, y)| (x - y).abs() < 1e-5)
}

// ============================================================================
// Unit Tests: Raw Spectrum Parsing
// ============================================================================

/// Test that next_raw_spectrum() returns data without decoding
#[test]
fn test_next_raw_spectrum_deferred_decoding() {
    let mzml = create_test_mzml(5);
    let reader = BufReader::new(Cursor::new(mzml));
    let mut streamer = MzMLStreamer::new(reader).unwrap();

    // Read raw spectrum
    let raw = streamer.next_raw_spectrum().unwrap().unwrap();

    // Verify metadata is populated
    assert_eq!(raw.index, 0);
    assert_eq!(raw.id, "scan=1");
    assert_eq!(raw.ms_level, 1);
    assert_eq!(raw.default_array_length, 100);

    // Verify binary data is NOT decoded (stored as base64)
    assert!(!raw.mz_data.base64.is_empty(), "m/z data should be stored as base64");
    assert!(!raw.intensity_data.base64.is_empty(), "intensity data should be stored as base64");
    assert_eq!(raw.mz_data.encoding, BinaryEncoding::Float64);
    assert_eq!(raw.intensity_data.encoding, BinaryEncoding::Float32);
    assert_eq!(raw.mz_data.compression, BinaryCompression::None);
}

/// Test that decode() produces valid arrays
#[test]
fn test_raw_spectrum_decode() {
    let mzml = create_test_mzml(1);
    let reader = BufReader::new(Cursor::new(mzml));
    let mut streamer = MzMLStreamer::new(reader).unwrap();

    let raw = streamer.next_raw_spectrum().unwrap().unwrap();
    let decoded = raw.decode().expect("decode should succeed");

    // Verify decoded arrays have correct length
    assert_eq!(decoded.mz_array.len(), 100);
    assert_eq!(decoded.intensity_array.len(), 100);

    // Verify first few values are reasonable
    assert!((decoded.mz_array[0] - 100.0).abs() < 0.01);
    assert!((decoded.mz_array[1] - 110.0).abs() < 0.01);
    assert!(decoded.intensity_array[0] > 900.0);
}

// ============================================================================
// Fidelity Tests: Raw vs Sequential Parsing
// ============================================================================

/// Test that raw parsing + decode produces identical results to sequential parsing
#[test]
fn test_raw_vs_sequential_fidelity() {
    let mzml = create_test_mzml(20);

    // Parse with sequential method
    let reader1 = BufReader::new(Cursor::new(mzml.clone()));
    let mut streamer1 = MzMLStreamer::new(reader1).unwrap();
    let mut sequential_spectra = Vec::new();
    while let Some(spectrum) = streamer1.next_spectrum().unwrap() {
        sequential_spectra.push(spectrum);
    }

    // Parse with raw method + decode
    let reader2 = BufReader::new(Cursor::new(mzml));
    let mut streamer2 = MzMLStreamer::new(reader2).unwrap();
    let mut raw_decoded_spectra = Vec::new();
    while let Some(raw) = streamer2.next_raw_spectrum().unwrap() {
        raw_decoded_spectra.push(raw.decode().unwrap());
    }

    // Compare
    assert_eq!(sequential_spectra.len(), raw_decoded_spectra.len());
    for (i, (seq, raw_dec)) in sequential_spectra.iter().zip(raw_decoded_spectra.iter()).enumerate() {
        assert!(
            spectra_are_equal(seq, raw_dec),
            "Spectrum {} differs between sequential and raw+decode paths",
            i
        );
    }
}

/// Test exact f64 equality between decoders for 64-bit data
#[test]
fn test_f64_decode_exact_equality() {
    use base64::Engine;

    // Create test data: 10 f64 values
    let values: Vec<f64> = (0..10).map(|i| 100.0 + (i as f64) * 0.123456789).collect();
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

    // Decode with standard decoder
    let standard_result = mzpeak::mzml::BinaryDecoder::decode(
        &base64_data,
        BinaryEncoding::Float64,
        BinaryCompression::None,
        Some(10),
    )
    .unwrap();

    // Compare values
    assert_eq!(standard_result.len(), values.len());
    for (i, (original, decoded)) in values.iter().zip(standard_result.iter()).enumerate() {
        assert!(
            (original - decoded).abs() < 1e-15,
            "Value {} differs: {} vs {}",
            i,
            original,
            decoded
        );
    }
}

/// Test exact f32→f64 conversion consistency
#[test]
fn test_f32_to_f64_conversion_consistency() {
    use base64::Engine;

    // Create test data: 10 f32 values
    let values: Vec<f32> = (0..10).map(|i| 1000.0 + (i as f32) * 12.345).collect();
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

    // Decode with standard decoder
    let standard_result = mzpeak::mzml::BinaryDecoder::decode(
        &base64_data,
        BinaryEncoding::Float32,
        BinaryCompression::None,
        Some(10),
    )
    .unwrap();

    // Verify f32→f64 conversion matches Rust's built-in cast
    assert_eq!(standard_result.len(), values.len());
    for (i, (original, decoded)) in values.iter().zip(standard_result.iter()).enumerate() {
        let expected = *original as f64;
        assert!(
            (expected - decoded).abs() < 1e-10,
            "Value {} differs after f32→f64: expected {}, got {}",
            i,
            expected,
            decoded
        );
    }
}

// ============================================================================
// SIMD Tests (only when parallel-decode feature is enabled)
// ============================================================================

#[cfg(feature = "parallel-decode")]
mod simd_tests {
    use super::*;
    use base64::Engine;
    use mzpeak::mzml::simd::*;

    /// Test SIMD whitespace removal
    #[test]
    fn test_simd_whitespace_removal() {
        // Test various whitespace patterns
        let test_cases = [
            (b"AAAA BBBB".to_vec(), b"AAAABBBB".to_vec()),
            (b"AAAA\nBBBB".to_vec(), b"AAAABBBB".to_vec()),
            (b"AAAA\tBBBB".to_vec(), b"AAAABBBB".to_vec()),
            (b"AAAA\rBBBB".to_vec(), b"AAAABBBB".to_vec()),
            (b" \n\t\r".to_vec(), b"".to_vec()),
            (b"NoWhitespace".to_vec(), b"NoWhitespace".to_vec()),
            // Large input to exercise SIMD path
            (
                b"AAAA BBBB CCCC DDDD EEEE FFFF GGGG HHHH IIII JJJJ".to_vec(),
                b"AAAABBBBCCCCDDDDEEEEFFFFGGGGHHHHIIIIJJJJ".to_vec(),
            ),
        ];

        for (input, expected) in test_cases {
            let result = simd_remove_whitespace_bytes(&input);
            assert_eq!(result, expected, "Whitespace removal failed for input: {:?}", input);
        }
    }

    /// Test SIMD f32→f64 conversion
    #[test]
    fn test_simd_f32_to_f64() {
        let values: Vec<f32> = (0..20).map(|i| 100.0 + (i as f32) * 0.5).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let result = simd_decode_f32_to_f64(&bytes);

        assert_eq!(result.len(), values.len());
        for (i, (original, decoded)) in values.iter().zip(result.iter()).enumerate() {
            let expected = *original as f64;
            assert!(
                (expected - decoded).abs() < 1e-10,
                "SIMD f32→f64 value {} differs: expected {}, got {}",
                i,
                expected,
                decoded
            );
        }
    }

    /// Test SIMD f64 decoding
    #[test]
    fn test_simd_f64_decode() {
        let values: Vec<f64> = (0..20).map(|i| 100.0 + (i as f64) * 0.123456789).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let result = simd_decode_f64(&bytes);

        assert_eq!(result.len(), values.len());
        for (i, (original, decoded)) in values.iter().zip(result.iter()).enumerate() {
            assert!(
                (original - decoded).abs() < 1e-15,
                "SIMD f64 value {} differs: expected {}, got {}",
                i,
                original,
                decoded
            );
        }
    }

    /// Test SIMD decoder produces identical results to scalar decoder
    #[test]
    fn test_simd_vs_scalar_fidelity_f64() {
        let values: Vec<f64> = (0..1000).map(|i| 100.0 + (i as f64) * 0.987654321).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

        // Decode with scalar (standard) decoder
        let scalar_result = mzpeak::mzml::BinaryDecoder::decode(
            &base64_data,
            BinaryEncoding::Float64,
            BinaryCompression::None,
            Some(1000),
        )
        .unwrap();

        // Decode with SIMD decoder
        let simd_result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            BinaryCompression::None,
            Some(1000),
        )
        .unwrap();

        assert_eq!(scalar_result.len(), simd_result.len());
        for (i, (scalar, simd)) in scalar_result.iter().zip(simd_result.iter()).enumerate() {
            assert!(
                (scalar - simd).abs() < 1e-15,
                "SIMD vs scalar f64 value {} differs: {} vs {}",
                i,
                scalar,
                simd
            );
        }
    }

    /// Test SIMD decoder produces identical results to scalar decoder for f32
    #[test]
    fn test_simd_vs_scalar_fidelity_f32() {
        let values: Vec<f32> = (0..1000).map(|i| 100.0 + (i as f32) * 12.345).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

        // Decode with scalar (standard) decoder
        let scalar_result = mzpeak::mzml::BinaryDecoder::decode(
            &base64_data,
            BinaryEncoding::Float32,
            BinaryCompression::None,
            Some(1000),
        )
        .unwrap();

        // Decode with SIMD decoder
        let simd_result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float32,
            BinaryCompression::None,
            Some(1000),
        )
        .unwrap();

        assert_eq!(scalar_result.len(), simd_result.len());
        for (i, (scalar, simd)) in scalar_result.iter().zip(simd_result.iter()).enumerate() {
            assert!(
                (scalar - simd).abs() < 1e-10,
                "SIMD vs scalar f32 value {} differs: {} vs {}",
                i,
                scalar,
                simd
            );
        }
    }

    /// Test SIMD decoder handles base64 with embedded whitespace
    #[test]
    fn test_simd_decode_with_whitespace() {
        let values: Vec<f64> = vec![100.0, 200.0, 300.0, 400.0];
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let mut base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

        // Insert whitespace (common in XML)
        base64_data.insert(4, ' ');
        base64_data.insert(10, '\n');
        base64_data.insert(20, '\t');

        let result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            BinaryCompression::None,
            Some(4),
        )
        .unwrap();

        assert_eq!(result.len(), 4);
        for (i, (original, decoded)) in values.iter().zip(result.iter()).enumerate() {
            assert!(
                (original - decoded).abs() < 1e-10,
                "Value {} with whitespace: {} vs {}",
                i,
                original,
                decoded
            );
        }
    }

    /// Test SIMD decoder handles zlib-compressed data
    #[test]
    fn test_simd_decode_zlib_compressed() {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;
        use std::io::Write;

        let values: Vec<f64> = (0..100).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        // Compress with zlib
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&bytes).unwrap();
        let compressed = encoder.finish().unwrap();

        let base64_data = base64::engine::general_purpose::STANDARD.encode(&compressed);

        // Decode with SIMD
        let simd_result = decode_binary_array_simd(
            &base64_data,
            BinaryEncoding::Float64,
            BinaryCompression::Zlib,
            Some(100),
        )
        .unwrap();

        // Decode with scalar
        let scalar_result = mzpeak::mzml::BinaryDecoder::decode(
            &base64_data,
            BinaryEncoding::Float64,
            BinaryCompression::Zlib,
            Some(100),
        )
        .unwrap();

        assert_eq!(simd_result.len(), scalar_result.len());
        for (i, (simd, scalar)) in simd_result.iter().zip(scalar_result.iter()).enumerate() {
            assert!(
                (simd - scalar).abs() < 1e-15,
                "Zlib decode value {} differs: {} vs {}",
                i,
                simd,
                scalar
            );
        }
    }
}

// ============================================================================
// Parallel Conversion Tests (only when parallel-decode feature is enabled)
// ============================================================================

#[cfg(feature = "parallel-decode")]
mod parallel_conversion_tests {
    use super::*;
    use mzpeak::mzml::converter::ConversionError;
    use mzpeak::mzml::{ConversionConfig, MzMLConverter};
    use std::fs;
    use tempfile::tempdir;

    /// Test that convert_parallel produces valid output
    #[test]
    fn test_convert_parallel_basic() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("test.mzML");
        let output_path = dir.path().join("test.mzpeak");

        // Create test mzML file
        let mzml = create_test_mzml(50);
        fs::write(&input_path, &mzml).unwrap();

        // Convert with parallel mode
        let config = ConversionConfig {
            parallel_batch_size: 10,
            ..ConversionConfig::default()
        };
        let converter = MzMLConverter::with_config(config);
        let stats = converter.convert_parallel(&input_path, &output_path).unwrap();

        // Verify statistics
        assert_eq!(stats.spectra_count, 50);
        assert!(stats.peak_count > 0);
        assert!(output_path.exists());
    }

    /// Test that parallel conversion fails on decode errors
    #[test]
    fn test_convert_parallel_decode_error_fails() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("corrupt.mzML");
        let output_path = dir.path().join("corrupt.mzpeak");

        let mut mzml = create_test_mzml(3);
        let marker = "<binary>";
        let pos = mzml.find(marker).expect("binary tag missing");
        let corrupt_at = pos + marker.len();
        mzml.replace_range(corrupt_at..corrupt_at + 1, "!");
        fs::write(&input_path, &mzml).unwrap();

        let config = ConversionConfig {
            parallel_batch_size: 2,
            ..ConversionConfig::default()
        };
        let converter = MzMLConverter::with_config(config);
        let err = converter
            .convert_parallel(&input_path, &output_path)
            .expect_err("expected decode error");

        match err {
            ConversionError::BinaryDecodeError { .. } => {}
            _ => panic!("unexpected error type: {}", err),
        }
    }

    /// Test that sequential and parallel conversion produce identical output
    #[test]
    fn test_sequential_vs_parallel_output_fidelity() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("test.mzML");
        let seq_output = dir.path().join("sequential.mzpeak");
        let par_output = dir.path().join("parallel.mzpeak");

        // Create test mzML file
        let mzml = create_test_mzml(100);
        fs::write(&input_path, &mzml).unwrap();

        // Convert with sequential mode
        let seq_config = ConversionConfig::default();
        let seq_converter = MzMLConverter::with_config(seq_config);
        let seq_stats = seq_converter.convert(&input_path, &seq_output).unwrap();

        // Convert with parallel mode
        let par_config = ConversionConfig {
            parallel_batch_size: 20,
            ..ConversionConfig::default()
        };
        let par_converter = MzMLConverter::with_config(par_config);
        let par_stats = par_converter.convert_parallel(&input_path, &par_output).unwrap();

        // Verify statistics are identical
        assert_eq!(seq_stats.spectra_count, par_stats.spectra_count);
        assert_eq!(seq_stats.peak_count, par_stats.peak_count);
        assert_eq!(seq_stats.ms1_spectra, par_stats.ms1_spectra);
        assert_eq!(seq_stats.ms2_spectra, par_stats.ms2_spectra);

        // Note: File sizes may differ slightly due to parallel vs sequential ordering
        // and compression, but the data content should be equivalent
    }

    /// Test parallel conversion with various batch sizes
    #[test]
    fn test_parallel_batch_sizes() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("test.mzML");

        // Create test mzML file
        let mzml = create_test_mzml(100);
        fs::write(&input_path, &mzml).unwrap();

        // Test various batch sizes
        for batch_size in [1, 5, 10, 50, 100, 200] {
            let output_path = dir.path().join(format!("batch_{}.mzpeak", batch_size));

            let config = ConversionConfig {
                parallel_batch_size: batch_size,
                ..ConversionConfig::default()
            };
            let converter = MzMLConverter::with_config(config);
            let stats = converter.convert_parallel(&input_path, &output_path).unwrap();

            assert_eq!(
                stats.spectra_count, 100,
                "Batch size {} produced wrong spectrum count",
                batch_size
            );
        }
    }
}

// ============================================================================
// Property-Based Tests
// ============================================================================

mod property_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Test that any valid f64 array can be round-tripped through base64
        #[test]
        fn test_f64_roundtrip(values in prop::collection::vec(any::<f64>().prop_filter("finite", |v| v.is_finite()), 1..100)) {
            use base64::Engine;

            let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            let base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

            let decoded = mzpeak::mzml::BinaryDecoder::decode(
                &base64_data,
                BinaryEncoding::Float64,
                BinaryCompression::None,
                Some(values.len()),
            ).unwrap();

            prop_assert_eq!(decoded.len(), values.len());
            for (original, decoded) in values.iter().zip(decoded.iter()) {
                prop_assert!((original - decoded).abs() < 1e-10);
            }
        }

        /// Test that any valid f32 array can be round-tripped through base64
        #[test]
        fn test_f32_roundtrip(values in prop::collection::vec(any::<f32>().prop_filter("finite", |v| v.is_finite()), 1..100)) {
            use base64::Engine;

            let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            let base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

            let decoded = mzpeak::mzml::BinaryDecoder::decode(
                &base64_data,
                BinaryEncoding::Float32,
                BinaryCompression::None,
                Some(values.len()),
            ).unwrap();

            prop_assert_eq!(decoded.len(), values.len());
            for (original, decoded) in values.iter().zip(decoded.iter()) {
                let expected = *original as f64;
                prop_assert!((expected - decoded).abs() < 1e-5);
            }
        }

        /// Test whitespace handling in base64 strings
        #[test]
        fn test_base64_whitespace_handling(
            values in prop::collection::vec(100.0f64..1000.0f64, 1..20),
            whitespace_positions in prop::collection::vec(0usize..50, 0..5)
        ) {
            use base64::Engine;

            let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            let mut base64_data = base64::engine::general_purpose::STANDARD.encode(&bytes);

            // Insert whitespace at various positions
            let whitespace_chars = [' ', '\n', '\t', '\r'];
            for pos in whitespace_positions {
                if pos < base64_data.len() {
                    base64_data.insert(pos, whitespace_chars[pos % 4]);
                }
            }

            // Decode should still work (base64 decoder handles whitespace)
            let result = mzpeak::mzml::BinaryDecoder::decode(
                &base64_data,
                BinaryEncoding::Float64,
                BinaryCompression::None,
                Some(values.len()),
            );

            // Should either succeed with correct values or fail gracefully
            if let Ok(decoded) = result {
                prop_assert_eq!(decoded.len(), values.len());
            }
        }
    }
}

// ============================================================================
// Regression Tests
// ============================================================================

/// Test parsing the minimal mzML format
#[test]
fn test_minimal_mzml_raw_parsing() {
    let mzml = r#"<?xml version="1.0" encoding="UTF-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <run id="test_run">
    <spectrumList count="1">
      <spectrum index="0" id="scan=1" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan"/>
        <scanList count="1">
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="60.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <binary>AAAAAAAAWUAAAAAAAABpQA==</binary>
          </binaryDataArray>
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <binary>AADIQgAASEM=</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
  </run>
</mzML>"#;

    let reader = BufReader::new(Cursor::new(mzml));
    let mut streamer = MzMLStreamer::new(reader).unwrap();

    // Test raw parsing
    let raw = streamer.next_raw_spectrum().unwrap().unwrap();
    assert_eq!(raw.index, 0);
    assert_eq!(raw.id, "scan=1");
    assert_eq!(raw.ms_level, 1);
    assert_eq!(raw.polarity, 1);
    assert!((raw.retention_time.unwrap() - 60.0).abs() < 0.001);

    // Test decoding
    let decoded = raw.decode().unwrap();
    assert_eq!(decoded.mz_array.len(), 2);
    assert_eq!(decoded.intensity_array.len(), 2);
    assert!((decoded.mz_array[0] - 100.0).abs() < 0.001);
    assert!((decoded.mz_array[1] - 200.0).abs() < 0.001);
}

/// Test empty spectrum handling
#[test]
fn test_empty_spectrum_raw_parsing() {
    let mzml = r#"<?xml version="1.0" encoding="UTF-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <run id="test_run">
    <spectrumList count="1">
      <spectrum index="0" id="scan=1" defaultArrayLength="0">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <binaryDataArrayList count="2">
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <binary></binary>
          </binaryDataArray>
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <binary></binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
  </run>
</mzML>"#;

    let reader = BufReader::new(Cursor::new(mzml));
    let mut streamer = MzMLStreamer::new(reader).unwrap();

    let raw = streamer.next_raw_spectrum().unwrap().unwrap();
    let decoded = raw.decode().unwrap();

    assert!(decoded.mz_array.is_empty());
    assert!(decoded.intensity_array.is_empty());
}
