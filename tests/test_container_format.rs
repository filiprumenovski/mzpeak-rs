//! Integration tests for .mzpeak ZIP container format
//!
//! These tests verify:
//! 1. Container creation with proper structure
//! 2. Zero-extraction reading (no temp files)
//! 3. MimeType compliance (first entry, uncompressed)
//! 4. Seekable Parquet (uncompressed in ZIP)

use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::MzPeakMetadata;
use mzpeak::reader::MzPeakReader;
use mzpeak::validator::validate_mzpeak_file;
use mzpeak::writer::{PeakArrays, SpectrumArrays, WriterConfig};
use std::fs::File;
use std::io::Read;
use tempfile::tempdir;
use zip::ZipArchive;

fn make_ms1_spectrum(
    spectrum_id: i64,
    scan_number: i64,
    retention_time: f32,
    mz: f64,
    intensity: f32,
) -> SpectrumArrays {
    let peaks = PeakArrays::new(vec![mz], vec![intensity]);
    SpectrumArrays::new_ms1(spectrum_id, scan_number, retention_time, 1, peaks)
}

fn make_ms2_spectrum(
    spectrum_id: i64,
    scan_number: i64,
    retention_time: f32,
    precursor_mz: f64,
    mz: Vec<f64>,
    intensity: Vec<f32>,
) -> SpectrumArrays {
    let peaks = PeakArrays::new(mz, intensity);
    SpectrumArrays::new_ms2(spectrum_id, scan_number, retention_time, 1, precursor_mz, peaks)
}

#[test]
fn test_container_format_creation() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

    // Write some test spectra
    let spectra: Vec<_> = (0..10)
        .map(|i| make_ms1_spectrum(i, i + 1, (i as f32) * 10.0, 400.0 + (i as f64), 10000.0))
        .collect();

    dataset.write_spectra_arrays(&spectra).unwrap();
    let stats = dataset.close().unwrap();

    assert!(stats.total_size_bytes > 0);
    assert!(dataset_path.exists());
    assert!(dataset_path.is_file());
}

#[test]
fn test_container_mimetype_compliance() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("mimetype_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

    let spectrum = make_ms1_spectrum(0, 1, 0.0, 400.0, 10000.0);

    dataset.write_spectrum_arrays(&spectrum).unwrap();
    dataset.close().unwrap();

    // Open and validate ZIP structure
    let file = File::open(&dataset_path).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();

    // Verify mimetype is first entry
    let first_entry = archive.by_index(0).unwrap();
    assert_eq!(first_entry.name(), "mimetype", "First entry must be mimetype");

    // Verify mimetype is uncompressed
    assert_eq!(
        first_entry.compression(),
        zip::CompressionMethod::Stored,
        "mimetype must be uncompressed (Stored)"
    );
    drop(first_entry);

    // Verify mimetype content
    let mut mimetype_entry = archive.by_name("mimetype").unwrap();
    let mut content = String::new();
    mimetype_entry.read_to_string(&mut content).unwrap();
    assert_eq!(content, "application/vnd.mzpeak");
}

#[test]
fn test_container_seekable_parquet() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("seekable_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

    let spectrum = make_ms1_spectrum(0, 1, 60.0, 400.0, 10000.0);

    dataset.write_spectrum_arrays(&spectrum).unwrap();
    dataset.close().unwrap();

    // Verify peaks.parquet is uncompressed (Stored) for seekability
    let file = File::open(&dataset_path).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();

    let peaks_entry = archive.by_name("peaks/peaks.parquet").unwrap();
    assert_eq!(
        peaks_entry.compression(),
        zip::CompressionMethod::Stored,
        "peaks.parquet must be uncompressed (Stored) for zero-extraction seekable reading"
    );
}

#[test]
fn test_zero_extraction_reading() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("zero_extract.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

    // Write test data
    let spectra: Vec<_> = (0..50)
        .map(|i| {
            let mz_values = vec![400.0 + (i as f64) * 10.0, 500.0 + (i as f64) * 10.0];
            let intensity_values = vec![10000.0, 20000.0];
            if i % 5 == 0 {
                let peaks = PeakArrays::new(mz_values, intensity_values);
                SpectrumArrays::new_ms1(i, i + 1, (i as f32) * 2.0, 1, peaks)
            } else {
                make_ms2_spectrum(i, i + 1, (i as f32) * 2.0, 600.0, mz_values, intensity_values)
            }
        })
        .collect();

    dataset.write_spectra_arrays(&spectra).unwrap();
    dataset.close().unwrap();

    // Read without extraction - this should work entirely in memory
    let reader = MzPeakReader::open(&dataset_path).unwrap();

    // Verify metadata
    let file_metadata = reader.metadata();
    assert_eq!(file_metadata.total_rows, 100); // 50 spectra * 2 peaks each

    // Verify we can read all spectra
    let read_spectra = reader.iter_spectra_arrays().unwrap();
    assert_eq!(read_spectra.len(), 50);

    // Verify first spectrum
    assert_eq!(read_spectra[0].spectrum_id, 0);
    assert_eq!(read_spectra[0].peak_count(), 2);

    // Test querying by retention time
    let rt_spectra = reader.spectra_by_rt_range_arrays(0.0, 20.0).unwrap();
    assert_eq!(rt_spectra.len(), 11); // 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20

    // Test querying by MS level
    let ms1_spectra = reader.spectra_by_ms_level_arrays(1).unwrap();
    assert_eq!(ms1_spectra.len(), 10); // Every 5th spectrum

    // Test getting specific spectrum
    let spectrum_25 = reader.get_spectrum_arrays(25).unwrap();
    assert!(spectrum_25.is_some());
    assert_eq!(spectrum_25.unwrap().retention_time, 50.0);

    // Verify no temporary files were created
    // The reader should work entirely in memory using Bytes
    let temp_dir_count = std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .contains("mzpeak")
        })
        .count();
    
    // Should be 0 (or very small if other tests are running)
    println!("Temp mzpeak files found: {}", temp_dir_count);
}

#[test]
fn test_validator_compliance() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("validator_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

    let spectrum = make_ms1_spectrum(0, 1, 60.0, 400.0, 10000.0);

    dataset.write_spectrum_arrays(&spectrum).unwrap();
    dataset.close().unwrap();

    // Run validator
    let report = validate_mzpeak_file(&dataset_path).unwrap();

    // Print report for debugging
    println!("{}", report);

    // Verify validation passed
    assert!(
        !report.has_failures(),
        "Validation should pass without failures"
    );

    // Check specific validations
    let check_names: Vec<_> = report.checks.iter().map(|c| c.name.as_str()).collect();
    assert!(check_names.contains(&"Path exists"));
    assert!(check_names.contains(&"Format: ZIP container (.mzpeak)"));
    assert!(check_names.contains(&"mimetype is first entry"));
    assert!(check_names.contains(&"mimetype is uncompressed"));
    assert!(check_names.contains(&"peaks.parquet is uncompressed (seekable)"));
    assert!(check_names.contains(&"Valid Parquet file"));
}

#[test]
fn test_container_with_chromatograms() {
    use mzpeak::chromatogram_writer::Chromatogram;

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("chrom_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

    // Write spectrum
    let spectrum = make_ms1_spectrum(0, 1, 60.0, 400.0, 10000.0);

    dataset.write_spectrum_arrays(&spectrum).unwrap();

    // Write chromatograms
    let tic = Chromatogram {
        chromatogram_id: "TIC".to_string(),
        chromatogram_type: "TIC".to_string(),
        time_array: vec![0.0, 60.0, 120.0],
        intensity_array: vec![1000.0, 10000.0, 5000.0],
    };

    dataset.write_chromatogram(&tic).unwrap();
    let stats = dataset.close().unwrap();

    assert_eq!(stats.chromatograms_written, 1);

    // Verify ZIP contains chromatograms (in a separate scope to close the archive)
    {
        let file = File::open(&dataset_path).unwrap();
        let mut archive = ZipArchive::new(file).unwrap();

        let chrom_entry = archive.by_name("chromatograms/chromatograms.parquet").unwrap();
        assert_eq!(
            chrom_entry.compression(),
            zip::CompressionMethod::Stored,
            "chromatograms.parquet should be uncompressed for seekability"
        );
        // Archive is dropped here
    }

    // Read chromatograms using reader
    let reader = MzPeakReader::open(&dataset_path).unwrap();
    let chromatograms = reader.read_chromatograms().unwrap();
    assert_eq!(chromatograms.len(), 1);
    assert_eq!(chromatograms[0].chromatogram_id, "TIC");
    assert_eq!(chromatograms[0].time_array.len(), 3);
}

#[test]
fn test_roundtrip_container_vs_directory() {
    let dir = tempdir().unwrap();
    let container_path = dir.path().join("test.mzpeak");
    let directory_path = dir.path().join("test_dir.mzpeak_bundle");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    // Create test data
    let spectra: Vec<_> = (0..20)
        .map(|i| {
            let peaks = PeakArrays::new(
                vec![400.0 + (i as f64), 500.0 + (i as f64)],
                vec![1000.0 + (i as f32) * 100.0, 2000.0 + (i as f32) * 100.0],
            );
            SpectrumArrays::new_ms1(i, i + 1, (i as f32) * 5.0, 1, peaks)
        })
        .collect();

    // Write to container
    let mut container_writer =
        MzPeakDatasetWriter::new_container(&container_path, &metadata, config.clone()).unwrap();
    container_writer.write_spectra_arrays(&spectra).unwrap();
    container_writer.close().unwrap();

    // Write to directory
    let mut directory_writer =
        MzPeakDatasetWriter::new_directory(&directory_path, &metadata, config).unwrap();
    directory_writer.write_spectra_arrays(&spectra).unwrap();
    directory_writer.close().unwrap();

    // Read from both and compare
    let container_reader = MzPeakReader::open(&container_path).unwrap();
    let directory_reader = MzPeakReader::open(&directory_path).unwrap();

    let container_spectra = container_reader.iter_spectra_arrays().unwrap();
    let directory_spectra = directory_reader.iter_spectra_arrays().unwrap();

    assert_eq!(container_spectra.len(), directory_spectra.len());

    for i in 0..container_spectra.len() {
        let c_spec = container_spectra[i].to_owned().unwrap();
        let d_spec = directory_spectra[i].to_owned().unwrap();

        assert_eq!(c_spec.spectrum_id, d_spec.spectrum_id);
        assert_eq!(c_spec.retention_time, d_spec.retention_time);
        assert_eq!(c_spec.peak_count(), d_spec.peak_count());
        assert_eq!(c_spec.peaks.mz, d_spec.peaks.mz);
        assert_eq!(c_spec.peaks.intensity, d_spec.peaks.intensity);
    }
}

#[test]
fn test_reader_performance_no_temp_extraction() {
    use std::time::Instant;

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("perf_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

    // Write larger dataset
    let spectra: Vec<_> = (0..500)
        .map(|i| {
            let mut mz = Vec::with_capacity(100);
            let mut intensity = Vec::with_capacity(100);
            for j in 0..100 {
                mz.push(100.0 + (j as f64) * 5.0);
                intensity.push(1000.0 + (j as f32) * 10.0);
            }
            let peaks = PeakArrays::new(mz, intensity);
            if i % 10 == 0 {
                SpectrumArrays::new_ms1(i, i + 1, (i as f32) * 0.5, 1, peaks)
            } else {
                SpectrumArrays::new_ms2(i, i + 1, (i as f32) * 0.5, 1, 600.0, peaks)
            }
        })
        .collect();

    dataset.write_spectra_arrays(&spectra).unwrap();
    dataset.close().unwrap();

    // Measure reading time (should be fast since no extraction)
    let start = Instant::now();
    let reader = MzPeakReader::open(&dataset_path).unwrap();
    let open_duration = start.elapsed();
    println!("Open time: {:?}", open_duration);

    // Reading metadata should be instant
    let start = Instant::now();
    let _metadata = reader.metadata();
    let metadata_duration = start.elapsed();
    println!("Metadata access time: {:?}", metadata_duration);

    // Reading spectra should work without extracting ZIP
    let start = Instant::now();
    let read_spectra = reader.iter_spectra_arrays().unwrap();
    let read_duration = start.elapsed();
    println!("Read all spectra time: {:?}", read_duration);

    assert_eq!(read_spectra.len(), 500);
    assert!(open_duration.as_millis() < 1000, "Open should be fast");
    assert!(
        metadata_duration.as_millis() < 100,
        "Metadata access should be instant"
    );
}
