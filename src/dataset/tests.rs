use super::*;
use crate::metadata::MzPeakMetadata;
use crate::schema::MZPEAK_MIMETYPE;
use crate::writer::{SpectrumBuilder, WriterConfig};
use std::fs;
use std::fs::File;
use std::io::Read;
use tempfile::tempdir;

// ==================== Directory Mode Tests ====================

#[test]
fn test_directory_mode_creation() {
    let dir = tempdir().unwrap();
    // Use a path without .mzpeak extension to force directory mode
    let dataset_path = dir.path().join("test_dir.mzpeak_dir");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    // Verify directory structure
    assert_eq!(dataset.mode(), OutputMode::Directory);
    assert!(dataset_path.exists());
    assert!(dataset_path.is_dir());
    assert!(dataset.peaks_dir().unwrap().exists());
    assert!(dataset.chromatograms_dir().unwrap().exists());
}

#[test]
fn test_directory_mode_already_exists() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("existing_dir");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    // Create first dataset
    let _dataset1 =
        MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config.clone()).unwrap();

    // Try to create again - should fail
    let result = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config);
    assert!(result.is_err());
}

#[test]
fn test_directory_mode_write_spectrum() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("write_test_dir");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .add_peak(500.0, 20000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();

    let stats = dataset.close().unwrap();
    assert_eq!(stats.peak_stats.spectra_written, 1);
    assert_eq!(stats.peak_stats.peaks_written, 2);
}

#[test]
fn test_directory_mode_metadata_json_created() {
    use crate::metadata::{RunParameters, SdrfMetadata, SourceFileInfo};

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("metadata_test_dir");

    let mut metadata = MzPeakMetadata::new();
    metadata.sdrf = Some(SdrfMetadata::new("test_sample"));
    metadata.run_parameters = Some(RunParameters::new());
    metadata.source_file = Some(SourceFileInfo::new("test.raw"));

    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(0.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Verify metadata.json exists and is valid JSON
    let metadata_json_path = dataset_path.join("metadata.json");
    assert!(metadata_json_path.exists());

    let json_content = fs::read_to_string(&metadata_json_path).unwrap();
    let json_value: serde_json::Value = serde_json::from_str(&json_content).unwrap();

    assert!(json_value.get("format_version").is_some());
    assert!(json_value.get("created").is_some());
    assert!(json_value.get("converter").is_some());
    assert!(json_value.get("sdrf").is_some());
    assert!(json_value.get("source_file").is_some());
}

#[test]
fn test_directory_mode_peaks_file_created() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("peaks_test_dir");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Verify peaks file exists
    let peaks_file = dataset_path.join("peaks").join("peaks.parquet");
    assert!(peaks_file.exists());
    assert!(peaks_file.metadata().unwrap().len() > 0);
}

// ==================== Container Mode Tests ====================

#[test]
fn test_container_mode_creation() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();

    // Should be container mode since path ends with .mzpeak
    assert_eq!(dataset.mode(), OutputMode::Container);
    // Container shouldn't have peaks_dir/chromatograms_dir
    assert!(dataset.peaks_dir().is_none());
    assert!(dataset.chromatograms_dir().is_none());
}

#[test]
fn test_container_mode_already_exists() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("existing.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    // Create first dataset and close it
    let mut dataset1 =
        MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config.clone()).unwrap();
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(0.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();
    dataset1.write_spectrum(&spectrum).unwrap();
    dataset1.close().unwrap();

    // Try to create again - should fail
    let result = MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config);
    assert!(result.is_err());
}

#[test]
fn test_container_mode_write_spectrum() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("write_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .add_peak(500.0, 20000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();

    let stats = dataset.close().unwrap();
    // Note: In container mode, stats tracking is simplified
    assert!(stats.total_size_bytes > 0);
}

#[test]
fn test_container_mode_zip_structure() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("structure_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Open and verify ZIP structure
    let file = File::open(&dataset_path).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();

    // Verify mimetype is first and uncompressed
    {
        let mimetype_entry = archive.by_index(0).unwrap();
        assert_eq!(mimetype_entry.name(), "mimetype");
        assert_eq!(mimetype_entry.compression(), zip::CompressionMethod::Stored);
    }

    // Verify metadata.json exists and is compressed
    {
        let metadata_entry = archive.by_name("metadata.json").unwrap();
        assert_eq!(
            metadata_entry.compression(),
            zip::CompressionMethod::Deflated
        );
    }

    // Verify peaks/peaks.parquet exists and is UNCOMPRESSED (critical for seekability)
    {
        let peaks_entry = archive.by_name("peaks/peaks.parquet").unwrap();
        assert_eq!(peaks_entry.compression(), zip::CompressionMethod::Stored);
    }
}

#[test]
fn test_container_mode_with_chromatograms() {
    use crate::prelude::Chromatogram;

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("chrom_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();

    // Write chromatograms
    let chrom1 = Chromatogram {
        chromatogram_id: "TIC".to_string(),
        chromatogram_type: "TIC".to_string(),
        time_array: vec![60.0, 120.0],
        intensity_array: vec![1000.0, 2000.0],
    };
    let chrom2 = Chromatogram {
        chromatogram_id: "BPC".to_string(),
        chromatogram_type: "BPC".to_string(),
        time_array: vec![60.0, 120.0],
        intensity_array: vec![1500.0, 2500.0],
    };

    dataset.write_chromatogram(&chrom1).unwrap();
    dataset.write_chromatogram(&chrom2).unwrap();

    let stats = dataset.close().unwrap();
    assert_eq!(stats.chromatograms_written, 2);

    // Open and verify ZIP structure includes chromatograms
    let file = File::open(&dataset_path).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();

    // Verify mimetype
    {
        let mimetype_entry = archive.by_index(0).unwrap();
        assert_eq!(mimetype_entry.name(), "mimetype");
    }

    // Verify peaks/peaks.parquet exists
    {
        let peaks_entry = archive.by_name("peaks/peaks.parquet").unwrap();
        assert_eq!(peaks_entry.compression(), zip::CompressionMethod::Stored);
    }

    // Verify chromatograms/chromatograms.parquet exists and is uncompressed
    {
        let chrom_entry = archive.by_name("chromatograms/chromatograms.parquet").unwrap();
        assert_eq!(chrom_entry.compression(), zip::CompressionMethod::Stored);
        assert!(chrom_entry.size() > 0);
    }
}

#[test]
fn test_container_mode_mimetype_content() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("mimetype_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(0.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Verify mimetype content
    let file = File::open(&dataset_path).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    let mut mimetype_entry = archive.by_name("mimetype").unwrap();

    let mut content = String::new();
    mimetype_entry.read_to_string(&mut content).unwrap();
    assert_eq!(content, MZPEAK_MIMETYPE);
}

#[test]
fn test_container_mode_metadata_json_content() {
    use crate::metadata::{SdrfMetadata, SourceFileInfo};

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("metadata_content_test.mzpeak");

    let mut metadata = MzPeakMetadata::new();
    metadata.sdrf = Some(SdrfMetadata::new("test_sample"));
    metadata.source_file = Some(SourceFileInfo::new("test.raw"));

    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(0.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Extract and verify metadata.json content
    let file = File::open(&dataset_path).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    let mut metadata_entry = archive.by_name("metadata.json").unwrap();

    let mut content = String::new();
    metadata_entry.read_to_string(&mut content).unwrap();

    let json_value: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(json_value.get("format_version").is_some());
    assert!(json_value.get("created").is_some());
    assert!(json_value.get("converter").is_some());
    assert!(json_value.get("sdrf").is_some());
    assert!(json_value.get("source_file").is_some());
}

// ==================== Auto-detection Tests ====================

#[test]
fn test_auto_detection_container_mode() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("auto_container.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();
    assert_eq!(dataset.mode(), OutputMode::Container);
}

#[test]
fn test_auto_detection_directory_mode_no_extension() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("auto_directory");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();
    assert_eq!(dataset.mode(), OutputMode::Directory);
}

#[test]
fn test_auto_detection_directory_mode_other_extension() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("auto_directory.parquet");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let dataset = MzPeakDatasetWriter::new(&dataset_path, &metadata, config).unwrap();
    assert_eq!(dataset.mode(), OutputMode::Directory);
}

#[test]
fn test_write_multiple_spectra_directory() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("multi_test_dir");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    let spectra: Vec<_> = (0..10)
        .map(|i| {
            SpectrumBuilder::new(i, i + 1)
                .ms_level(1)
                .retention_time((i as f32) * 10.0)
                .polarity(1)
                .add_peak(400.0 + (i as f64), 10000.0)
                .build()
        })
        .collect();

    dataset.write_spectra(&spectra).unwrap();

    let stats = dataset.close().unwrap();
    assert_eq!(stats.peak_stats.spectra_written, 10);
    assert_eq!(stats.peak_stats.peaks_written, 10);
}

#[test]
fn test_write_multiple_spectra_container() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("multi_test.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let mut dataset = MzPeakDatasetWriter::new_container(&dataset_path, &metadata, config).unwrap();

    let spectra: Vec<_> = (0..10)
        .map(|i| {
            SpectrumBuilder::new(i, i + 1)
                .ms_level(1)
                .retention_time((i as f32) * 10.0)
                .polarity(1)
                .add_peak(400.0 + (i as f64), 10000.0)
                .build()
        })
        .collect();

    dataset.write_spectra(&spectra).unwrap();

    let stats = dataset.close().unwrap();
    assert!(stats.total_size_bytes > 0);
}
