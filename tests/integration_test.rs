//! Integration tests for mzPeak
//!
//! These tests verify the full pipeline from data creation to reading.

use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::{MzPeakMetadata, RunParameters, SdrfMetadata, SourceFileInfo};
use mzpeak::writer::{MzPeakWriter, Peak, SpectrumBuilder, WriterConfig};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::{self, File};
use tempfile::tempdir;

/// Test the complete write-read cycle
#[test]
fn test_write_read_cycle() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.mzpeak.parquet");

    // Create metadata
    let mut metadata = MzPeakMetadata::new();
    metadata.sdrf = Some(SdrfMetadata::new("test_sample"));
    metadata.run_parameters = Some(RunParameters::new());
    metadata.source_file = Some(SourceFileInfo::new("test.raw"));

    // Create writer
    let config = WriterConfig::default();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, config).unwrap();

    // Create and write spectra
    let spectra: Vec<_> = (0..100)
        .map(|i| {
            let mut builder = SpectrumBuilder::new(i, i + 1)
                .ms_level(if i % 10 == 0 { 1 } else { 2 })
                .retention_time((i as f32) * 0.5)
                .polarity(1);

            // Add precursor for MS2
            if i % 10 != 0 {
                builder = builder.precursor(500.0 + (i as f64) * 0.1, Some(2), Some(1e6));
            }

            // Add peaks
            for j in 0..50 {
                builder = builder.add_peak(100.0 + (j as f64) * 10.0, 1000.0 + (j as f32) * 100.0);
            }

            builder.build()
        })
        .collect();

    writer.write_spectra(&spectra).unwrap();
    let stats = writer.finish().unwrap();

    // Verify write statistics
    assert_eq!(stats.spectra_written, 100);
    assert_eq!(stats.peaks_written, 5000);

    // Read and verify the file
    let file = File::open(&path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();

    // Verify schema (18 original columns + 3 MSI spatial columns = 21)
    assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 21);

    // Verify row count
    assert_eq!(metadata.file_metadata().num_rows(), 5000);

    // Verify key-value metadata exists
    let kv = metadata.file_metadata().key_value_metadata().unwrap();
    let format_version = kv.iter().find(|kv| kv.key == "mzpeak:format_version");
    assert!(format_version.is_some());
}

/// Test writing empty file
#[test]
fn test_empty_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty.mzpeak.parquet");

    let metadata = MzPeakMetadata::new();
    let writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default()).unwrap();

    let stats = writer.finish().unwrap();
    assert_eq!(stats.spectra_written, 0);
    assert_eq!(stats.peaks_written, 0);
}

/// Test large batch writes
#[test]
fn test_large_batch() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("large.mzpeak.parquet");

    let metadata = MzPeakMetadata::new();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default()).unwrap();

    // Create many spectra with many peaks
    let spectra: Vec<_> = (0..1000)
        .map(|i| {
            let peaks: Vec<Peak> = (0..100)
                .map(|j| Peak {
                    mz: 100.0 + (j as f64),
                    intensity: 1000.0,
                    ion_mobility: None,
                })
                .collect();

            SpectrumBuilder::new(i, i + 1)
                .ms_level(1)
                .retention_time((i as f32) * 0.1)
                .polarity(1)
                .peaks(peaks)
                .build()
        })
        .collect();

    writer.write_spectra(&spectra).unwrap();
    let stats = writer.finish().unwrap();

    assert_eq!(stats.spectra_written, 1000);
    assert_eq!(stats.peaks_written, 100_000);
}

/// Test MS2 spectrum with full precursor info
#[test]
fn test_ms2_spectrum() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ms2.mzpeak.parquet");

    let metadata = MzPeakMetadata::new();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default()).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(2)
        .retention_time(60.5)
        .polarity(1)
        .precursor(500.2534, Some(2), Some(1e7))
        .isolation_window(0.7, 0.7)
        .collision_energy(30.0)
        .injection_time(50.5)
        .add_peak(150.1, 10000.0)
        .add_peak(250.2, 20000.0)
        .add_peak(350.3, 5000.0)
        .build();

    assert_eq!(spectrum.ms_level, 2);
    assert_eq!(spectrum.precursor_mz, Some(500.2534));
    assert_eq!(spectrum.precursor_charge, Some(2));
    assert_eq!(spectrum.collision_energy, Some(30.0));

    writer.write_spectrum(&spectrum).unwrap();
    let stats = writer.finish().unwrap();

    assert_eq!(stats.spectra_written, 1);
    assert_eq!(stats.peaks_written, 3);
}

/// Test metadata serialization
#[test]
fn test_metadata_roundtrip() {
    use mzpeak::metadata::{InstrumentConfig, LcConfig};

    let dir = tempdir().unwrap();
    let path = dir.path().join("metadata.mzpeak.parquet");

    // Create comprehensive metadata
    let mut metadata = MzPeakMetadata::new();

    let mut sdrf = SdrfMetadata::new("HeLa_sample_01");
    sdrf.organism = Some("Homo sapiens".to_string());
    sdrf.instrument = Some("Orbitrap Exploris 480".to_string());
    metadata.sdrf = Some(sdrf);

    let mut instrument = InstrumentConfig::new();
    instrument.model = Some("Orbitrap Exploris 480".to_string());
    instrument.vendor = Some("Thermo Fisher Scientific".to_string());
    metadata.instrument = Some(instrument);

    let mut lc = LcConfig::new();
    lc.system_model = Some("Dionex UltiMate 3000".to_string());
    lc.flow_rate_ul_min = Some(300.0);
    metadata.lc_config = Some(lc);

    let mut run_params = RunParameters::new();
    run_params.start_time = Some("2024-01-15T10:00:00Z".to_string());
    run_params.spray_voltage_kv = Some(2.1);
    metadata.run_parameters = Some(run_params);

    let mut source = SourceFileInfo::new("HeLa_01.raw");
    source.path = Some("/data/raw/HeLa_01.raw".to_string());
    source.format = Some("Thermo RAW".to_string());
    metadata.source_file = Some(source);

    // Write file
    let mut writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default()).unwrap();

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(0.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();

    writer.write_spectrum(&spectrum).unwrap();
    writer.finish().unwrap();

    // Read and verify metadata
    let file = File::open(&path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let file_metadata = reader.metadata().file_metadata();

    let kv = file_metadata.key_value_metadata().unwrap();

    // Check SDRF metadata
    let sdrf_kv = kv.iter().find(|kv| kv.key == "mzpeak:sdrf_metadata").unwrap();
    let sdrf_json = sdrf_kv.value.as_ref().unwrap();
    assert!(sdrf_json.contains("HeLa_sample_01"));
    assert!(sdrf_json.contains("Homo sapiens"));

    // Check instrument config
    let inst_kv = kv.iter().find(|kv| kv.key == "mzpeak:instrument_config").unwrap();
    let inst_json = inst_kv.value.as_ref().unwrap();
    assert!(inst_json.contains("Orbitrap Exploris 480"));

    // Check LC config
    let lc_kv = kv.iter().find(|kv| kv.key == "mzpeak:lc_config").unwrap();
    let lc_json = lc_kv.value.as_ref().unwrap();
    assert!(lc_json.contains("Dionex UltiMate 3000"));
}

/// Test Dataset Bundle creation and structure (directory mode)
#[test]
fn test_dataset_bundle_structure() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("test_bundle");

    let mut metadata = MzPeakMetadata::new();
    metadata.sdrf = Some(SdrfMetadata::new("bundle_test"));
    metadata.source_file = Some(SourceFileInfo::new("test.raw"));

    let config = WriterConfig::default();
    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    // Write test data
    let spectra: Vec<_> = (0..50)
        .map(|i| {
            SpectrumBuilder::new(i, i + 1)
                .ms_level(1)
                .retention_time((i as f32) * 0.5)
                .polarity(1)
                .add_peak(400.0 + (i as f64), 10000.0)
                .add_peak(500.0 + (i as f64), 15000.0)
                .build()
        })
        .collect();

    dataset.write_spectra(&spectra).unwrap();
    let stats = dataset.close().unwrap();

    // Verify statistics
    assert_eq!(stats.peak_stats.spectra_written, 50);
    assert_eq!(stats.peak_stats.peaks_written, 100);

    // Verify directory structure exists
    assert!(dataset_path.exists());
    assert!(dataset_path.is_dir());

    // Verify subdirectories
    let peaks_dir = dataset_path.join("peaks");
    let chromatograms_dir = dataset_path.join("chromatograms");

    assert!(peaks_dir.exists());
    assert!(peaks_dir.is_dir());
    assert!(chromatograms_dir.exists());
    assert!(chromatograms_dir.is_dir());

    // Verify metadata.json exists and is valid
    let metadata_json = dataset_path.join("metadata.json");
    assert!(metadata_json.exists());

    let json_content = fs::read_to_string(&metadata_json).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json_content).unwrap();

    assert!(json.get("format_version").is_some());
    assert!(json.get("created").is_some());
    assert!(json.get("converter").is_some());
    assert!(json.get("sdrf").is_some());
    assert!(json.get("source_file").is_some());

    // Verify peaks.parquet exists and is valid
    let peaks_file = peaks_dir.join("peaks.parquet");
    assert!(peaks_file.exists());

    let file = File::open(&peaks_file).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();

    // Verify peak file has correct number of rows
    assert_eq!(parquet_metadata.file_metadata().num_rows(), 100);

    // Verify peak file has correct schema (18 original + 3 MSI = 21)
    assert_eq!(parquet_metadata.file_metadata().schema_descr().num_columns(), 21);

    // Verify peak file has metadata in footer
    let kv = parquet_metadata.file_metadata().key_value_metadata().unwrap();
    let format_version = kv.iter().find(|kv| kv.key == "mzpeak:format_version");
    assert!(format_version.is_some());
}

/// Test Dataset Bundle with comprehensive metadata (directory mode)
#[test]
fn test_dataset_bundle_full_metadata() {
    use mzpeak::metadata::{InstrumentConfig, LcConfig};

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("full_metadata");

    // Create comprehensive metadata
    let mut metadata = MzPeakMetadata::new();

    let mut sdrf = SdrfMetadata::new("sample_01");
    sdrf.organism = Some("Homo sapiens".to_string());
    sdrf.instrument = Some("Orbitrap Exploris 480".to_string());
    metadata.sdrf = Some(sdrf);

    let mut instrument = InstrumentConfig::new();
    instrument.model = Some("Orbitrap Exploris 480".to_string());
    instrument.vendor = Some("Thermo Fisher Scientific".to_string());
    metadata.instrument = Some(instrument);

    let mut lc = LcConfig::new();
    lc.system_model = Some("Dionex UltiMate 3000".to_string());
    lc.flow_rate_ul_min = Some(300.0);
    metadata.lc_config = Some(lc);

    let mut run_params = RunParameters::new();
    run_params.start_time = Some("2024-01-15T10:00:00Z".to_string());
    run_params.spray_voltage_kv = Some(2.1);
    metadata.run_parameters = Some(run_params);

    let mut source = SourceFileInfo::new("sample_01.raw");
    source.path = Some("/data/raw/sample_01.raw".to_string());
    source.format = Some("Thermo RAW".to_string());
    metadata.source_file = Some(source);

    let config = WriterConfig::default();
    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    // Write minimal data
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(0.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();

    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Verify all metadata is present in metadata.json
    let metadata_json = dataset_path.join("metadata.json");
    let json_content = fs::read_to_string(&metadata_json).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json_content).unwrap();

    // Check SDRF
    assert!(json["sdrf"]["source_name"].as_str().unwrap() == "sample_01");
    assert!(json["sdrf"]["organism"].as_str().unwrap() == "Homo sapiens");

    // Check instrument
    assert!(json["instrument"]["model"].as_str().unwrap() == "Orbitrap Exploris 480");
    assert!(json["instrument"]["vendor"].as_str().unwrap() == "Thermo Fisher Scientific");

    // Check LC config
    assert!(json["lc_config"]["system_model"].as_str().unwrap() == "Dionex UltiMate 3000");
    assert!(json["lc_config"]["flow_rate_ul_min"].as_f64().unwrap() == 300.0);

    // Check run parameters
    assert!(json["run_parameters"]["start_time"].as_str().unwrap() == "2024-01-15T10:00:00Z");
    assert!(json["run_parameters"]["spray_voltage_kv"].as_f64().unwrap() == 2.1);

    // Check source file
    assert!(json["source_file"]["name"].as_str().unwrap() == "sample_01.raw");
    assert!(json["source_file"]["path"].as_str().unwrap() == "/data/raw/sample_01.raw");
}

/// Test that Dataset Bundle cannot be created twice at the same location
#[test]
fn test_dataset_bundle_already_exists() {
    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("duplicate.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    // Create first dataset
    let mut dataset1 = MzPeakDatasetWriter::new(&dataset_path, &metadata, config.clone()).unwrap();
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(0.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();
    dataset1.write_spectrum(&spectrum).unwrap();
    dataset1.close().unwrap();

    // Try to create second dataset at same location - should fail
    let result = MzPeakDatasetWriter::new(&dataset_path, &metadata, config);
    assert!(result.is_err());
}

/// Test reading chromatograms from dataset bundle (directory mode)
#[test]
fn test_read_chromatograms_directory() {
    use mzpeak::chromatogram_writer::{Chromatogram, ChromatogramWriter, ChromatogramWriterConfig};
    use mzpeak::reader::MzPeakReader;

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("test_chrom.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    // Write a spectrum
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();
    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Write chromatograms
    let chrom_dir = dataset_path.join("chromatograms");
    fs::create_dir_all(&chrom_dir).unwrap();
    let chrom_path = chrom_dir.join("chromatograms.parquet");

    let chrom_config = ChromatogramWriterConfig::default();
    let mut chrom_writer = ChromatogramWriter::new_file(&chrom_path, &metadata, chrom_config).unwrap();

    let tic = Chromatogram::new(
        "TIC".to_string(),
        "total ion current chromatogram".to_string(),
        vec![10.0, 20.0, 30.0, 40.0],
        vec![1000.0, 2000.0, 1500.0, 1200.0],
    ).unwrap();

    let bpc = Chromatogram::new(
        "BPC".to_string(),
        "base peak chromatogram".to_string(),
        vec![10.0, 20.0, 30.0, 40.0],
        vec![800.0, 1800.0, 1400.0, 1000.0],
    ).unwrap();

    chrom_writer.write_chromatogram(&tic).unwrap();
    chrom_writer.write_chromatogram(&bpc).unwrap();
    chrom_writer.finish().unwrap();

    // Read back using MzPeakReader
    let reader = MzPeakReader::open(&dataset_path).unwrap();
    let chromatograms = reader.read_chromatograms().unwrap();

    assert_eq!(chromatograms.len(), 2);

    // Verify TIC
    assert_eq!(chromatograms[0].chromatogram_id, "TIC");
    assert_eq!(chromatograms[0].chromatogram_type, "total ion current chromatogram");
    assert_eq!(chromatograms[0].time_array.len(), 4);
    assert_eq!(chromatograms[0].time_array[0], 10.0);
    assert_eq!(chromatograms[0].intensity_array[1], 2000.0);

    // Verify BPC
    assert_eq!(chromatograms[1].chromatogram_id, "BPC");
    assert_eq!(chromatograms[1].time_array.len(), 4);
}

/// Test reading mobilograms from dataset bundle (directory mode)
#[test]
fn test_read_mobilograms_directory() {
    use mzpeak::mobilogram_writer::{Mobilogram, MobilogramWriter, MobilogramWriterConfig};
    use mzpeak::reader::MzPeakReader;

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("test_mob.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    // Write a spectrum
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();
    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Write mobilograms
    let mob_dir = dataset_path.join("mobilograms");
    fs::create_dir_all(&mob_dir).unwrap();
    let mob_path = mob_dir.join("mobilograms.parquet");

    let mob_config = MobilogramWriterConfig::default();
    let mut mob_writer = MobilogramWriter::new_file(&mob_path, &metadata, mob_config).unwrap();

    let eim1 = Mobilogram::new(
        "EIM_500.25".to_string(),
        "extracted ion mobilogram".to_string(),
        vec![0.5, 0.6, 0.7, 0.8, 0.9],
        vec![1000.0, 2000.0, 3000.0, 2500.0, 1500.0],
    ).unwrap();

    let eim2 = Mobilogram::new(
        "EIM_600.30".to_string(),
        "extracted ion mobilogram".to_string(),
        vec![0.5, 0.6, 0.7, 0.8, 0.9],
        vec![500.0, 800.0, 1200.0, 900.0, 600.0],
    ).unwrap();

    mob_writer.write_mobilogram(&eim1).unwrap();
    mob_writer.write_mobilogram(&eim2).unwrap();
    mob_writer.finish().unwrap();

    // Read back using MzPeakReader
    let reader = MzPeakReader::open(&dataset_path).unwrap();
    let mobilograms = reader.read_mobilograms().unwrap();

    assert_eq!(mobilograms.len(), 2);

    // Verify EIM1
    assert_eq!(mobilograms[0].mobilogram_id, "EIM_500.25");
    assert_eq!(mobilograms[0].mobilogram_type, "extracted ion mobilogram");
    assert_eq!(mobilograms[0].mobility_array.len(), 5);
    assert_eq!(mobilograms[0].mobility_array[0], 0.5);
    assert_eq!(mobilograms[0].intensity_array[2], 3000.0);

    // Verify EIM2
    assert_eq!(mobilograms[1].mobilogram_id, "EIM_600.30");
    assert_eq!(mobilograms[1].mobility_array.len(), 5);
}

/// Test reading when chromatogram file doesn't exist (should return empty)
#[test]
fn test_read_chromatograms_missing_file() {
    use mzpeak::reader::MzPeakReader;

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("no_chrom.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    // Write only a spectrum, no chromatograms
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();
    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Read back - should get empty chromatograms
    let reader = MzPeakReader::open(&dataset_path).unwrap();
    let chromatograms = reader.read_chromatograms().unwrap();
    assert_eq!(chromatograms.len(), 0);
}

/// Test reading when mobilogram file doesn't exist (should return empty)
#[test]
fn test_read_mobilograms_missing_file() {
    use mzpeak::reader::MzPeakReader;

    let dir = tempdir().unwrap();
    let dataset_path = dir.path().join("no_mob.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut dataset = MzPeakDatasetWriter::new_directory(&dataset_path, &metadata, config).unwrap();

    // Write only a spectrum, no mobilograms
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();
    dataset.write_spectrum(&spectrum).unwrap();
    dataset.close().unwrap();

    // Read back - should get empty mobilograms
    let reader = MzPeakReader::open(&dataset_path).unwrap();
    let mobilograms = reader.read_mobilograms().unwrap();
    assert_eq!(mobilograms.len(), 0);
}

/// Test reading chromatograms from ZIP container
#[test]
fn test_read_chromatograms_zip_container() {
    use mzpeak::chromatogram_writer::{Chromatogram, ChromatogramWriter, ChromatogramWriterConfig};
    use mzpeak::reader::MzPeakReader;
    use mzpeak::dataset::OutputMode;

    let dir = tempdir().unwrap();
    let container_path = dir.path().join("test_with_chrom.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    
    // Create container with chromatograms
    let mut dataset = MzPeakDatasetWriter::new(&container_path, &metadata, config).unwrap();

    // Write a spectrum
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();
    dataset.write_spectrum(&spectrum).unwrap();

    // Write chromatograms
    let tic = Chromatogram::new(
        "TIC".to_string(),
        "total ion current chromatogram".to_string(),
        vec![10.0, 20.0, 30.0],
        vec![1000.0, 2000.0, 1500.0],
    ).unwrap();

    let bpc = Chromatogram::new(
        "BPC".to_string(),
        "base peak chromatogram".to_string(),
        vec![10.0, 20.0, 30.0],
        vec![800.0, 1800.0, 1400.0],
    ).unwrap();

    dataset.write_chromatogram(&tic).unwrap();
    dataset.write_chromatogram(&bpc).unwrap();
    dataset.close().unwrap();

    // Read back from ZIP container
    let reader = MzPeakReader::open(&container_path).unwrap();
    let chromatograms = reader.read_chromatograms().unwrap();

    assert_eq!(chromatograms.len(), 2);
    assert_eq!(chromatograms[0].chromatogram_id, "TIC");
    assert_eq!(chromatograms[0].time_array.len(), 3);
    assert_eq!(chromatograms[1].chromatogram_id, "BPC");
}

/// Test reading mobilograms from ZIP container
#[test]
fn test_read_mobilograms_zip_container() {
    use mzpeak::mobilogram_writer::{Mobilogram, MobilogramWriter, MobilogramWriterConfig};
    use mzpeak::reader::MzPeakReader;

    let dir = tempdir().unwrap();
    let container_path = dir.path().join("test_with_mob.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    
    // Create container with mobilograms
    let mut dataset = MzPeakDatasetWriter::new(&container_path, &metadata, config).unwrap();

    // Write a spectrum
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .build();
    dataset.write_spectrum(&spectrum).unwrap();

    // Write mobilograms
    let eim1 = Mobilogram::new(
        "EIM_500.25".to_string(),
        "extracted ion mobilogram".to_string(),
        vec![0.5, 0.6, 0.7, 0.8],
        vec![1000.0, 2000.0, 3000.0, 2500.0],
    ).unwrap();

    dataset.write_mobilogram(&eim1).unwrap();
    dataset.close().unwrap();

    // Read back from ZIP container
    let reader = MzPeakReader::open(&container_path).unwrap();
    let mobilograms = reader.read_mobilograms().unwrap();

    assert_eq!(mobilograms.len(), 1);
    assert_eq!(mobilograms[0].mobilogram_id, "EIM_500.25");
    assert_eq!(mobilograms[0].mobility_array.len(), 4);
}

/// Test mzML conversion with chromatograms
#[test]
fn test_mzml_conversion_with_chromatograms() {
    use mzpeak::mzml::converter::{ConversionConfig, MzMLConverter};
    use mzpeak::reader::MzPeakReader;
    use std::io::Write;

    let dir = tempdir().unwrap();
    let mzml_path = dir.path().join("test_with_chromatograms.mzML");
    let output_path = dir.path().join("output.mzpeak");

    // Create a minimal valid mzML file with chromatograms
    let mzml_content = r#"<?xml version="1.0" encoding="utf-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <cvList count="1">
    <cv id="MS" fullName="Proteomics Standards Initiative Mass Spectrometry Ontology" version="4.1.0" URI="https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo"/>
  </cvList>
  <fileDescription>
    <fileContent>
      <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
    </fileContent>
  </fileDescription>
  <softwareList count="1">
    <software id="mzpeak-test" version="1.0.0">
      <cvParam cvRef="MS" accession="MS:1000799" name="custom unreleased software tool"/>
    </software>
  </softwareList>
  <instrumentConfigurationList count="1">
    <instrumentConfiguration id="IC1">
      <cvParam cvRef="MS" accession="MS:1000031" name="instrument model"/>
    </instrumentConfiguration>
  </instrumentConfigurationList>
  <dataProcessingList count="1">
    <dataProcessing id="DP1">
      <processingMethod order="1" softwareRef="mzpeak-test">
        <cvParam cvRef="MS" accession="MS:1000035" name="peak picking"/>
      </processingMethod>
    </dataProcessing>
  </dataProcessingList>
  <run id="test_run" defaultInstrumentConfigurationRef="IC1">
    <spectrumList count="2" defaultDataProcessingRef="DP1">
      <spectrum index="0" id="scan=1" defaultArrayLength="3">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000504" name="base peak m/z" value="445.34"/>
        <cvParam cvRef="MS" accession="MS:1000505" name="base peak intensity" value="120000"/>
        <cvParam cvRef="MS" accession="MS:1000285" name="total ion current" value="200000"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="30.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA2kAAAAAAAADsQAAAAAAAAPRA</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA8kAAAAAAAAD0QAAAAAAAAN5A</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
      <spectrum index="1" id="scan=2" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="31.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAAPkAAAAAAAABAQA==</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA8kAAAAAAAADwQA==</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
    <chromatogramList count="2" defaultDataProcessingRef="DP1">
      <chromatogram index="0" id="TIC" defaultArrayLength="3">
        <cvParam cvRef="MS" accession="MS:1000235" name="total ion current chromatogram"/>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000595" name="time array" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA3kAAAAAAAADgQAAAAAAAAOFA</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAACEEAAAAAAAAIQQAAAAAAAAhB</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </chromatogram>
      <chromatogram index="1" id="BPC" defaultArrayLength="3">
        <cvParam cvRef="MS" accession="MS:1000628" name="basepeak chromatogram"/>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000595" name="time array" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA3kAAAAAAAADgQAAAAAAAAOFA</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAACEEAAAAAAAAIQQAAAAAAAAhB</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </chromatogram>
    </chromatogramList>
  </run>
</mzML>"#;

    // Write mzML file
    let mut file = File::create(&mzml_path).unwrap();
    file.write_all(mzml_content.as_bytes()).unwrap();
    drop(file);

    // Convert mzML to mzPeak
    let config = ConversionConfig {
        include_chromatograms: true,
        ..Default::default()
    };
    let converter = MzMLConverter::with_config(config);
    let stats = converter.convert(&mzml_path, &output_path).unwrap();

    // Verify statistics
    assert_eq!(stats.spectra_count, 2, "Should have converted 2 spectra");
    assert_eq!(stats.chromatograms_converted, 2, "Should have converted 2 chromatograms");

    // Read back and verify chromatograms
    let reader = MzPeakReader::open(&output_path).unwrap();
    let chromatograms = reader.read_chromatograms().unwrap();

    assert_eq!(chromatograms.len(), 2, "Should have 2 chromatograms");
    
    // Verify TIC chromatogram
    let tic = chromatograms.iter().find(|c| c.chromatogram_id == "TIC").unwrap();
    assert_eq!(tic.chromatogram_type, "TIC");
    assert_eq!(tic.time_array.len(), 3);
    assert_eq!(tic.intensity_array.len(), 3);
    
    // Verify BPC chromatogram
    let bpc = chromatograms.iter().find(|c| c.chromatogram_id == "BPC").unwrap();
    assert_eq!(bpc.chromatogram_type, "BPC");
    assert_eq!(bpc.time_array.len(), 3);
    assert_eq!(bpc.intensity_array.len(), 3);

    // Verify spectrum reading still works
    let reader = MzPeakReader::open(&output_path).unwrap();
    let spectra = reader.iter_spectra().unwrap();
    assert_eq!(spectra.len(), 2, "Should have 2 spectra");
}
