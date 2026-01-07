//! Integration tests for mzPeak
//!
//! These tests verify the full pipeline from data creation to reading.

use mzpeak::metadata::{MzPeakMetadata, RunParameters, SdrfMetadata, SourceFileInfo};
use mzpeak::writer::{MzPeakWriter, Peak, SpectrumBuilder, WriterConfig};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
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

    // Verify schema
    assert_eq!(metadata.file_metadata().schema_descr().num_columns(), 17);

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
    let mut writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default()).unwrap();

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
