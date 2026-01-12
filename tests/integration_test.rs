//! Integration tests for mzPeak
//!
//! These tests verify the full pipeline from data creation to reading.

use mzpeak::dataset::MzPeakDatasetWriter;
use mzpeak::metadata::{MzPeakMetadata, RunParameters, SdrfMetadata, SourceFileInfo};
use mzpeak::reader::MzPeakReader;
use mzpeak::writer::{
    MzPeakWriter, OptionalColumnBuf, PeakArrays, SpectrumArrays, WriterConfig,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::{self, File};
use tempfile::tempdir;

fn peak_arrays_from_pairs(pairs: &[(f64, f32)]) -> PeakArrays {
    let mut mz = Vec::with_capacity(pairs.len());
    let mut intensity = Vec::with_capacity(pairs.len());
    for (mz_value, intensity_value) in pairs {
        mz.push(*mz_value);
        intensity.push(*intensity_value);
    }
    PeakArrays::new(mz, intensity)
}

fn make_ms1_spectrum(
    spectrum_id: i64,
    scan_number: i64,
    retention_time: f32,
    polarity: i8,
    peaks: &[(f64, f32)],
) -> SpectrumArrays {
    let peak_arrays = peak_arrays_from_pairs(peaks);
    SpectrumArrays::new_ms1(spectrum_id, scan_number, retention_time, polarity, peak_arrays)
}

fn make_ms2_spectrum(
    spectrum_id: i64,
    scan_number: i64,
    retention_time: f32,
    polarity: i8,
    precursor_mz: f64,
    peaks: &[(f64, f32)],
) -> SpectrumArrays {
    let peak_arrays = peak_arrays_from_pairs(peaks);
    SpectrumArrays::new_ms2(
        spectrum_id,
        scan_number,
        retention_time,
        polarity,
        precursor_mz,
        peak_arrays,
    )
}

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
            let ms_level = if i % 10 == 0 { 1 } else { 2 };
            let peaks: Vec<(f64, f32)> = (0..50)
                .map(|j| {
                    (
                        100.0 + (j as f64) * 10.0,
                        1000.0 + (j as f32) * 100.0,
                    )
                })
                .collect();

            if ms_level == 1 {
                make_ms1_spectrum(i, i + 1, (i as f32) * 0.5, 1, &peaks)
            } else {
                let mut spectrum =
                    make_ms2_spectrum(i, i + 1, (i as f32) * 0.5, 1, 500.0 + (i as f64) * 0.1, &peaks);
                spectrum.precursor_charge = Some(2);
                spectrum.precursor_intensity = Some(1e6);
                spectrum
            }
        })
        .collect();

    writer.write_spectra_arrays(&spectra).unwrap();
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

/// Test SoA write/read cycle with SpectrumArrays
#[test]
fn test_write_read_cycle_arrays() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("soa.mzpeak.parquet");

    let metadata = MzPeakMetadata::new();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default()).unwrap();

    let spectrum1 = SpectrumArrays::new_ms1(
        0,
        1,
        10.0,
        1,
        PeakArrays::new(vec![100.0, 200.0], vec![10.0, 20.0]),
    );

    let mut peaks2 = PeakArrays::new(vec![150.0, 250.0, 350.0], vec![15.0, 25.0, 35.0]);
    peaks2.ion_mobility = OptionalColumnBuf::WithValidity {
        values: vec![1.1, 1.2, 1.3],
        validity: vec![true, false, true],
    };
    let mut spectrum2 = SpectrumArrays::new_ms2(1, 2, 11.0, 1, 500.0, peaks2);
    spectrum2.precursor_charge = Some(2);
    spectrum2.precursor_intensity = Some(1e5);

    let spectra = vec![spectrum1, spectrum2];
    writer.write_spectra_arrays(&spectra).unwrap();
    let stats = writer.finish().unwrap();

    assert_eq!(stats.spectra_written, 2);
    assert_eq!(stats.peaks_written, 5);

    let reader = MzPeakReader::open(&path).unwrap();

    let spectrum_view = reader.get_spectrum_arrays(1).unwrap().unwrap();
    assert_eq!(spectrum_view.ms_level, 2);
    assert_eq!(spectrum_view.precursor_charge, Some(2));
    let spectrum = spectrum_view.to_owned().unwrap();
    assert_eq!(spectrum.peaks.mz.len(), 3);

    let expected_mz = [150.0, 250.0, 350.0];
    for (idx, (actual, expected)) in spectrum
        .peaks
        .mz
        .iter()
        .zip(expected_mz.iter())
        .enumerate()
    {
        assert!(
            (actual - expected).abs() < 1e-6,
            "mz mismatch at {}: actual {} expected {}",
            idx,
            actual,
            expected
        );
    }
    let expected_intensity = [15.0_f32, 25.0, 35.0];
    for (idx, (actual, expected)) in spectrum
        .peaks
        .intensity
        .iter()
        .zip(expected_intensity.iter())
        .enumerate()
    {
        assert!(
            (actual - expected).abs() < 1e-3,
            "intensity mismatch at {}: actual {} expected {}",
            idx,
            actual,
            expected
        );
    }

    match spectrum.peaks.ion_mobility {
        OptionalColumnBuf::WithValidity { values, validity } => {
            let actual: Vec<Option<f64>> = values
                .iter()
                .zip(validity.iter())
                .map(|(value, is_valid)| if *is_valid { Some(*value) } else { None })
                .collect();
            assert_eq!(actual, vec![Some(1.1), None, Some(1.3)]);
            assert_eq!(validity, vec![true, false, true]);
        }
        other => panic!("unexpected ion mobility layout: {:?}", other),
    }
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
            let mut mz = Vec::with_capacity(100);
            let mut intensity = Vec::with_capacity(100);
            for j in 0..100 {
                mz.push(100.0 + (j as f64));
                intensity.push(1000.0);
            }
            let peaks = PeakArrays::new(mz, intensity);
            SpectrumArrays::new_ms1(i, i + 1, (i as f32) * 0.1, 1, peaks)
        })
        .collect();

    writer.write_spectra_arrays(&spectra).unwrap();
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

    let mut spectrum = make_ms2_spectrum(
        0,
        1,
        60.5,
        1,
        500.2534,
        &[(150.1, 10000.0), (250.2, 20000.0), (350.3, 5000.0)],
    );
    spectrum.precursor_charge = Some(2);
    spectrum.precursor_intensity = Some(1e7);
    spectrum.isolation_window_lower = Some(0.7);
    spectrum.isolation_window_upper = Some(0.7);
    spectrum.collision_energy = Some(30.0);
    spectrum.injection_time = Some(50.5);

    assert_eq!(spectrum.ms_level, 2);
    assert_eq!(spectrum.precursor_mz, Some(500.2534));
    assert_eq!(spectrum.precursor_charge, Some(2));
    assert_eq!(spectrum.collision_energy, Some(30.0));

    writer.write_spectrum_arrays(&spectrum).unwrap();
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

    let spectrum = make_ms1_spectrum(0, 1, 0.0, 1, &[(400.0, 10000.0)]);

    writer.write_spectrum_arrays(&spectrum).unwrap();
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
            make_ms1_spectrum(
                i,
                i + 1,
                (i as f32) * 0.5,
                1,
                &[
                    (400.0 + (i as f64), 10000.0),
                    (500.0 + (i as f64), 15000.0),
                ],
            )
        })
        .collect();

    dataset.write_spectra_arrays(&spectra).unwrap();
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
    let spectrum = make_ms1_spectrum(0, 1, 0.0, 1, &[(400.0, 10000.0)]);

    dataset.write_spectrum_arrays(&spectrum).unwrap();
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
    let spectrum = make_ms1_spectrum(0, 1, 0.0, 1, &[(400.0, 10000.0)]);
    dataset1.write_spectrum_arrays(&spectrum).unwrap();
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
    let spectrum = make_ms1_spectrum(0, 1, 60.0, 1, &[(400.0, 10000.0)]);
    dataset.write_spectrum_arrays(&spectrum).unwrap();
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
    let spectrum = make_ms1_spectrum(0, 1, 60.0, 1, &[(400.0, 10000.0)]);
    dataset.write_spectrum_arrays(&spectrum).unwrap();
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
    let spectrum = make_ms1_spectrum(0, 1, 60.0, 1, &[(400.0, 10000.0)]);
    dataset.write_spectrum_arrays(&spectrum).unwrap();
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
    let spectrum = make_ms1_spectrum(0, 1, 60.0, 1, &[(400.0, 10000.0)]);
    dataset.write_spectrum_arrays(&spectrum).unwrap();
    dataset.close().unwrap();

    // Read back - should get empty mobilograms
    let reader = MzPeakReader::open(&dataset_path).unwrap();
    let mobilograms = reader.read_mobilograms().unwrap();
    assert_eq!(mobilograms.len(), 0);
}

/// Test reading chromatograms from ZIP container
#[test]
fn test_read_chromatograms_zip_container() {
    use mzpeak::chromatogram_writer::Chromatogram;
    use mzpeak::reader::MzPeakReader;

    let dir = tempdir().unwrap();
    let container_path = dir.path().join("test_with_chrom.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    
    // Create container with chromatograms
    let mut dataset = MzPeakDatasetWriter::new(&container_path, &metadata, config).unwrap();

    // Write a spectrum
    let spectrum = make_ms1_spectrum(0, 1, 60.0, 1, &[(400.0, 10000.0)]);
    dataset.write_spectrum_arrays(&spectrum).unwrap();

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
    use mzpeak::mobilogram_writer::Mobilogram;
    use mzpeak::reader::MzPeakReader;

    let dir = tempdir().unwrap();
    let container_path = dir.path().join("test_with_mob.mzpeak");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    
    // Create container with mobilograms
    let mut dataset = MzPeakDatasetWriter::new(&container_path, &metadata, config).unwrap();

    // Write a spectrum
    let spectrum = make_ms1_spectrum(0, 1, 60.0, 1, &[(400.0, 10000.0)]);
    dataset.write_spectrum_arrays(&spectrum).unwrap();

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
    let spectra = reader.iter_spectra_arrays().unwrap();
    assert_eq!(spectra.len(), 2, "Should have 2 spectra");
}

// ==================== Property Testing ====================

use proptest::prelude::*;

/// Generate arbitrary peak triplets (mz, intensity, ion mobility).
fn arb_peak() -> impl Strategy<Value = (f64, f32, Option<f64>)> {
    (
        100.0..3000.0_f64,              // mz range
        0.0..1_000_000.0_f32,           // intensity range
        prop::option::of(0.0..2.0_f64), // optional ion mobility
    )
}

/// Generate arbitrary SpectrumArrays data - split into nested tuples to avoid 12-element limit.
fn arb_spectrum() -> impl Strategy<Value = SpectrumArrays> {
    // Core fields (under 12 elements)
    let core_fields = (
        0..1_000_000_i64,                          // spectrum_id
        1..1_000_000_i64,                          // scan_number
        1..11_i16,                                 // ms_level (1-10)
        0.0..10_000.0_f32,                         // retention_time
        prop::bool::ANY.prop_map(|b| if b { 1 } else { -1 }), // polarity
        prop::collection::vec(arb_peak(), 1..50),  // peaks (1-50 per spectrum)
    );

    // Optional precursor fields (under 12 elements)
    let precursor_fields = (
        prop::option::of(200.0..2000.0_f64),    // precursor_mz
        prop::option::of(1..10_i16),            // precursor_charge
        prop::option::of(0.0..1_000_000.0_f32), // precursor_intensity
        prop::option::of(0.0..10.0_f32),        // isolation_window_lower
        prop::option::of(0.0..10.0_f32),        // isolation_window_upper
        prop::option::of(0.0..100.0_f32),       // collision_energy
        prop::option::of(0.0..1000.0_f32),      // injection_time
    );

    (core_fields, precursor_fields).prop_map(|(core, precursor)| {
        let (spectrum_id, scan_number, ms_level, retention_time, polarity, peaks) = core;
        let (
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            isolation_window_lower,
            isolation_window_upper,
            collision_energy,
            injection_time,
        ) = precursor;

        let mut mz = Vec::with_capacity(peaks.len());
        let mut intensity = Vec::with_capacity(peaks.len());
        let mut ion_values = Vec::with_capacity(peaks.len());
        let mut validity = Vec::with_capacity(peaks.len());
        let mut has_any = false;
        let mut all_present = true;

        for (mz_value, intensity_value, ion_mobility) in peaks {
            mz.push(mz_value);
            intensity.push(intensity_value);
            match ion_mobility {
                Some(v) => {
                    ion_values.push(v);
                    validity.push(true);
                    has_any = true;
                }
                None => {
                    ion_values.push(0.0);
                    validity.push(false);
                    all_present = false;
                }
            }
        }

        let ion_mobility = if !has_any {
            OptionalColumnBuf::all_null(mz.len())
        } else if all_present {
            OptionalColumnBuf::AllPresent(ion_values)
        } else {
            OptionalColumnBuf::WithValidity {
                values: ion_values,
                validity,
            }
        };

        SpectrumArrays {
            spectrum_id,
            scan_number,
            ms_level,
            retention_time,
            polarity,
            precursor_mz,
            precursor_charge,
            precursor_intensity,
            isolation_window_lower,
            isolation_window_upper,
            collision_energy,
            total_ion_current: None,
            base_peak_mz: None,
            base_peak_intensity: None,
            injection_time,
            pixel_x: None,
            pixel_y: None,
            pixel_z: None,
            peaks: PeakArrays {
                mz,
                intensity,
                ion_mobility,
            },
        }
    })
}

fn expand_optional_f64(column: &OptionalColumnBuf<f64>) -> Vec<Option<f64>> {
    match column {
        OptionalColumnBuf::AllNull { len } => vec![None; *len],
        OptionalColumnBuf::AllPresent(values) => values.iter().copied().map(Some).collect(),
        OptionalColumnBuf::WithValidity { values, validity } => values
            .iter()
            .zip(validity.iter())
            .map(|(value, is_valid)| if *is_valid { Some(*value) } else { None })
            .collect(),
    }
}

/// Test that all 10 Parquet footer metadata keys are written correctly
#[test]
fn test_all_footer_metadata_keys() {
    use mzpeak::metadata::{InstrumentConfig, LcConfig, ProcessingHistory, ProcessingStep};
    use std::collections::HashMap;

    let dir = tempdir().unwrap();
    let path = dir.path().join("full_metadata.mzpeak.parquet");

    // Create comprehensive metadata with ALL fields populated
    let mut metadata = MzPeakMetadata::new();

    // SDRF metadata
    let mut sdrf = SdrfMetadata::new("Sample_001");
    sdrf.organism = Some("Homo sapiens".to_string());
    sdrf.instrument = Some("Q Exactive HF".to_string());
    metadata.sdrf = Some(sdrf);

    // Instrument config
    let mut instrument = InstrumentConfig::new();
    instrument.model = Some("Q Exactive HF".to_string());
    instrument.vendor = Some("Thermo Fisher Scientific".to_string());
    instrument.serial_number = Some("SN12345".to_string());
    metadata.instrument = Some(instrument);

    // LC config
    let mut lc = LcConfig::new();
    lc.system_model = Some("Dionex UltiMate 3000".to_string());
    lc.flow_rate_ul_min = Some(300.0);
    metadata.lc_config = Some(lc);

    // Run parameters
    let mut run_params = RunParameters::new();
    run_params.start_time = Some("2024-01-15T10:00:00Z".to_string());
    run_params.spray_voltage_kv = Some(2.1);
    run_params.operator = Some("Test Operator".to_string());
    metadata.run_parameters = Some(run_params);

    // Source file
    let mut source = SourceFileInfo::new("sample.raw");
    source.path = Some("/data/raw/sample.raw".to_string());
    source.format = Some("Thermo RAW".to_string());
    source.sha256 = Some("abc123def456".to_string());
    metadata.source_file = Some(source);

    // Processing history
    let mut history = ProcessingHistory::new();
    let mut params = HashMap::new();
    params.insert("peak_picking_algorithm".to_string(), "centroid".to_string());
    history.add_step(ProcessingStep {
        order: 1,
        software: "mzpeak-rs".to_string(),
        version: Some("0.1.0".to_string()),
        processing_type: "conversion".to_string(),
        timestamp: Some("2024-01-15T10:30:00Z".to_string()),
        parameters: params,
        cv_params: Default::default(),
    });
    metadata.processing_history = Some(history);

    // Raw file checksum (new field)
    metadata.raw_file_checksum = Some("sha256:abcdef1234567890".to_string());

    // Write file
    let mut writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default()).unwrap();
    let spectrum = make_ms1_spectrum(0, 1, 0.0, 1, &[(400.0, 10000.0)]);
    writer.write_spectrum_arrays(&spectrum).unwrap();
    writer.finish().unwrap();

    // Read and verify ALL metadata keys are present
    let file = File::open(&path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let file_metadata = reader.metadata().file_metadata();
    let kv = file_metadata.key_value_metadata().unwrap();

    // Helper to find key
    let find_key = |key: &str| -> Option<String> {
        kv.iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.clone())
    };

    // 1. mzpeak:format_version (always present)
    let format_version = find_key("mzpeak:format_version");
    assert!(format_version.is_some(), "mzpeak:format_version missing");
    assert_eq!(format_version.unwrap(), "1.0.0");

    // 2. mzpeak:conversion_timestamp (always present)
    let timestamp = find_key("mzpeak:conversion_timestamp");
    assert!(timestamp.is_some(), "mzpeak:conversion_timestamp missing");
    assert!(timestamp.unwrap().contains("T"), "Timestamp should be ISO 8601 format");

    // 3. mzpeak:converter_info (always present)
    let converter_info = find_key("mzpeak:converter_info");
    assert!(converter_info.is_some(), "mzpeak:converter_info missing");
    assert!(converter_info.unwrap().contains("mzpeak-rs"), "Converter info should contain mzpeak-rs");

    // 4. mzpeak:sdrf_metadata
    let sdrf_json = find_key("mzpeak:sdrf_metadata");
    assert!(sdrf_json.is_some(), "mzpeak:sdrf_metadata missing");
    let sdrf_str = sdrf_json.unwrap();
    assert!(sdrf_str.contains("Sample_001"), "SDRF should contain source_name");
    assert!(sdrf_str.contains("Homo sapiens"), "SDRF should contain organism");

    // 5. mzpeak:instrument_config
    let instrument_json = find_key("mzpeak:instrument_config");
    assert!(instrument_json.is_some(), "mzpeak:instrument_config missing");
    let instrument_str = instrument_json.unwrap();
    assert!(instrument_str.contains("Q Exactive HF"), "Instrument should contain model");
    assert!(instrument_str.contains("SN12345"), "Instrument should contain serial number");

    // 6. mzpeak:lc_config
    let lc_json = find_key("mzpeak:lc_config");
    assert!(lc_json.is_some(), "mzpeak:lc_config missing");
    let lc_str = lc_json.unwrap();
    assert!(lc_str.contains("Dionex"), "LC config should contain system model");

    // 7. mzpeak:run_parameters
    let run_json = find_key("mzpeak:run_parameters");
    assert!(run_json.is_some(), "mzpeak:run_parameters missing");
    let run_str = run_json.unwrap();
    assert!(run_str.contains("2024-01-15T10:00:00Z"), "Run params should contain start_time");
    assert!(run_str.contains("Test Operator"), "Run params should contain operator");

    // 8. mzpeak:source_file
    let source_json = find_key("mzpeak:source_file");
    assert!(source_json.is_some(), "mzpeak:source_file missing");
    let source_str = source_json.unwrap();
    assert!(source_str.contains("sample.raw"), "Source file should contain name");
    assert!(source_str.contains("Thermo RAW"), "Source file should contain format");

    // 9. mzpeak:processing_history
    let history_json = find_key("mzpeak:processing_history");
    assert!(history_json.is_some(), "mzpeak:processing_history missing");
    let history_str = history_json.unwrap();
    assert!(history_str.contains("conversion"), "Processing history should contain processing_type");
    assert!(history_str.contains("mzpeak-rs"), "Processing history should contain software");

    // 10. mzpeak:raw_file_checksum
    let checksum = find_key("mzpeak:raw_file_checksum");
    assert!(checksum.is_some(), "mzpeak:raw_file_checksum missing");
    assert_eq!(checksum.unwrap(), "sha256:abcdef1234567890");
}

/// Test MzPeakMetadata roundtrip through Parquet footer
#[test]
fn test_metadata_from_parquet_roundtrip() {
    use mzpeak::metadata::{InstrumentConfig, LcConfig, ProcessingHistory, ProcessingStep};
    use mzpeak::reader::MzPeakReader;
    use std::collections::HashMap;

    let dir = tempdir().unwrap();
    let path = dir.path().join("roundtrip_metadata.mzpeak.parquet");

    // Create metadata
    let mut original = MzPeakMetadata::new();
    original.sdrf = Some(SdrfMetadata::new("Roundtrip_Sample"));

    let mut instrument = InstrumentConfig::new();
    instrument.model = Some("Test Instrument".to_string());
    original.instrument = Some(instrument);

    let mut lc = LcConfig::new();
    lc.flow_rate_ul_min = Some(500.0);
    original.lc_config = Some(lc);

    let mut run = RunParameters::new();
    run.spray_voltage_kv = Some(3.5);
    original.run_parameters = Some(run);

    let mut source = SourceFileInfo::new("roundtrip.raw");
    source.sha256 = Some("checksum123".to_string());
    original.source_file = Some(source);

    let mut history = ProcessingHistory::new();
    history.add_step(ProcessingStep {
        order: 1,
        software: "test".to_string(),
        version: Some("1.0".to_string()),
        processing_type: "test_processing".to_string(),
        timestamp: None,
        parameters: HashMap::new(),
        cv_params: Default::default(),
    });
    original.processing_history = Some(history);

    original.raw_file_checksum = Some("sha256:roundtrip_test".to_string());

    // Write file
    let mut writer = MzPeakWriter::new_file(&path, &original, WriterConfig::default()).unwrap();
    let spectrum = make_ms1_spectrum(0, 1, 0.0, 1, &[(100.0, 1000.0)]);
    writer.write_spectrum_arrays(&spectrum).unwrap();
    writer.finish().unwrap();

    // Read back using MzPeakReader
    let reader = MzPeakReader::open(&path).unwrap();
    let file_metadata = reader.metadata();

    // Verify mzpeak_metadata was parsed
    assert!(file_metadata.mzpeak_metadata.is_some(), "mzpeak_metadata should be parsed from footer");
    let parsed = file_metadata.mzpeak_metadata.as_ref().unwrap();

    // Verify each component roundtripped correctly
    assert!(parsed.sdrf.is_some(), "SDRF should roundtrip");
    assert_eq!(parsed.sdrf.as_ref().unwrap().source_name, "Roundtrip_Sample");

    assert!(parsed.instrument.is_some(), "Instrument should roundtrip");
    assert_eq!(parsed.instrument.as_ref().unwrap().model, Some("Test Instrument".to_string()));

    assert!(parsed.lc_config.is_some(), "LC config should roundtrip");
    assert_eq!(parsed.lc_config.as_ref().unwrap().flow_rate_ul_min, Some(500.0));

    assert!(parsed.run_parameters.is_some(), "Run params should roundtrip");
    assert_eq!(parsed.run_parameters.as_ref().unwrap().spray_voltage_kv, Some(3.5));

    assert!(parsed.source_file.is_some(), "Source file should roundtrip");
    assert_eq!(parsed.source_file.as_ref().unwrap().name, "roundtrip.raw");

    assert!(parsed.processing_history.is_some(), "Processing history should roundtrip");
    assert_eq!(parsed.processing_history.as_ref().unwrap().steps.len(), 1);
    assert_eq!(parsed.processing_history.as_ref().unwrap().steps[0].software, "test");

    assert!(parsed.raw_file_checksum.is_some(), "Raw file checksum should roundtrip");
    assert_eq!(parsed.raw_file_checksum.as_ref().unwrap(), "sha256:roundtrip_test");
}

/// Test processing history serialization
#[test]
fn test_processing_history_footer_serialization() {
    use mzpeak::metadata::{ProcessingHistory, ProcessingStep};
    use std::collections::HashMap;

    let dir = tempdir().unwrap();
    let path = dir.path().join("processing_history.mzpeak.parquet");

    // Create metadata with complex processing history
    let mut metadata = MzPeakMetadata::new();
    let mut history = ProcessingHistory::new();

    // First step: conversion
    let mut params1 = HashMap::new();
    params1.insert("input_format".to_string(), "mzML".to_string());
    params1.insert("output_format".to_string(), "mzPeak".to_string());
    history.add_step(ProcessingStep {
        order: 1,
        software: "mzpeak-rs".to_string(),
        version: Some("0.1.0".to_string()),
        processing_type: "conversion".to_string(),
        timestamp: Some("2024-01-15T10:00:00Z".to_string()),
        parameters: params1,
        cv_params: Default::default(),
    });

    // Second step: peak picking
    let mut params2 = HashMap::new();
    params2.insert("algorithm".to_string(), "centroid".to_string());
    params2.insert("snr_threshold".to_string(), "3.0".to_string());
    history.add_step(ProcessingStep {
        order: 2,
        software: "msconvert".to_string(),
        version: Some("3.0.0".to_string()),
        processing_type: "peak picking".to_string(),
        timestamp: Some("2024-01-15T09:00:00Z".to_string()),
        parameters: params2,
        cv_params: Default::default(),
    });

    metadata.processing_history = Some(history);

    // Write file
    let mut writer = MzPeakWriter::new_file(&path, &metadata, WriterConfig::default()).unwrap();
    let spectrum = make_ms1_spectrum(0, 1, 0.0, 1, &[(100.0, 1000.0)]);
    writer.write_spectrum_arrays(&spectrum).unwrap();
    writer.finish().unwrap();

    // Read and verify processing history
    let file = File::open(&path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let file_metadata = reader.metadata().file_metadata();
    let kv = file_metadata.key_value_metadata().unwrap();

    let history_json = kv.iter()
        .find(|kv| kv.key == "mzpeak:processing_history")
        .and_then(|kv| kv.value.clone())
        .expect("processing_history should be present");

    // Verify JSON contains both steps
    assert!(history_json.contains("conversion"), "Should contain conversion step");
    assert!(history_json.contains("peak picking"), "Should contain peak picking step");
    assert!(history_json.contains("mzpeak-rs"), "Should contain mzpeak-rs software");
    assert!(history_json.contains("msconvert"), "Should contain msconvert software");
    assert!(history_json.contains("centroid"), "Should contain algorithm parameter");
    assert!(history_json.contains("snr_threshold"), "Should contain snr_threshold parameter");

    // Parse back and verify structure
    let parsed_history: ProcessingHistory = serde_json::from_str(&history_json).unwrap();
    assert_eq!(parsed_history.steps.len(), 2, "Should have 2 processing steps");
    assert_eq!(parsed_history.steps[0].order, 1);
    assert_eq!(parsed_history.steps[0].processing_type, "conversion");
    assert_eq!(parsed_history.steps[1].order, 2);
    assert_eq!(parsed_history.steps[1].processing_type, "peak picking");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Property test: Round-trip data integrity
    /// Tests that any generated spectrum can be written and read back with exact equality
    #[test]
    fn property_roundtrip_spectrum(spectra in prop::collection::vec(arb_spectrum(), 1..10)) {
        use mzpeak::reader::MzPeakReader;
        
        let dir = tempdir().unwrap();
        let path = dir.path().join("proptest.mzpeak.parquet");
        
        // Write spectra
        let metadata = MzPeakMetadata::new();
        let config = WriterConfig::default();
        let mut writer = MzPeakWriter::new_file(&path, &metadata, config).unwrap();
        
        writer.write_spectra_arrays(&spectra).unwrap();
        let write_stats = writer.finish().unwrap();
        
        // Calculate expected totals
        let expected_spectra = spectra.len();
        let expected_peaks: usize = spectra.iter().map(|s| s.peak_count()).sum();
        
        prop_assert_eq!(write_stats.spectra_written, expected_spectra, "Spectra count mismatch");
        prop_assert_eq!(write_stats.peaks_written, expected_peaks, "Peak count mismatch");
        
        // Read back
        let reader = MzPeakReader::open(&path).unwrap();
        let read_spectra = reader.iter_spectra_arrays().unwrap();
        
        prop_assert_eq!(read_spectra.len(), spectra.len(), "Read spectrum count mismatch");
        
        // Verify each spectrum
        for (original, read_back) in spectra.iter().zip(read_spectra.iter()) {
            let read_back = read_back.to_owned().unwrap();

            prop_assert_eq!(read_back.spectrum_id, original.spectrum_id, "spectrum_id mismatch");
            prop_assert_eq!(read_back.scan_number, original.scan_number, "scan_number mismatch");
            prop_assert_eq!(read_back.ms_level, original.ms_level, "ms_level mismatch");
            prop_assert_eq!(read_back.retention_time, original.retention_time, "retention_time mismatch");
            prop_assert_eq!(read_back.polarity, original.polarity, "polarity mismatch");
            prop_assert_eq!(read_back.precursor_mz, original.precursor_mz, "precursor_mz mismatch");
            prop_assert_eq!(read_back.precursor_charge, original.precursor_charge, "precursor_charge mismatch");
            prop_assert_eq!(read_back.precursor_intensity, original.precursor_intensity, "precursor_intensity mismatch");
            prop_assert_eq!(read_back.isolation_window_lower, original.isolation_window_lower, "isolation_window_lower mismatch");
            prop_assert_eq!(read_back.isolation_window_upper, original.isolation_window_upper, "isolation_window_upper mismatch");
            prop_assert_eq!(read_back.collision_energy, original.collision_energy, "collision_energy mismatch");
            prop_assert_eq!(read_back.injection_time, original.injection_time, "injection_time mismatch");

            prop_assert_eq!(read_back.peaks.mz.len(), original.peaks.mz.len(), "peak count mismatch");
            prop_assert_eq!(read_back.peaks.mz, original.peaks.mz.as_slice(), "peak mz mismatch");
            prop_assert_eq!(
                read_back.peaks.intensity,
                original.peaks.intensity.as_slice(),
                "peak intensity mismatch"
            );

            let read_im = expand_optional_f64(&read_back.peaks.ion_mobility);
            let orig_im = expand_optional_f64(&original.peaks.ion_mobility);
            prop_assert_eq!(read_im, orig_im, "peak ion_mobility mismatch");
        }
    }
}
