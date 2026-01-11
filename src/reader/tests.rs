use super::*;
use crate::metadata::MzPeakMetadata;
use crate::writer::{MzPeakWriter, SpectrumBuilder, WriterConfig};
use tempfile::tempdir;

#[test]
fn test_read_write_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("test.parquet");

    // Write some test data
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

    let spectrum1 = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 1000.0)
        .add_peak(500.0, 2000.0)
        .build();

    let spectrum2 = SpectrumBuilder::new(1, 2)
        .ms_level(2)
        .retention_time(65.0)
        .polarity(1)
        .precursor(450.0, Some(2), Some(5000.0))
        .add_peak(200.0, 500.0)
        .add_peak(250.0, 1500.0)
        .add_peak(300.0, 750.0)
        .build();

    writer.write_spectrum(&spectrum1)?;
    writer.write_spectrum(&spectrum2)?;
    writer.finish()?;

    // Read back
    let reader = MzPeakReader::open(&path)?;

    assert_eq!(reader.total_peaks(), 5);

    let spectra = reader.iter_spectra()?;
    assert_eq!(spectra.len(), 2);

    assert_eq!(spectra[0].spectrum_id, 0);
    assert_eq!(spectra[0].peaks.len(), 2);
    assert_eq!(spectra[0].ms_level, 1);

    assert_eq!(spectra[1].spectrum_id, 1);
    assert_eq!(spectra[1].peaks.len(), 3);
    assert_eq!(spectra[1].ms_level, 2);
    assert_eq!(spectra[1].precursor_mz, Some(450.0));

    Ok(())
}

#[test]
fn test_get_spectrum_by_id() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("test.parquet");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

    for i in 0..10 {
        let spectrum = SpectrumBuilder::new(i, i + 1)
            .ms_level(1)
            .retention_time(i as f32 * 10.0)
            .polarity(1)
            .add_peak(400.0 + i as f64, 1000.0)
            .build();
        writer.write_spectrum(&spectrum)?;
    }
    writer.finish()?;

    let reader = MzPeakReader::open(&path)?;

    let spectrum = reader.get_spectrum(5)?.expect("Should find spectrum 5");
    assert_eq!(spectrum.spectrum_id, 5);
    assert_eq!(spectrum.retention_time, 50.0);

    let missing = reader.get_spectrum(100)?;
    assert!(missing.is_none());

    Ok(())
}

#[test]
fn test_file_summary() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("test.parquet");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

    // Write 5 MS1 and 5 MS2 spectra
    for i in 0..10 {
        let ms_level = if i % 2 == 0 { 1 } else { 2 };
        let mut builder = SpectrumBuilder::new(i, i + 1)
            .ms_level(ms_level)
            .retention_time(i as f32 * 10.0)
            .polarity(1)
            .add_peak(400.0 + i as f64 * 100.0, 1000.0);

        if ms_level == 2 {
            builder = builder.precursor(450.0, Some(2), None);
        }

        writer.write_spectrum(&builder.build())?;
    }
    writer.finish()?;

    let reader = MzPeakReader::open(&path)?;
    let summary = reader.summary()?;

    assert_eq!(summary.num_spectra, 10);
    assert_eq!(summary.num_ms1_spectra, 5);
    assert_eq!(summary.num_ms2_spectra, 5);
    assert_eq!(summary.total_peaks, 10);

    Ok(())
}

#[test]
fn test_spectra_by_rt_range() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("test.parquet");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

    for i in 0..10 {
        let spectrum = SpectrumBuilder::new(i, i + 1)
            .ms_level(1)
            .retention_time(i as f32 * 10.0) // 0, 10, 20, ..., 90
            .polarity(1)
            .add_peak(400.0, 1000.0)
            .build();
        writer.write_spectrum(&spectrum)?;
    }
    writer.finish()?;

    let reader = MzPeakReader::open(&path)?;

    // Query RT range 25-55 should get spectra with RT 30, 40, 50
    let spectra = reader.spectra_by_rt_range(25.0, 55.0)?;
    assert_eq!(spectra.len(), 3);
    assert_eq!(spectra[0].retention_time, 30.0);
    assert_eq!(spectra[1].retention_time, 40.0);
    assert_eq!(spectra[2].retention_time, 50.0);

    Ok(())
}
