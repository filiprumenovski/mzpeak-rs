use super::*;
use crate::metadata::MzPeakMetadata;
use crate::writer::{MzPeakWriter, PeakArrays, SpectrumArrays, WriterConfig};
use tempfile::tempdir;

#[test]
fn test_read_write_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("test.parquet");

    // Write some test data
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

    let peaks1 = PeakArrays::new(vec![400.0, 500.0], vec![1000.0, 2000.0]);
    let spectrum1 = SpectrumArrays::new_ms1(0, 1, 60.0, 1, peaks1);

    let peaks2 = PeakArrays::new(vec![200.0, 250.0, 300.0], vec![500.0, 1500.0, 750.0]);
    let mut spectrum2 = SpectrumArrays::new_ms2(1, 2, 65.0, 1, 450.0, peaks2);
    spectrum2.precursor_charge = Some(2);
    spectrum2.precursor_intensity = Some(5000.0);

    writer.write_spectrum_arrays(&spectrum1)?;
    writer.write_spectrum_arrays(&spectrum2)?;
    writer.finish()?;

    // Read back
    let reader = MzPeakReader::open(&path)?;

    assert_eq!(reader.total_peaks(), 5);

    let spectra = reader.iter_spectra_arrays()?;
    assert_eq!(spectra.len(), 2);

    assert_eq!(spectra[0].spectrum_id, 0);
    assert_eq!(spectra[0].peak_count(), 2);
    assert_eq!(spectra[0].ms_level, 1);

    assert_eq!(spectra[1].spectrum_id, 1);
    assert_eq!(spectra[1].peak_count(), 3);
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
        let peaks = PeakArrays::new(vec![400.0 + i as f64], vec![1000.0]);
        let spectrum = SpectrumArrays::new_ms1(i, i + 1, i as f32 * 10.0, 1, peaks);
        writer.write_spectrum_arrays(&spectrum)?;
    }
    writer.finish()?;

    let reader = MzPeakReader::open(&path)?;

    let spectrum = reader
        .get_spectrum_arrays(5)?
        .expect("Should find spectrum 5");
    assert_eq!(spectrum.spectrum_id, 5);
    assert_eq!(spectrum.retention_time, 50.0);

    let missing = reader.get_spectrum_arrays(100)?;
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
        let peaks = PeakArrays::new(vec![400.0 + i as f64 * 100.0], vec![1000.0]);
        let spectrum = if ms_level == 2 {
            let mut ms2 = SpectrumArrays::new_ms2(i, i + 1, i as f32 * 10.0, 1, 450.0, peaks);
            ms2.precursor_charge = Some(2);
            ms2
        } else {
            SpectrumArrays::new_ms1(i, i + 1, i as f32 * 10.0, 1, peaks)
        };

        writer.write_spectrum_arrays(&spectrum)?;
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
        let peaks = PeakArrays::new(vec![400.0], vec![1000.0]);
        let spectrum = SpectrumArrays::new_ms1(i, i + 1, i as f32 * 10.0, 1, peaks);
        writer.write_spectrum_arrays(&spectrum)?;
    }
    writer.finish()?;

    let reader = MzPeakReader::open(&path)?;

    // Query RT range 25-55 should get spectra with RT 30, 40, 50
    let spectra = reader.spectra_by_rt_range_arrays(25.0, 55.0)?;
    assert_eq!(spectra.len(), 3);
    assert_eq!(spectra[0].retention_time, 30.0);
    assert_eq!(spectra[1].retention_time, 40.0);
    assert_eq!(spectra[2].retention_time, 50.0);

    Ok(())
}

#[test]
fn test_spectrum_arrays_view_segments() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().join("test.parquet");

    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();
    let mut writer = MzPeakWriter::new_file(&path, &metadata, config)?;

    let peaks = PeakArrays::new(vec![100.0, 200.0, 300.0], vec![10.0, 20.0, 30.0]);
    let spectrum = SpectrumArrays::new_ms1(0, 1, 10.0, 1, peaks);

    writer.write_spectrum_arrays(&spectrum)?;
    writer.finish()?;

    let reader = MzPeakReader::open_with_config(&path, ReaderConfig { batch_size: 2 })?;
    let mut iter = reader.iter_spectra_arrays_streaming()?;
    let view = iter.next().unwrap()?;

    assert_eq!(view.peak_count(), 3);

    let mz_arrays = view.mz_arrays()?;
    assert!(mz_arrays.len() >= 2);

    let mz: Vec<f64> = mz_arrays
        .iter()
        .flat_map(|array| array.values().iter().copied())
        .collect();
    assert_eq!(mz, vec![100.0, 200.0, 300.0]);

    let owned = view.to_owned()?;
    assert_eq!(owned.peaks.mz, vec![100.0, 200.0, 300.0]);
    assert_eq!(owned.peaks.intensity, vec![10.0, 20.0, 30.0]);

    Ok(())
}
