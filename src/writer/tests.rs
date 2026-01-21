use super::*;
use crate::metadata::MzPeakMetadata;
use std::io::Cursor;

#[test]
fn test_spectrum_arrays_statistics() {
    let peaks = PeakArrays::new(
        vec![100.0, 200.0, 300.0],
        vec![1000.0, 2000.0, 500.0],
    );
    let mut spectrum = SpectrumArrays::new_ms2(0, 1, 100.5, 1, 500.25, peaks);
    spectrum.precursor_charge = Some(2);
    spectrum.precursor_intensity = Some(1e6);
    spectrum.collision_energy = Some(30.0);
    spectrum.compute_statistics();

    assert_eq!(spectrum.spectrum_id, 0);
    assert_eq!(spectrum.ms_level, 2);
    assert_eq!(spectrum.peak_count(), 3);
    assert!(spectrum.total_ion_current.is_some());
    assert_eq!(spectrum.base_peak_intensity, Some(2000.0));
}

#[test]
fn test_write_spectrum_arrays() -> Result<(), WriterError> {
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let buffer = Cursor::new(Vec::new());
    let mut writer = MzPeakWriter::new(buffer, &metadata, config)?;

    let peaks = PeakArrays::new(vec![400.0, 500.0], vec![10000.0, 20000.0]);
    let spectrum = SpectrumArrays::new_ms1(0, 1, 60.0, 1, peaks);

    writer.write_spectrum_arrays(&spectrum)?;

    let stats = writer.finish()?;
    assert_eq!(stats.spectra_written, 1);
    assert_eq!(stats.peaks_written, 2);

    Ok(())
}

#[test]
fn test_write_owned_batch() -> Result<(), WriterError> {
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let buffer = Cursor::new(Vec::new());
    let mut writer = MzPeakWriter::new(buffer, &metadata, config)?;

    // Create owned batch with required columns only
    let batch = OwnedColumnarBatch::new(
        vec![100.0, 200.0, 300.0],     // mz
        vec![1000.0, 2000.0, 500.0],   // intensity
        vec![0, 0, 0],                  // spectrum_id
        vec![1, 1, 1],                  // scan_number
        vec![1, 1, 1],                  // ms_level
        vec![60.0, 60.0, 60.0],         // retention_time
        vec![1, 1, 1],                  // polarity
    );

    // The batch is consumed - ownership is transferred
    writer.write_owned_batch(batch)?;

    let stats = writer.finish()?;
    assert_eq!(stats.peaks_written, 3);

    Ok(())
}

#[test]
fn test_write_owned_batch_with_optional_columns() -> Result<(), WriterError> {
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let buffer = Cursor::new(Vec::new());
    let mut writer = MzPeakWriter::new(buffer, &metadata, config)?;

    // Create owned batch with some optional columns
    let mut batch = OwnedColumnarBatch::new(
        vec![100.0, 200.0, 300.0, 400.0],   // mz
        vec![1000.0, 2000.0, 500.0, 3000.0], // intensity
        vec![0, 0, 1, 1],                    // spectrum_id (2 spectra)
        vec![1, 1, 2, 2],                    // scan_number
        vec![1, 1, 2, 2],                    // ms_level (MS1 and MS2)
        vec![60.0, 60.0, 120.0, 120.0],     // retention_time
        vec![1, 1, 1, 1],                    // polarity
    );

    // Set optional columns with mixed validity
    batch.precursor_mz = OptionalColumnBuf::WithValidity {
        values: vec![0.0, 0.0, 500.25, 500.25],
        validity: vec![false, false, true, true],
    };
    batch.precursor_charge = OptionalColumnBuf::WithValidity {
        values: vec![0, 0, 2, 2],
        validity: vec![false, false, true, true],
    };

    writer.write_owned_batch(batch)?;

    let stats = writer.finish()?;
    assert_eq!(stats.peaks_written, 4);

    Ok(())
}

#[test]
fn test_owned_columnar_batch_as_columnar_batch() {
    // Test that we can borrow an OwnedColumnarBatch as a ColumnarBatch view
    let owned = OwnedColumnarBatch::new(
        vec![100.0, 200.0],
        vec![1000.0, 2000.0],
        vec![0, 0],
        vec![1, 1],
        vec![1, 1],
        vec![60.0, 60.0],
        vec![1, 1],
    );

    let borrowed = owned.as_columnar_batch();
    assert_eq!(borrowed.len(), 2);
    assert_eq!(borrowed.mz, &[100.0, 200.0]);
    assert_eq!(borrowed.intensity, &[1000.0, 2000.0]);
}

#[test]
fn test_spectrum_v2_try_from_rejects_mixed_ion_mobility() {
    let mut peaks = PeakArrays::new(vec![100.0, 200.0], vec![1000.0, 2000.0]);
    peaks.ion_mobility = OptionalColumnBuf::WithValidity {
        values: vec![1.0, 2.0],
        validity: vec![true, false],
    };
    let spectrum = SpectrumArrays::new_ms1(0, 1, 10.0, 1, peaks);

    let result = SpectrumV2::try_from_spectrum_arrays(spectrum);
    assert!(result.is_err());
}

#[test]
fn test_spectrum_v2_try_from_range_checks() {
    let peaks = PeakArrays::new(vec![100.0], vec![1000.0]);
    let mut spectrum = SpectrumArrays::new_ms1(i64::MAX, 1, 10.0, 1, peaks);
    spectrum.precursor_charge = Some(200);

    let result = SpectrumV2::try_from_spectrum_arrays(spectrum);
    assert!(result.is_err());
}
