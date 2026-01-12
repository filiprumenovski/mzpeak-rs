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
