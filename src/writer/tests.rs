use super::*;
use crate::metadata::MzPeakMetadata;
use std::io::Cursor;

#[test]
fn test_spectrum_builder() {
    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(2)
        .retention_time(100.5)
        .polarity(1)
        .precursor(500.25, Some(2), Some(1e6))
        .collision_energy(30.0)
        .add_peak(100.0, 1000.0)
        .add_peak(200.0, 2000.0)
        .add_peak(300.0, 500.0)
        .build();

    assert_eq!(spectrum.spectrum_id, 0);
    assert_eq!(spectrum.ms_level, 2);
    assert_eq!(spectrum.peaks.len(), 3);
    assert!(spectrum.total_ion_current.is_some());
    assert_eq!(spectrum.base_peak_intensity, Some(2000.0));
}

#[test]
fn test_write_spectra() -> Result<(), WriterError> {
    let metadata = MzPeakMetadata::new();
    let config = WriterConfig::default();

    let buffer = Cursor::new(Vec::new());
    let mut writer = MzPeakWriter::new(buffer, &metadata, config)?;

    let spectrum = SpectrumBuilder::new(0, 1)
        .ms_level(1)
        .retention_time(60.0)
        .polarity(1)
        .add_peak(400.0, 10000.0)
        .add_peak(500.0, 20000.0)
        .build();

    writer.write_spectrum(&spectrum)?;

    let stats = writer.finish()?;
    assert_eq!(stats.spectra_written, 1);
    assert_eq!(stats.peaks_written, 2);

    Ok(())
}
