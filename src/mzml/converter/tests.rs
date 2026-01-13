use super::MzMLConverter;
use super::super::models::*;
use crate::ingest::IngestSpectrumConverter;

#[test]
fn test_spectrum_conversion() {
    let mzml_spectrum = MzMLSpectrum {
        index: 0,
        id: "scan=1".to_string(),
        ms_level: 1,
        polarity: 1,
        retention_time: Some(60.0),
        mz_array: vec![100.0, 200.0, 300.0],
        intensity_array: vec![1000.0, 2000.0, 500.0],
        ..Default::default()
    };

    let converter = MzMLConverter::new();
    let ingest = converter.build_ingest_spectrum(mzml_spectrum);
    let mut contract = IngestSpectrumConverter::new();
    let spectrum = contract
        .convert(ingest)
        .expect("mzML conversion should satisfy ingest contract");

    assert_eq!(spectrum.spectrum_id, 0);
    assert_eq!(spectrum.ms_level, 1);
    assert_eq!(spectrum.polarity, 1);
    assert_eq!(spectrum.retention_time, 60.0);
    assert_eq!(spectrum.peak_count(), 3);
}

#[test]
fn test_contract_sequence_ordering() {
    let mzml_spectrum1 = MzMLSpectrum {
        index: 0,
        id: "scan=1".to_string(),
        ms_level: 1,
        polarity: 1,
        retention_time: Some(60.0),
        mz_array: vec![100.0],
        intensity_array: vec![1000.0],
        ..Default::default()
    };

    let mzml_spectrum2 = MzMLSpectrum {
        index: 1,
        id: "scan=2".to_string(),
        ms_level: 1,
        polarity: 1,
        retention_time: Some(61.0),
        mz_array: vec![110.0],
        intensity_array: vec![900.0],
        ..Default::default()
    };

    let converter = MzMLConverter::new();
    let ingest1 = converter.build_ingest_spectrum(mzml_spectrum1);
    let ingest2 = converter.build_ingest_spectrum(mzml_spectrum2);
    let mut contract = IngestSpectrumConverter::new();

    contract
        .convert(ingest1)
        .expect("mzML conversion should satisfy ingest contract");
    contract
        .convert(ingest2)
        .expect("mzML conversion should satisfy ingest contract");
}

#[test]
fn test_ms2_spectrum_conversion() {
    let mzml_spectrum = MzMLSpectrum {
        index: 1,
        id: "scan=2".to_string(),
        ms_level: 2,
        polarity: 1,
        retention_time: Some(61.0),
        precursors: vec![Precursor {
            selected_ion_mz: Some(500.25),
            selected_ion_charge: Some(2),
            isolation_window_lower: Some(0.8),
            isolation_window_upper: Some(0.8),
            collision_energy: Some(30.0),
            ..Default::default()
        }],
        mz_array: vec![150.0, 250.0],
        intensity_array: vec![500.0, 1000.0],
        ..Default::default()
    };

    let converter = MzMLConverter::new();
    let ingest = converter.build_ingest_spectrum(mzml_spectrum);
    let mut contract = IngestSpectrumConverter::new();
    let spectrum = contract
        .convert(ingest)
        .expect("mzML conversion should satisfy ingest contract");

    assert_eq!(spectrum.ms_level, 2);
    assert_eq!(spectrum.precursor_mz, Some(500.25));
    assert_eq!(spectrum.precursor_charge, Some(2));
    assert_eq!(spectrum.collision_energy, Some(30.0));
}

#[test]
fn test_chromatogram_conversion() {
    let mzml_chrom = MzMLChromatogram {
        index: 0,
        id: "TIC".to_string(),
        default_array_length: 3,
        chromatogram_type: ChromatogramType::TIC,
        time_array: vec![0.0, 1.0, 2.0],
        intensity_array: vec![100.0, 200.0, 150.0],
        precursor_mz: None,
        product_mz: None,
        cv_params: vec![],
    };

    let converter = MzMLConverter::new();
    let chrom = converter.convert_chromatogram(&mzml_chrom).unwrap();

    assert_eq!(chrom.chromatogram_id, "TIC");
    assert_eq!(chrom.chromatogram_type, "TIC");
    assert_eq!(chrom.time_array.len(), 3);
    assert_eq!(chrom.intensity_array.len(), 3);
    assert_eq!(chrom.time_array, vec![0.0, 1.0, 2.0]);
}
