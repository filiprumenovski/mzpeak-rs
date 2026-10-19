use arrow::array::{Array, UInt32Array};
use mzpeak::writer::{PeakArraysV2, PeaksWriterV2, PeaksWriterV2Config, SpectrumMetadata};
use mzpeak::{dataset::MzPeakDatasetWriterV2, schema::manifest::Modality};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::io::Cursor;

#[test]
fn direct_writer_preserves_unsigned_ids_and_arrow_metadata() {
    let mut writer = PeaksWriterV2::new(
        Cursor::new(Vec::new()),
        &PeaksWriterV2Config::default(),
        false,
    )
    .unwrap();
    writer
        .write_peaks(
            u32::MAX,
            &PeakArraysV2::new(vec![123.123456789], vec![456.75]),
        )
        .unwrap();
    let bytes = writer.finish_into_inner().unwrap().into_inner();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    assert_eq!(
        builder.schema().as_ref(),
        mzpeak::schema::create_peaks_schema_v2_arc(false).as_ref()
    );
    let batch = builder.build().unwrap().next().unwrap().unwrap();
    let ids = batch
        .column_by_name("spectrum_id")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    assert!(!ids.is_null(0));
    assert_eq!(ids.value(0), u32::MAX);
}

#[test]
fn invalid_peak_arrays_do_not_advance_dataset_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer =
        MzPeakDatasetWriterV2::new(dir.path().join("contract.mzpeak"), Modality::LcMs, None)
            .unwrap();
    let metadata = SpectrumMetadata::new_ms1(0, Some(1), 0.0, 1, 2);
    let peaks = PeakArraysV2::new(vec![100.0], vec![1.0]);
    assert!(writer.write_spectrum_v2(&metadata, &peaks).is_err());
    assert_eq!(writer.stats(), (0, 0));
    let malformed = PeakArraysV2 {
        mz: vec![100.0, 200.0],
        intensity: vec![1.0],
        ion_mobility: None,
    };
    assert!(writer.write_spectrum_v2(&metadata, &malformed).is_err());
    assert_eq!(writer.stats(), (0, 0));
}

#[test]
fn empty_ion_mobility_spectrum_can_roundtrip_again() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("empty-4d.mzpeak");
    let mut writer = MzPeakDatasetWriterV2::new(&output, Modality::LcImsMs, None).unwrap();
    let metadata = SpectrumMetadata::new_ms1(0, Some(1), 0.0, 1, 0);
    let peaks = PeakArraysV2::with_ion_mobility(vec![], vec![], vec![]);
    writer.write_spectrum_v2(&metadata, &peaks).unwrap();
    writer.close().unwrap();
    let reader = mzpeak::reader::MzPeakReader::open(output).unwrap();
    let spectra = reader.iter_spectra_arrays().unwrap();
    let roundtripped =
        mzpeak::writer::SpectrumV2::try_from_spectrum_arrays(spectra[0].to_owned().unwrap())
            .unwrap();
    assert_eq!(roundtripped.peaks.ion_mobility, Some(vec![]));
}
