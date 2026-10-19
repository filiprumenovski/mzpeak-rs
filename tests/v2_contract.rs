use arrow::array::{Array, UInt32Array};
use mzpeak::writer::{PeakArraysV2, PeaksWriterV2, PeaksWriterV2Config, SpectrumMetadata};
use mzpeak::{dataset::MzPeakDatasetWriterV2, schema::manifest::Modality};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::io::Cursor;

#[test]
fn validator_accepts_unsigned_ids_and_ms_levels() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("unsigned.mzpeak");
    let mut writer = MzPeakDatasetWriterV2::new(&output, Modality::LcMs, None).unwrap();
    for id in [u32::MAX - 1, u32::MAX] {
        let mut metadata = SpectrumMetadata::new_ms1(id, Some(1), 0.0, 1, 1);
        metadata.ms_level = 255;
        writer
            .write_spectrum_v2(&metadata, &PeakArraysV2::new(vec![100.0], vec![1.0]))
            .unwrap();
    }
    writer.close().unwrap();
    let report = mzpeak::validator::validate_mzpeak_file(&output).unwrap();
    assert_eq!((report.spectra, report.peaks), (2, 2));
}

#[test]
fn writer_rejects_zero_ms_level_without_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("invalid.mzpeak");
    let mut writer = MzPeakDatasetWriterV2::new(&output, Modality::LcMs, None).unwrap();
    let mut metadata = SpectrumMetadata::new_ms1(0, Some(1), 0.0, 1, 1);
    metadata.ms_level = 0;
    assert!(writer
        .write_spectrum_v2(&metadata, &PeakArraysV2::new(vec![100.0], vec![1.0]))
        .is_err());
    assert_eq!(writer.stats(), (0, 0));
    drop(writer);
    assert!(!output.exists());
}

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
    let roundtripped = spectra[0].to_owned().unwrap();
    assert_eq!(roundtripped.peaks.ion_mobility, Some(vec![]));
}

#[test]
fn invalid_numbers_compression_and_abandoned_writes_publish_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("abandoned.mzpeak");
    let mut writer = MzPeakDatasetWriterV2::new(&output, Modality::LcMs, None).unwrap();
    let metadata = SpectrumMetadata::new_ms1(0, None, 0.0, 1, 1);
    for peaks in [
        PeakArraysV2::new(vec![f64::NAN], vec![1.0]),
        PeakArraysV2::new(vec![100.0], vec![-1.0]),
    ] {
        assert!(writer.write_spectrum_v2(&metadata, &peaks).is_err());
    }
    assert_eq!(writer.stats(), (0, 0));
    writer
        .write_spectrum_v2(&metadata, &PeakArraysV2::new(vec![100.0], vec![1.0]))
        .unwrap();
    assert!(!output.exists());
    drop(writer);
    assert!(!output.exists());
    let config = mzpeak::dataset::DatasetWriterV2Config {
        peaks_config: PeaksWriterV2Config {
            compression: mzpeak::writer::CompressionType::Zstd(100),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(MzPeakDatasetWriterV2::with_config(&output, Modality::LcMs, None, config).is_err());
    assert!(!output.exists());
}

#[test]
fn close_does_not_overwrite_a_destination_created_after_open() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("race.mzpeak");
    let writer = MzPeakDatasetWriterV2::new(&output, Modality::LcMs, None).unwrap();
    std::fs::write(&output, b"other writer").unwrap();
    assert!(writer.close().is_err());
    assert_eq!(std::fs::read(output).unwrap(), b"other writer");
}

#[cfg(feature = "mzml")]
#[test]
fn truncated_or_trailing_xml_does_not_publish_output() {
    let dir = tempfile::tempdir().unwrap();
    for (i, xml) in [
        "<mzML><run><spectrumList count=\"0\"></spectrumList>",
        "<mzML><run><spectrumList count=\"0\"></spectrumList></run></mzML><unexpected/>",
    ]
    .iter()
    .enumerate()
    {
        let input = dir.path().join(format!("input{i}.mzML"));
        let output = dir.path().join(format!("output{i}.mzpeak"));
        std::fs::write(&input, xml).unwrap();
        assert!(mzpeak::mzml::MzMLConverter::new()
            .convert(&input, &output)
            .is_err());
        assert!(!output.exists());
    }
}

#[test]
fn metadata_extensions_survive_without_wrapper_types() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("metadata.mzpeak");
    let mut metadata = mzpeak::metadata::MzPeakMetadata::new();
    metadata.extensions.insert(
        "source_native_metadata".into(),
        serde_json::json!({"instrument":"example","settings":[1,2,3]}),
    );
    let mut writer = MzPeakDatasetWriterV2::new(&output, Modality::LcMs, None).unwrap();
    writer.set_metadata(metadata.clone());
    writer.close().unwrap();
    let reader = mzpeak::reader::MzPeakReader::open(output).unwrap();
    assert_eq!(reader.metadata().extensions, metadata.extensions);
    assert_eq!(reader.iter_spectra_arrays_streaming().unwrap().count(), 0);
}

#[test]
fn mismatched_peak_ids_fail_once_instead_of_becoming_end_of_file() {
    use std::io::{Read, Write};
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.mzpeak");
    let corrupt = dir.path().join("corrupt.mzpeak");
    let mut writer = MzPeakDatasetWriterV2::new(&source, Modality::LcMs, None).unwrap();
    writer
        .write_spectrum_v2(
            &SpectrumMetadata::new_ms1(0, None, 0.0, 1, 1),
            &PeakArraysV2::new(vec![100.0], vec![1.0]),
        )
        .unwrap();
    writer.close().unwrap();
    let mut archive = zip::ZipArchive::new(std::fs::File::open(&source).unwrap()).unwrap();
    let mut data = Vec::new();
    archive
        .by_name("peaks/peaks.parquet")
        .unwrap()
        .read_to_end(&mut data)
        .unwrap();
    let batch = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let mut columns = batch.columns().to_vec();
    columns[0] = std::sync::Arc::new(UInt32Array::from(vec![17]));
    let batch = arrow::record_batch::RecordBatch::try_new(batch.schema(), columns).unwrap();
    let mut parquet =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None).unwrap();
    parquet.write(&batch).unwrap();
    let changed = parquet.into_inner().unwrap();
    let mut zip = zip::ZipWriter::new(std::fs::File::create(&corrupt).unwrap());
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).unwrap();
        let name = entry.name().to_owned();
        zip.start_file(
            &name,
            zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored),
        )
        .unwrap();
        if name == "peaks/peaks.parquet" {
            zip.write_all(&changed).unwrap();
        } else {
            std::io::copy(&mut entry, &mut zip).unwrap();
        }
    }
    zip.finish().unwrap();
    let reader = mzpeak::reader::MzPeakReader::open(&corrupt).unwrap();
    let mut spectra = reader.iter_spectra_arrays_streaming().unwrap();
    assert!(spectra.next().unwrap().is_err());
    assert!(spectra.next().is_none());
    assert!(mzpeak::validator::validate_mzpeak_file(corrupt).is_err());
}

#[test]
fn ignored_chromatogram_error_prevents_publishing() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("partial.mzpeak");
    let mut writer = MzPeakDatasetWriterV2::new(&output, Modality::LcMs, None).unwrap();
    let invalid = mzpeak::chromatogram_writer::Chromatogram {
        chromatogram_id: "TIC".into(),
        chromatogram_type: "TIC".into(),
        time_array: vec![0.0],
        intensity_array: vec![],
    };
    assert!(writer.write_chromatogram(&invalid).is_err());
    assert!(writer.close().is_err());
    assert!(!output.exists());
}
