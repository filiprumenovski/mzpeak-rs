use byteorder::{LittleEndian, WriteBytesExt};
use mzpeak::mzml::MzMLConverter;
use mzpeak::reader::MzPeakReader;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

fn append_f64s(buffer: &mut Vec<u8>, values: &[f64]) -> (u64, usize) {
    let offset = buffer.len() as u64;
    for &val in values {
        buffer.write_f64::<LittleEndian>(val).unwrap();
    }
    let length = buffer.len() - offset as usize;
    (offset, length)
}

fn append_f32s(buffer: &mut Vec<u8>, values: &[f32]) -> (u64, usize) {
    let offset = buffer.len() as u64;
    for &val in values {
        buffer.write_f32::<LittleEndian>(val).unwrap();
    }
    let length = buffer.len() - offset as usize;
    (offset, length)
}

#[test]
fn test_imzml_conversion_roundtrip() {
    let dir = tempdir().unwrap();
    let imzml_path = dir.path().join("test.imzML");
    let ibd_path = dir.path().join("test.ibd");
    let output_path = dir.path().join("test.mzpeak");

    let mz1 = [100.0, 200.0, 300.0];
    let intens1 = [10.0_f32, 20.0_f32, 30.0_f32];
    let mz2 = [150.0, 250.0];
    let intens2 = [15.0_f32, 25.0_f32];

    let mut ibd_data = Vec::new();
    let (mz1_offset, mz1_len) = append_f64s(&mut ibd_data, &mz1);
    let (int1_offset, int1_len) = append_f32s(&mut ibd_data, &intens1);
    let (mz2_offset, mz2_len) = append_f64s(&mut ibd_data, &mz2);
    let (int2_offset, int2_len) = append_f32s(&mut ibd_data, &intens2);

    let mut ibd_file = File::create(&ibd_path).unwrap();
    ibd_file.write_all(&ibd_data).unwrap();

    let imzml_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <run id="imzml_run">
    <spectrumList count="2">
      <spectrum index="0" id="scan=1" defaultArrayLength="3">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan"/>
        <scanList count="1">
          <scan>
            <cvParam cvRef="IMS" accession="IMS:1000050" name="position x" value="1"/>
            <cvParam cvRef="IMS" accession="IMS:1000051" name="position y" value="1"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <cvParam cvRef="IMS" accession="IMS:1000103" name="external offset" value="{mz1_offset}"/>
            <cvParam cvRef="IMS" accession="IMS:1000102" name="external array length" value="{mz1_len}"/>
            <binary/>
          </binaryDataArray>
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <cvParam cvRef="IMS" accession="IMS:1000103" name="external offset" value="{int1_offset}"/>
            <cvParam cvRef="IMS" accession="IMS:1000102" name="external array length" value="{int1_len}"/>
            <binary/>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
      <spectrum index="1" id="scan=2" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan"/>
        <scanList count="1">
          <scan>
            <cvParam cvRef="IMS" accession="IMS:1000050" name="position x" value="1"/>
            <cvParam cvRef="IMS" accession="IMS:1000051" name="position y" value="2"/>
            <cvParam cvRef="IMS" accession="IMS:1000052" name="position z" value="3"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <cvParam cvRef="IMS" accession="IMS:1000103" name="external offset" value="{mz2_offset}"/>
            <cvParam cvRef="IMS" accession="IMS:1000102" name="external array length" value="{mz2_len}"/>
            <binary/>
          </binaryDataArray>
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <cvParam cvRef="IMS" accession="IMS:1000103" name="external offset" value="{int2_offset}"/>
            <cvParam cvRef="IMS" accession="IMS:1000102" name="external array length" value="{int2_len}"/>
            <binary/>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
  </run>
</mzML>"#
    );

    let mut imzml_file = File::create(&imzml_path).unwrap();
    imzml_file.write_all(imzml_xml.as_bytes()).unwrap();

    let converter = MzMLConverter::new();
    let stats = converter.convert(&imzml_path, &output_path).unwrap();
    assert_eq!(stats.spectra_count, 2);
    assert_eq!(stats.peak_count, 5);

    let reader = MzPeakReader::open(&output_path).unwrap();
    let spectra = reader.iter_spectra().unwrap();
    assert_eq!(spectra.len(), 2);

    let s1 = &spectra[0];
    assert_eq!(s1.pixel_x, Some(1));
    assert_eq!(s1.pixel_y, Some(1));
    assert_eq!(s1.pixel_z, None);
    assert_eq!(s1.peaks.len(), 3);
    assert!((s1.peaks[0].mz - 100.0).abs() < 1e-6);
    assert!((s1.peaks[0].intensity - 10.0).abs() < 1e-6);

    let s2 = &spectra[1];
    assert_eq!(s2.pixel_x, Some(1));
    assert_eq!(s2.pixel_y, Some(2));
    assert_eq!(s2.pixel_z, Some(3));
    assert_eq!(s2.peaks.len(), 2);
    assert!((s2.peaks[1].mz - 250.0).abs() < 1e-6);
    assert!((s2.peaks[1].intensity - 25.0).abs() < 1e-6);
}
