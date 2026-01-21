use super::*;
use std::io::BufReader;

const MINIMAL_MZML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <run id="test_run">
    <spectrumList count="1">
      <spectrum index="0" id="scan=1" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000130" name="positive scan"/>
        <scanList count="1">
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="60.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <binary>AAAAAAAAWUAAAAAAAABpQA==</binary>
          </binaryDataArray>
          <binaryDataArray>
            <cvParam cvRef="MS" accession="MS:1000521" name="32-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <binary>AADIQgAASEM=</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
  </run>
</mzML>"#;

#[test]
fn test_parse_minimal_mzml() {
    let reader = std::io::Cursor::new(MINIMAL_MZML);
    let mut streamer = MzMLStreamer::new(BufReader::new(reader)).unwrap();

    let spectrum = streamer.next_spectrum().unwrap().unwrap();

    assert_eq!(spectrum.index, 0);
    assert_eq!(spectrum.id, "scan=1");
    assert_eq!(spectrum.ms_level, 1);
    assert_eq!(spectrum.polarity, 1);
    assert!((spectrum.retention_time.unwrap() - 60.0).abs() < 0.001);
    assert_eq!(spectrum.mz_array.len(), 2);
    assert_eq!(spectrum.intensity_array.len(), 2);
    assert!((spectrum.mz_array[0] - 100.0).abs() < 0.001);
    assert!((spectrum.mz_array[1] - 200.0).abs() < 0.001);
}

#[test]
fn test_scan_number_extraction() {
    let spectrum = crate::mzml::models::MzMLSpectrum {
        id: "controllerType=0 controllerNumber=1 scan=12345".to_string(),
        ..Default::default()
    };
    assert_eq!(spectrum.scan_number(), Some(12345));

    let spectrum2 = crate::mzml::models::MzMLSpectrum {
        id: "scan=999".to_string(),
        ..Default::default()
    };
    assert_eq!(spectrum2.scan_number(), Some(999));
}
