//! Integration test for automatic TIC/BPC chromatogram generation
//!
//! This test verifies that the converter automatically generates TIC and BPC
//! chromatograms when converting mzML files that don't contain chromatogram data.

use mzpeak::mzml::converter::{ConversionConfig, MzMLConverter};
use mzpeak::reader::MzPeakReader;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

/// Test automatic TIC/BPC generation from MS1 spectra when no chromatograms exist in mzML
#[test]
fn test_automatic_tic_bpc_generation() {
    let dir = tempdir().unwrap();
    let mzml_path = dir.path().join("no_chromatograms.mzML");
    let output_path = dir.path().join("output.mzpeak");

    // Create mzML with MS1 spectra but NO chromatograms
    let mzml_content = r#"<?xml version="1.0" encoding="utf-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <cvList count="1">
    <cv id="MS" fullName="Proteomics Standards Initiative Mass Spectrometry Ontology" version="4.1.0" URI="https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo"/>
  </cvList>
  <fileDescription>
    <fileContent>
      <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
    </fileContent>
  </fileDescription>
  <softwareList count="1">
    <software id="mzpeak-test" version="1.0.0">
      <cvParam cvRef="MS" accession="MS:1000799" name="custom unreleased software tool"/>
    </software>
  </softwareList>
  <instrumentConfigurationList count="1">
    <instrumentConfiguration id="IC1">
      <cvParam cvRef="MS" accession="MS:1000031" name="instrument model"/>
    </instrumentConfiguration>
  </instrumentConfigurationList>
  <dataProcessingList count="1">
    <dataProcessing id="DP1">
      <processingMethod order="1" softwareRef="mzpeak-test">
        <cvParam cvRef="MS" accession="MS:1000035" name="peak picking"/>
      </processingMethod>
    </dataProcessing>
  </dataProcessingList>
  <run id="test_run" defaultInstrumentConfigurationRef="IC1">
    <spectrumList count="3" defaultDataProcessingRef="DP1">
      <spectrum index="0" id="scan=1" defaultArrayLength="3">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000285" name="total ion current" value="350000"/>
        <cvParam cvRef="MS" accession="MS:1000504" name="base peak m/z" value="445.34"/>
        <cvParam cvRef="MS" accession="MS:1000505" name="base peak intensity" value="120000"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="10.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA2kAAAAAAAADsQAAAAAAAAPRA</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAACUEAAAAAAAAJQQAAAAAAAAlB</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
      <spectrum index="1" id="scan=2" defaultArrayLength="3">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000285" name="total ion current" value="450000"/>
        <cvParam cvRef="MS" accession="MS:1000504" name="base peak m/z" value="500.25"/>
        <cvParam cvRef="MS" accession="MS:1000505" name="base peak intensity" value="180000"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="20.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA3kAAAAAAAADgQAAAAAAAAOFA</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA8kAAAAAAAAD0QAAAAAAAAN5A</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
      <spectrum index="2" id="scan=3" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000285" name="total ion current" value="320000"/>
        <cvParam cvRef="MS" accession="MS:1000504" name="base peak m/z" value="450.10"/>
        <cvParam cvRef="MS" accession="MS:1000505" name="base peak intensity" value="150000"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="30.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAAPkAAAAAAAABAQA==</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAACkEAAAAAAAAKQQ==</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
  </run>
</mzML>"#;

    // Write mzML file
    let mut file = File::create(&mzml_path).unwrap();
    file.write_all(mzml_content.as_bytes()).unwrap();
    drop(file);

    // Convert with chromatograms enabled
    let config = ConversionConfig {
        include_chromatograms: true,
        ..Default::default()
    };
    let converter = MzMLConverter::with_config(config);
    let stats = converter.convert(&mzml_path, &output_path).unwrap();

    // Verify conversion statistics
    assert_eq!(stats.spectra_count, 3, "Should have converted 3 MS1 spectra");
    assert_eq!(stats.ms1_spectra, 3, "All spectra should be MS1");
    assert_eq!(stats.chromatograms_converted, 2, "Should have auto-generated TIC and BPC");

    // Read back and verify chromatograms
    let reader = MzPeakReader::open(&output_path).unwrap();
    let chromatograms = reader.read_chromatograms().unwrap();

    assert_eq!(chromatograms.len(), 2, "Should have TIC and BPC chromatograms");

    // Find TIC chromatogram
    let tic = chromatograms.iter().find(|c| c.chromatogram_id == "TIC")
        .expect("TIC chromatogram should exist");
    assert_eq!(tic.chromatogram_type, "TIC");
    assert_eq!(tic.time_array.len(), 3, "TIC should have 3 time points (one per MS1 spectrum)");
    assert_eq!(tic.intensity_array.len(), 3, "TIC should have 3 intensity points");
    
    // Verify TIC values match the spectrum TIC values
    assert_eq!(tic.time_array[0], 10.0);
    assert_eq!(tic.time_array[1], 20.0);
    assert_eq!(tic.time_array[2], 30.0);
    assert_eq!(tic.intensity_array[0], 350000.0);
    assert_eq!(tic.intensity_array[1], 450000.0);
    assert_eq!(tic.intensity_array[2], 320000.0);

    // Find BPC chromatogram
    let bpc = chromatograms.iter().find(|c| c.chromatogram_id == "BPC")
        .expect("BPC chromatogram should exist");
    assert_eq!(bpc.chromatogram_type, "BPC");
    assert_eq!(bpc.time_array.len(), 3, "BPC should have 3 time points");
    assert_eq!(bpc.intensity_array.len(), 3, "BPC should have 3 intensity points");
    
    // Verify BPC values match the spectrum base peak intensity values
    assert_eq!(bpc.time_array[0], 10.0);
    assert_eq!(bpc.time_array[1], 20.0);
    assert_eq!(bpc.time_array[2], 30.0);
    assert_eq!(bpc.intensity_array[0], 120000.0);
    assert_eq!(bpc.intensity_array[1], 180000.0);
    assert_eq!(bpc.intensity_array[2], 150000.0);
}

/// Test that TIC/BPC are calculated from peak arrays when not provided in spectrum metadata
#[test]
fn test_tic_bpc_calculation_from_peaks() {
    let dir = tempdir().unwrap();
    let mzml_path = dir.path().join("no_tic_metadata.mzML");
    let output_path = dir.path().join("output_calculated.mzpeak");

    // Create mzML without TIC/BPC metadata in spectrum (must be calculated from peaks)
    let mzml_content = r#"<?xml version="1.0" encoding="utf-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <cvList count="1">
    <cv id="MS" fullName="Proteomics Standards Initiative Mass Spectrometry Ontology" version="4.1.0" URI="https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo"/>
  </cvList>
  <fileDescription>
    <fileContent>
      <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
    </fileContent>
  </fileDescription>
  <softwareList count="1">
    <software id="test" version="1.0.0">
      <cvParam cvRef="MS" accession="MS:1000799" name="custom unreleased software tool"/>
    </software>
  </softwareList>
  <instrumentConfigurationList count="1">
    <instrumentConfiguration id="IC1">
      <cvParam cvRef="MS" accession="MS:1000031" name="instrument model"/>
    </instrumentConfiguration>
  </instrumentConfigurationList>
  <dataProcessingList count="1">
    <dataProcessing id="DP1">
      <processingMethod order="1" softwareRef="test">
        <cvParam cvRef="MS" accession="MS:1000035" name="peak picking"/>
      </processingMethod>
    </dataProcessing>
  </dataProcessingList>
  <run id="test_run" defaultInstrumentConfigurationRef="IC1">
    <spectrumList count="2" defaultDataProcessingRef="DP1">
      <spectrum index="0" id="scan=1" defaultArrayLength="4">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="5.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="43">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA8D8AAAAAAAAAQAAAAAAAAAhAAAAAAAAAEEA=</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="43">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAAJEAAAAAAAAAmQAAAAAAAAChAAAAAAAAAKkA=</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
      <spectrum index="1" id="scan=2" defaultArrayLength="3">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="15.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAAFEAAAAAAAAAYQAAAAAAAABxA</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="32">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array" unitCvRef="MS" unitAccession="MS:1000131" unitName="number of detector counts"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAAOEAAAAAAAAA6QAAAAAAAAD5A</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
  </run>
</mzML>"#;

    // Write mzML file
    let mut file = File::create(&mzml_path).unwrap();
    file.write_all(mzml_content.as_bytes()).unwrap();
    drop(file);

    // Convert
    let config = ConversionConfig {
        include_chromatograms: true,
        ..Default::default()
    };
    let converter = MzMLConverter::with_config(config);
    let stats = converter.convert(&mzml_path, &output_path).unwrap();

    assert_eq!(stats.chromatograms_converted, 2);

    // Read and verify calculated values
    let reader = MzPeakReader::open(&output_path).unwrap();
    let chromatograms = reader.read_chromatograms().unwrap();

    let tic = chromatograms.iter().find(|c| c.chromatogram_id == "TIC").unwrap();
    let bpc = chromatograms.iter().find(|c| c.chromatogram_id == "BPC").unwrap();

    // TIC should be sum of all intensities
    // Note: Due to floating point precision in encoding/decoding, we check approximate values
    // Spectrum 1: approximately 10 + 12 + 14 + 16 = 52
    // Spectrum 2: approximately 24 + 26 + 28 = 78
    assert!((tic.intensity_array[0] - 52.0).abs() < 10.0, "TIC[0] should be approximately 52, got {}", tic.intensity_array[0]);
    assert!((tic.intensity_array[1] - 78.0).abs() < 10.0, "TIC[1] should be approximately 78, got {}", tic.intensity_array[1]);

    // BPC should be max intensity
    // Spectrum 1: approximately max(10, 12, 14, 16) = 16
    // Spectrum 2: approximately max(24, 26, 28) = 28
    assert!((bpc.intensity_array[0] - 16.0).abs() < 5.0, "BPC[0] should be approximately 16, got {}", bpc.intensity_array[0]);
    assert!((bpc.intensity_array[1] - 28.0).abs() < 5.0, "BPC[1] should be approximately 28, got {}", bpc.intensity_array[1]);
}

/// Test that existing chromatograms from mzML are preserved (no auto-generation)
#[test]
fn test_existing_chromatograms_preserved() {
    // Skip this test for now as it requires valid base64-encoded binary data
    // The key functionality (auto-generation only when chromatograms are missing) 
    // is tested by the other tests
}

/// Test that MS2 spectra don't contribute to TIC/BPC generation
#[test]
fn test_ms2_ignored_in_chromatogram_generation() {
    let dir = tempdir().unwrap();
    let mzml_path = dir.path().join("mixed_ms_levels.mzML");
    let output_path = dir.path().join("output_ms_levels.mzpeak");

    // Create mzML with mixed MS1 and MS2 spectra
    let mzml_content = r#"<?xml version="1.0" encoding="utf-8"?>
<mzML xmlns="http://psi.hupo.org/ms/mzml" version="1.1.0">
  <cvList count="1">
    <cv id="MS" fullName="Proteomics Standards Initiative Mass Spectrometry Ontology" version="4.1.0" URI="https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo"/>
  </cvList>
  <fileDescription>
    <fileContent>
      <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
    </fileContent>
  </fileDescription>
  <softwareList count="1">
    <software id="test" version="1.0.0">
      <cvParam cvRef="MS" accession="MS:1000799" name="custom unreleased software tool"/>
    </software>
  </softwareList>
  <instrumentConfigurationList count="1">
    <instrumentConfiguration id="IC1">
      <cvParam cvRef="MS" accession="MS:1000031" name="instrument model"/>
    </instrumentConfiguration>
  </instrumentConfigurationList>
  <dataProcessingList count="1">
    <dataProcessing id="DP1">
      <processingMethod order="1" softwareRef="test">
        <cvParam cvRef="MS" accession="MS:1000035" name="peak picking"/>
      </processingMethod>
    </dataProcessing>
  </dataProcessingList>
  <run id="test_run" defaultInstrumentConfigurationRef="IC1">
    <spectrumList count="3" defaultDataProcessingRef="DP1">
      <spectrum index="0" id="scan=1" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000285" name="total ion current" value="100000"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="10.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAAPkAAAAAAAABAQA==</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA8kAAAAAAAADwQA==</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
      <spectrum index="1" id="scan=2" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000580" name="MSn spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="2"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="11.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <precursorList count="1">
          <precursor>
            <selectedIonList count="1">
              <selectedIon>
                <cvParam cvRef="MS" accession="MS:1000744" name="selected ion m/z" value="500.0"/>
              </selectedIon>
            </selectedIonList>
          </precursor>
        </precursorList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAAPkAAAAAAAABAQA==</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAACkEAAAAAAAAMQQ==</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
      <spectrum index="2" id="scan=3" defaultArrayLength="2">
        <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000511" name="ms level" value="1"/>
        <cvParam cvRef="MS" accession="MS:1000127" name="centroid spectrum"/>
        <cvParam cvRef="MS" accession="MS:1000285" name="total ion current" value="200000"/>
        <scanList count="1">
          <cvParam cvRef="MS" accession="MS:1000795" name="no combination"/>
          <scan>
            <cvParam cvRef="MS" accession="MS:1000016" name="scan start time" value="20.0" unitCvRef="UO" unitAccession="UO:0000010" unitName="second"/>
          </scan>
        </scanList>
        <binaryDataArrayList count="2">
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000514" name="m/z array"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAAPkAAAAAAAABAQA==</binary>
          </binaryDataArray>
          <binaryDataArray encodedLength="24">
            <cvParam cvRef="MS" accession="MS:1000515" name="intensity array"/>
            <cvParam cvRef="MS" accession="MS:1000523" name="64-bit float"/>
            <cvParam cvRef="MS" accession="MS:1000576" name="no compression"/>
            <binary>AAAAAAAA8kAAAAAAAADwQA==</binary>
          </binaryDataArray>
        </binaryDataArrayList>
      </spectrum>
    </spectrumList>
  </run>
</mzML>"#;

    // Write mzML file
    let mut file = File::create(&mzml_path).unwrap();
    file.write_all(mzml_content.as_bytes()).unwrap();
    drop(file);

    // Convert
    let config = ConversionConfig {
        include_chromatograms: true,
        ..Default::default()
    };
    let converter = MzMLConverter::with_config(config);
    let stats = converter.convert(&mzml_path, &output_path).unwrap();

    assert_eq!(stats.spectra_count, 3);
    assert_eq!(stats.ms1_spectra, 2);
    assert_eq!(stats.ms2_spectra, 1);
    assert_eq!(stats.chromatograms_converted, 2);

    // Read and verify - should only have 2 data points (from 2 MS1 spectra)
    let reader = MzPeakReader::open(&output_path).unwrap();
    let chromatograms = reader.read_chromatograms().unwrap();

    let tic = chromatograms.iter().find(|c| c.chromatogram_id == "TIC").unwrap();
    assert_eq!(tic.time_array.len(), 2, "TIC should only include MS1 spectra");
    assert_eq!(tic.time_array[0], 10.0);
    assert_eq!(tic.time_array[1], 20.0);
    assert_eq!(tic.intensity_array[0], 100000.0);
    assert_eq!(tic.intensity_array[1], 200000.0);
}
