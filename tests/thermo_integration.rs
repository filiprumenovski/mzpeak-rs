#![cfg(feature = "thermo")]

use mzpeak::reader::{MzPeakReader, ReaderConfig};
use mzpeak::thermo::{convert_thermo, ThermoConversionConfig};

/// An actual RAW corpus is opt-in; no proprietary input is checked in.
#[test]
#[ignore = "set MZPEAK_TEST_RAW to a supported local RAW file"]
fn native_raw_roundtrip() {
    let input = std::env::var_os("MZPEAK_TEST_RAW").expect("MZPEAK_TEST_RAW is required");
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("native.mzpeak");
    let stats = convert_thermo(&input, &output, &ThermoConversionConfig::default()).unwrap();
    let reader = MzPeakReader::open(output).unwrap();
    let spectra = reader.iter_spectra_arrays().unwrap();
    assert_eq!(spectra.len(), stats.spectra_count);
    assert_eq!(
        spectra.iter().map(|s| s.peak_count()).sum::<usize>(),
        stats.peak_count
    );
    assert!(stats.peak_count > 0);
    assert_eq!(reader.total_spectra() as usize, stats.spectra_count);
}

#[test]
fn missing_rawr_is_an_actionable_error() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("source.raw");
    let output = dir.path().join("out.mzpeak");
    std::fs::write(&input, b"untouched source").unwrap();
    let config = ThermoConversionConfig {
        rawr_binary: Some(dir.path().join("missing-rawr")),
        ..Default::default()
    };
    let error = convert_thermo(&input, &output, &config).unwrap_err();
    assert!(error.to_string().contains("MZPEAK_RAWR"));
    assert!(!output.exists());
    assert_eq!(std::fs::read(input).unwrap(), b"untouched source");
}

#[cfg(unix)]
mod process {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine};
    use std::os::unix::fs::PermissionsExt;

    fn fixture(count: usize) -> String {
        let masses = STANDARD.encode(
            [400.123456789_f64, 500.987654321_f64]
                .into_iter()
                .flat_map(f64::to_le_bytes)
                .collect::<Vec<_>>(),
        );
        let intensities = STANDARD.encode(
            [10.5_f32, 20.25_f32]
                .into_iter()
                .flat_map(f32::to_le_bytes)
                .collect::<Vec<_>>(),
        );
        format!(
            r#"<?xml version="1.0"?>
<mzML version="1.1.0"><run id="test"><spectrumList count="{count}">
<spectrum index="0" id="controllerType=0 controllerNumber=1 scan=41" defaultArrayLength="2">
<cvParam accession="MS:1000511" name="ms level" value="1"/>
<cvParam accession="MS:1000127" name="centroid spectrum"/>
<cvParam accession="MS:1000130" name="positive scan"/>
<cvParam accession="MS:1000285" name="total ion current" value="1234.5"/>
<cvParam accession="MS:1000504" name="base peak m/z" value="500.987654321"/>
<cvParam accession="MS:1000505" name="base peak intensity" value="777"/>
<scanList count="1"><scan><cvParam accession="MS:1000016" name="scan start time" value="0.5" unitAccession="UO:0000031" unitName="minute"/>
<cvParam accession="MS:1000927" name="ion injection time" value="12.5"/></scan></scanList>
<binaryDataArrayList count="2">
<binaryDataArray><cvParam accession="MS:1000523" name="64-bit float"/><cvParam accession="MS:1000576" name="no compression"/><cvParam accession="MS:1000514" name="m/z array"/><binary>{masses}</binary></binaryDataArray>
<binaryDataArray><cvParam accession="MS:1000521" name="32-bit float"/><cvParam accession="MS:1000576" name="no compression"/><cvParam accession="MS:1000515" name="intensity array"/><binary>{intensities}</binary></binaryDataArray>
</binaryDataArrayList></spectrum>
<spectrum index="1" id="controllerType=0 controllerNumber=1 scan=42" defaultArrayLength="0">
<cvParam accession="MS:1000511" name="ms level" value="2"/><cvParam accession="MS:1000127" name="centroid spectrum"/>
<cvParam accession="MS:1000129" name="negative scan"/>
<scanList count="1"><scan><cvParam accession="MS:1000016" name="scan start time" value="0.75" unitAccession="UO:0000031" unitName="minute"/></scan></scanList>
<precursorList count="1"><precursor><isolationWindow>
<cvParam accession="MS:1000827" name="isolation window target m/z" value="500"/>
<cvParam accession="MS:1000828" name="isolation window lower offset" value="1.5"/>
<cvParam accession="MS:1000829" name="isolation window upper offset" value="2.5"/>
</isolationWindow><selectedIonList count="1"><selectedIon>
<cvParam accession="MS:1000744" name="selected ion m/z" value="500"/>
<cvParam accession="MS:1000041" name="charge state" value="2"/>
</selectedIon></selectedIonList><activation><cvParam accession="MS:1000045" name="collision energy" value="30"/></activation></precursor></precursorList>
<binaryDataArrayList count="0"/></spectrum>
</spectrumList></run></mzML>"#
        )
    }

    fn setup(
        dir: &std::path::Path,
        count: usize,
        status: i32,
    ) -> (std::path::PathBuf, ThermoConversionConfig) {
        let input = dir.join("source with spaces; echo injected.raw");
        std::fs::write(&input, b"source remains intact").unwrap();
        let binary = dir.join("fake rawr");
        let script = format!("#!/bin/sh\n[ \"$1\" = --color ] || exit 91\ncat <<'MZPEAK_FIXTURE'\n{}\nMZPEAK_FIXTURE\necho 'fixture diagnostic' >&2\nexit {status}\n", fixture(count));
        std::fs::write(&binary, script).unwrap();
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
        (
            input,
            ThermoConversionConfig {
                batch_size: 1,
                rawr_binary: Some(binary),
                ..Default::default()
            },
        )
    }

    #[test]
    fn pipe_preserves_precise_peaks_native_numbers_metadata_and_empty_scans() {
        let dir = tempfile::tempdir().unwrap();
        let (input, config) = setup(dir.path(), 2, 0);
        let output = dir.path().join("out.mzpeak");
        let stats = convert_thermo(&input, &output, &config).unwrap();
        assert_eq!(
            (
                stats.spectra_count,
                stats.peak_count,
                stats.ms1_spectra,
                stats.ms2_spectra
            ),
            (2, 2, 1, 1)
        );
        // One peak per Arrow batch exercises a spectrum spanning batch boundaries.
        let reader =
            MzPeakReader::open_with_config(&output, ReaderConfig { batch_size: 1 }).unwrap();
        let spectra = reader.iter_spectra_arrays().unwrap();
        assert_eq!(spectra.len(), 2);
        let first = spectra[0].to_owned().unwrap();
        assert_eq!(
            (
                first.metadata.spectrum_id,
                first.metadata.scan_number,
                first.metadata.ms_level
            ),
            (0, Some(41), 1)
        );
        assert_eq!(first.metadata.retention_time, 30.0);
        assert_eq!(first.peaks.mz, [400.123456789, 500.987654321]);
        assert_eq!(first.peaks.intensity, [10.5, 20.25]);
        assert_eq!(first.metadata.total_ion_current, Some(1234.5));
        assert_eq!(first.metadata.base_peak_intensity, Some(777.0));
        assert_eq!(first.metadata.injection_time, Some(12.5));
        let empty = reader
            .get_spectrum_arrays(1)
            .unwrap()
            .unwrap()
            .to_owned()
            .unwrap();
        assert_eq!(
            (
                empty.metadata.scan_number,
                empty.metadata.ms_level,
                empty.metadata.polarity
            ),
            (Some(42), 2, -1)
        );
        assert_eq!(empty.metadata.retention_time, 45.0);
        assert_eq!(empty.peak_count(), 0);
        assert_eq!(empty.metadata.precursor_mz, Some(500.0));
        assert_eq!(empty.metadata.precursor_charge, Some(2));
        assert_eq!(empty.metadata.isolation_window_lower, Some(1.5));
        assert_eq!(empty.metadata.isolation_window_upper, Some(2.5));
        assert_eq!(empty.metadata.collision_energy, Some(30.0));
        assert_eq!(reader.get_spectra_arrays(&[1, 0]).unwrap().len(), 2);
        assert!(reader.get_spectrum_arrays(999).unwrap().is_none());
        assert_eq!(std::fs::read(input).unwrap(), b"source remains intact");
    }

    #[test]
    fn late_child_failure_never_publishes_partial_output() {
        let dir = tempfile::tempdir().unwrap();
        let (input, config) = setup(dir.path(), 2, 7);
        let output = dir.path().join("out.mzpeak");
        let error = convert_thermo(input, &output, &config)
            .unwrap_err()
            .to_string();
        assert!(error.contains("fixture diagnostic"), "{error}");
        assert!(!output.exists());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[test]
    fn mismatched_header_count_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let (input, config) = setup(dir.path(), 3, 0);
        let output = dir.path().join("out.mzpeak");
        let error = convert_thermo(input, &output, &config)
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected 3 spectra, received 2"), "{error}");
        assert!(!output.exists());
    }

    #[test]
    fn existing_output_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let (input, config) = setup(dir.path(), 2, 0);
        let output = dir.path().join("out.mzpeak");
        std::fs::write(&output, b"original").unwrap();
        assert!(convert_thermo(input, &output, &config).is_err());
        assert_eq!(std::fs::read(output).unwrap(), b"original");
    }
}
