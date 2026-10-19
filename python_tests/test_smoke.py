"""Installed-wheel checks; Rust tests cover data paths in detail."""
import pytest
import mzpeak


def test_version_and_public_api():
    assert mzpeak.__version__ == "0.1.0"
    assert mzpeak.MzMLConverter()
    assert mzpeak.ThermoConverter()


def test_missing_raw_is_a_python_io_error(tmp_path):
    with pytest.raises(mzpeak.MzPeakIOError):
        mzpeak.convert_thermo(str(tmp_path / "missing.raw"), str(tmp_path / "out.mzpeak"))
    assert not (tmp_path / "out.mzpeak").exists()


def test_profile_request_fails_explicitly(tmp_path):
    converter = mzpeak.ThermoConverter(centroid_spectra=False)
    with pytest.raises(mzpeak.MzPeakValidationError, match="profile output"):
        converter.convert(str(tmp_path / "input.raw"), str(tmp_path / "out.mzpeak"))


def test_v2_writer_and_reader_preserve_values_and_empty_spectra(tmp_path):
    import numpy as np
    output = str(tmp_path / "roundtrip.mzpeak")
    writer = mzpeak.MzPeakDatasetWriterV2(output)
    metadata = mzpeak.SpectrumMetadata(0, 41, 1, 30.0, 1, 2)
    peaks = mzpeak.PeakArraysV2(np.array([400.123456789, 500.987654321]), np.array([10.5, 20.25], dtype=np.float32))
    writer.write_spectrum_v2(metadata, peaks)
    writer.write_spectrum_v2(mzpeak.SpectrumMetadata(1, 42, 2, 45.0, -1, 0), mzpeak.PeakArraysV2(np.array([], dtype=np.float64), np.array([], dtype=np.float32)))
    writer.close()
    reader = mzpeak.MzPeakReader(output)
    spectra = list(reader.iter_spectra_arrays())
    assert len(spectra) == 2
    assert spectra[0].scan_number == 41
    np.testing.assert_array_equal(spectra[0].mz_array, peaks.mz_array)
    np.testing.assert_array_equal(spectra[0].intensity_array, peaks.intensity_array)
    assert spectra[1].scan_number == 42
    assert len(spectra[1]) == 0
