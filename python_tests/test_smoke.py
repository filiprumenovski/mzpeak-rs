import numpy as np
import pytest
import mzpeak


def test_current_api():
    assert mzpeak.FORMAT_VERSION == '2.0'
    assert mzpeak.Reader and mzpeak.Writer and mzpeak.convert and mzpeak.validate


def test_writer_reader_empty_scan_metadata_and_exact_values(tmp_path):
    path = str(tmp_path / 'roundtrip.mzpeak')
    mz = np.array([400.123456789, 500.987654321], dtype=np.float64)
    intensity = np.array([10.5, 20.25], dtype=np.float32)
    with mzpeak.Writer(path) as writer:
        writer.set_metadata({'provenance': {'method': 'manual test'}})
        writer.write({'scan_number': 41, 'retention_time': 30.0}, mz, intensity)
        writer.write({'scan_number': 42, 'ms_level': 2, 'precursor_mz': 500.0}, np.array([], dtype=np.float64), np.array([], dtype=np.float32))
    reader = mzpeak.Reader(path, batch_size=1)
    spectra = list(reader)
    assert len(spectra) == 2
    assert spectra[0].metadata['scan_number'] == 41
    np.testing.assert_array_equal(spectra[0].mz, mz)
    np.testing.assert_array_equal(spectra[0].intensity, intensity)
    assert len(spectra[1]) == 0 and spectra[1].metadata['precursor_mz'] == 500.0
    assert reader.manifest()['spectrum_count'] == 2
    assert reader.metadata()['provenance']['method'] == 'manual test'
    assert mzpeak.validate(path) == {'spectra': 2, 'peaks': 2, 'chromatograms': 0}


def test_imaging_mobility_and_empty_arrays(tmp_path):
    path = str(tmp_path / 'imaging.mzpeak')
    with mzpeak.Writer(path, ion_mobility=True, imaging=True) as writer:
        writer.write({'pixel_x': 1, 'pixel_y': 2}, np.array([], dtype=np.float64), np.array([], dtype=np.float32), np.array([], dtype=np.float64))
    spectrum = next(iter(mzpeak.Reader(path)))
    assert spectrum.metadata['pixel_y'] == 2
    assert spectrum.ion_mobility is not None and len(spectrum.ion_mobility) == 0


def test_failed_or_abandoned_write_publishes_nothing(tmp_path):
    path = str(tmp_path / 'failed.mzpeak')
    with pytest.raises(RuntimeError):
        with mzpeak.Writer(path) as writer:
            writer.write({}, np.array([np.nan]), np.array([1], dtype=np.float32))
    assert not (tmp_path / 'failed.mzpeak').exists()
    with pytest.raises(ValueError):
        with mzpeak.Writer(path):
            raise ValueError('caller failure')
    assert not (tmp_path / 'failed.mzpeak').exists()


def test_missing_input_and_invalid_compression(tmp_path):
    with pytest.raises(RuntimeError):
        mzpeak.convert(str(tmp_path / 'missing.raw'), str(tmp_path / 'out.mzpeak'))
    with pytest.raises(RuntimeError):
        mzpeak.Writer(str(tmp_path / 'out.mzpeak'), compression_level=100)
    assert not (tmp_path / 'out.mzpeak').exists()
