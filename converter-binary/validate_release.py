"""Compare the complete experimental export with the fixed reference artifacts."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile
import pyarrow as pa
import pyarrow.parquet as pq
from convert import restore_summaries
from contextlib import closing
from mzpeak_compat.reader import Reader
from vendor_oracle import spectra



def canonical_acquisition(metadata):
    metadata = dict(metadata)
    # The official writer serializes HashMap.values(); list position is not identity.
    configurations = metadata['instrument_configuration_list']
    assert len({v['id'] for v in configurations}) == len(configurations), 'Duplicate instrument configuration ID'
    metadata['instrument_configuration_list'] = sorted(configurations, key=lambda v: v['id'])
    return metadata


def normalize_source_locations(expected, actual):
    """Accept a moved source only when its full source record and SHA-1 agree."""
    import re
    left = expected['file_description']['source_files']
    right = actual['file_description']['source_files']
    assert len(left) == len(right) == 1, 'Relocation requires one RAW source'
    a, b = left[0], right[0]
    assert {k:v for k,v in a.items() if k != 'location'} == {k:v for k,v in b.items() if k != 'location'}, 'Source identity or checksum differs'
    hashes = [p['value'] for p in a['parameters'] if p['accession'] == 'MS:1000569']
    assert len(hashes) == 1 and re.fullmatch('[0-9a-fA-F]{40}', hashes[0]), 'Source SHA-1 required for relocation'
    change = {'reference_location': a['location'], 'archive_location': b['location'], 'source_sha1': hashes[0]}
    a['location'] = b['location']
    return change


def validate(folder, raw, reference):
    # The reference is a pinned, independent metadata export of this same RAW.
    raw, reference = Path(raw), Path(reference)
    report = {'raw': str(raw), 'reference': str(reference),
              'reference_scope': 'vendor metadata-only export plus independent RAW summaries'}
    with zipfile.ZipFile(reference) as official, \
            zipfile.ZipFile(folder / 'complete.experimental.zip') as archive:
        assert archive.testzip() is None
        manifest = json.loads(archive.read('manifest.json'))
        assert manifest['bounds'] == {'mz_ppm': .01, 'intensity_relative': .001}
        for name, expected in manifest['sha256'].items():
            with archive.open(name) as stream:
                assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected, name
        original = json.loads(official.read('mzpeak_index.json'))['metadata']
        actual = json.loads(archive.read('acquisition/acquisition.json'))['metadata']
        original['software_list'] = [v for v in original['software_list'] if v['id'] != 'mzpeak_prototyping_convert1']
        original['data_processing_method_list'] = [v for v in original['data_processing_method_list'] if v['id'] != 'mzpeak_conversion1']
        original = canonical_acquisition(original)
        actual = canonical_acquisition(actual)
        report['source_location_normalization'] = normalize_source_locations(original, actual)
        assert original == actual, [key for key in original if original[key] != actual.get(key)]
        report['acquisition_metadata_exact_except_converter_provenance_and_source_location'] = True
        tables = {}
        for name in official.namelist():
            if not name.endswith('.parquet') or name in ('spectra_data.parquet', 'spectra_peaks.parquet'):
                continue
            a = pq.read_table(pa.BufferReader(official.read(name)))
            b = pq.read_table(pa.BufferReader(archive.read('acquisition/' + name)))
            assert a.column_names == b.column_names, name
            skip = {'number_of_data_points', 'number_of_peaks'} if name == 'spectra_metadata.parquet' else set()
            if name == 'spectra_metadata.parquet':
                assert all(v in (None, 0) for col in ('number_of_data_points', 'number_of_peaks') for v in a[col].to_pylist()), 'Reference contains signal counts'
                skip.update(('lowest_observed_mz', 'highest_observed_mz', 'base_peak_mz', 'base_peak_intensity', 'total_ion_current'))
            absent_auxiliary = False
            for col in a.column_names:
                if col not in skip:
                    if name == 'spectra_metadata.parquet' and col == 'auxiliary_arrays':
                        assert all(v is None for v in a[col].to_pylist()), 'Metadata oracle unexpectedly contains auxiliary arrays'
                        assert all(v == [] for v in b[col].to_pylist()), 'Native output contains unvalidated auxiliary arrays'
                        absent_auxiliary = True
                    else:
                        assert a[col].equals(b[col]), (name, col)
            tables[name] = {'rows': len(a), 'columns_exact': len(a.column_names) - len(skip) - int(absent_auxiliary)}
            if absent_auxiliary:
                tables[name]['absent_auxiliary_arrays_null_vs_empty'] = True
            if name == 'spectra_metadata.parquet':
                source_metadata = b
            if skip:
                rows = [json.loads(line) for line in (folder / 'source-summaries.jsonl').read_text().splitlines()]
                for count, rep in [('number_of_data_points', 'profile'), ('number_of_peaks', 'centroid')]:
                    expected = [r['count'] if r['representation'] == rep else None for r in rows]
                    assert b[count].to_pylist() == expected
                assert sum(b['number_of_data_points'].fill_null(0).to_numpy()) + sum(b['number_of_peaks'].fill_null(0).to_numpy()) == manifest['points']
                # A metadata mismatch must fail rather than silently pairing different scans.
                try:
                    restore_summaries(b, rows[:-1])
                    raise AssertionError('Accepted truncated source summaries')
                except ValueError:
                    pass
        report['tables'] = tables
        report['all_source_point_counts_preserved'] = True
        with closing(Reader(folder / 'complete.experimental.zip')) as reader:
            import numpy as np
            errors = [0., 0.]; points = zeros = scans = 0; tic_error = 0.
            digest = hashlib.sha256()
            limits = [manifest['bounds']['mz_ppm'] * 1e-6, manifest['bounds']['intensity_relative']]
            for name in ('signal.bin', 'signal.index.zst'):
                with archive.open(name) as stream:
                    assert hashlib.file_digest(stream, 'sha256').hexdigest() == manifest['sha256'][name]
            summary_columns = {name: source_metadata[name].to_pylist() for name in
                ('lowest_observed_mz', 'highest_observed_mz', 'base_peak_mz', 'base_peak_intensity', 'total_ion_current')}
            summary_max_ppm = 0.
            for rec, mz, intensity in spectra(raw):
                index = rec['index']; saved = reader.directory[index]
                for key in ['index', 'id', 'count', 'ms_level', 'representation', 'retention_time_minutes']:
                    assert saved[key] == rec[key], (index, key)
                restored = reader.read(index)
                for axis, source in enumerate([mz, intensity]):
                    digest.update(source.tobytes())
                    actual = restored[axis]
                    assert len(actual) == len(source) and np.isfinite(actual).all()
                    mask = source != 0
                    assert np.array_equal(actual == 0, source == 0)
                    error = float(np.max(abs(actual[mask] - source[mask]) / abs(source[mask]), initial=0))
                    assert error <= limits[axis], (index, axis, error, limits[axis])
                    errors[axis] = max(errors[axis], error)
                source_f32 = intensity.astype(np.float32)
                bpi = float(source_f32.max(initial=0.))
                expected_summaries = {
                    'lowest_observed_mz': float(mz.min()) if len(mz) else 0.,
                    'highest_observed_mz': float(mz.max()) if len(mz) else 0.,
                    'base_peak_mz': float(mz[source_f32.argmax()]) if bpi > 0 else 0.,
                    'base_peak_intensity': bpi,
                    # Rust/reference source summaries accumulate sequentially in f32.
                    'total_ion_current': float(np.cumsum(source_f32, dtype=np.float32)[-1]) if len(mz) else 0.}
                for name, expected in expected_summaries.items():
                    got = summary_columns[name][index]
                    if got == expected:
                        continue
                    assert (manifest.get('calibration_policy') == 'physical_source_event'
                            and name in ('lowest_observed_mz', 'highest_observed_mz', 'base_peak_mz')
                            and expected > 0 and got > 0 and np.isfinite(got)), (index, name, expected, got)
                    error_ppm = abs(got-expected) / expected * 1e6
                    allowance = manifest['calibration_difference_budget_ppm']
                    assert 0 < allowance <= .001 and allowance <= manifest['bounds']['mz_ppm'] * .1
                    assert error_ppm <= allowance, (index, name, error_ppm, allowance)
                    summary_max_ppm = max(summary_max_ppm, error_ppm)
                total = float(intensity.sum())
                if total: tic_error = max(tic_error, abs(float(restored[1].sum()) / total - 1))
                scans += 1; points += len(mz); zeros += int(np.count_nonzero(intensity == 0))
            assert scans == len(reader.directory) == manifest['spectra'] and points == manifest['points']
            report.update(all_spectra_all_points_validated=True, all_source_summaries_validated=True,
                          max_source_summary_mz_difference_ppm=summary_max_ppm, spectra=scans, points=points,
                          max_mz_error_ppm=errors[0] * 1e6, max_intensity_relative_error=errors[1],
                          max_tic_relative_error=tic_error, zeros_preserved=zeros,
                          source_array_sha256=digest.hexdigest(), declared_bounds=manifest['bounds'])
    (folder / 'validation.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('folder', type=Path)
    parser.add_argument('--raw', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    args = parser.parse_args()
    validate(args.folder, args.raw, args.reference)
