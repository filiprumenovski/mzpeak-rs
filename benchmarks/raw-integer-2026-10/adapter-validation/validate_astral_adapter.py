"""Fail-closed, bounded-memory adapter contract validation on saved Astral archives.

The original archives are read only. Numerical bounds are fixed, not fitted.
Coordinate filtering is checked against each reader's complete decoded arrays;
quantization-edge membership changes are counted separately from decoder errors.
"""
import argparse
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import resource
import signal
import faulthandler
import sys
import time
import traceback

import numpy as np
import pandas as pd
from numpy.testing import assert_array_equal, assert_allclose
from mzpeak import MzPeakFile as Official
from mzpeak.util import Span
from mzpeak_compat import MzPeakFile, _official_zero_mask
from native_batch import library_path

NATIVE = Path(os.environ['MZPEAK_COMPAT_ARCHIVE'])
OFFICIAL = Path(os.environ['MZPEAK_OFFICIAL_ARCHIVE'])
EXPECTED_HASHES = {
    'native': 'fa08ba81bab5ec9aa3e4b69da8c87916cfcf3d1a937c753596a543c4aadc343e',
    'official': '4f8aa8784e7e865a01d2fa4bec12351c441e681300e65a3695266c9df01c0c65',
}
CALIBRATION_COLUMNS = {'lowest_observed_mz', 'highest_observed_mz', 'base_peak_mz'}
REPORT = {'status': 'running', 'sections': {}, 'failures': [], 'method': {
    'signal_mz_limit_ppm': .01, 'signal_intensity_relative_limit': .001,
    'metadata_physical_event_mz_limit_ppm': .001, 'seed': 20261021,
    'all_point_batch_scan_limit': 4096, 'all_point_batch_point_limit': 8000000,
    'scope': 'Adapter versus saved official-default archive; not a conversion or speed benchmark',
    'coordinate_policy': 'Inclusive comparisons on decoded values. Cross-format boundary membership is audited, not assumed identical.',
}}
OUTPUT = None


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save():
    REPORT['peak_validator_process_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    OUTPUT.write_text(json.dumps(REPORT, indent=2, allow_nan=False) + '\n')


def section(name, fn):
    print('START', name, flush=True)
    started = time.perf_counter()
    try:
        value = fn()
        REPORT['sections'][name] = {'status': 'passed', 'result': value}
    except Exception as exc:
        REPORT['sections'][name] = {'status': 'failed', 'error': str(exc), 'traceback': traceback.format_exc()}
        REPORT['failures'].append(name)
        traceback.print_exc()
    REPORT['sections'][name]['elapsed_seconds'] = time.perf_counter() - started
    save()
    print('END', name, REPORT['sections'][name]['status'], flush=True)


class ReferenceTimeout(TimeoutError):
    pass


def reference_query(label, fn):
    def timeout(signum, frame):
        faulthandler.dump_traceback(file=sys.stderr, all_threads=False)
        raise ReferenceTimeout(label)
    previous = signal.signal(signal.SIGALRM, timeout)
    signal.alarm(30)
    try:
        return fn()
    except ReferenceTimeout:
        REPORT.setdefault('reference_timeouts', []).append({'query':label, 'limit_seconds':30,
            'comparison':'Native result checked against official scalar reads; original reference query did not complete.'})
        print('REFERENCE_TIMEOUT', label, flush=True)
        return None
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def compare_arrays(a, b, indexed=False):
    keys = ['m/z array', 'intensity array'] + (['spectrum_index'] if indexed else [])
    for k in keys:
        assert k in a and k in b, (k, a.keys(), b.keys())
        assert a[k].shape == b[k].shape, (k, a[k].shape, b[k].shape)
    if indexed:
        assert_array_equal(a['spectrum_index'], b['spectrum_index'])
    mz, intensity = a['m/z array'], a['intensity array']
    bm, bi = b['m/z array'], b['intensity array']
    assert np.all(np.isfinite(mz)) and np.all(np.isfinite(bm))
    assert np.all(bm > 0) and np.all(np.isfinite(intensity)) and np.all(np.isfinite(bi))
    assert_array_equal(intensity == 0, bi == 0)
    em = float(np.max(np.abs(mz - bm) / bm, initial=0)) * 1e6
    nonzero = bi != 0
    ei = float(np.max(np.abs(intensity[nonzero] - bi[nonzero]) / np.abs(bi[nonzero]), initial=0))
    assert em <= .01 and ei <= .001, (em, ei)
    return len(mz), em, ei


def scalar(a, b):
    assert a.keys() == b.keys(), (a.keys(), b.keys())
    for k in ('index', 'id', 'data_kind'):
        assert a[k] == b[k], (k, a[k], b[k])
    compare_arrays(a, b)


def frame_compare(a, b, calibration=False):
    if a is None or b is None:
        assert a is b
        return {'rows': 0}
    assert_array_equal(a.index, b.index)
    assert_array_equal(a.columns, b.columns)
    audited = {}
    for col in a.columns:
        try:
            pd.testing.assert_series_equal(a[col], b[col], check_exact=True)
        except AssertionError:
            if not calibration or col not in CALIBRATION_COLUMNS:
                raise
            x = a[col].to_numpy(dtype=float, na_value=np.nan)
            y = b[col].to_numpy(dtype=float, na_value=np.nan)
            assert_array_equal(np.isnan(x), np.isnan(y))
            valid = ~np.isnan(x)
            assert np.all(x[valid] > 0) and np.all(y[valid] > 0)
            error = float(np.max(np.abs(x[valid]-y[valid])/y[valid], initial=0))*1e6
            assert error <= .001, (col, error)
            audited[col] = {'different_values': int(np.count_nonzero(x[valid] != y[valid])), 'max_error_ppm': error}
    return {'rows': len(a), 'columns': len(a.columns), 'physical_event_summary_audit': audited}


def main():
    global OUTPUT
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    OUTPUT = args.output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    REPORT['environment'] = {'python': sys.version, 'platform': platform.platform(), 'hostname': platform.node(),
                             'slurm_job_id': os.getenv('SLURM_JOB_ID'), 'native_library_sha256': sha(library_path())}
    import orbitrap_convert, native_batch, bench, integer_codecs, more_optimization, quantize, mzpeak
    REPORT['dependency_sha256'] = {Path(module.__file__).name: sha(module.__file__) for module in
        (orbitrap_convert, native_batch, bench, integer_codecs, more_optimization, quantize)}
    REPORT['dependency_versions'] = {'numpy': np.__version__, 'pandas': pd.__version__}
    REPORT['official_python_sha256'] = {p.name:sha(p) for p in Path(mzpeak.__file__).parent.glob('*.py')}
    REPORT['source_sha256'] = sha(__file__)
    REPORT['adapter_sha256'] = sha(Path(__file__).with_name('mzpeak_compat.py'))
    started = time.perf_counter()
    def identities():
        result = {name: {'sha256': sha(path), 'bytes': path.stat().st_size}
                  for name, path in [('native', NATIVE), ('official', OFFICIAL)]}
        for name, data in result.items():
            assert data['sha256'] == EXPECTED_HASHES[name], (name, data)
        return result
    section('fixture_identity', identities)
    if REPORT['failures']:
        raise SystemExit(1)
    print('Opening three public readers', flush=True)
    n = MzPeakFile(NATIVE)
    f = MzPeakFile(NATIVE, filter_profile_zeros=True)
    o = Official(OFFICIAL)
    REPORT['three_reader_open_seconds'] = time.perf_counter() - started - REPORT['sections']['fixture_identity']['elapsed_seconds']
    assert len(n) == len(f) == len(o) == 744726
    directory = n._signal.directory
    profiles = [i for i in range(len(n)) if directory[i]['representation'] == 'profile']
    peaks = [i for i in range(len(n)) if directory[i]['representation'] == 'centroid']
    rng = np.random.default_rng(20261021)
    selected = sorted(set([0, 1, len(n)-1] + [ids[j] for ids in (profiles, peaks)
                        for j in [0, len(ids)//2, len(ids)-1, *rng.integers(0, len(ids), 32).tolist()]]))
    REPORT['coverage'] = {'scans': len(n), 'profile_scans': len(profiles), 'centroid_scans': len(peaks),
                          'scalar_scan_indices': selected}
    save()

    def metadata():
        data = {'spectra': frame_compare(f.spectra, o.spectra, calibration=True)}
        for name in ('scans', 'precursors', 'selected_ions', 'chromatograms'):
            data[name] = frame_compare(getattr(f, name), getattr(o, name))
        assert n.quantization_bounds == {'mz_ppm': .01, 'intensity_relative': .001}
        return data
    section('all_metadata_frames', metadata)

    def acquisition():
        from validate_complete import canonical_acquisition, normalize_source_locations
        original = json.loads(o._archive.read('mzpeak_index.json'))['metadata']
        actual = json.loads(n._archive.read('acquisition/acquisition.json'))['metadata']
        original['software_list'] = [v for v in original['software_list'] if v['id'] != 'mzpeak_prototyping_convert1']
        original['data_processing_method_list'] = [v for v in original['data_processing_method_list'] if v['id'] != 'mzpeak_conversion1']
        original, actual = canonical_acquisition(original), canonical_acquisition(actual)
        expected_source = original['file_description']['source_files']
        actual_source = actual['file_description']['source_files']
        assert len(expected_source) == len(actual_source) == 1
        # This fixture's official converter staged the same RAW under a short name.
        rename = {'official_name':expected_source[0]['name'], 'native_name':actual_source[0]['name']}
        assert rename == {'official_name':'astral.raw',
                          'native_name':'20231206_HAP1_1ug_60min_DIA_2Th_5e4_3p5ms_rep01.raw'}
        expected_source[0]['name'] = actual_source[0]['name']
        relocation = normalize_source_locations(original, actual)
        assert relocation['source_sha1'] == '272b6e7f39a812050bdb3969d045af34643cbe13'
        assert original == actual, [key for key in original if original[key] != actual.get(key)]
        return {'exact_except_converter_provenance_and_audited_source_name_and_location': True,
                'source_identity_except_staging_name_and_location_and_sha1_equal': True,
                'staged_source_rename':rename, 'source_sha1': relocation['source_sha1'],
                'prior_source_sha256':'e18a895300ed8b10605cd9cb4013f0336609392410fedc96ca16db0c61382832'}
    section('acquisition_metadata_and_source_identity', acquisition)

    def zero_examples():
        for source, expected in [([], []), ([0], []), ([1], [0]), ([0,0,0], []),
                                 ([0,0,1,0,0,0,2,0,0], [1,2,3,5,6,7]), ([0,1,0], [0,1,2])]:
            assert_array_equal(np.flatnonzero(_official_zero_mask(np.array(source))), expected)
        return {'synthetic_empty_all_zero_singleton_edge_and_interior_runs': 6}
    section('zero_filter_edge_cases', zero_examples)

    def lookup():
        comparisons = 0
        for i in selected:
            a,b = f[i],o[i]
            scalar(a,b); comparisons += 1
            scalar(f[a['id']], o[b['id']]); comparisons += 1
            if comparisons % 20 == 0: print('SCALAR_COMPARISONS', comparisons, flush=True)
        for query in ([3, 1, 17, 3], slice(15, 22), slice(15, 22, 2), slice(len(n)-4, len(n))):
            a = f[query]
            b = reference_query('spectrum objects '+repr(query),lambda:o[query])
            if b is None:
                ids = list(range(*query.indices(len(o)))) if isinstance(query,slice) else query
                b = [o[i] for i in ids]
            assert len(a) == len(b)
            for x, y in zip(a, b):
                scalar(x, y); comparisons += 1
        assert f[[]] == o[[]] == []
        for i in selected:
            a, b = f.read_peaks_for(i), o.read_peaks_for(i)
            assert bool(a) == bool(b)
            if a: compare_arrays(a,b)
        return {'spectrum_comparisons': comparisons, 'read_peaks_for_queries': len(selected)}
    section('scalar_id_list_slice', lookup)

    def iterators():
        comparisons = 0
        for prefer in (False, True):
            f.prefer_peaks = o.prefer_peaks = prefer
            for start in (0, len(n)//2, len(n)-20):
                def read_segment(reader):
                    it = iter(reader)
                    it.seek(start)
                    return list(itertools.islice(it,20))
                a = read_segment(f)
                b = reference_query(f'iterator start={start}, prefer_peaks={prefer}',lambda:read_segment(o))
                if b is None: b = [o[i] for i in range(start,start+20)]
                assert len(a) == len(b) == 20
                for x,y in zip(a,b):
                    scalar(x,y); comparisons += 1
            f.prefer_peaks = o.prefer_peaks = False
        assert comparisons == 120
        return {'forward_seek_spectrum_comparisons': comparisons}
    section('iterator_forward_seek_and_prefer_peaks', iterators)

    def retention():
        ts = o.spectra['time'].to_numpy(dtype=float)
        queries = [ts[0]-1, ts[-1]+1, float('inf')]
        for i in selected[:16]:
            queries.extend([ts[i], np.nextafter(ts[i], -np.inf), np.nextafter(ts[i], np.inf)])
            if i+1 < len(ts): queries.append((ts[i]+ts[i+1])/2)
        for t in queries:
            assert f.time.resolve(float(t)) == o.time.resolve(float(t)), t
            scalar(f.time[float(t)], o.time[float(t)])
        for i in (0, len(n)//2, len(n)-4):
            q = slice(ts[i], ts[min(i+3, len(n)-1)])
            assert f.time.resolve(q) == o.time.resolve(q)
            a = f.time[q]
            b = reference_query('time slice '+repr(q),lambda:o.time[q])
            if b is None:
                b = [o[j] for j in range(*o.time.resolve(q).indices(len(o)))]
            assert len(a) == len(b)
            for x, y in zip(a,b): scalar(x,y)
        return {'scalar_time_queries': len(queries), 'time_ranges': 3, 'includes_midpoints_and_nextafter': True}
    section('retention_time_boundaries', retention)

    def ranges():
        checked = 0
        for attr, ids in (('spectrum_data', profiles), ('spectrum_peak_data', peaks)):
            a, b = getattr(f, attr), getattr(o, attr)
            for query in ([ids[2], ids[0], ids[2]], slice(ids[0], ids[3]), slice(ids[0], ids[3], 2)):
                compare_arrays(a.read_data_for_range(query), b.read_data_for_range(query), True)
                checked += 1
        return {'same_argument_range_queries': checked}
    section('range_list_duplicate_and_slice_contract', ranges)

    def query_workloads():
        result = {}
        for label, level, attr in [('xic',1,'spectrum_data'), ('ms2_batch',2,'spectrum_peak_data')]:
            ids = [i for i in range(len(n)) if directory[i]['ms_level'] == level
                   and 20 <= directory[i]['retention_time_minutes'] <= 25]
            stats = {'scans':len(ids), 'points_compared':0, 'max_mz_error_ppm':0.,
                     'max_intensity_relative_error':0., 'boundary_membership_differences':0,
                     'max_xic_relative_error':0.}
            assert ids
            for start in range(0,len(ids),256):
                group = ids[start:start+256]
                a = getattr(f,attr).read_data_for_range(group)
                b = getattr(o,attr).read_data_for_range(group)
                count,em,ei = compare_arrays(a,b,True)
                stats['points_compared'] += count
                stats['max_mz_error_ppm'] = max(stats['max_mz_error_ppm'],em)
                stats['max_intensity_relative_error'] = max(stats['max_intensity_relative_error'],ei)
                if label == 'xic':
                    lo,hi = 524.3*(1-5e-6),524.3*(1+5e-6)
                    ak = (a['m/z array']>=lo)&(a['m/z array']<=hi)
                    bk = (b['m/z array']>=lo)&(b['m/z array']<=hi)
                    changes = int(np.count_nonzero(ak != bk))
                    stats['boundary_membership_differences'] += changes
                    if changes:
                        y = b['m/z array'][ak != bk]
                        assert np.all(np.minimum(abs(y-lo),abs(y-hi))/y*1e6 <= .01)
                    ax = np.bincount(a['spectrum_index'][ak].astype(int)-group[0],weights=a['intensity array'][ak],minlength=group[-1]-group[0]+1)
                    bx = np.bincount(b['spectrum_index'][bk].astype(int)-group[0],weights=b['intensity array'][bk],minlength=group[-1]-group[0]+1)
                    if not changes:
                        assert_allclose(ax,bx,rtol=.001,atol=0)
                    nz = bx != 0
                    stats['max_xic_relative_error'] = max(stats['max_xic_relative_error'],float(np.max(abs(ax[nz]-bx[nz])/abs(bx[nz]),initial=0)))
            result[label] = stats
        return result
    section('same_xic_and_ms2_queries', query_workloads)

    def all_points():
        totals = {'scans_checked': 0, 'native_points': 0, 'official_points': 0, 'native_zeros': 0,
                  'official_zeros': 0, 'removed_profile_zeros': 0, 'max_mz_error_ppm': 0.,
                  'max_intensity_relative_error': 0., 'batches': 0, 'maximum_native_batch_points': 0}
        start = 0
        while start < len(n):
            end, points = start, 0
            while end < len(n) and end-start < 4096:
                count = directory[end]['count']
                if end > start and points+count > 8000000: break
                points += count; end += 1
            for rep, attr in (('profile', 'spectrum_data'), ('centroid', 'spectrum_peak_data')):
                ids = [i for i in range(start,end) if directory[i]['representation'] == rep]
                if not ids: continue
                a = getattr(n,attr).read_data_for_range(ids)
                c = getattr(f,attr).read_data_for_range(ids) if rep == 'profile' else a
                b = getattr(o,attr).read_data_for_range(ids)
                count, em, ei = compare_arrays(c, b, True)
                ai, av = a['spectrum_index'], a['intensity array']
                # Independent vector mask blocks neighbour retention across scan boundaries.
                keep = av != 0
                if rep == 'profile':
                    keep = keep.copy()
                    same = ai[1:] == ai[:-1]
                    keep[1:] |= (av[:-1] != 0) & same
                    keep[:-1] |= (av[1:] != 0) & same
                else:
                    keep = np.ones(len(av), dtype=bool)
                for key in a:
                    assert_array_equal(a[key][keep], c[key])
                expected_counts = np.array([directory[i]['count'] for i in ids], dtype=np.int64)
                got = np.bincount(ai.astype(np.int64)-start, minlength=end-start)
                assert_array_equal(got[np.array(ids)-start], expected_counts)
                metadata_count = f.spectrum_metadata.data_point_count(ids) if rep == 'profile' else f.spectrum_metadata.peak_count(ids)
                filtered_counts = np.bincount(c['spectrum_index'].astype(np.int64)-start, minlength=end-start)
                assert_array_equal(filtered_counts[np.array(ids)-start], np.asarray(metadata_count, dtype=np.int64))
                totals['native_points'] += len(av); totals['official_points'] += count
                totals['native_zeros'] += int(np.count_nonzero(av == 0))
                totals['official_zeros'] += int(np.count_nonzero(b['intensity array'] == 0))
                totals['removed_profile_zeros'] += int(np.count_nonzero(~keep))
                totals['max_mz_error_ppm'] = max(totals['max_mz_error_ppm'], em)
                totals['max_intensity_relative_error'] = max(totals['max_intensity_relative_error'], ei)
                totals['maximum_native_batch_points'] = max(totals['maximum_native_batch_points'], len(av))
                del a,b,c,ai,av,keep
            totals['scans_checked'] += end-start
            totals['batches'] += 1
            start = end
            if totals['batches'] % 10 == 0:
                REPORT['all_point_progress'] = dict(totals); save()
                print('ALL_POINTS', start, '/', len(n), flush=True)
        assert totals['native_points'] == 1511811155
        assert totals['native_zeros'] == 162582445
        assert totals['native_points'] - totals['official_points'] == totals['removed_profile_zeros']
        return totals
    section('all_scans_all_points_and_zero_policy', all_points)

    def boundaries():
        result = {'scan_probes': 0, 'windows': 0, 'boundary_membership_differences': 0,
                  'max_crossing_distance_ppm': 0., 'official_coordinate_argument_ignored': 0,
                  'identical_membership_windows': 0, 'examples': []}
        probes = [ids[j] for ids in (profiles, peaks) for j in (0,len(ids)//4,len(ids)//2,len(ids)-1)]
        for i in probes:
            attr = 'spectrum_data' if directory[i]['representation'] == 'profile' else 'spectrum_peak_data'
            na, oa = getattr(f,attr), getattr(o,attr)
            a,b = na[i],oa[i]
            compare_arrays(a,b)
            x,y = a['m/z array'],b['m/z array']
            if not len(x): continue
            result['scan_probes'] += 1
            windows = [(500.,600.), (524.3*(1-5e-6),524.3*(1+5e-6)), (0.,1.), (float(y[-1]+1),float(y[-1]+2))]
            for j in sorted(set([0,len(y)//2,len(y)-1])):
                for v in (float(x[j]),float(y[j])):
                    windows.extend([(v,v), (np.nextafter(v,-np.inf),v), (v,np.nextafter(v,np.inf))])
            for lo,hi in windows:
                nk,ok = (x>=lo)&(x<=hi),(y>=lo)&(y<=hi)
                ac = na.read_data_for_range([i], Span(lo,hi))
                bc = oa.read_data_for_range([i], Span(lo,hi))
                for key in ('m/z array','intensity array'):
                    assert_array_equal(ac[key], a[key][nk])
                # The pinned official implementation currently ignores coordinate_range.
                if np.array_equal(bc['m/z array'], y[ok]):
                    pass
                elif np.array_equal(bc['m/z array'], y):
                    result['official_coordinate_argument_ignored'] += 1
                else:
                    raise AssertionError(('Unexpected official coordinate behaviour', i,lo,hi))
                changed = nk != ok
                crossings = int(np.count_nonzero(changed))
                if crossings:
                    distance = np.minimum(np.abs(y[changed]-lo),np.abs(y[changed]-hi))/y[changed]*1e6
                    maximum = float(np.max(distance))
                    assert maximum <= .01, (i,lo,hi,maximum)
                    result['max_crossing_distance_ppm'] = max(result['max_crossing_distance_ppm'],maximum)
                    if len(result['examples']) < 8:
                        result['examples'].append({'scan': i,'lo':float(lo),'hi':float(hi),'native_points':int(nk.sum()),'official_points_after_explicit_filter':int(ok.sum())})
                else:
                    result['identical_membership_windows'] += 1
                    compare_arrays({k:a[k][nk] for k in a},{k:b[k][ok] for k in b})
                result['boundary_membership_differences'] += crossings
                result['windows'] += 1
        return result
    section('inclusive_mz_boundaries_and_quantization_crossings', boundaries)

    def chromatograms():
        for method in ('extract_tic','extract_bpc'):
            for a,b in zip(getattr(f,method)(),getattr(o,method)()): assert_array_equal(a,b)
        count = len(o.chromatograms) if o.chromatograms is not None else 0
        for i in range(count):
            a,b = f.read_chromatogram(i),o.read_chromatogram(i)
            assert a.keys() == b.keys()
            for key in a:
                if key.endswith(' array'): assert_array_equal(a[key],b[key])
        return {'tic_bpc_exact': True,'chromatograms_checked':count}
    section('chromatograms_tic_bpc', chromatograms)

    def lifetime():
        assert n.check_archive_integrity() == (True,[])
        with MzPeakFile(NATIVE.parent) as r:
            compare_arrays(r[0],n[0])
        assert r._signal.file.closed and r._signal.mm.closed and r._archive.fp is None
        r.close()
        return {'native_manifest_checksums': True, 'directory_input_and_idempotent_close': True}
    section('integrity_and_lifetime', lifetime)

    def flattened():
        a = MzPeakFile(NATIVE, flatten_columns=True, filter_profile_zeros=True)
        b = Official(OFFICIAL, flatten_columns=True)
        try:
            frame_compare(a.spectra,b.spectra,calibration=True)
            for i in (profiles[0],peaks[0],len(n)-1): scalar(a[i],b[i])
        finally:
            a.close(); b.close()
        return {'flattened_metadata_rows':len(n), 'spectrum_queries':3}
    section('flattened_columns', flattened)
    for reader in (n,f,o): reader.close()
    boundary = REPORT['sections']['inclusive_mz_boundaries_and_quantization_crossings'].get('result',{})
    REPORT['compatibility_limits'] = {
        'bit_identical_signal': False,
        'same_exact_boundary_membership': boundary.get('boundary_membership_differences',0) == 0,
        'same_coordinate_argument_behavior': boundary.get('official_coordinate_argument_ignored',0) == 0,
        'all_reference_queries_completed':not REPORT.get('reference_timeouts'),
        'scope': 'Pinned Python API paths listed in sections; excludes SQL, URI inputs, wavelength signal, and Python fallback decoder.'}
    REPORT['status'] = 'failed' if REPORT['failures'] else 'passed_with_documented_reference_limits' 
    REPORT['elapsed_seconds'] = time.perf_counter()-started
    save()
    print('FINAL',REPORT['status'],REPORT['failures'],flush=True)
    return bool(REPORT['failures'])


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        REPORT['status'] = 'failed'
        REPORT['fatal_error'] = traceback.format_exc()
        if OUTPUT is not None: save()
        raise
