"""Check published arithmetic and validation evidence; does not rerun RAW decoding."""
import json
import math
from pathlib import Path
import statistics

EVIDENCE = Path(__file__).resolve().parent / 'evidence'

def read(name):
    return json.loads((EVIDENCE / name).read_text())

def close(actual, expected):
    assert math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-10), (actual, expected)

def geomean(values):
    return math.exp(statistics.mean(math.log(x) for x in values))

def verify():
    rows = read('conversion-results.json')
    aggregates = read('aggregate.json')
    validations = {r['case']: r['validation'] for r in read('validation.json')}
    cases = {}
    for row in rows:
        case = cases.setdefault(row['case'], {})
        assert row['mode'] not in case
        case[row['mode']] = row
        runs = row['measured_runs']
        successful = [r for r in runs if r['returncode'] == 0 and not r['timed_out'] and r['valid_archive']]
        eligible = (len(runs) == len(successful) == row['required_repeats'] and row['validation_passed'])
        assert row['eligible'] == eligible, row['case']
        if eligible:
            assert len({r['block'] for r in runs}) == len(runs)
            fingerprints = [r.get('signal_hashes') for r in runs]
            assert fingerprints[0] and all(f == fingerprints[0] for f in fingerprints)
            assert validations[row['case']][row['mode']]['valid']
            for metric, source in [('median_seconds', 'wall_seconds'),
                                   ('median_tree_rss_bytes', 'sampled_tree_rss_bytes'),
                                   ('median_peak_process_rss_bytes', 'max_single_process_rss_bytes')]:
                close(row['metrics'][metric], statistics.median(r[source] for r in runs))
            assert all(r['output_bytes'] == row['metrics']['output_bytes'] for r in runs)
    assert len(rows) == 39 and len(cases) == 13
    assert all(set(c) == {'rawr', 'vendor', 'official_default_z3'} for c in cases.values())
    assert len({c['rawr']['source_sha256'] for c in cases.values()}) == 13
    assert sum(c['rawr']['source_bytes'] for c in cases.values()) == 49_477_787_195
    for label, candidate, baseline in [('rawr_vs_official_default_z3', 'rawr', 'official_default_z3'),
                                       ('vendor_vs_official_default_z3', 'vendor', 'official_default_z3'),
                                       ('rawr_vs_vendor', 'rawr', 'vendor')]:
        pairs = [(c[candidate], c[baseline]) for c in cases.values() if c[candidate]['eligible'] and c[baseline]['eligible']]
        expected = aggregates[label]
        assert len(pairs) == expected['eligible_files']
        assert expected['reported_files'] == expected['planned_files'] == 13
        speeds = []
        for c, b in pairs:
            cr = {r['block']: r['wall_seconds'] for r in c['measured_runs']}
            br = {r['block']: r['wall_seconds'] for r in b['measured_runs']}
            assert cr.keys() == br.keys()
            speeds.append(geomean([br[k] / cr[k] for k in cr]))
        speed = geomean(speeds)
        reduction = 100 * (1 - sum(c['metrics']['output_bytes'] for c, b in pairs) / sum(b['metrics']['output_bytes'] for c, b in pairs))
        close(speed, expected['geomean_speedup'])
        close(reduction, expected['total_size_reduction_percent'])
        close(sum(b['metrics']['median_seconds'] for c, b in pairs) / sum(c['metrics']['median_seconds'] for c, b in pairs), expected['aggregate_time_speedup'])
        print(f'{label}: {len(pairs)}/13 eligible; {speed:.4f}x; {reduction:.4f}% smaller')
    accepted = [v['rawr']['details'] for v in validations.values() if v.get('rawr', {}).get('valid')]
    assert len(accepted) == 12
    for v in accepted:
        assert v['all_spectra_all_points_validated'] and v['all_source_point_counts_preserved']
        assert v['all_source_summaries_validated']
        assert v['max_mz_error_ppm'] <= .01
        assert v['max_intensity_relative_error'] <= .001
        assert v['max_source_summary_mz_difference_ppm'] <= .001
    assert sum(v['points'] for v in accepted) == 3_355_346_983
    assert sum(v['zeros_preserved'] for v in accepted) == 1_009_282_702
    print('39 cells checked; 12 native files / 3,355,346,983 points independently validated in recorded evidence.')

if __name__ == '__main__':
    verify()
