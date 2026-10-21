"""Render the latest matched default and delta comparisons; --check verifies the README."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parent
README = ROOT.parents[1] / 'README.md'
START = '<!-- BEGIN INSTRUMENT COMPARISON -->'
END = '<!-- END INSTRUMENT COMPARISON -->'


def read(name):
    return json.loads((ROOT / 'evidence' / name).read_text())


def close(a, b):
    assert math.isclose(a, b, rel_tol=1e-10), (a, b)


def verify_cell(row):
    runs = row['measured_runs']
    valid = row.get('validation_passed', row.get('validation', {}).get('valid', False))
    eligible = valid and len(runs) == row['required_repeats'] and all(
        r.get('valid_archive') and r['returncode'] == 0 and not r['timed_out'] for r in runs)
    assert eligible == row['eligible']
    if not eligible:
        return
    assert len({r['block'] for r in runs}) == len(runs)
    assert {r['block'] for r in runs} == set(range(row['required_repeats']))
    all_runs = runs + row['excluded_runs']
    assert all(r.get('signal_hashes') == runs[0]['signal_hashes'] for r in all_runs if r.get('valid_archive'))
    assert runs[0]['signal_hashes']
    for metric, key in [('median_seconds', 'wall_seconds'), ('output_bytes', 'output_bytes'),
                        ('median_peak_process_rss_bytes', 'max_single_process_rss_bytes')]:
        close(row['metrics'][metric], statistics.median(r[key] for r in runs))
    if 'validation' in row:
        detail = row['validation']['details']
        if row['mode'] in ('rawr', 'vendor'):
            assert detail['all_spectra_all_points_validated'] and detail['all_source_point_counts_preserved']
            assert detail['max_mz_error_ppm'] <= .01
            assert detail['max_intensity_relative_error'] <= .001
            assert detail['all_source_summaries_validated']
            assert detail['max_source_summary_mz_difference_ppm'] <= .001
        else:
            assert detail['retained_values_exact']


def speedup(native, official):
    if not (native['eligible'] and official['eligible']):
        return None
    assert native['validation']['details']['source_array_sha256'] == official['validation']['details']['source_array_sha256']
    n = {r['block']: r['wall_seconds'] for r in native['measured_runs']}
    o = {r['block']: r['wall_seconds'] for r in official['measured_runs']}
    assert n.keys() == o.keys()
    return statistics.geometric_mean(o[b] / n[b] for b in n)


def render():
    official_single = read('astral-official-single.json')
    log = (ROOT / 'evidence/astral-official-single.time').read_bytes()
    assert hashlib.sha256(log).hexdigest() == official_single['published_log_sha256']
    assert b'22:54.09' in log and b'Exit status: 0' in log
    assert official_single['wall_seconds'] == 1374.09 and official_single['measured_conversions'] == 1
    assert not official_single['comparison_eligible']
    coverage = read('coverage.json')
    assert len({c['instrument_display'].rstrip('*') for c in coverage}) == 11
    primary = read('conversion-results.json')
    validations = {r['case']: r['validation'] for r in read('validation.json')}
    for row in primary:
        row['validation'] = validations[row['case']].get(row['mode'], {})
    delta = read('native-delta-results.json')['rows']
    campaigns = [('Default', primary, 'official_default_z3'), ('Delta', delta, 'official_delta_z3')]
    by_campaign = []
    aggregate = []
    for name, rows, baseline in campaigns:
        cells = {(r['case'], r['mode']): r for r in rows}
        assert len(cells) == len(rows)
        for row in rows:
            verify_cell(row)
        pairs = [(cells[c['case'], 'rawr'], cells[c['case'], baseline]) for c in coverage]
        eligible = [(n, o) for n, o in pairs if n['eligible'] and o['eligible']]
        assert len(pairs) == 13 and len(eligible) == 11
        ratio = statistics.geometric_mean(speedup(n, o) for n, o in eligible)
        reduction = 100 * (1 - sum(n['metrics']['output_bytes'] for n, o in eligible) /
                           sum(o['metrics']['output_bytes'] for n, o in eligible))
        aggregate.append((ratio, reduction))
        by_campaign.append(pairs)
    close(aggregate[0][0], read('aggregate.json')['rawr_vs_official_default_z3']['geomean_speedup'])
    close(aggregate[0][1], read('aggregate.json')['rawr_vs_official_default_z3']['total_size_reduction_percent'])
    summary = read('supplementary-summary.json')
    delta_files = [f for f in summary['files'] if 'official_delta_z3' in f['ratios']]
    close(aggregate[1][1], 100 * (1 - sum(f['metrics']['rawr']['output_bytes'] for f in delta_files) /
                                   sum(f['metrics']['official_delta_z3']['output_bytes'] for f in delta_files)))
    close(aggregate[1][0], summary['aggregate']['official_delta_z3']['geomean_speedup'])
    lines = [START, '**13 RAW files · 49.48 GB · 11 instrument models represented · two official baselines**', '',
        'The table uses the latest available validated comparison for each baseline. Every speedup pairs native and official runs from the same campaign.', '',
        '| Official baseline | Eligible files | Conversion speedup | Less output storage |',
        '|:--|--:|--:|--:|']
    for (name, _, _), (ratio, reduction) in zip(campaigns, aggregate):
        lines.append(f'| **{name}, Zstd 3** | **11/13** | **{ratio:.2f}×** | **{reduction:.2f}%** |')
    lines.extend(['',
        'Speedups are paired geometric means with equal weight per file. Storage reduction uses combined bytes on eligible files. Failed baselines receive no ratio.', '',
        '### Full instrument range: native versus official default and delta', '',
        'One row per file. Times and archive sizes are shown directly. Official time and size refer to default encoding. Delta means `delta:50`; both official modes use Zstd 3.', '',
        'Each baseline uses its latest matched campaign. Default and delta used different nodes and native builds. No timing is transferred between them.', '',
        '| Instrument / acquisition | Native s | Official s | Native GB | Official GB | Default speedup | Delta speedup |',
        '|:--|--:|--:|--:|--:|--:|--:|'])
    storage_rows = []
    def value(row, key, scale, digits):
        return f"{row['metrics'][key] / scale:,.{digits}f}" if row['eligible'] else '—'
    def instrument(c):
        name = c['instrument_display'].replace('Orbitrap ', '').replace('LTQ Velos', 'LTQ Orbitrap Velos').rstrip('*')
        if c['case'] == '00-HeLa_control':
            return name + ' (control)'
        if c['case'] == '11-PXD046453':
            name += ' (30 min)'
        elif c['case'] == '12-PXD049028':
            name += ' (60 min)'
        return f"[{name}]({c['project_url']})"
    for i, c in enumerate(coverage):
        pairs = [p[i] for p in by_campaign]
        for n, o in pairs:
            assert n['source_sha256'] == o['source_sha256'] == c['source_sha256']
            assert n['source_bytes'] == o['source_bytes'] == pairs[0][0]['source_bytes']
        speeds, savings = [], []
        for n, o in pairs:
            ratio = speedup(n, o)
            unavailable = 'Official timeout' if n['eligible'] and 'Timeout' in o['status'] else 'Metadata failed'
            if c['case'] == '12-PXD049028' and o['mode'] == 'official_default_z3':
                unavailable = 'Not paired¹'
            speeds.append(f'**{ratio:.2f}×**' if ratio is not None else unavailable)
            savings.append(f"{100*(1-n['metrics']['output_bytes']/o['metrics']['output_bytes']):.1f}%" if ratio is not None else '—')
        sample = c['acquisition_sample_label']
        if c['case'] == '12-PXD049028':
            sample += '; **native validated**'
        native, official = pairs[0]
        times = [value(native, 'median_seconds', 1, 2), value(official, 'median_seconds', 1, 2)]
        sizes = [value(native, 'output_bytes', 1e9, 3), value(official, 'output_bytes', 1e9, 3)]
        if c['case'] == '12-PXD049028':
            times[1] = f"{official_single['wall_seconds']:,.2f}¹"
            sizes[1] = f"{official_single['output_bytes']/1e9:.3f}¹"
        cells = [instrument(c)+'<br>'+sample, *times, *sizes, *speeds]
        storage_rows.append('| '+' | '.join([instrument(c), *savings])+' |')
        lines.append('| '+' | '.join(cells)+' |')
    astral = next(r for r in primary if r['case'] == '12-PXD049028' and r['mode'] == 'rawr')
    assert astral['eligible']
    am = astral['metrics']
    av = astral['validation']['details']
    assert official_single['source_sha256'] == astral['source_sha256']
    query = read('astral-queries.json')
    assert official_single['query_validation'] == query['validation']
    assert official_single['archive_sha256_recorded_by_query_benchmark'] == query['sha256']['official.mzpeak']
    lines.extend(['',
        '¹ **Astral’s official values are from a completed single run on mem4.** Native values are the three-run median on mem1. Official validation covered query selections only. These are separate runs, not a paired speedup, and the official single run is excluded from aggregates. All other displayed times are campaign medians.', '',
        '<details>', '<summary>Astral run provenance and validation</summary>', '',
        '#### Completed big Astral conversions', '',
        '**Same 21.78 GB RAW file; separate allocations and validation scopes. These times do not form a paired speedup.**', '',
        '| Conversion | Runs | Time, s | Peak RAM, GiB | Archive, GB | Validation |',
        '|:--|--:|--:|--:|--:|:--|',
        f"| Native, latest campaign (mem1) | 3 | **{am['median_seconds']:.2f}** median | {am['median_peak_process_rss_bytes']/2**30:.2f} | **{am['output_bytes']/1e9:.2f}** | All {av['points']:,} points |",
        f"| Official default, query setup (mem4) | 1 | **{official_single['wall_seconds']:.2f}** | {official_single['max_single_process_rss_bytes']/2**30:.2f} | **{official_single['output_bytes']/1e9:.2f}** | Query selections only |", '',
        f"Native validation covered all **{av['spectra']:,} scans** and preserved all **{av['zeros_preserved']:,} zeros**. The official run exited successfully in **22m 54s**. Its selected query arrays passed comparison; whole-archive official validation is not recorded for this run. It is excluded from speedup and storage-saving aggregates. Official delta has no completed Astral conversion in the saved evidence.", '',
        '[Successful official conversion evidence](benchmarks/raw-integer-2026-10/evidence/astral-official-single.json) · [Timing log](benchmarks/raw-integer-2026-10/evidence/astral-official-single.time) · [Query validation](benchmarks/raw-integer-2026-10/evidence/astral-queries.json)', '',
        '</details>', '',
        'The separate 30-minute Astral file failed complete metadata export. Dashes mean no eligible measurement.', '',
        '**Precision:** ours keeps all source points within 0.01 ppm m/z and 0.1% intensity error. Official default and delta retain exact values after profile-zero filtering. Storage savings include this difference.', '',
        '**RAM is not a consistent win.** Native conversion uses more memory on several instruments. Expand the measurements below for every time, RAM value, and archive size.', '',
        '<details>', '<summary><strong>Full measurements: time, RAM, and archive size</strong></summary>', '',
        'Each cell contains one value. Times and RAM values are medians. RAM is GNU time maximum process RSS, not concurrent process-tree memory. GiB means 2³⁰ bytes; MB and GB are decimal units.', ''])
    for (name, _, _), pairs in zip(campaigns, by_campaign):
        lines.extend([f'#### Native versus official {name.lower()}', '',
            '| Instrument | Native s | Official s | Native GiB | Official GiB | Native MB | Official MB |',
            '|:--|--:|--:|--:|--:|--:|--:|'])
        for c, (n, o) in zip(coverage, pairs):
            cells = [instrument(c)]
            for key, scale, digits in [('median_seconds', 1, 2), ('median_peak_process_rss_bytes', 2**30, 2), ('output_bytes', 1e6, 1)]:
                cells.extend([value(n, key, scale, digits), value(o, key, scale, digits)])
            lines.append('| '+' | '.join(cells)+' |')
        lines.append('')
    lines.extend(['#### Storage saved versus each official baseline', '',
        '| Instrument | Default | Delta |', '|:--|--:|--:|', *storage_rows, ''])
    lines.extend(['A reported measurement requires all declared repeats and independent all-point validation. A timeout means a required run exceeded 600 seconds. Warmups are excluded. No ratio uses a failed baseline.', '',
        '</details>', '', '<details>', '<summary>Campaigns, repeats, and source evidence</summary>', '',
        'Default uses the later native/vendor campaign on mem1 (8 CPUs, 64 GiB). Delta uses the latest campaign that measured delta on tta1 (8 CPUs, 32 GiB). The native signal binaries differ: `f27b8331` and `53a4ac2e`. Each ratio uses its own matched native run. Delta has not been measured with the later binary.', '',
        'Default retains seven repeats for the first three files and three for the rest. Delta retains seven for the first eight files and three for the rest. Each mode has one excluded warmup. The user-requested amendment preserved completed runs and frozen binaries.', '',
        'The selected RAW files range from 0.116 GB to 21.777 GB. Instrument links open their source project. The Astral30 model comes from its project record; its reader reported an unknown model. These data cover Thermo RAW, not other vendor formats.', '',
        '[Instrument and acquisition evidence](benchmarks/raw-integer-2026-10/evidence/coverage.json) · [Default campaign runs](benchmarks/raw-integer-2026-10/evidence/conversion-results.json) · [Delta campaign runs and validation](benchmarks/raw-integer-2026-10/evidence/native-delta-results.json)', '',
        '</details>', '', '<details>', '<summary>Thermo-reader option and earlier campaign history</summary>', '',
        'Our Thermo RawFileReader path is **1.74× faster** and uses **74.43% less combined storage** than official default on 10/13 eligible files. Its corrected process-tree RSS ratio is 0.71× official. These measurements belong to the mem1 campaign. [Full reader-mode results](benchmarks/raw-integer-2026-10/RESULTS.md).', '',
        'The original hybrid campaign used vendor code for metadata and calibration. Its 268 attempts and recovery limits remain in the [historical evidence](benchmarks/raw-integer-2026-10/evidence/original-warrior.json). Those timings are not used above.', '',
        '</details>', END])
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    text = README.read_text()
    assert text.count(START) == text.count(END) == 1
    result = text[:text.index(START)] + render() + text[text.index(END)+len(END):]
    if args.check:
        assert result == text, 'README comparison differs from published evidence'
        print('13 files / 26 matched comparisons verified, including failures and both aggregates.')
    else:
        README.write_text(result)
        print('Updated the latest native/default/delta comparison.')


if __name__ == '__main__':
    main()
