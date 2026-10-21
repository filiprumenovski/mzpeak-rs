"""Generate the complete README comparison from the published evidence.

Run with --check to detect stale or manually changed table cells.
"""
import argparse
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parent
README = ROOT.parents[1] / 'README.md'
START = '<!-- BEGIN INSTRUMENT COMPARISON -->'
END = '<!-- END INSTRUMENT COMPARISON -->'
MODES = ('rawr', 'vendor', 'official_default_z3')

def render_original(coverage):
    data = json.loads((ROOT/'evidence/original-warrior.json').read_text())
    cases = data['cases']
    modes = ('rawr', 'official_default_z3', 'official_delta_z3')
    assert len(cases) == 13
    assert sum(len(c['runs']) for c in cases) == 268
    assert sum(bool(r.get('valid_archive')) for c in cases for r in c['runs']) == 261
    labels = {c['case']:c for c in coverage}
    totals = {m:0 for m in modes}
    validated = points = eligible = 0
    lines = ['### Original Warrior campaign: instrument and acquisition coverage', '',
        '**13 RAW files · 49.48 GB · 268 conversion attempts · two official baselines.**', '',
        'This is the original October 19–20 campaign on Warrior, job 40625404. It used eight CPUs and a 32 GiB allocation. The inventory contained 334 RAW files (548.43 GB). We selected 13 files; we did not convert the full inventory.', '',
        'The experimental pipeline used rawr-ms for signal reading and vendor code for metadata and calibration. These measurements precede the full-native converter.', '',
        'Each result cell shows **median seconds / archive MB**. MB and GB are decimal units. Both official modes used Zstd level 3. Delta used `delta:50`.', '',
        '| Instrument / dataset | Acquisition / sample | RAW GB | Experimental<br>s / MB | Official default<br>s / MB | Official delta<br>s / MB | Recovery result |',
        '|:--|:--|--:|--:|--:|--:|:--|']
    for case in cases:
        c = labels[case['id']]
        assert c['source_sha256'] == case['source_sha256']
        recovered = 'recovery_summary' in case
        if recovered:
            validated += 1
            points += case['recovery_summary']['points']
            for mode in modes:
                assert case['recovery_validation'][mode]['valid']
                linked = case['recovery_hash_links'][mode]
                successful = [r for r in case['runs'] if r['variant']==mode and r.get('valid_archive')]
                assert linked['original_runs_matched'] == len(successful)
                assert all(r['signal_hashes']==linked['signal_hashes'] for r in successful)
            detail = case['recovery_validation']['rawr']['details']
            assert detail['max_mz_error_ppm'] <= .01
            assert detail['max_intensity_relative_error'] <= .001
        values = []
        counts = []
        for mode in modes:
            runs = [r for r in case['runs'] if r['variant']==mode and r['phase']=='measure']
            good = [r for r in runs if r.get('valid_archive') and r['returncode']==0 and not r['timed_out']]
            counts.append(len(good))
            if recovered and good:
                size = statistics.median(r['output_bytes'] for r in good)
                totals[mode] += size
                values.append(f"{statistics.median(r['wall_seconds'] for r in good):.1f} / {size/1e6:,.1f}")
            else:
                values.append('Timeout' if any(r['timed_out'] for r in case['runs'] if r['variant']==mode) else 'Metadata failure')
        eligible += recovered and counts == [7,7,7]
        status = 'Excluded'
        if recovered:
            status = 'Validated; 7 repeats' if counts == [7,7,7] else 'Validated signal; 4 experimental repeats*'
        source = f"[{c['dataset']}]({c['project_url']})" if c.get('project_url') else 'HeLa control'
        cells = [c['instrument_display']+'<br>'+source,c['acquisition_sample_label'],f"{case['source_bytes']/1e9:.3f}",*values,status]
        lines.append('| '+' | '.join(cells)+' |')
    assert (validated, points, eligible) == (11, 1843535828, 10)
    assert totals == {'rawr':3466949822,'official_default_z3':12975103923,'official_delta_z3':9034407352}
    lines.extend(['',
        '**11/13 files passed recovery validation: 1,843,535,828 source points.** Their experimental archives total **3.47 GB**, compared with **12.98 GB** for official default and **9.03 GB** for official delta. Combined storage is **73.28%** and **61.63%** smaller, respectively.', '',
        'The experimental error limits are 0.01 ppm for m/z and 0.1% for intensity. Official output retains exact values after profile-zero filtering. These are different precision requirements.', '',
        '*Eclipse completed four measured experimental runs, then timed out. The table shows the median of those four runs. Only 10/13 files qualify for a seven-repeat performance comparison. Warmups and recovery conversions are excluded from timing.', '',
        '**Recovery limits:** the first validator failed. Regenerated signal payloads matched every successful original run. Seven files also matched original metadata bytes. Four files passed semantic checks on regenerated metadata; their deleted original metadata bytes cannot be checked again.', '',
        '**RAM correction:** the original process-tree sampler missed child processes. Its RAM column is invalid and is omitted. The later reader comparison below uses the corrected sampler.', '',
        'Instrument and acquisition labels use the [coverage evidence](benchmarks/raw-integer-2026-10/evidence/coverage.json). The Astral30 model comes from its project record; the reader reported an unknown model.', '',
        '[Original measurements, recovery checks, and source hashes →](benchmarks/raw-integer-2026-10/evidence/original-warrior.json)', '',
        '<details>', '<summary>Later native / Thermo-reader / official comparison, including corrected RAM</summary>', ''])
    return '\n'.join(lines)

def render():
    rows = json.loads((ROOT/'evidence/conversion-results.json').read_text())
    coverage = json.loads((ROOT/'evidence/coverage.json').read_text())
    cases = {}
    for row in rows:
        cases.setdefault(row['case'], {})[row['mode']] = row
    assert len(rows) == 39 and len(cases) == len(coverage) == 13
    assert {c['case'] for c in coverage} == set(cases)
    lines = [START, render_original(coverage), '### Later reader comparison', '',
        'All 13 selected files appear below. Each cell uses the order **N / V / O**: native rawr-ms, our Thermo-reader path, and official default.', '',
        'Our limits are 0.01 ppm for m/z and 0.1% for intensity. Official default preserves retained values exactly.', '',
        'Times and memory values are medians. Memory is sampled process-tree RSS in GiB. Archive sizes use decimal MB. RAW sizes use decimal GB.', '',
        '| Instrument / dataset | Acquisition / sample | RAW GB | Time, s<br>N / V / O | RSS, GiB<br>N / V / O | Archive, MB<br>N / V / O | Speedup vs O<br>N / V | Less storage vs O<br>N / V | Status<br>N / V / O |',
        '|:--|:--|--:|--:|--:|--:|--:|--:|:--|']
    def metric(row, key, divisor=1, digits=2):
        return f"{row['metrics'][key]/divisor:,.{digits}f}" if row['eligible'] else '—'
    for c in coverage:
        variants = cases[c['case']]
        n = variants['rawr']
        assert c['source_sha256'] == n['source_sha256']
        assert all(r['source_sha256'] == n['source_sha256'] for r in variants.values())
        source = f"[{c['dataset']}]({c['project_url']})" if c.get('project_url') else 'HeLa control'
        values = []
        for key, divisor, digits in [('median_seconds',1,2),('median_tree_rss_bytes',2**30,2),('output_bytes',1e6,1)]:
            values.append('<br>'.join(metric(variants[m],key,divisor,digits) for m in MODES))
        speed, storage = [], []
        official = variants['official_default_z3']
        for mode in MODES[:2]:
            candidate = variants[mode]
            if not (candidate['eligible'] and official['eligible']):
                speed.append('—'); storage.append('—'); continue
            cr = {r['block']:r['wall_seconds'] for r in candidate['measured_runs']}
            br = {r['block']:r['wall_seconds'] for r in official['measured_runs']}
            assert cr.keys() == br.keys()
            ratio = math.exp(statistics.mean(math.log(br[b]/cr[b]) for b in cr))
            speed.append(f'{ratio:.2f}×')
            reduction = 100*(1-candidate['metrics']['output_bytes']/official['metrics']['output_bytes'])
            storage.append(f'{reduction:.1f}%')
        status = '<br>'.join('Pass' if variants[m]['eligible'] else 'Timeout' if 'Timeout' in variants[m]['status'] else 'Metadata' for m in MODES)
        cells = [c['instrument_display']+'<br>'+source,c['acquisition_sample_label'],f"{n['source_bytes']/1e9:.3f}",*values,'<br>'.join(speed),'<br>'.join(storage),status]
        lines.append('| '+' | '.join(cells)+' |')
    lines.extend(['',
        '**Pass** requires all declared repeats and independent validation. **Timeout** means that a required run exceeded 600 seconds. **Metadata** means that complete metadata export failed.', '',
        'A dash means no eligible measurement or ratio. Exploris vendor output passed numerical validation, but one repeat timed out. Its comparison remains excluded.', '',
        'Speedups use paired geometric means. They can differ from ratios of displayed median times. The slow Eclipse vendor repeat remains in the calculation.', '',
        'Acquisition and sample labels use selected filenames, stored scan metadata, and linked project records. The [coverage evidence](benchmarks/raw-integer-2026-10/evidence/coverage.json) records each label and its basis.', '',
        '*The Astral30 instrument name comes from the project record. The benchmark reader returned an unknown instrument model, and all complete conversions failed.', '',
        'This table covers Thermo RAW files. It does not claim benchmarks for every instrument or acquisition method in each source project.', '', '</details>', END])
    return '\n'.join(lines)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check',action='store_true')
    args=parser.parse_args()
    text=README.read_text()
    assert text.count(START)==text.count(END)==1
    start=text.index(START);end=text.index(END)+len(END)
    result=text[:start]+render()+text[end:]
    if args.check:
        assert result==text, 'README comparison differs from published evidence'
        print('Both campaigns match the evidence: original 268 attempts and later 39 mode cells.')
    else:
        README.write_text(result)
        print('Updated the complete README comparison.')

if __name__=='__main__':
    main()
