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

def render():
    rows = json.loads((ROOT/'evidence/conversion-results.json').read_text())
    coverage = json.loads((ROOT/'evidence/coverage.json').read_text())
    cases = {}
    for row in rows:
        cases.setdefault(row['case'], {})[row['mode']] = row
    assert len(rows) == 39 and len(cases) == len(coverage) == 13
    assert {c['case'] for c in coverage} == set(cases)
    lines = [START, '### Full instrument comparison', '',
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
        'This table covers Thermo RAW files. It does not claim benchmarks for every instrument or acquisition method in each source project.', END])
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
        print('All 13 instrument rows / 39 mode cells match the published evidence.')
    else:
        README.write_text(result)
        print('Updated the complete README comparison.')

if __name__=='__main__':
    main()
