"""Render the README figure from published paired conversion measurements.

Requires matplotlib; run from any directory. No new benchmark measurements.
"""
import json
import math
from pathlib import Path
import statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
rows = json.loads((ROOT / 'evidence/conversion-results.json').read_text())
cases = {}
for row in rows:
    cases.setdefault(row['case'], {})[row['mode']] = row
names = ['HeLa · QE HF', 'Fusion', 'Eclipse', 'Ascend', 'QE HF', 'Lumos',
         'Velos', 'HF-X', 'Q Exactive', 'QE Plus', 'Exploris']
selected = [case for case in cases.values() if case['rawr']['eligible'] and case['official_default_z3']['eligible']]
assert len(selected) == len(names) == 11
plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 11, 'svg.hashsalt': 'mzpeak-study-2026-10'})
fig, ax = plt.subplots(figsize=(11.5, 6.8), facecolor='#f8fafb')
ax.set_facecolor('#f8fafb')
for mode, offset, color, label in [('rawr', -.17, '#087f8c', 'Native rawr-ms'), ('vendor', .17, '#b8662c', 'Thermo reader')]:
    for i, case in enumerate(selected):
        if not case[mode]['eligible']:
            ax.text(.08, i + offset, 'excluded: repeat timeout', color='#65717c', fontsize=9, va='center')
            continue
        candidate = {r['block']: r['wall_seconds'] for r in case[mode]['measured_runs']}
        baseline = {r['block']: r['wall_seconds'] for r in case['official_default_z3']['measured_runs']}
        assert candidate.keys() == baseline.keys()
        value = math.exp(statistics.mean(math.log(baseline[k] / candidate[k]) for k in candidate))
        ax.barh(i + offset, value, height=.29, color=color, label=label if i == 0 else None, zorder=3)
        ax.text(value + .06, i + offset, f'{value:.2f}×', va='center', fontsize=9, color='#233340')
ax.axvline(1, color='#83919b', linestyle='--', linewidth=1, zorder=2)
ax.set_yticks(range(len(names)), names)
ax.invert_yaxis()
ax.set_xlim(0, 6.6)
ax.set_xlabel('Paired geometric-mean conversion speedup versus official default / Zstd 3', labelpad=12)
ax.grid(axis='x', color='#dfe6ea', zorder=0)
ax.tick_params(axis='both', length=0, colors='#334854')
for spine in ax.spines.values(): spine.set_visible(False)
fig.suptitle('Direct RAW conversion: every eligible file', x=.03, y=.97, ha='left', fontsize=20, fontweight='bold', color='#142630')
fig.text(.03,.915,'Experimental integer archive · same inputs · complete conversion timing · independent validation',fontsize=10,color='#52636b')
ax.legend(loc='lower left', bbox_to_anchor=(0, 1.01), ncol=2, frameon=False)
fig.text(.03,.025,'Astral30: no eligible modes. Big Astral: native validated; official timed out, so no speedup ratio.',fontsize=10,color='#52636b')
fig.subplots_adjust(left=.18,right=.94,top=.81,bottom=.14)
fig.savefig(ROOT/'conversion-speed.svg',metadata={'Date':None},facecolor=fig.get_facecolor())
if __name__ == '__main__':
    print('Wrote conversion-speed.svg from published measurements.')
