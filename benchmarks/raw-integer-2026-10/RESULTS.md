# Per-file conversion results

Eight-CPU, 64 GiB allocation on mem1; warm filesystem cache; Zstd level 3. Times and RSS are medians. Sizes are decimal MB. A dash means the required repeats or independent validation did not pass; it is not a zero.

| Dataset / instrument | Mode | Repeats | Seconds | Tree RSS MiB | Max-process RSS MiB | Output MB | Status |
|---|---|---:|---:|---:|---:|---:|---|
| HeLa_control / Q Exactive HF Orbitrap | rawr | 7/7 | 14.86 | 1684.32 | 1663.95 | 240.47 | Validated |
| HeLa_control / Q Exactive HF Orbitrap | vendor | 7/7 | 34.69 | 757.96 | 509.67 | 240.47 | Validated |
| HeLa_control / Q Exactive HF Orbitrap | official_default_z3 | 7/7 | 61.82 | 1273.15 | 1285.59 | 1080.10 | Validated |
| PXD017803 / Orbitrap Fusion | rawr | 7/7 | 3.26 | 159.07 | 159.28 | 26.37 | Validated |
| PXD017803 / Orbitrap Fusion | vendor | 7/7 | 14.82 | 321.26 | 238.61 | 26.37 | Validated |
| PXD017803 / Orbitrap Fusion | official_default_z3 | 7/7 | 16.97 | 308.35 | 309.59 | 45.41 | Validated |
| PXD023203 / Orbitrap Eclipse | rawr | 7/7 | 7.44 | 226.00 | 227.82 | 63.47 | Validated |
| PXD023203 / Orbitrap Eclipse | vendor | 7/7 | 34.68 | 350.05 | 226.82 | 63.47 | Validated |
| PXD023203 / Orbitrap Eclipse | official_default_z3 | 7/7 | 40.06 | 271.74 | 271.14 | 107.14 | Validated |
| PXD043660 / Orbitrap Ascend | rawr | 3/3 | 15.33 | 1576.52 | 1555.70 | 222.73 | Validated |
| PXD043660 / Orbitrap Ascend | vendor | 3/3 | 33.62 | 445.81 | 240.36 | 222.73 | Validated |
| PXD043660 / Orbitrap Ascend | official_default_z3 | 3/3 | 59.79 | 656.12 | 656.19 | 829.94 | Validated |
| PXD019848 / Q Exactive HF Orbitrap | rawr | 3/3 | 14.99 | 1615.98 | 1597.58 | 231.61 | Validated |
| PXD019848 / Q Exactive HF Orbitrap | vendor | 3/3 | 35.43 | 821.80 | 576.86 | 231.61 | Validated |
| PXD019848 / Q Exactive HF Orbitrap | official_default_z3 | 3/3 | 61.07 | 1344.26 | 1347.91 | 1032.02 | Validated |
| PXD046211 / Orbitrap Fusion Lumos | rawr | 3/3 | 14.32 | 1374.89 | 1354.25 | 193.43 | Validated |
| PXD046211 / Orbitrap Fusion Lumos | vendor | 3/3 | 40.52 | 464.46 | 282.25 | 193.43 | Validated |
| PXD046211 / Orbitrap Fusion Lumos | official_default_z3 | 3/3 | 64.81 | 776.82 | 778.77 | 620.72 | Validated |
| PXD005780 / LTQ Orbitrap Velos | rawr | 3/3 | 14.65 | 2533.54 | 2512.83 | 351.84 | Validated |
| PXD005780 / LTQ Orbitrap Velos | vendor | 3/3 | 29.90 | 492.38 | 390.43 | 351.83 | Validated |
| PXD005780 / LTQ Orbitrap Velos | official_default_z3 | 3/3 | 68.08 | 1061.23 | 1061.62 | 1451.40 | Validated |
| PXD053241 / Q Exactive HF-X Orbitrap | rawr | 3/3 | 26.44 | 1445.50 | 1424.79 | 347.68 | Validated |
| PXD053241 / Q Exactive HF-X Orbitrap | vendor | 3/3 | 72.43 | 598.23 | 380.79 | 347.68 | Validated |
| PXD053241 / Q Exactive HF-X Orbitrap | official_default_z3 | 3/3 | 109.28 | 574.67 | 574.16 | 1130.40 | Validated |
| PXD000612 / Q Exactive Orbitrap | rawr | 3/3 | 23.31 | 4441.07 | 4420.45 | 629.73 | Validated |
| PXD000612 / Q Exactive Orbitrap | vendor | 3/3 | 43.41 | 589.25 | 327.55 | 629.72 | Validated |
| PXD000612 / Q Exactive Orbitrap | official_default_z3 | 3/3 | 104.03 | 1109.47 | 1126.91 | 2615.96 | Validated |
| PXD002370 / Q Exactive Plus Orbitrap | rawr | 3/3 | 34.33 | 5378.57 | 5357.86 | 801.62 | Validated |
| PXD002370 / Q Exactive Plus Orbitrap | vendor | 3/3 | 70.40 | 609.61 | 365.25 | 801.62 | Validated |
| PXD002370 / Q Exactive Plus Orbitrap | official_default_z3 | 3/3 | 148.04 | 908.30 | 913.81 | 3246.58 | Validated |
| PXD063416 / Orbitrap Exploris 480 | rawr | 3/3 | 22.26 | 423.04 | 402.42 | 376.76 | Validated |
| PXD063416 / Orbitrap Exploris 480 | vendor | 2/3 | — | — | — | — | Timeout (600 s) |
| PXD063416 / Orbitrap Exploris 480 | official_default_z3 | 3/3 | 115.42 | 459.50 | 459.19 | 815.43 | Validated |
| PXD046453 / Unknown instrument of Thermo Fisher Scientific | rawr | 0/3 | — | — | — | — | Unsupported instrument metadata |
| PXD046453 / Unknown instrument of Thermo Fisher Scientific | vendor | 0/3 | — | — | — | — | Unsupported instrument metadata |
| PXD046453 / Unknown instrument of Thermo Fisher Scientific | official_default_z3 | 0/3 | — | — | — | — | Unsupported instrument metadata |
| PXD049028 / Orbitrap Astral | rawr | 3/3 | 268.10 | 3731.69 | 3711.02 | 5467.44 | Validated |
| PXD049028 / Orbitrap Astral | vendor | 0/3 | — | — | — | — | Timeout (600 s) |
| PXD049028 / Orbitrap Astral | official_default_z3 | 0/3 | — | — | — | — | Timeout (600 s) |

Tree RSS is summed across processes at 50 ms intervals, including worker-thread children. It can double-count shared pages and miss brief peaks. GNU time maximum single-process RSS is a different metric.

The first three completed cases retain seven measured repeats. Remaining cases use the first three randomized measured blocks after the user-requested amendment; one warmup per variant is excluded. Extra pre-amendment runs remain in the original audit records.

Exploris vendor output passed validation, but the third measured conversion timed out. Astral30 signal encoding completed in both custom paths, but instrument metadata assembly failed. Big Astral vendor and official timed out during warmup. No speedup is computed for these failed comparisons.
