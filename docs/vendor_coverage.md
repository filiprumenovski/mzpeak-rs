# Input coverage

| Input | Current path | Verification |
| --- | --- | --- |
| Thermo RAW | Bundled native rawr executable → piped mzML → checked columns | Process failure/metadata fixtures and an opt-in real RAW roundtrip |
| mzML | Streaming XML, f32/f64 arrays, plain or zlib | Parser, property, sequential/parallel and chromatogram tests |
| imzML | mzML metadata with external IBD arrays | Imaging coordinates and peak roundtrip |
| Bruker TDF | Optional pinned timsrust dependency | Synthetic MS1/MS2/mobility/imaging tests; external instrument corpus not part of CI |
| Other vendors | Convert externally to mzML | No direct WIFF/Agilent/Waters reader is implemented |

Thermo conversion selects source-saved centroids, without .NET or vendor peak
picking. It preserves the exported scan metadata and represented numeric values,
not the entire RAW file. The current exporter omits instrument identity,
auxiliary devices, profile arrays and some vendor metadata. RAW collision
energy is normalized energy and its unit is recorded in processing provenance.

The bundled executable is for macOS Apple Silicon. Other platforms require a
compatible `rawr` executable through `MZPEAK_RAWR` or PATH. Unsupported packets,
nonzero process exits and truncated streams fail the conversion. Existing
outputs are not overwritten. v2 retains empty scans; the legacy v1 adapter
rejects empty RAW scans rather than silently dropping them.

The ingestion contract uses seconds for retention time, f64 for m/z and f32 for
intensity. Spectrum IDs follow stream order and native scan numbers remain
separate. Equal array and validity lengths are checked before writing. Missing
values remain explicit nulls. Recorded scan statistics take precedence over
statistics computed from centroids.

Mobility units are source dependent: the Bruker reader reports inverse reduced
mobility (1/K₀). Existing generic drift-time schema annotations do not establish
a universal physical unit; consumers must check source provenance. Full
cross-vendor unit normalization requires a versioned schema change and corpus
validation.

These independent v1/v2 layouts are not the current HUPO-PSI mzPeak format.
