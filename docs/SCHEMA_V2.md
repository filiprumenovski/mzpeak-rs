# Experimental v2 layout

This describes this repository's format version `2.0`, not the current
HUPO-PSI mzPeak specification. Rust schema builders in `src/schema` define the
exact Arrow field order, types, nullability and CV annotations.

## Archive

The first ZIP member is `mimetype`, stored without compression, containing
exactly `application/vnd.mzpeak+v2` with no newline. The remaining members are:

- `manifest.json`: format/schema versions, modality flags, spectrum/peak counts,
  creation time, converter and optional vendor provenance.
- `metadata.json`: experiment/source metadata and extensible JSON fields.
- `spectra/spectra.parquet`: one row per spectrum.
- `peaks/peaks.parquet`: contiguous peak rows grouped by spectrum.
- Optional `chromatograms/chromatograms.parquet`: chromatogram traces.

Parquet members use ZIP method Stored so the reader can seek directly into
them. JSON members may be compressed. The reader requires both manifest
versions to equal `2.0`, checks modality flags and exact table fields, and
compares footer row counts with the manifest. v1 and directory bundles are
unsupported.

## Spectrum rows

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| spectrum_id | UInt32 | No | Strictly increasing dataset ID; gaps allowed |
| scan_number | Int32 | Yes | Native source scan number |
| ms_level | UInt8 | No | Positive MS level |
| retention_time | Float32 | No | Seconds |
| polarity | Int8 | No | Positive 1, negative -1, unknown 0 |
| peak_offset | UInt64 | No | Row offset into the peak table |
| peak_count | UInt32 | No | Number of associated peak rows |
| precursor_mz | Float64 | Yes | Selected ion m/z |
| precursor_charge | Int8 | Yes | Charge state |
| precursor_intensity | Float32 | Yes | Selected ion intensity |
| isolation_window_lower | Float32 | Yes | Lower offset |
| isolation_window_upper | Float32 | Yes | Upper offset |
| collision_energy | Float32 | Yes | Source energy value; consult provenance for units |
| total_ion_current | Float64 | Yes | Recorded scan statistic when available |
| base_peak_mz | Float64 | Yes | Recorded base peak m/z when available |
| base_peak_intensity | Float32 | Yes | Recorded base peak intensity when available |
| injection_time | Float32 | Yes | Milliseconds |
| pixel_x | UInt16 | Yes | Imaging x position |
| pixel_y | UInt16 | Yes | Imaging y position |
| pixel_z | UInt16 | Yes | Optional imaging z position |

Offsets are cumulative row counts, including zero-length spectra. Streaming
readback verifies offsets, IDs, required values and the absence of unreferenced
peak rows. `has_precursor_info` records whether a precursor m/z was present.
The single precursor row cannot represent every multi-window acquisition.

## Peak rows and modalities

| Column | Arrow type | Nullable |
|---|---|---|
| spectrum_id | UInt32 | No |
| mz | Float64 | No |
| intensity | Float32 | No |
| ion_mobility | Float64 | Yes in Arrow schema; current writer/reader require values |

`lc-ms` and `msi` omit ion mobility. `lc-ims-ms` and `msi-ims` include it.
Imaging modalities require x/y positions; other modalities omit all pixel
positions. Mobility values retain the source adapter's representation; consult
source metadata rather than assuming a universal unit.

The writer checks array lengths, finite positive m/z, finite nonnegative
intensity/mobility, modality and representable metadata before writing. These
widths preserve the represented values, not arbitrary higher-precision source
measurements or omitted vendor metadata. RAW ingestion uses saved centroids.

## Chromatograms and encoding

Chromatogram rows contain required UTF-8 `chromatogram_id` and
`chromatogram_type`, a Float64 `time_array` list in seconds, and a Float32
`intensity_array` list of the same length. Child values are non-null.

The default is ZSTD level 3, with 10,000 spectrum rows or 500,000 peak rows per
row group and 1 MiB data pages. Integer delta encoding, floating-point
BYTE_STREAM_SPLIT and selected metadata dictionaries reduce repeated data.
Encoding choices are writer settings, not semantic format requirements.
`docs/validation` records measured performance; no fixed storage saving or
cross-vendor fidelity is implied by the layout.
