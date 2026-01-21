# Thin Waist v1 (mzML-based)

This is the first formal ingestion contract, derived from the mzML reader.
All vendor readers should target this structure (or a strict superset).

## Core Record Types

SpectrumRecord (per spectrum):
- spectrum_id: i64 (mzML `index`)
- scan_number: i64 (parsed from mzML `id`, fallback `index + 1`)
- ms_level: i16
- retention_time: f32 (seconds)
- polarity: i8 (1, -1, or 0 for unknown)
- precursor_mz: Option<f64>
- precursor_charge: Option<i16>
- precursor_intensity: Option<f32>
- isolation_window_lower: Option<f32>
- isolation_window_upper: Option<f32>
- collision_energy: Option<f32>
- total_ion_current: Option<f64>
- base_peak_mz: Option<f64>
- base_peak_intensity: Option<f32>
- injection_time: Option<f32>
- pixel_x: Option<i32>
- pixel_y: Option<i32>
- pixel_z: Option<i32>
- peaks: PeakArrays

PeakArrays (per spectrum):
- mz: Vec<f64>
- intensity: Vec<f32>
- ion_mobility: OptionalColumnBuf<f64>

## Required Invariants

- All peak arrays for a spectrum must have identical lengths.
- spectrum_id values are contiguous in the stream (no interleaving).
- Units: retention_time in seconds; m/z in Th; ion_mobility in ms unless
  documented otherwise.
- Missing data must be explicit (None or all-null buffers).

## mzML-to-Contract Mapping

- spectrum_id: `MzMLSpectrum.index`
- scan_number: `MzMLSpectrum.scan_number()` (parsed from `id`)
- ms_level: `MzMLSpectrum.ms_level`
- retention_time: `MzMLSpectrum.retention_time` (seconds)
- polarity: `MzMLSpectrum.polarity`
- peaks.mz: `MzMLSpectrum.mz_array`
- peaks.intensity: `MzMLSpectrum.intensity_array` (cast to f32)
- peaks.ion_mobility: `MzMLSpectrum.ion_mobility_array` when present
- total_ion_current: `MzMLSpectrum.total_ion_current` (or computed)
- base_peak_mz: `MzMLSpectrum.base_peak_mz` (or computed)
- base_peak_intensity: `MzMLSpectrum.base_peak_intensity` (or computed)
- injection_time: `MzMLSpectrum.ion_injection_time`
- pixel_x/y/z: `MzMLSpectrum.pixel_x/y/z`
- precursor fields: first entry in `MzMLSpectrum.precursors`

## Extensions (Stored Out-of-Band For Now)

These mzML fields are not part of v1, but must not be lost. Store them in
metadata or an extension map until promoted to core:

- spectrum_native_id (mzML `id`)
- default_array_length
- centroided flag
- scan window lower/upper
- lowest/highest m/z
- filter_string
- preset_scan_configuration
- precision flags (mz/intensity 32/64-bit)
- cv_params and user_params
- multi-precursor lists (beyond the first precursor)

## Rationale

This contract matches current `SpectrumArrays` while making required fields
explicit and documenting unit and ordering constraints. It is the minimum
bar for vendor readers and keeps the core stable while allowing extensions.

## Future v2 Candidates

- Multiple precursors per spectrum
- Explicit spectrum_native_id column
- Scan window bounds as core fields
- Centroid/profile flag as core field
- Additional per-peak arrays (e.g., signal-to-noise)
