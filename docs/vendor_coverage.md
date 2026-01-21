# Vendor + MS Type Coverage Matrix (Thin Waist Contract)

This document defines the thin-waist ingestion contract and tracks coverage
for vendor file types and MS acquisition modes. It is the source of truth for
what "comprehensive coverage" means in mzpeak-rs.

## Scope

Vendors / file types:
- Thermo RAW
- Bruker TDF
- SCIEX WIFF
- Agilent D
- Waters RAW

MS types / modes:
- DDA
- DIA / SWATH
- SRM / MRM
- PRM
- MALDI
- MSI
- Ion mobility: TIMS / FAIMS / DTIMS

## Thin Waist Contract (v1)

All vendor readers and the mzML reader must emit data that conforms to this
contract before writing to Parquet.

Required spectrum-level fields:
- spectrum_id (i64, unique, contiguous by write order)
- scan_number (i64, vendor native where possible)
- ms_level (i16)
- retention_time (f32, seconds)
- polarity (i8: 1, -1, 0 for unknown)
- peaks.mz (Vec<f64>)
- peaks.intensity (Vec<f32>)

Optional spectrum-level fields:
- precursor_mz, precursor_charge, precursor_intensity
- isolation_window_lower, isolation_window_upper
- collision_energy
- total_ion_current, base_peak_mz, base_peak_intensity
- injection_time
- pixel_x, pixel_y, pixel_z (MSI)

Optional per-peak fields:
- ion_mobility (Vec<f64> + validity)

Invariants:
- All peak arrays for a spectrum must have identical lengths.
- Each spectrum_id must be contiguous in the stream (no interleaving).
- Units must match the contract (RT seconds, m/z in Th, mobility in ms unless
  explicitly documented).
- Missing data must be represented explicitly as None or all-null buffers.


## Required Test Artifacts

Each vendor/mode entry must include:
- A minimal public sample file (or synthetic fixture) under `tests/fixtures/`
- A conversion test that validates required fields + invariants
- A readback test that checks roundtrip metadata consistency
- A validator run that confirms schema + required columns

## Next Actions

1) Define a formal `IngestSpectrum` struct or trait boundary for vendor readers.
2) Create per-vendor adapter stubs that emit the contract.
3) Add a compliance checklist for each vendor/mode.
4) Track completion in `audit/issues/issue-010-vendor-coverage/issue.md`.
