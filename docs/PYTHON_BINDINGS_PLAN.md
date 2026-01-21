# Python Bindings Integration Plan

This document captures the current state of the PyO3 bindings, gaps to close, and a
checklist of API surface to expose. Items will be checked off as they are completed.

## Current State (Observed)

- PyO3 bindings exist in `src/python/*` with type stubs in `python/mzpeak.pyi`.
- The Python module is gated behind the `python` feature, but the feature is empty
  in `Cargo.toml` and required Rust deps are not declared.
- `pyproject.toml` already uses maturin with `features = ["python"]`.
- Python API coverage is largely v1-focused (single peaks parquet). v2 container
  writer exists in Rust but is not exposed in Python, and the reader only opens
  `peaks/peaks.parquet` for v1 datasets/containers.
- Conversion bindings exist for mzML only, but Python config omits many fields
  present in Rust (`output_format`, `streaming_config`, `writer_config`, etc.).
- Vendor conversions (TDF, Thermo RAW) exist in Rust but are not exposed in Python.

## Gaps and Risks

- Build wiring is incomplete: missing `pyo3`, `pyo3-log`, `numpy` dependencies.
- README still says Python bindings are disabled.
- v2 reader support is missing in Rust, which blocks a proper Python v2 API.
- Python metadata access is limited to raw key-value metadata; parsed metadata
  (`MzPeakMetadata`) is not exposed.
- Validator, schema helpers, and controlled vocabulary are not exposed in Python.
- Numpy is required for array APIs; packaging should reflect this explicitly.

## Proposed Python API Coverage

### Core I/O (v1 + v2)
- v1 (existing): `MzPeakReader`, `MzPeakWriter`, `MzPeakDatasetWriter`, Arrow export.
- v2 (needed): `MzPeakDatasetWriterV2`, v2 reader support (spectra + peaks),
  `SpectrumMetadata`, `PeakArraysV2`, `SpectrumV2`, `DatasetV2Stats`, `Modality`.

### Conversion
- mzML conversion: expose full `ConversionConfig` (including output format, streaming,
  writer config, modality override, SDRF path).
- Bruker TDF conversion bindings (v2 container path).
- Thermo RAW conversion bindings (v2 container path).
- Optional low-level streamers for custom pipelines.

### Metadata, Schema, Validation
- Bind `MzPeakMetadata` and sub-structures (instrument, LC, run, SDRF, provenance).
- Expose `validate_mzpeak_file` and validation report types.
- Bind schema helpers/constants and `Modality` helpers.
- Expose CV helpers for metadata authoring.

### Performance / Advanced Writes
- Bind `RollingWriter` and `AsyncMzPeakWriter`.
- Bind `OwnedColumnarBatch` for zero-copy bulk writes.
- Bind thin-waist ingestion types (`IngestSpectrum`, `IngestSpectrumConverter`).

## Implementation Checklist

### Phase 1 - Build and Packaging
- [x] Wire Rust `python` feature to required deps (`pyo3`, `pyo3-log`, `numpy`).
- [x] Update README to reflect Python bindings are available again.
- [x] Add Python packaging extras for numpy usage.

### Phase 2 - v2 Core I/O
- [x] Add Rust reader support for v2 containers (spectra + peaks).
- [x] Python bindings for v2 reader (spectra + peaks access).
- [x] Python bindings for `MzPeakDatasetWriterV2` and v2 types.
- [x] Arrow streaming access for v2 tables.

### Phase 3 - Conversion
- [x] Expand `ConversionConfig` bindings (streaming, output format, writer config).
- [x] Bind TDF conversion (v2 container output).
- [x] Bind Thermo RAW conversion (v2 container output).
- [ ] Optional bindings for low-level streamers.

### Phase 4 - Metadata / Schema / Validation
- [x] Bind `MzPeakMetadata` and sub-structures.
- [x] Expose parsed metadata on `FileMetadata`.
- [x] Bind validator and report types.
- [x] Bind schema helpers/constants and CV utilities.

### Phase 5 - Performance / Advanced Writes
- [x] Bind `RollingWriter` and `AsyncMzPeakWriter`.
- [x] Bind `OwnedColumnarBatch`.
- [x] Bind thin-waist ingestion types.

## Progress Log

- 2026-01-20: Document created; implementation begins.
- 2026-01-20: Wired `python` feature deps, updated README, added numpy extras in pyproject.
- 2026-01-20: Added Python bindings for v2 writer and v2 spectrum/peaks types.
- 2026-01-20: Synced Python reader/writer to SoA APIs and updated demo CLI to SpectrumArrays.
- 2026-01-20: Added Python bindings for structured metadata types (MzPeakMetadata and all 
  sub-structures: InstrumentConfig, LcConfig, RunParameters, SdrfMetadata, SourceFileInfo,
  ProcessingHistory, ImagingMetadata, VendorHints). Added validation bindings (ValidationReport,
  ValidationCheck, CheckStatus, validate_mzpeak_file). Added CV utilities (CvTerm, CvParamList,
  MsTerms, UnitTerms). Added parsed_metadata() method to FileMetadata. Updated mzpeak.pyi stubs.
- 2026-01-20: Added Rust v2 reader support (spectra_v2.rs) with SpectrumMetadataView,
  SpectrumMetadataIterator, iter_spectra_metadata(), has_spectra_table(), is_v2_format(),
  spectra_metadata_by_rt_range(), spectra_metadata_by_ms_level(). Updated reader config
  with ZipContainerV2 and DirectoryV2 source types. Auto-detection of v1 vs v2 format.
- 2026-01-20: Added Python bindings for advanced writers: RollingWriter (auto-sharding by
  peak count), AsyncMzPeakWriter (background compression/I/O), OwnedColumnarBatch (zero-copy
  bulk writes). Added thin-waist ingestion types: IngestSpectrum, IngestSpectrumConverter.
  Updated writer.rs with PyRollingWriter, PyAsyncMzPeakWriter, PyOwnedColumnarBatch,
  PyIngestSpectrum, PyIngestSpectrumConverter.
- 2026-01-20: Completed Phase 3 conversion bindings. PyConversionConfig already exposes full
  configuration (output_format, streaming_config, writer_config via compression_level/row_group_size,
  sdrf_path, modality). PyTdfConverter and PyThermoConverter classes were already implemented
  in converter.rs with TdfConversionStats and ThermoConversionStats. Added Python type stubs
  for TdfConverter, TdfConversionStats, ThermoConverter, ThermoConversionStats, convert_tdf(),
  and convert_thermo() convenience functions to mzpeak.pyi.
- 2026-01-20: Added Python v2 reader bindings (has_spectra_table, is_v2_format, iter_spectra_metadata,
  read_spectra_batch, get_spectrum_metadata, spectra_metadata_by_rt_range, spectra_metadata_by_ms_level,
  total_spectra). Created PySpectrumMetadataView and PySpectrumMetadataViewIterator. All Phase 2
  items now complete.
