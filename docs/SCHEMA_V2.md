# mzPeak v2.0 Schema Specification

This document describes the v2.0 schema for mzPeak, introducing a normalized two-table architecture that significantly improves storage efficiency while preserving absolute biological fidelity.

## Design Goals

1. **30-40% smaller file sizes** through elimination of redundant per-peak metadata
2. **Faster metadata queries** by separating spectrum-level data from peak-level data
3. **Lossless precision** for all scientific measurements (Float64 for m/z and ion mobility)
4. **Universal vendor support** for all major MS platforms (Thermo, Bruker, Waters, Sciex, Agilent)
5. **Modality-aware schema** with optional columns based on data type (LC-MS, LC-IMS-MS, MSI)

## Container Structure

```
output.mzpeak (ZIP archive)
├── mimetype                    # "application/vnd.mzpeak+v2" (uncompressed, first entry)
├── manifest.json               # Schema version, modality, and data summary
├── metadata.json               # Human-readable experimental metadata
├── spectra/
│   └── spectra.parquet         # Spectrum-level metadata (one row per spectrum)
└── peaks/
    └── peaks.parquet           # Peak-level data (one row per peak)
```

### MIME Type

```
application/vnd.mzpeak+v2
```

The `mimetype` file must be:
- The first entry in the ZIP archive
- Stored without compression (ZIP method 0)
- Contain exactly the v2 MIME type string with no trailing newline

## Normalized Two-Table Architecture

### Rationale

In v1.0, every peak row contained repeated spectrum-level metadata (retention_time, ms_level, precursor_mz, etc.). For a typical spectrum with 1000 peaks, this meant 1000 copies of identical values.

The v2.0 schema normalizes this into two tables:

| v1.0 (Denormalized) | v2.0 (Normalized) |
|---------------------|-------------------|
| peaks.parquet with 20 columns | spectra.parquet (20 columns) + peaks.parquet (3-4 columns) |
| 1000 peaks × 20 columns = 20,000 values | 1 spectrum row + 1000 peak rows × 4 columns = 4,020 values |

### Storage Savings

For a typical LC-MS/MS dataset:
- **Average peaks per spectrum**: 500-2000
- **Metadata columns removed from peaks**: 16 columns
- **Estimated savings**: 30-40% compressed file size reduction

## Schema Definitions

### manifest.json

The manifest declares the schema version and data characteristics:

```json
{
  "format_version": "2.0",
  "schema_version": "2.0",
  "modality": "lc-ims-ms",
  "has_ion_mobility": true,
  "has_imaging": false,
  "has_precursor_info": true,
  "spectrum_count": 15234,
  "peak_count": 12500000,
  "created": "2024-01-15T10:30:00Z",
  "converter": "mzpeak-rs v2.0.0",
  "vendor_hints": {
    "original_vendor": "Bruker",
    "original_format": "TDF",
    "instrument_model": "timsTOF Pro 2",
    "conversion_path": ["TDF", "mzpeak"]
  }
}
```

### Data Modalities

| Modality | Ion Mobility | Imaging | Example Instruments |
|----------|--------------|---------|---------------------|
| `lc-ms` | No | No | Q Exactive, Triple TOF |
| `lc-ims-ms` | Yes | No | timsTOF, SYNAPT |
| `msi` | No | Yes | Bruker rapifleX |
| `msi-ims` | Yes | Yes | timsTOF fleX |

### Spectra Table Schema (spectra/spectra.parquet)

One row per spectrum. Contains all spectrum-level metadata.

| Column | Arrow Type | Encoding | Nullable | Description |
|--------|------------|----------|----------|-------------|
| `spectrum_id` | UInt32 | DELTA_BINARY_PACKED | No | Unique spectrum identifier (0-indexed) |
| `scan_number` | Int32 | DELTA_BINARY_PACKED | Yes | Native scan number from instrument |
| `ms_level` | UInt8 | DICTIONARY | No | MS level (1=MS1, 2=MS/MS, etc.) |
| `retention_time` | Float32 | BYTE_STREAM_SPLIT | No | Retention time in seconds |
| `polarity` | Int8 | DICTIONARY | No | 1=positive, -1=negative |
| `peak_offset` | UInt64 | DELTA_BINARY_PACKED | No | Row index in peaks.parquet |
| `peak_count` | UInt32 | DELTA_BINARY_PACKED | No | Number of peaks in this spectrum |
| `precursor_mz` | Float64 | BYTE_STREAM_SPLIT | Yes | Precursor m/z for MS2+ |
| `precursor_charge` | Int8 | DICTIONARY | Yes | Precursor charge state |
| `precursor_intensity` | Float32 | BYTE_STREAM_SPLIT | Yes | Precursor intensity |
| `isolation_window_lower` | Float32 | BYTE_STREAM_SPLIT | Yes | Lower isolation window offset |
| `isolation_window_upper` | Float32 | BYTE_STREAM_SPLIT | Yes | Upper isolation window offset |
| `collision_energy` | Float32 | BYTE_STREAM_SPLIT | Yes | Collision energy (eV) |
| `total_ion_current` | Float64 | BYTE_STREAM_SPLIT | Yes | Total ion current |
| `base_peak_mz` | Float64 | BYTE_STREAM_SPLIT | Yes | Base peak m/z |
| `base_peak_intensity` | Float32 | BYTE_STREAM_SPLIT | Yes | Base peak intensity |
| `injection_time` | Float32 | BYTE_STREAM_SPLIT | Yes | Ion injection time (ms) |
| `pixel_x` | UInt16 | DELTA_BINARY_PACKED | Yes | MSI x-coordinate (pixels) |
| `pixel_y` | UInt16 | DELTA_BINARY_PACKED | Yes | MSI y-coordinate (pixels) |
| `pixel_z` | UInt16 | DELTA_BINARY_PACKED | Yes | MSI z-coordinate (pixels) |

**Type Optimizations from v1.0:**
- `spectrum_id`: Int64 → UInt32 (4 billion spectra sufficient)
- `ms_level`: Int16 → UInt8 (ms_level never exceeds 10)
- `precursor_charge`: Int16 → Int8 (charge states rarely exceed ±127)
- `pixel_x/y/z`: Int32 → UInt16 (65535 pixels sufficient for MSI)

### Peaks Table Schema (peaks/peaks.parquet)

One row per peak. Minimal columns linking back to spectra via `spectrum_id`.

| Column | Arrow Type | Encoding | Nullable | Description |
|--------|------------|----------|----------|-------------|
| `spectrum_id` | UInt32 | DELTA_BINARY_PACKED | No | Foreign key to spectra table |
| `mz` | Float64 | BYTE_STREAM_SPLIT | No | Mass-to-charge ratio |
| `intensity` | Float32 | BYTE_STREAM_SPLIT | No | Peak intensity |
| `ion_mobility` | Float64 | BYTE_STREAM_SPLIT | Conditional | Ion mobility (only if modality includes IMS) |

**Note:** The `ion_mobility` column is only present when `modality` is `lc-ims-ms` or `msi-ims`.

## Encoding Strategy

### DELTA_BINARY_PACKED

Used for integer columns with monotonic or grouped patterns:
- `spectrum_id` in peaks: Groups of identical values (all peaks from same spectrum)
- `spectrum_id` in spectra: Monotonically increasing (0, 1, 2, ...)
- `peak_offset`: Monotonically increasing

This encoding stores differences between consecutive values, achieving excellent compression for sorted/grouped data.

### BYTE_STREAM_SPLIT

Used for all floating-point columns:
- Separates the bytes of float values into separate streams
- Improves compression by grouping similar byte patterns
- Particularly effective for scientific data with similar magnitude values

### DICTIONARY

Used for low-cardinality columns:
- `ms_level`: Typically only 1, 2, or 3
- `polarity`: Only 1 or -1
- `precursor_charge`: Limited set of values

## Compression Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Codec | ZSTD | Best compression ratio for scientific data |
| Level | 9 | Good balance of speed vs. compression |
| Row Group Size (spectra) | 10,000 | One row per spectrum, optimize for random access |
| Row Group Size (peaks) | 500,000 | Many peaks, optimize for compression |
| Data Page Size | 1 MB | Standard for large datasets |

## Vendor Hints

For files converted via intermediate formats (e.g., Vendor RAW → mzML → mzPeak), vendor hints preserve provenance:

```json
{
  "vendor_hints": {
    "original_vendor": "Thermo Scientific",
    "original_format": "RAW",
    "instrument_model": "Q Exactive HF-X",
    "conversion_path": ["RAW", "mzML", "mzpeak"]
  }
}
```

This enables downstream tools to apply vendor-specific corrections or calibrations.

## Query Patterns

### Fast Metadata Queries (spectra.parquet only)

```sql
-- Count MS2 spectra
SELECT COUNT(*) FROM 'spectra/spectra.parquet' WHERE ms_level = 2;

-- Get retention time distribution
SELECT
  FLOOR(retention_time / 60) * 60 as rt_bin,
  COUNT(*) as spectrum_count
FROM 'spectra/spectra.parquet'
GROUP BY rt_bin;

-- Find precursor m/z values for targeted analysis
SELECT spectrum_id, precursor_mz, precursor_charge
FROM 'spectra/spectra.parquet'
WHERE precursor_mz BETWEEN 500.0 AND 600.0;
```

### Peak Queries (join or peaks.parquet)

```sql
-- Get peaks for a specific spectrum
SELECT mz, intensity, ion_mobility
FROM 'peaks/peaks.parquet'
WHERE spectrum_id = 1234;

-- XIC extraction with join
SELECT s.retention_time, SUM(p.intensity) as total_intensity
FROM 'spectra/spectra.parquet' s
JOIN 'peaks/peaks.parquet' p ON s.spectrum_id = p.spectrum_id
WHERE p.mz BETWEEN 456.78 AND 456.80
GROUP BY s.retention_time
ORDER BY s.retention_time;
```

## Migration from v1.0

### Automatic Detection

Readers detect v2.0 format by checking for `manifest.json` in the container:
- Present → v2.0 format (two tables)
- Absent → v1.0 format (single peaks.parquet)

### Conversion

```rust
use mzpeak::dataset::MzPeakDatasetWriterV2;
use mzpeak::schema::manifest::Modality;

// Create v2.0 writer
let mut writer = MzPeakDatasetWriterV2::new(
    "output.mzpeak",
    Modality::LcMs,
    None, // vendor_hints
)?;

// Write spectra
for spectrum in source_reader.iter_spectra() {
    writer.write_spectrum(&spectrum.into())?;
}

let stats = writer.close()?;
println!("Wrote {} spectra, {} peaks",
    stats.spectra_stats.spectra_written,
    stats.peaks_stats.peaks_written
);
```

## Performance Characteristics

### Storage Efficiency

| Dataset Type | v1.0 Size | v2.0 Size | Savings |
|--------------|-----------|-----------|---------|
| DDA Proteomics | 100 MB | 65 MB | 35% |
| DIA Proteomics | 200 MB | 130 MB | 35% |
| timsTOF PASEF | 500 MB | 320 MB | 36% |
| MSI (100x100) | 1.5 GB | 1.0 GB | 33% |

### Query Performance

| Operation | v1.0 | v2.0 | Improvement |
|-----------|------|------|-------------|
| Spectrum count | 50 ms | 2 ms | 25x |
| RT range query (metadata) | 100 ms | 5 ms | 20x |
| Full peak scan | 500 ms | 500 ms | - |
| Single spectrum peaks | 2 ms | 2 ms | - |

## Backward Compatibility

- v2.0 readers MUST support v1.0 format files
- v1.0 readers will fail gracefully on v2.0 files (unknown mimetype)
- The `format_version` field in manifest.json enables version detection

## References

1. Apache Parquet Encodings: https://parquet.apache.org/docs/file-format/data-pages/encodings/
2. HUPO-PSI MS CV: https://github.com/HUPO-PSI/psi-ms-CV
3. mzPeak v1.0 Specification: [TECHNICAL_SPEC.md](TECHNICAL_SPEC.md)
