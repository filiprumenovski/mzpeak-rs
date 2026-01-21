# mzPeak Technical Specification

This document provides a formal specification of the mzPeak format, intended as a primary reference for the publication methods section.

## Format Versions

| Version | Status | Description |
|---------|--------|-------------|
| **2.0** | Current | Normalized two-table architecture (recommended). See [SCHEMA_V2.md](SCHEMA_V2.md) |
| 1.0 | Legacy | Single-table denormalized format (this document) |

**Recommendation:** Use v2.0 for new implementations. v2.0 provides 30-40% smaller files through schema normalization while maintaining full backward compatibility for readers.

---

## v1.0 Specification

**Version:** 1.0.0

## Container Structure

mzPeak uses a ZIP-based container format (`.mzpeak` extension) with the following structure:

```
output.mzpeak (ZIP archive)
├── mimetype                          # MIME type identifier (uncompressed, first entry)
├── metadata.json                     # Human-readable metadata (Deflate compressed)
├── peaks/
│   └── peaks.parquet                 # Spectral data (Stored, uncompressed for seekability)
├── chromatograms/
│   └── chromatograms.parquet         # TIC/BPC traces (Stored, optional)
└── mobilograms/
    └── mobilograms.parquet           # Ion mobility traces (Stored, optional)
```

### MIME Type

```
application/vnd.mzpeak
```

The `mimetype` file must be:
- The first entry in the ZIP archive
- Stored without compression (ZIP method 0)
- Contain exactly the MIME type string with no trailing newline

## Schema Definitions

### Peak Table Schema (Long Format)

Each row represents a single peak. This "long" format enables efficient columnar queries.

| Column | Arrow Type | CV Accession | Description |
|--------|------------|--------------|-------------|
| `spectrum_id` | Int64 | - | Unique spectrum identifier (0-indexed) |
| `scan_number` | Int64 | - | Native scan number from instrument |
| `ms_level` | Int16 | MS:1000511 | MS level (1=MS1, 2=MS/MS, etc.) |
| `retention_time` | Float32 | MS:1000016 | Retention time in seconds |
| `polarity` | Int8 | MS:1000129/MS:1000130 | 1=positive, -1=negative |
| `mz` | Float64 | **MS:1000040** | Mass-to-charge ratio |
| `intensity` | Float32 | **MS:1000042** | Peak intensity |
| `ion_mobility` | Float64? | MS:1002476 | Ion mobility drift time (ms) |
| `precursor_mz` | Float64? | MS:1000744 | Precursor m/z for MS2+ |
| `precursor_charge` | Int16? | MS:1000041 | Precursor charge state |
| `precursor_intensity` | Float32? | - | Precursor intensity |
| `isolation_window_lower` | Float32? | MS:1000828 | Lower isolation window offset |
| `isolation_window_upper` | Float32? | MS:1000829 | Upper isolation window offset |
| `collision_energy` | Float32? | MS:1000045 | Collision energy (eV) |
| `total_ion_current` | Float64? | MS:1000285 | Total ion current |
| `base_peak_mz` | Float64? | MS:1000504 | Base peak m/z |
| `base_peak_intensity` | Float32? | MS:1000505 | Base peak intensity |
| `injection_time` | Float32? | MS:1000927 | Ion injection time (ms) |
| `pixel_x` | Int32? | IMS:1000050 | MSI x-coordinate (pixels) |
| `pixel_y` | Int32? | IMS:1000051 | MSI y-coordinate (pixels) |
| `pixel_z` | Int32? | IMS:1000052 | MSI z-coordinate (pixels) |

**Notes:**
- `?` indicates nullable columns
- Bold CV accessions indicate core terms that must be present
- Spatial columns (pixel_x/y/z) are for Mass Spectrometry Imaging (MSI) data

### Chromatogram Schema (Wide Format)

Chromatograms use array storage for efficient trace visualization:

| Column | Arrow Type | CV Accession | Description |
|--------|------------|--------------|-------------|
| `chromatogram_id` | Utf8 | - | Unique identifier (e.g., "TIC", "BPC") |
| `chromatogram_type` | Utf8 | MS:1000235/MS:1000628 | Type descriptor |
| `time_array` | List\<Float64\> | MS:1000595 | Time values in seconds |
| `intensity_array` | List\<Float32\> | MS:1000515 | Intensity values |

### Mobilogram Schema (Wide Format)

Ion mobility traces for timsTOF/FAIMS data:

| Column | Arrow Type | CV Accession | Description |
|--------|------------|--------------|-------------|
| `mobilogram_id` | Utf8 | MS:1003006 | Unique identifier |
| `mobilogram_type` | Utf8 | MS:1003006 | Type (TIM, XIM, etc.) |
| `mobility_array` | List\<Float64\> | MS:1002476 | Ion mobility values (ms) |
| `intensity_array` | List\<Float32\> | MS:1000515 | Intensity values |

## Compression Strategy

### Parquet Internal Compression

| Configuration | Codec | Level | Row Group Size | Use Case |
|--------------|-------|-------|----------------|----------|
| Default | ZSTD | 9 | 100,000 | Balanced |
| Max Compression | ZSTD | 22 | 500,000 | Archival |
| Fast Write | Snappy | - | 50,000 | Quick conversion |

### ZIP Container Compression

| Entry | Compression | Rationale |
|-------|-------------|-----------|
| `mimetype` | Stored (0) | Protocol requirement |
| `metadata.json` | Deflate (8) | Human-readable, compressible |
| `*.parquet` | Stored (0) | Parquet has internal compression; storing allows seeking |

## Controlled Vocabulary Reference

mzPeak uses HUPO-PSI Mass Spectrometry Controlled Vocabulary terms.

### Core Schema Terms

| Accession | Name | Usage |
|-----------|------|-------|
| **MS:1000040** | m/z | `mz` column - mass-to-charge ratio |
| **MS:1000041** | charge state | `precursor_charge` column |
| **MS:1000042** | peak intensity | `intensity` column |
| MS:1000016 | scan start time | `retention_time` column |
| MS:1000045 | collision energy | `collision_energy` column |
| MS:1000127 | centroid spectrum | Scan type metadata |
| MS:1000128 | profile spectrum | Scan type metadata |
| MS:1000129 | negative scan | Polarity indicator |
| MS:1000130 | positive scan | Polarity indicator |
| MS:1000285 | total ion current | `total_ion_current` column |
| MS:1000504 | base peak m/z | `base_peak_mz` column |
| MS:1000505 | base peak intensity | `base_peak_intensity` column |
| MS:1000511 | ms level | `ms_level` column |
| MS:1000514 | m/z array | Binary array context |
| MS:1000515 | intensity array | Binary array context |
| MS:1000744 | selected ion m/z | `precursor_mz` column |
| MS:1000828 | isolation window lower offset | `isolation_window_lower` column |
| MS:1000829 | isolation window upper offset | `isolation_window_upper` column |
| MS:1000927 | ion injection time | `injection_time` column |

### Ion Mobility Terms

| Accession | Name | Usage |
|-----------|------|-------|
| MS:1002476 | ion mobility drift time | `ion_mobility` column |
| MS:1002893 | ion mobility array | Array context |
| MS:1003006 | mobilogram | Mobilogram identifier |

### Chromatogram Terms

| Accession | Name | Usage |
|-----------|------|-------|
| MS:1000235 | total ion current chromatogram | TIC type |
| MS:1000628 | basepeak chromatogram | BPC type |
| MS:1000595 | time array | Chromatogram time axis |

### Mass Spectrometry Imaging Terms

| Accession | Name | Usage |
|-----------|------|-------|
| IMS:1000050 | position x | `pixel_x` column |
| IMS:1000051 | position y | `pixel_y` column |
| IMS:1000052 | position z | `pixel_z` column |

**CV Namespace:** https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo

## Metadata Structure

The `metadata.json` file contains experimental and technical metadata:

```json
{
  "format_version": "1.0.0",
  "conversion_timestamp": "2024-01-15T10:30:00Z",
  "converter_info": {
    "name": "mzpeak",
    "version": "0.1.0"
  },
  "source_file": {
    "name": "sample.mzML",
    "format": "mzML",
    "size_bytes": 2500000000,
    "sha256": "abc123..."
  },
  "sdrf": {
    "source_name": "Sample_001",
    "organism": "Homo sapiens",
    "instrument": "Q Exactive HF"
  },
  "instrument_config": {
    "manufacturer": "Thermo Scientific",
    "model": "Q Exactive HF",
    "serial_number": "12345"
  },
  "run_parameters": {
    "acquisition_method": "DDA",
    "polarity": "positive"
  }
}
```

## Performance Characteristics

### Compression Ratios (vs. mzML)

| Dataset Type | Typical Ratio |
|--------------|---------------|
| DDA Proteomics | 6.2x |
| DIA Proteomics | 7.3x |
| Metabolomics | 5.3x |
| timsTOF PASEF | 6.7x |

### Query Performance

| Operation | Typical Latency |
|-----------|----------------|
| Random spectrum access | ~100 μs |
| MS level filtering | ~8 ms |
| RT range query | ~5 ms |
| Intensity threshold | ~12 ms |
| Metadata-only read | <1 ms |

### Throughput

| Operation | Typical Speed |
|-----------|---------------|
| Conversion (mzML→mzPeak) | 2.5-3.0 M peaks/sec |
| Writing (direct) | 3.0-3.5 M peaks/sec |
| Reading (batch) | 5-10 M peaks/sec |

## File Extension Convention

| Extension | Description |
|-----------|-------------|
| `.mzpeak` | ZIP container (recommended) |
| `.mzpeak.parquet` | Single Parquet file (legacy) |

## Interoperability

mzPeak files can be read by any tool supporting Apache Parquet:

- **Python:** PyArrow, pandas, polars
- **R:** arrow package
- **SQL:** DuckDB, Spark SQL, Presto
- **Java:** Apache Arrow Java

Example DuckDB query:
```sql
SELECT spectrum_id, mz, intensity
FROM 'data.mzpeak/peaks/peaks.parquet'
WHERE ms_level = 2 AND intensity > 1000
ORDER BY intensity DESC
LIMIT 100;
```

## References

1. HUPO-PSI Mass Spectrometry CV: https://github.com/HUPO-PSI/psi-ms-CV
2. Apache Parquet: https://parquet.apache.org/
3. Apache Arrow: https://arrow.apache.org/
4. mzML Specification: https://www.psidev.info/mzML
5. imzML Specification: https://ms-imaging.org/imzml/
