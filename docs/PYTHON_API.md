# mzPeak Python API Reference

Comprehensive documentation for the mzPeak Python bindings, providing high-performance mass spectrometry data I/O with zero-copy Arrow integration.

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Reading Data](#reading-data)
5. [Writing Data](#writing-data)
6. [Data Types](#data-types)
7. [Conversion](#conversion)
8. [Validation](#validation)
9. [Metadata](#metadata)
10. [Controlled Vocabulary](#controlled-vocabulary)
11. [Advanced Features](#advanced-features)
12. [Performance Tips](#performance-tips)

---

## Installation

### From PyPI (when published)

```bash
pip install mzpeak
```

### From Source

```bash
# Clone the repository
git clone https://github.com/your-org/mzpeak-rs
cd mzpeak-rs

# Build and install with maturin
pip install maturin
maturin develop --features python --release
```

### Optional Dependencies

```bash
# For Arrow/DataFrame integration
pip install mzpeak[arrow]      # pyarrow support
pip install mzpeak[pandas]     # pandas DataFrames
pip install mzpeak[polars]     # polars DataFrames
pip install mzpeak[numpy]      # numpy arrays
pip install mzpeak[full]       # all integrations
```

---

## Quick Start

### Reading an mzPeak File

```python
import mzpeak

# Open and read spectra
with mzpeak.MzPeakReader("sample.mzpeak") as reader:
    # Get file summary
    summary = reader.summary()
    print(f"Total spectra: {summary.num_spectra}")
    print(f"MS1 spectra: {summary.num_ms1_spectra}")
    print(f"MS2 spectra: {summary.num_ms2_spectra}")
    
    # Iterate over spectra
    for spectrum in reader.iter_spectra():
        print(f"Scan {spectrum.scan_number}: {spectrum.num_peaks} peaks")
```

### Converting mzML to mzPeak

```python
import mzpeak

# Simple conversion
mzpeak.convert("input.mzML", "output.mzpeak")

# With configuration
config = mzpeak.ConversionConfig()
config.batch_size = 1000
config.output_format = mzpeak.OutputFormat.V2Container

stats = mzpeak.convert("input.mzML", "output.mzpeak", config)
print(f"Converted {stats.spectra_written} spectra")
```

### Writing Spectra

```python
import mzpeak
import numpy as np

with mzpeak.MzPeakWriter("output.mzpeak") as writer:
    # Build and write spectra
    spectrum = (mzpeak.SpectrumBuilder(spectrum_id=0, scan_number=1)
        .ms_level(1)
        .retention_time(60.5)
        .polarity(1)
        .add_peak(150.0, 1000.0)
        .add_peak(175.5, 2500.0)
        .add_peak(200.0, 500.0)
        .build())
    
    writer.write_spectrum(spectrum)
```

---

## Core Concepts

### mzPeak Format Versions

mzPeak supports two format versions:

| Version | Description | Use Case |
|---------|-------------|----------|
| **V1 (Parquet)** | Single Parquet file with denormalized data | Simple analyses, smaller files |
| **V2 (Container)** | ZIP container with normalized tables | Production, large files, efficient queries |

### V2 Container Structure

```
sample.mzpeak/
├── mimetype                 # Format identifier
├── metadata.json           # File and run metadata
├── spectra/
│   └── spectra.parquet     # Spectrum metadata table
├── peaks/
│   └── peaks.parquet       # Peak data table
└── traces/                 # Optional chromatograms
    ├── tic.parquet
    └── bpc.parquet
```

### Data Model

The v2 format uses a **normalized two-table design**:

**Spectra Table** - One row per spectrum:
- `spectrum_id` (u32) - Unique identifier
- `scan_number` (i32, nullable) - Original scan number
- `ms_level` (u8) - MS level (1, 2, etc.)
- `retention_time` (f32) - RT in seconds
- `polarity` (i8) - Positive (1), negative (-1), or unknown (0)
- `peak_count` (u32) - Number of peaks
- Precursor fields for MS2+
- Optional imaging coordinates

**Peaks Table** - One row per peak:
- `spectrum_id` (u32) - Foreign key to spectra
- `mz` (f64) - Mass-to-charge ratio
- `intensity` (f32) - Signal intensity
- `ion_mobility` (f32, nullable) - For IMS data

---

## Reading Data

### MzPeakReader

The primary interface for reading mzPeak files.

```python
import mzpeak

# Context manager (recommended)
with mzpeak.MzPeakReader("sample.mzpeak") as reader:
    # File information
    summary = reader.summary()
    metadata = reader.metadata()
    
    # Check format version
    if reader.is_v2():
        print("Reading v2 container format")
```

### File Summary

```python
summary = reader.summary()

print(f"Number of spectra: {summary.num_spectra}")
print(f"MS1 spectra: {summary.num_ms1_spectra}")
print(f"MS2 spectra: {summary.num_ms2_spectra}")
print(f"Total peaks: {summary.total_peaks}")
print(f"RT range: {summary.min_rt:.2f} - {summary.max_rt:.2f} seconds")
print(f"m/z range: {summary.min_mz:.4f} - {summary.max_mz:.4f}")
```

### Iterating Spectra

```python
# Basic iteration
for spectrum in reader.iter_spectra():
    print(f"Spectrum {spectrum.spectrum_id}: MS{spectrum.ms_level}")

# With numpy arrays (zero-copy when possible)
for arrays in reader.iter_spectrum_arrays():
    mz = arrays.mz          # numpy.ndarray[float64]
    intensity = arrays.intensity  # numpy.ndarray[float32]
    
# Memory-efficient views (no data copy)
for view in reader.iter_spectrum_arrays_view():
    # Access data without copying
    mz_view = view.mz_view()
    intensity_view = view.intensity_view()

# Metadata-only iteration (fastest)
for meta in reader.iter_spectrum_metadata():
    if meta.ms_level == 2:
        print(f"MS2 at RT={meta.retention_time:.2f}s")
```

### Random Access

```python
# Get specific spectrum by ID
spectrum = reader.get_spectrum(spectrum_id=42)

# Get spectrum by scan number
spectrum = reader.get_spectrum_by_scan(scan_number=100)

# Batch retrieval
spectra = reader.get_spectra([1, 2, 3, 10, 20])
```

### Filtering

```python
# Filter by MS level
ms2_spectra = list(reader.iter_spectra(ms_level=2))

# Filter by retention time range
spectra = list(reader.iter_spectra(rt_min=60.0, rt_max=120.0))

# Filter by m/z range
spectra = list(reader.iter_spectra(mz_min=400.0, mz_max=1200.0))

# Combined filters
filtered = list(reader.iter_spectra(
    ms_level=2,
    rt_min=60.0,
    rt_max=180.0,
    mz_min=400.0,
    mz_max=1600.0
))
```

### DataFrame Integration

```python
# Convert to Arrow Table (zero-copy)
arrow_table = reader.to_arrow()

# Convert to pandas DataFrame
df = reader.to_pandas()

# Convert to polars DataFrame
df = reader.to_polars()

# Get specific columns
df = reader.to_pandas(columns=["spectrum_id", "retention_time", "ms_level"])
```

### V2-Specific Reading

```python
if reader.is_v2():
    # Get v2 statistics
    stats = reader.get_v2_stats()
    print(f"Spectra file size: {stats.spectra_file_size} bytes")
    print(f"Peaks file size: {stats.peaks_file_size} bytes")
    
    # Read spectra table directly
    spectra_table = reader.read_spectra_table()
    
    # Read peaks for specific spectra
    peaks = reader.read_peaks_for_spectra([1, 2, 3])
```

---

## Writing Data

### MzPeakWriter (V1 Format)

```python
import mzpeak

config = mzpeak.WriterConfig()
config.row_group_size = 100000
config.compression = "zstd"
config.compression_level = 9

with mzpeak.MzPeakWriter("output.parquet", config) as writer:
    for spectrum in generate_spectra():
        writer.write_spectrum(spectrum)
    
    stats = writer.finish()
    print(f"Wrote {stats.spectra_written} spectra")
```

### MzPeakDatasetWriterV2 (V2 Format)

```python
import mzpeak

with mzpeak.MzPeakDatasetWriterV2("output.mzpeak") as writer:
    # Write spectrum metadata and peaks separately
    metadata = mzpeak.SpectrumMetadata(
        spectrum_id=0,
        scan_number=1,
        ms_level=1,
        retention_time=60.0,
        polarity=1,
        peak_count=100
    )
    
    peaks = mzpeak.PeakArraysV2(
        mz=np.array([100.0, 200.0, 300.0], dtype=np.float64),
        intensity=np.array([1000.0, 2000.0, 500.0], dtype=np.float32)
    )
    
    writer.write_spectrum_v2(metadata, peaks)
```

### SpectrumBuilder

Fluent API for constructing spectra:

```python
import mzpeak

# MS1 spectrum
ms1 = (mzpeak.SpectrumBuilder(spectrum_id=0, scan_number=1)
    .ms_level(1)
    .retention_time(60.0)
    .polarity(1)  # positive
    .total_ion_current(1e6)
    .base_peak_mz(500.0)
    .base_peak_intensity(50000.0)
    .add_peak(400.0, 1000.0)
    .add_peak(500.0, 50000.0)
    .add_peak(600.0, 2000.0)
    .build())

# MS2 spectrum with precursor
ms2 = (mzpeak.SpectrumBuilder(spectrum_id=1, scan_number=2)
    .ms_level(2)
    .retention_time(60.5)
    .polarity(1)
    .precursor_mz(500.0)
    .precursor_charge(2)
    .precursor_intensity(50000.0)
    .isolation_window(499.0, 501.0)
    .collision_energy(30.0)
    .add_peaks_from_arrays(mz_array, intensity_array)
    .build())
```

### RollingWriter (Streaming)

For memory-efficient streaming writes:

```python
import mzpeak

# Configure rolling writer
writer = mzpeak.RollingWriter(
    output_path="output.mzpeak",
    spectra_per_file=100000,  # Split into multiple files
    flush_interval=10000      # Flush every N spectra
)

try:
    for spectrum in stream_spectra():
        writer.write(spectrum)
        
        # Check progress
        stats = writer.stats()
        if stats.spectra_written % 10000 == 0:
            print(f"Written {stats.spectra_written} spectra")
finally:
    writer.close()
```

### AsyncMzPeakWriter (Pipeline Integration)

For async/concurrent processing pipelines:

```python
import mzpeak

# Create async writer
writer = mzpeak.AsyncMzPeakWriter("output.mzpeak")

# Submit batches asynchronously
batch = mzpeak.OwnedColumnarBatch.from_spectra(spectra_list)
writer.submit_batch(batch)

# Writer processes batches in background
# ...

# Wait for completion
writer.flush()
writer.close()
```

### IngestSpectrum (Conversion Pipeline)

For building conversion pipelines:

```python
import mzpeak

# Create spectrum using ingestion contract
spectrum = mzpeak.IngestSpectrum(
    native_id="scan=100",
    scan_number=100,
    ms_level=1,
    retention_time=60.0,
    polarity=1,
    mz=np.array([100.0, 200.0], dtype=np.float64),
    intensity=np.array([1000.0, 2000.0], dtype=np.float32)
)

# Convert to internal format
converter = mzpeak.IngestSpectrumConverter()
internal = converter.convert(spectrum)
```

---

## Data Types

### Peak

Basic peak representation:

```python
peak = mzpeak.Peak(mz=500.0, intensity=10000.0)

print(f"m/z: {peak.mz}")
print(f"Intensity: {peak.intensity}")
```

### Spectrum

Full spectrum with metadata and peaks:

```python
spectrum = reader.get_spectrum(0)

# Metadata
print(f"ID: {spectrum.spectrum_id}")
print(f"Scan: {spectrum.scan_number}")
print(f"MS Level: {spectrum.ms_level}")
print(f"RT: {spectrum.retention_time} seconds")
print(f"Polarity: {spectrum.polarity}")  # 1=positive, -1=negative
print(f"Peak count: {spectrum.num_peaks}")

# Peak access
for peak in spectrum.peaks:
    print(f"  {peak.mz:.4f} @ {peak.intensity:.1f}")

# As numpy arrays
mz, intensity = spectrum.to_arrays()
```

### SpectrumArrays

Spectrum with numpy array access:

```python
arrays = mzpeak.SpectrumArrays(
    spectrum_id=0,
    scan_number=1,
    ms_level=1,
    retention_time=60.0,
    polarity=1,
    mz=np.array([100.0, 200.0, 300.0], dtype=np.float64),
    intensity=np.array([1000.0, 2000.0, 500.0], dtype=np.float32)
)

# Direct array access
print(f"m/z array: {arrays.mz}")
print(f"Intensity array: {arrays.intensity}")

# Optional ion mobility
if arrays.ion_mobility is not None:
    print(f"Ion mobility: {arrays.ion_mobility}")
```

### SpectrumMetadata (V2)

Metadata-only view for efficient filtering:

```python
# Create metadata
meta = mzpeak.SpectrumMetadata(
    spectrum_id=0,
    scan_number=1,
    ms_level=1,
    retention_time=60.0,
    polarity=1,
    peak_count=100,
    total_ion_current=1e6,
    base_peak_mz=500.0,
    base_peak_intensity=50000.0
)

# Convenience constructors
ms1_meta = mzpeak.SpectrumMetadata.new_ms1(
    spectrum_id=0,
    scan_number=1,
    retention_time=60.0,
    polarity=1,
    peak_count=100
)

ms2_meta = mzpeak.SpectrumMetadata.new_ms2(
    spectrum_id=1,
    scan_number=2,
    retention_time=60.5,
    polarity=1,
    peak_count=50,
    precursor_mz=500.0,
    precursor_charge=2
)
```

### PeakArraysV2

V2 format peak arrays:

```python
import numpy as np

peaks = mzpeak.PeakArraysV2(
    mz=np.array([100.0, 200.0, 300.0], dtype=np.float64),
    intensity=np.array([1000.0, 2000.0, 500.0], dtype=np.float32),
    ion_mobility=np.array([1.0, 1.1, 1.2], dtype=np.float32)  # optional
)

print(f"Peak count: {len(peaks)}")
print(f"m/z range: {peaks.mz.min():.4f} - {peaks.mz.max():.4f}")
```

### Chromatogram

Time-intensity trace:

```python
chrom = reader.get_chromatogram("TIC")

print(f"Type: {chrom.chromatogram_type}")
print(f"Points: {len(chrom.time)}")

# Access as arrays
time = chrom.time          # numpy array of time points
intensity = chrom.intensity  # numpy array of intensities
```

### Mobilogram

Ion mobility trace:

```python
mobilogram = reader.get_mobilogram(precursor_mz=500.0)

mobility = mobilogram.mobility    # numpy array
intensity = mobilogram.intensity  # numpy array
```

---

## Conversion

### Basic Conversion

```python
import mzpeak

# mzML to mzPeak
stats = mzpeak.convert("input.mzML", "output.mzpeak")

print(f"Spectra converted: {stats.spectra_written}")
print(f"Peaks written: {stats.peaks_written}")
print(f"Conversion time: {stats.elapsed_seconds:.2f}s")
```

### ConversionConfig

Fine-grained conversion control:

```python
config = mzpeak.ConversionConfig()

# Output format
config.output_format = mzpeak.OutputFormat.V2Container  # or V1Parquet

# Batching
config.batch_size = 1000  # Spectra per batch

# Precision
config.preserve_precision = True  # Keep original precision

# Compression
config.compression_level = 9  # 0-22 for zstd

# Modality hints
config.modality = mzpeak.Modality.LcMs  # or LcImsMs, Msi, MsiIms

stats = mzpeak.convert("input.mzML", "output.mzpeak", config)
```

### OutputFormat Enum

```python
# V1: Single denormalized Parquet file
mzpeak.OutputFormat.V1Parquet

# V2: ZIP container with normalized tables (recommended)
mzpeak.OutputFormat.V2Container
```

### Modality Enum

```python
# Standard LC-MS
mzpeak.Modality.LcMs

# LC-IMS-MS (ion mobility)
mzpeak.Modality.LcImsMs

# Mass spectrometry imaging
mzpeak.Modality.Msi

# MSI with ion mobility
mzpeak.Modality.MsiIms

# Check modality features
modality = mzpeak.Modality.LcImsMs
print(f"Has ion mobility: {modality.has_ion_mobility()}")
print(f"Has imaging: {modality.has_imaging()}")
```

### Sharded Conversion

For very large files:

```python
# Split output into multiple shards
stats = mzpeak.convert_with_sharding(
    input_path="huge_file.mzML",
    output_dir="output_shards/",
    spectra_per_shard=1000000
)
```

### MzMLConverter Class

For more control over mzML conversion:

```python
converter = mzpeak.MzMLConverter("input.mzML")

# Get file info before conversion
info = converter.get_info()
print(f"Estimated spectra: {info.spectrum_count}")

# Convert with progress callback
def progress(current, total):
    print(f"Progress: {current}/{total}")

converter.convert("output.mzpeak", progress_callback=progress)
```

### StreamingConfig

For streaming conversion:

```python
config = mzpeak.StreamingConfig()
config.input_buffer_size = 65536  # 64KB buffer
config.streaming_mode = True

# Use with conversion
conv_config = mzpeak.ConversionConfig()
conv_config.streaming = config
```

---

## Validation

### Validating Files

```python
import mzpeak

# Validate an mzPeak file
report = mzpeak.validate_mzpeak_file("sample.mzpeak")

print(f"Valid: {report.is_valid()}")
print(f"Warnings: {report.warning_count()}")
print(f"Errors: {report.error_count()}")

# Iterate through checks
for check in report.checks:
    print(f"[{check.status}] {check.name}: {check.message}")
```

### CheckStatus Enum

```python
# Check passed
mzpeak.CheckStatus.Ok

# Non-fatal issue
mzpeak.CheckStatus.Warning

# Fatal issue
mzpeak.CheckStatus.Failed

# Check status
status = mzpeak.CheckStatus.Warning
print(f"Is OK: {status.is_ok()}")
print(f"Is warning: {status.is_warning()}")
print(f"Is failed: {status.is_failed()}")
```

### ValidationCheck

Individual validation result:

```python
for check in report.checks:
    print(f"Check: {check.name}")
    print(f"Category: {check.category}")
    print(f"Status: {check.status}")
    print(f"Message: {check.message}")
    if check.details:
        print(f"Details: {check.details}")
```

### ValidationReport

Full validation results:

```python
report = mzpeak.validate_mzpeak_file("sample.mzpeak")

# Summary
print(f"Total checks: {len(report.checks)}")
print(f"Passed: {report.ok_count()}")
print(f"Warnings: {report.warning_count()}")
print(f"Failed: {report.error_count()}")

# Filter by status
errors = [c for c in report.checks if c.status == mzpeak.CheckStatus.Failed]
warnings = [c for c in report.checks if c.status == mzpeak.CheckStatus.Warning]

# Check overall validity
if report.is_valid():
    print("File is valid!")
else:
    print("File has errors:")
    for error in errors:
        print(f"  - {error.message}")
```

---

## Metadata

### MzPeakMetadata

Top-level file metadata container:

```python
# Read from file
with mzpeak.MzPeakReader("sample.mzpeak") as reader:
    metadata = reader.get_mzpeak_metadata()

# Or create new
metadata = mzpeak.MzPeakMetadata()
metadata.instrument = mzpeak.InstrumentConfig()
metadata.lc = mzpeak.LcConfig()
metadata.run = mzpeak.RunParameters()
metadata.sdrf = mzpeak.SdrfMetadata("sample_1")

# Access components
print(f"Instrument: {metadata.instrument}")
print(f"LC config: {metadata.lc}")
print(f"Run params: {metadata.run}")
```

### InstrumentConfig

Mass spectrometer configuration:

```python
instrument = mzpeak.InstrumentConfig()

# Basic info
instrument.vendor = "Thermo Fisher Scientific"
instrument.model = "Orbitrap Exploris 480"
instrument.serial_number = "ABC123"

# Ionization
instrument.ionization_type = "electrospray ionization"

# Mass analyzers
analyzer = mzpeak.MassAnalyzerConfig(
    analyzer_type="orbitrap",
    order=1
)
analyzer.resolution = 120000
analyzer.mass_range_low = 200.0
analyzer.mass_range_high = 2000.0

instrument.add_analyzer(analyzer)

print(f"Instrument: {instrument.vendor} {instrument.model}")
print(f"Analyzers: {len(instrument.analyzers)}")
```

### MassAnalyzerConfig

Individual mass analyzer settings:

```python
analyzer = mzpeak.MassAnalyzerConfig(
    analyzer_type="orbitrap",
    order=1
)

analyzer.resolution = 120000
analyzer.mass_range_low = 200.0
analyzer.mass_range_high = 2000.0
analyzer.scan_rate = 10.0  # Hz

print(f"Type: {analyzer.analyzer_type}")
print(f"Resolution: {analyzer.resolution}")
```

### LcConfig

Liquid chromatography configuration:

```python
lc = mzpeak.LcConfig()

# System info
lc.system = "Vanquish Neo"
lc.flow_rate = 0.3  # mL/min
lc.solvent_a = "Water + 0.1% FA"
lc.solvent_b = "Acetonitrile + 0.1% FA"

# Column
column = mzpeak.ColumnInfo()
column.name = "Acclaim PepMap 100"
column.length_mm = 150.0
column.inner_diameter_um = 75.0
column.particle_size_um = 2.0
column.temperature_c = 40.0

lc.column = column

# Gradient
gradient = mzpeak.GradientProgram()
gradient.add_step(mzpeak.GradientStep(time_min=0.0, percent_b=2.0))
gradient.add_step(mzpeak.GradientStep(time_min=60.0, percent_b=35.0))
gradient.add_step(mzpeak.GradientStep(time_min=70.0, percent_b=90.0))

lc.gradient = gradient

print(f"LC System: {lc.system}")
print(f"Column: {lc.column.name}")
```

### ColumnInfo

LC column details:

```python
column = mzpeak.ColumnInfo()

column.name = "Acclaim PepMap 100"
column.manufacturer = "Thermo Scientific"
column.chemistry = "C18"
column.length_mm = 150.0
column.inner_diameter_um = 75.0
column.particle_size_um = 2.0
column.pore_size_angstrom = 100.0
column.temperature_c = 40.0
```

### GradientProgram & GradientStep

LC gradient definition:

```python
# Create gradient program
gradient = mzpeak.GradientProgram()

# Add steps
gradient.add_step(mzpeak.GradientStep(
    time_min=0.0,
    percent_b=2.0,
    flow_rate_ul_min=300.0
))

gradient.add_step(mzpeak.GradientStep(
    time_min=60.0,
    percent_b=35.0,
    flow_rate_ul_min=300.0
))

# Access steps
for step in gradient.steps:
    print(f"  {step.time_min} min: {step.percent_b}% B")
```

### MobilePhase

Mobile phase composition:

```python
phase_a = mzpeak.MobilePhase(
    channel="A",
    composition="Water",
    ph=2.5
)

phase_b = mzpeak.MobilePhase(
    channel="B", 
    composition="Acetonitrile",
    ph=None
)
```

### RunParameters

Acquisition run parameters:

```python
run = mzpeak.RunParameters()

run.start_time = "2024-01-15T10:30:00Z"
run.end_time = "2024-01-15T12:30:00Z"
run.operator = "John Doe"
run.sample_name = "Sample_001"
run.sample_id = "S001"
run.injection_volume_ul = 2.0
run.comment = "Standard analysis"
```

### SdrfMetadata

Sample metadata (SDRF-compatible):

```python
sdrf = mzpeak.SdrfMetadata(source_name="sample_1")

sdrf.organism = "Homo sapiens"
sdrf.organism_part = "liver"
sdrf.disease = "healthy"
sdrf.cell_type = "hepatocyte"
sdrf.biological_replicate = 1
sdrf.technical_replicate = 1
sdrf.fraction = 1
sdrf.label = "label free"
sdrf.instrument = "Orbitrap Exploris 480"

# Custom attributes
sdrf.set_attribute("age", "45")
sdrf.set_attribute("sex", "male")

print(f"Sample: {sdrf.source_name}")
print(f"Organism: {sdrf.organism}")
```

### SourceFileInfo

Source file provenance:

```python
source = mzpeak.SourceFileInfo(name="raw_data.mzML")

source.location = "/data/raw/"
source.format = "mzML"
source.checksum = "sha256:abc123..."
source.checksum_type = "SHA-256"

print(f"Source: {source.name}")
print(f"Format: {source.format}")
```

### ProcessingHistory & ProcessingStep

Data processing provenance:

```python
history = mzpeak.ProcessingHistory()

# Add processing steps
history.add_step(mzpeak.ProcessingStep(
    order=1,
    software="msconvert",
    processing_type="format conversion"
))

history.add_step(mzpeak.ProcessingStep(
    order=2,
    software="mzpeak",
    processing_type="peak picking"
))

# Access steps
for step in history.steps:
    print(f"{step.order}. {step.software}: {step.processing_type}")
```

### ImagingMetadata

Mass spectrometry imaging metadata:

```python
imaging = mzpeak.ImagingMetadata()

imaging.pixel_size_x = 50.0  # micrometers
imaging.pixel_size_y = 50.0
imaging.grid_width = 100
imaging.grid_height = 100
imaging.scan_pattern = "serpentine"
imaging.scan_direction = "left_to_right"

print(f"Grid: {imaging.grid_width}x{imaging.grid_height}")
print(f"Pixel size: {imaging.pixel_size_x}x{imaging.pixel_size_y} µm")
```

### VendorHints

Vendor-specific information:

```python
hints = mzpeak.VendorHints()

hints.original_vendor = "Thermo"
hints.original_format = "raw"
hints.acquisition_software = "Xcalibur"
hints.acquisition_software_version = "4.5"

# Custom hints
hints.set_hint("orbitrap_resolution", "120000")
```

---

## Controlled Vocabulary

mzPeak uses PSI-MS controlled vocabulary terms for standardization.

### CvTerm

Basic CV term:

```python
# Create a term
term = mzpeak.CvTerm(
    accession="MS:1000511",
    name="ms level",
    value="2"
)

print(f"Accession: {term.accession}")
print(f"Name: {term.name}")
print(f"Value: {term.value}")
```

### CvParamList

List of CV parameters:

```python
params = mzpeak.CvParamList()

# Add terms
params.add(mzpeak.MsTerms.ms_level(1))
params.add(mzpeak.MsTerms.positive_scan())
params.add(mzpeak.MsTerms.base_peak_mz(500.0))

print(f"Parameters: {len(params)}")

# Iterate
for term in params:
    print(f"  {term}")

# Find by accession
term = params.get("MS:1000511")
```

### MsTerms

Factory for common MS ontology terms:

```python
# MS levels
mzpeak.MsTerms.ms_level(1)
mzpeak.MsTerms.ms_level(2)

# Polarity
mzpeak.MsTerms.positive_scan()
mzpeak.MsTerms.negative_scan()

# Spectrum properties
mzpeak.MsTerms.total_ion_current(1e6)
mzpeak.MsTerms.base_peak_mz(500.0)
mzpeak.MsTerms.base_peak_intensity(50000.0)

# Precursor
mzpeak.MsTerms.precursor_mz(500.0)
mzpeak.MsTerms.charge_state(2)
mzpeak.MsTerms.isolation_window_target(500.0)
mzpeak.MsTerms.isolation_window_lower(1.0)
mzpeak.MsTerms.isolation_window_upper(1.0)

# Activation
mzpeak.MsTerms.collision_energy(30.0)
mzpeak.MsTerms.hcd()
mzpeak.MsTerms.cid()
mzpeak.MsTerms.etd()

# Instrument
mzpeak.MsTerms.orbitrap()
mzpeak.MsTerms.quadrupole()
mzpeak.MsTerms.tof()
mzpeak.MsTerms.ion_trap()
```

### UnitTerms

Factory for unit ontology terms:

```python
# Time units
mzpeak.UnitTerms.second()
mzpeak.UnitTerms.minute()
mzpeak.UnitTerms.millisecond()

# Mass units
mzpeak.UnitTerms.dalton()
mzpeak.UnitTerms.thomson()  # m/z

# Other units
mzpeak.UnitTerms.electronvolt()
mzpeak.UnitTerms.volt_per_square_centimeter()  # ion mobility

# Create CV param with units
rt_term = mzpeak.MsTerms.retention_time(60.0)
rt_term.unit = mzpeak.UnitTerms.second()
```

---

## Advanced Features

### Zero-Copy Arrow Integration

```python
import pyarrow as pa

with mzpeak.MzPeakReader("sample.mzpeak") as reader:
    # Get Arrow table (zero-copy when possible)
    table = reader.to_arrow()
    
    # Use with pyarrow compute
    import pyarrow.compute as pc
    
    ms2_mask = pc.equal(table["ms_level"], 2)
    ms2_table = table.filter(ms2_mask)
    
    # Convert to other formats
    df = table.to_pandas()
    
    # Write to other Arrow formats
    pa.parquet.write_table(table, "export.parquet")
```

### Memory-Mapped Reading

```python
# Enable memory mapping for large files
reader = mzpeak.MzPeakReader("large_file.mzpeak", mmap=True)
```

### Parallel Processing

```python
from concurrent.futures import ThreadPoolExecutor

with mzpeak.MzPeakReader("sample.mzpeak") as reader:
    spectrum_ids = list(range(reader.summary().num_spectra))
    
    def process_spectrum(sid):
        spectrum = reader.get_spectrum(sid)
        # Process spectrum
        return result
    
    # Reader is thread-safe for concurrent access
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(process_spectrum, spectrum_ids))
```

### Custom Batch Processing

```python
with mzpeak.MzPeakReader("sample.mzpeak") as reader:
    batch_size = 1000
    
    for batch in reader.iter_batches(batch_size=batch_size):
        # batch is a list of spectra
        process_batch(batch)
```

### OwnedColumnarBatch

For building custom Arrow batches:

```python
import numpy as np

# Create batch from arrays
batch = mzpeak.OwnedColumnarBatch.from_arrays(
    spectrum_ids=np.array([0, 1, 2], dtype=np.uint32),
    scan_numbers=np.array([1, 2, 3], dtype=np.int32),
    ms_levels=np.array([1, 2, 2], dtype=np.uint8),
    retention_times=np.array([60.0, 60.5, 61.0], dtype=np.float32),
    polarities=np.array([1, 1, 1], dtype=np.int8),
    # ... more arrays
)

# Convert to Arrow
arrow_batch = batch.to_arrow()

# Or use with writer
writer.write_batch(batch)
```

---

## Performance Tips

### 1. Use Iterators for Large Files

```python
# ❌ Don't load all spectra into memory
all_spectra = list(reader.iter_spectra())  # May use lots of RAM

# ✅ Process one at a time
for spectrum in reader.iter_spectra():
    process(spectrum)
```

### 2. Use Metadata Iteration for Filtering

```python
# ❌ Loading full spectra just to filter
ms2_ids = [s.spectrum_id for s in reader.iter_spectra() 
           if s.ms_level == 2]

# ✅ Use metadata-only iteration
ms2_ids = [m.spectrum_id for m in reader.iter_spectrum_metadata() 
           if m.ms_level == 2]
```

### 3. Use Views for Read-Only Access

```python
# ❌ Copying data unnecessarily
for arrays in reader.iter_spectrum_arrays():
    mz = arrays.mz.copy()  # Extra copy

# ✅ Use views when not modifying
for view in reader.iter_spectrum_arrays_view():
    mz = view.mz_view()  # No copy
```

### 4. Batch Operations

```python
# ❌ Individual reads
for i in range(1000):
    spectrum = reader.get_spectrum(i)

# ✅ Batch reads
spectra = reader.get_spectra(list(range(1000)))
```

### 5. Use V2 Format for Large Files

```python
# V2 container is more efficient for:
# - Files > 1GB
# - Random access patterns
# - Metadata-only queries

config = mzpeak.ConversionConfig()
config.output_format = mzpeak.OutputFormat.V2Container
```

### 6. Configure Writer Appropriately

```python
config = mzpeak.WriterConfig()

# Larger row groups = better compression, slower random access
config.row_group_size = 100000  # Default

# Smaller for random access workloads
config.row_group_size = 10000

# Higher compression = smaller files, slower writes
config.compression_level = 9  # 0-22
```

### 7. Release GIL for Parallel Python

```python
# mzpeak releases the GIL during heavy I/O
# Safe to use with threading

from threading import Thread

def read_file(path):
    with mzpeak.MzPeakReader(path) as reader:
        return reader.summary()

# These run in parallel
threads = [Thread(target=read_file, args=(f,)) for f in files]
for t in threads:
    t.start()
for t in threads:
    t.join()
```

---

## Error Handling

### Exception Types

```python
import mzpeak

try:
    reader = mzpeak.MzPeakReader("nonexistent.mzpeak")
except mzpeak.MzPeakIOError as e:
    print(f"I/O error: {e}")

try:
    reader = mzpeak.MzPeakReader("corrupted.mzpeak")
except mzpeak.MzPeakFormatError as e:
    print(f"Format error: {e}")

try:
    mzpeak.validate_mzpeak_file("invalid.mzpeak")
except mzpeak.MzPeakValidationError as e:
    print(f"Validation error: {e}")

# Catch all mzpeak exceptions
try:
    # ...
except mzpeak.MzPeakException as e:
    print(f"mzpeak error: {e}")
```

---

## Version Information

```python
import mzpeak

# Library version
print(f"mzpeak version: {mzpeak.__version__}")

# Format version
print(f"Format version: {mzpeak.FORMAT_VERSION}")

# MIME type
print(f"MIME type: {mzpeak.MIMETYPE}")
```

---

## See Also

- [Technical Specification](TECHNICAL_SPEC.md)
- [Schema V2 Documentation](SCHEMA_V2.md)
- [Roadmap](ROADMAP.md)
- [Contributing Guide](../CONTRIBUTING.md)
