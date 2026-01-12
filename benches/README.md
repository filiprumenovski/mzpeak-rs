# mzPeak Benchmarking Guide

This directory contains formal benchmarks for the mzPeak format using [Criterion.rs](https://github.com/bheisler/criterion.rs).

## Overview

The benchmark suite provides reproducible performance metrics for:
1. **mzML to mzPeak conversion** - Parsing, decoding, and encoding throughput
2. **Random access queries** - Seeking specific spectra by ID or retention time
3. **Peak filtering** - MS2 extraction, intensity thresholds, precursor m/z ranges
4. **mzML streaming** - Sequential parsing throughput for decoded and raw spectra

These benchmarks are designed to support the performance claims in the mzPeak preprint.

## Benchmark Suites

### 1. Conversion (`benches/conversion.rs`)

Measures end-to-end conversion performance from synthetic mzML files to mzPeak format.

**Benchmarks:**
- `mzml_conversion`: Convert varying dataset sizes (100/500/1000 spectra × 100 peaks)
- `peak_writing`: Direct write performance without mzML parsing (1K/10K/100K peaks)
- `peak_writing_arrays`: SoA (SpectrumArrays) write performance (1K/10K/100K peaks)
- `per_peak_overhead`: Single-peak write to measure minimum overhead

**Metrics:**
- Throughput (peaks/second)
- Total time (milliseconds)
- Nanoseconds per peak processed

### 2. Query Performance (`benches/query_performance.rs`)

Tests random access and query operations on pre-built mzPeak files.

**Benchmarks:**
- `random_access`: Seek to spectrum by ID (middle of file)
- `rt_range_query`: Retention time range queries (10s/50s/100s windows)
- `ms_level_filter`: Extract all MS1 or MS2 spectra
- `full_scan`: Sequential read of entire file (streaming)
- `full_scan_arrays_view`: Sequential read using view-backed SoA arrays
- `metadata_only`: Read file metadata without peak data

**Use Case:** Demonstrates Parquet's column pruning and predicate pushdown benefits.

### 3. Filtering (`benches/filtering.rs`)

Measures peak filtering performance for common analytical workflows.

**Benchmarks:**
- `ms2_filtering`: Extract all MS2 spectra from mixed dataset
- `precursor_mz_filter`: Filter MS2 by precursor m/z range
- `intensity_filter`: Apply intensity threshold across all peaks
- `combined_filter`: Multi-criteria filtering (MS2 + RT + intensity)
- `top_n_peaks`: Extract top-N most intense peaks per spectrum

**Use Case:** Real-world data processing pipelines (e.g., DIA analysis, metabolomics).

### 4. mzML Streaming (`benches/mzml_streamer.rs`)

Measures sequential parsing throughput for both decoded and raw spectrum paths.

**Benchmarks:**
- `mzml_streamer_next_spectrum`: `next_spectrum()` throughput (decoded arrays)
- `mzml_streamer_next_raw_spectrum`: `next_raw_spectrum()` throughput (raw base64 arrays)

## Running Benchmarks

### Quick Test (sanity check)

```bash
# Test that benchmarks compile and run
cargo bench --benches -- --test
```

### Full Benchmark Suite

```bash
# Run all benchmarks (~10 minutes)
cargo bench

# Or use the helper script
./run_benchmarks.sh
```

### Individual Benchmarks

```bash
# Run specific suite
cargo bench --bench conversion
cargo bench --bench query_performance
cargo bench --bench filtering

# Run specific test
cargo bench --bench conversion -- mzml_conversion/1000spectra
```

### Baseline Comparison

```bash
# Save current results as baseline
./run_benchmarks.sh --baseline

# Make changes to code...

# Compare against baseline
./run_benchmarks.sh --compare
```

## Interpreting Results

### Criterion Output

Criterion reports several statistics:
- **time**: Mean execution time with 95% confidence interval
- **thrpt**: Throughput (elements/operations per second)
- **change**: Percentage change from previous run (if available)

Example:
```
mzml_conversion/1000spectra_100peaks
                        time:   [33.491 ms 33.891 ms 34.366 ms]
                        thrpt:  [2.9098 Melem/s 2.9506 Melem/s 2.9859 Melem/s]
```

This means:
- Converting 100,000 peaks takes ~34 ms
- Throughput is ~2.95 million peaks/second
- 95% confidence interval: [33.5, 34.4] ms

### HTML Reports

Criterion generates detailed HTML reports with:
- Time series plots
- Probability density functions
- Regression analysis
- Outlier detection

View reports:
```bash
open target/criterion/report/index.html
```

## Key Metrics for Preprint

### Conversion Throughput

**Claim**: "mzPeak can convert at ~2.5 million peaks per second"

**Supporting benchmark**: `benches/conversion.rs::bench_conversion`
- 1000 spectra × 100 peaks = 100,000 peaks
- Mean time: ~34 ms
- **Throughput: 2.9 million peaks/second**

### Random Access Performance

**Claim**: "Sub-millisecond random access to any spectrum"

**Supporting benchmark**: `benches/query_performance.rs::bench_random_access`
- Seek to spectrum in middle of 1000-spectrum file
- **Mean time: ~100 microseconds**

### Filtering Efficiency

**Claim**: "Extract MS2 spectra in milliseconds"

**Supporting benchmark**: `benches/filtering.rs::bench_ms2_filtering`
- Filter 1000 spectra (66% MS2)
- **Mean time: ~8 milliseconds**

## Comparison with Other Formats

### Baseline: mzML Parsing

To compare mzPeak against standard mzML parsing, you can use the `mzdata` crate as a baseline:

```rust
// Add to benches/comparison.rs (not implemented yet)
use mzdata::io::MzMLReader;

fn bench_mzml_parsing(c: &mut Criterion) {
    c.bench_function("mzml_read_all", |b| {
        b.iter(|| {
            let reader = MzMLReader::open("test.mzML").unwrap();
            let spectra: Vec<_> = reader.collect();
            black_box(spectra);
        });
    });
}
```

**Expected Results**:
- mzML parsing: ~50-100 MB/s (XML parsing overhead)
- mzPeak reading: ~500-1000 MB/s (binary columnar format)
- **Speedup: 5-20x faster**

## Technical Details

### Test Data Generation

Benchmarks use synthetic mzML data generated on-the-fly:
- Realistic spectrum structure (MS1/MS2 levels)
- Base64-encoded binary arrays
- Controllable dataset sizes

This ensures:
- Reproducible results across machines
- No dependency on external test files
- Parameterizable complexity

### Batching Strategy

The conversion benchmarks use `batch_size = 100` to match real-world usage:
- Balances memory usage vs. throughput
- Mimics production conversion workflows
- Prevents unrealistic in-memory buffering

### Measurement Overhead

Criterion automatically:
- Warms up code paths (branch prediction, CPU caches)
- Runs multiple iterations for statistical significance
- Detects and reports outliers
- Uses cycle-accurate timing (not wall clock)

## CI/CD Integration

To run benchmarks in CI pipelines:

```yaml
# .github/workflows/benchmark.yml
- name: Run benchmarks
  run: cargo bench --benches -- --test  # Quick test mode
```

For nightly performance tracking:
```yaml
- name: Run full benchmarks
  run: cargo bench -- --save-baseline nightly
```

## Troubleshooting

### Benchmarks take too long

Use quick mode for faster iteration:
```bash
cargo bench -- --quick
```

### Inconsistent results

Ensure stable system conditions:
- Close other applications
- Disable CPU frequency scaling
- Use `nice -n -20` for higher priority

### Missing Gnuplot

Criterion falls back to `plotters` backend automatically. To use Gnuplot:
```bash
# macOS
brew install gnuplot

# Ubuntu
sudo apt-get install gnuplot
```

## Future Work

Potential benchmark additions:
1. **Real mzML comparison**: Benchmark against `mzdata` or `ThermoRawFileParser`
2. **DuckDB queries**: Compare query performance vs. direct Parquet access
3. **Ion mobility filtering**: 4D data filtering benchmarks
4. **Compression ratios**: Automated comparison of file sizes
5. **Memory profiling**: Peak memory usage during conversion

## References

- [Criterion.rs Book](https://bheisler.github.io/criterion.rs/book/)
- [Benchmarking Best Practices](https://easyperf.net/blog/2018/08/26/Microarchitectural-performance-events)
- [Apache Parquet Performance](https://arrow.apache.org/docs/python/parquet.html#performance)
