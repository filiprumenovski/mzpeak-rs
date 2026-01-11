#!/usr/bin/env python3
"""
mzPeak Publication Benchmark Suite

This script runs comprehensive benchmarks for the mzPeak publication,
measuring compression ratios, conversion throughput, and query performance.

Usage:
    python benchmark_publication.py <input.mzML> [output_dir]

Requirements:
    pip install mzpeak pyarrow duckdb
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# Try to import mzpeak - if not available, we'll use CLI
try:
    import mzpeak
    MZPEAK_PYTHON = True
except ImportError:
    MZPEAK_PYTHON = False
    print("Note: mzpeak Python bindings not available, using CLI")


@dataclass
class SystemInfo:
    """System information for reproducibility"""
    platform: str
    platform_version: str
    python_version: str
    cpu: str
    cpu_cores: int
    memory_gb: float
    timestamp: str


@dataclass
class ConversionResult:
    """Results from mzML to mzPeak conversion"""
    input_file: str
    input_size_bytes: int
    input_size_gb: float
    output_file: str
    output_size_bytes: int
    output_size_gb: float
    compression_ratio: float
    conversion_time_seconds: float
    spectra_count: int
    ms1_spectra: int
    ms2_spectra: int
    peak_count: int
    peaks_per_second: float
    spectra_per_second: float
    mb_per_second: float


@dataclass
class QueryResult:
    """Results from query performance tests"""
    operation: str
    time_seconds: float
    time_ms: float
    result_count: Optional[int] = None


@dataclass
class BenchmarkResults:
    """Complete benchmark results"""
    system_info: SystemInfo
    conversion: ConversionResult
    queries: list
    duckdb_queries: list


def get_system_info() -> SystemInfo:
    """Collect system information for reproducibility"""
    import multiprocessing

    # Get memory info
    try:
        if platform.system() == "Darwin":
            # macOS
            result = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True, text=True
            )
            memory_bytes = int(result.stdout.strip())
            memory_gb = memory_bytes / (1024**3)
        else:
            # Linux
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        memory_kb = int(line.split()[1])
                        memory_gb = memory_kb / (1024**2)
                        break
    except Exception:
        memory_gb = 0.0

    # Get CPU info
    try:
        if platform.system() == "Darwin":
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True, text=True
            )
            cpu = result.stdout.strip()
        else:
            cpu = platform.processor() or "Unknown"
    except Exception:
        cpu = platform.processor() or "Unknown"

    return SystemInfo(
        platform=platform.system(),
        platform_version=platform.version(),
        python_version=platform.python_version(),
        cpu=cpu,
        cpu_cores=multiprocessing.cpu_count(),
        memory_gb=round(memory_gb, 1),
        timestamp=datetime.now().isoformat()
    )


def convert_mzml_to_mzpeak(input_path: str, output_path: str) -> ConversionResult:
    """Convert mzML to mzPeak and collect metrics"""
    input_path = Path(input_path)
    output_path = Path(output_path)

    input_size = input_path.stat().st_size

    print(f"\nConverting {input_path.name}...")
    print(f"  Input size: {input_size / (1024**3):.2f} GB")

    start_time = time.perf_counter()

    if MZPEAK_PYTHON:
        # Use Python bindings
        stats = mzpeak.convert(str(input_path), str(output_path))
        spectra_count = stats.spectra_count
        ms1_spectra = stats.ms1_spectra
        ms2_spectra = stats.ms2_spectra
        peak_count = stats.peak_count
    else:
        # Use CLI
        result = subprocess.run(
            ["cargo", "run", "--release", "--bin", "mzpeak-convert", "--",
             "convert", str(input_path), str(output_path)],
            capture_output=True, text=True, cwd=input_path.parent.parent
        )
        if result.returncode != 0:
            print(f"Conversion failed: {result.stderr}")
            sys.exit(1)

        # Parse output for stats
        output = result.stdout + result.stderr
        spectra_count = 0
        ms1_spectra = 0
        ms2_spectra = 0
        peak_count = 0

        for line in output.split('\n'):
            if 'Total spectra:' in line:
                spectra_count = int(line.split(':')[1].strip().replace(',', ''))
            elif 'MS1:' in line:
                ms1_spectra = int(line.split(':')[1].strip().replace(',', ''))
            elif 'MS2:' in line:
                ms2_spectra = int(line.split(':')[1].strip().replace(',', ''))
            elif 'Total peaks:' in line:
                peak_count = int(line.split(':')[1].strip().replace(',', ''))

    conversion_time = time.perf_counter() - start_time

    # Get output size
    if output_path.is_dir():
        output_size = sum(f.stat().st_size for f in output_path.rglob('*') if f.is_file())
    else:
        output_size = output_path.stat().st_size

    compression_ratio = input_size / output_size if output_size > 0 else 0
    peaks_per_second = peak_count / conversion_time if conversion_time > 0 else 0
    spectra_per_second = spectra_count / conversion_time if conversion_time > 0 else 0
    mb_per_second = (input_size / (1024**2)) / conversion_time if conversion_time > 0 else 0

    result = ConversionResult(
        input_file=input_path.name,
        input_size_bytes=input_size,
        input_size_gb=round(input_size / (1024**3), 3),
        output_file=output_path.name,
        output_size_bytes=output_size,
        output_size_gb=round(output_size / (1024**3), 3),
        compression_ratio=round(compression_ratio, 2),
        conversion_time_seconds=round(conversion_time, 2),
        spectra_count=spectra_count,
        ms1_spectra=ms1_spectra,
        ms2_spectra=ms2_spectra,
        peak_count=peak_count,
        peaks_per_second=round(peaks_per_second),
        spectra_per_second=round(spectra_per_second, 1),
        mb_per_second=round(mb_per_second, 1)
    )

    print(f"  Output size: {result.output_size_gb:.3f} GB")
    print(f"  Compression ratio: {result.compression_ratio:.2f}x")
    print(f"  Conversion time: {result.conversion_time_seconds:.2f}s")
    print(f"  Spectra: {result.spectra_count:,} (MS1: {result.ms1_spectra:,}, MS2: {result.ms2_spectra:,})")
    print(f"  Peaks: {result.peak_count:,}")
    print(f"  Throughput: {result.peaks_per_second:,.0f} peaks/s, {result.mb_per_second:.1f} MB/s")

    return result


def benchmark_mzpeak_queries(mzpeak_path: str, spectra_count: int) -> list:
    """Benchmark mzPeak reader operations"""
    results = []

    if not MZPEAK_PYTHON:
        print("\nSkipping mzPeak query benchmarks (Python bindings not available)")
        return results

    print("\nBenchmarking mzPeak reader operations...")

    # File opening / metadata access
    start = time.perf_counter()
    reader = mzpeak.MzPeakReader(mzpeak_path)
    summary = reader.summary()
    elapsed = time.perf_counter() - start
    results.append(QueryResult(
        operation="file_open_metadata",
        time_seconds=elapsed,
        time_ms=elapsed * 1000,
        result_count=summary.num_spectra
    ))
    print(f"  File open + metadata: {elapsed*1000:.2f} ms")

    # Random spectrum access (middle of file)
    target_id = spectra_count // 2
    start = time.perf_counter()
    spectrum = reader.get_spectrum(target_id)
    elapsed = time.perf_counter() - start
    results.append(QueryResult(
        operation="random_spectrum_access",
        time_seconds=elapsed,
        time_ms=elapsed * 1000,
        result_count=len(spectrum.peaks) if spectrum else 0
    ))
    print(f"  Random spectrum access (id={target_id}): {elapsed*1000:.2f} ms")

    # MS2 filtering
    start = time.perf_counter()
    ms2_spectra = reader.spectra_by_ms_level(2)
    elapsed = time.perf_counter() - start
    results.append(QueryResult(
        operation="ms2_filter",
        time_seconds=elapsed,
        time_ms=elapsed * 1000,
        result_count=len(ms2_spectra)
    ))
    print(f"  MS2 filter: {elapsed*1000:.2f} ms ({len(ms2_spectra):,} spectra)")

    # Full iteration (count peaks)
    start = time.perf_counter()
    total_peaks = 0
    for spec in reader.iter_spectra():
        total_peaks += len(spec.peaks)
    elapsed = time.perf_counter() - start
    results.append(QueryResult(
        operation="full_iteration",
        time_seconds=elapsed,
        time_ms=elapsed * 1000,
        result_count=total_peaks
    ))
    print(f"  Full iteration: {elapsed:.2f} s ({total_peaks:,} peaks)")

    return results


def benchmark_duckdb_queries(parquet_path: str) -> list:
    """Benchmark DuckDB SQL queries on mzPeak Parquet file"""
    results = []

    try:
        import duckdb
    except ImportError:
        print("\nSkipping DuckDB benchmarks (duckdb not installed)")
        return results

    print("\nBenchmarking DuckDB SQL queries...")

    # Find the peaks parquet file
    parquet_path = Path(parquet_path)
    if parquet_path.is_dir():
        peaks_file = parquet_path / "peaks" / "peaks.parquet"
    else:
        peaks_file = parquet_path

    if not peaks_file.exists():
        print(f"  Parquet file not found: {peaks_file}")
        return results

    queries = [
        ("count_ms2_spectra",
         f"SELECT COUNT(DISTINCT spectrum_id) FROM read_parquet('{peaks_file}') WHERE ms_level = 2"),
        ("precursor_mz_range",
         f"SELECT COUNT(*) FROM read_parquet('{peaks_file}') WHERE ms_level = 2 AND precursor_mz BETWEEN 500 AND 600"),
        ("aggregate_by_ms_level",
         f"SELECT ms_level, COUNT(*) as peak_count, AVG(intensity) as avg_intensity FROM read_parquet('{peaks_file}') GROUP BY ms_level"),
        ("high_intensity_peaks",
         f"SELECT COUNT(*) FROM read_parquet('{peaks_file}') WHERE intensity > 1e6"),
        ("rt_range_query",
         f"SELECT COUNT(DISTINCT spectrum_id) FROM read_parquet('{peaks_file}') WHERE retention_time BETWEEN 1000 AND 2000"),
    ]

    con = duckdb.connect()

    for name, query in queries:
        # Warmup
        con.execute(query).fetchall()

        # Timed run
        start = time.perf_counter()
        result = con.execute(query).fetchall()
        elapsed = time.perf_counter() - start

        result_count = result[0][0] if result and len(result[0]) > 0 else len(result)

        results.append(QueryResult(
            operation=f"duckdb_{name}",
            time_seconds=elapsed,
            time_ms=elapsed * 1000,
            result_count=result_count
        ))
        print(f"  {name}: {elapsed*1000:.2f} ms (result: {result_count:,})")

    con.close()
    return results


def print_summary(results: BenchmarkResults):
    """Print a formatted summary of benchmark results"""
    print("\n" + "="*70)
    print("BENCHMARK SUMMARY")
    print("="*70)

    print(f"\nSystem: {results.system_info.cpu}")
    print(f"Platform: {results.system_info.platform} {results.system_info.platform_version}")
    print(f"Memory: {results.system_info.memory_gb} GB")
    print(f"Timestamp: {results.system_info.timestamp}")

    conv = results.conversion
    print(f"\n--- Conversion ---")
    print(f"Input:  {conv.input_file} ({conv.input_size_gb:.2f} GB)")
    print(f"Output: {conv.output_file} ({conv.output_size_gb:.3f} GB)")
    print(f"Compression ratio: {conv.compression_ratio:.2f}x")
    print(f"Conversion time: {conv.conversion_time_seconds:.2f} seconds")
    print(f"Spectra: {conv.spectra_count:,} (MS1: {conv.ms1_spectra:,}, MS2: {conv.ms2_spectra:,})")
    print(f"Peaks: {conv.peak_count:,}")
    print(f"Throughput: {conv.peaks_per_second:,.0f} peaks/s")

    if results.queries:
        print(f"\n--- mzPeak Query Performance ---")
        for q in results.queries:
            print(f"{q.operation}: {q.time_ms:.2f} ms")

    if results.duckdb_queries:
        print(f"\n--- DuckDB SQL Performance ---")
        for q in results.duckdb_queries:
            print(f"{q.operation}: {q.time_ms:.2f} ms")

    print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(
        description="mzPeak Publication Benchmark Suite"
    )
    parser.add_argument("input", help="Input mzML file")
    parser.add_argument("output_dir", nargs="?", default="benchmark_output",
                       help="Output directory for results")
    parser.add_argument("--json", "-j", help="Output JSON file for results",
                       default="benchmark_results.json")

    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    mzpeak_output = output_dir / f"{input_path.stem}.mzpeak"

    print("="*70)
    print("mzPeak Publication Benchmark Suite")
    print("="*70)

    # Collect system info
    system_info = get_system_info()
    print(f"\nSystem: {system_info.cpu}")
    print(f"Memory: {system_info.memory_gb} GB, Cores: {system_info.cpu_cores}")

    # Run conversion benchmark
    conversion_result = convert_mzml_to_mzpeak(str(input_path), str(mzpeak_output))

    # Run query benchmarks
    query_results = benchmark_mzpeak_queries(str(mzpeak_output), conversion_result.spectra_count)

    # Run DuckDB benchmarks
    duckdb_results = benchmark_duckdb_queries(str(mzpeak_output))

    # Compile results
    results = BenchmarkResults(
        system_info=system_info,
        conversion=conversion_result,
        queries=query_results,
        duckdb_queries=duckdb_results
    )

    # Print summary
    print_summary(results)

    # Save JSON results
    json_path = output_dir / args.json
    with open(json_path, 'w') as f:
        # Convert dataclasses to dicts
        json_data = {
            "system_info": asdict(results.system_info),
            "conversion": asdict(results.conversion),
            "queries": [asdict(q) for q in results.queries],
            "duckdb_queries": [asdict(q) for q in results.duckdb_queries]
        }
        json.dump(json_data, f, indent=2)
    print(f"\nResults saved to: {json_path}")

    # Generate LaTeX table snippet
    latex_path = output_dir / "benchmark_table.tex"
    with open(latex_path, 'w') as f:
        f.write("% Compression Results\n")
        f.write("\\begin{tabular}{lrrrrrr}\n")
        f.write("\\toprule\n")
        f.write("\\textbf{File} & \\textbf{mzML (GB)} & \\textbf{Spectra} & \\textbf{Peaks (M)} & \\textbf{mzPeak (GB)} & \\textbf{Ratio} & \\textbf{Time (s)} \\\\\n")
        f.write("\\midrule\n")
        f.write(f"{conversion_result.input_file} & {conversion_result.input_size_gb:.2f} & "
                f"{conversion_result.spectra_count:,} & {conversion_result.peak_count/1e6:.1f} & "
                f"{conversion_result.output_size_gb:.3f} & {conversion_result.compression_ratio:.2f}$\\times$ & "
                f"{conversion_result.conversion_time_seconds:.1f} \\\\\n")
        f.write("\\bottomrule\n")
        f.write("\\end{tabular}\n")
    print(f"LaTeX table saved to: {latex_path}")


if __name__ == "__main__":
    main()
