#!/usr/bin/env python3
"""
Parser Comparison Benchmark: mzPeak vs pyteomics vs pymzml

This script compares file access performance between mzPeak and traditional
mzML parsers (pyteomics and pymzml).

Usage:
    python benchmark_pyteomics_comparison.py <input.mzML> <input.mzpeak>

Requirements:
    pip install mzpeak pyteomics pymzml numpy
"""

import argparse
import gc
import json
import statistics
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, List

import numpy as np


@dataclass
class TimingResult:
    """Timing result with statistics"""
    operation: str
    tool: str
    mean_seconds: float
    std_seconds: float
    min_seconds: float
    max_seconds: float
    runs: int
    result_count: Optional[int] = None


def time_operation(func, runs: int = 5, warmup: int = 1) -> tuple:
    """Time an operation multiple times and return statistics"""
    # Warmup
    for _ in range(warmup):
        result = func()
        gc.collect()

    # Timed runs
    times = []
    final_result = None
    for _ in range(runs):
        gc.collect()
        start = time.perf_counter()
        final_result = func()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return times, final_result


def benchmark_mzpeak(mzpeak_path: str, num_spectra: int, runs: int = 5) -> List[TimingResult]:
    """Benchmark mzPeak reader operations"""
    results = []

    try:
        import mzpeak
    except ImportError:
        print("  mzPeak Python bindings not available")
        return results

    print("\n  === mzPeak ===")

    # File open + metadata
    def open_metadata():
        reader = mzpeak.MzPeakReader(mzpeak_path)
        summary = reader.summary()
        return summary.num_spectra

    times, count = time_operation(open_metadata, runs=runs)
    results.append(TimingResult(
        operation="file_open_metadata",
        tool="mzpeak",
        mean_seconds=statistics.mean(times),
        std_seconds=statistics.stdev(times) if len(times) > 1 else 0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=runs,
        result_count=count
    ))
    print(f"    File open + metadata: {statistics.mean(times)*1000:.2f} ± {statistics.stdev(times)*1000:.2f} ms")

    # Random spectrum access
    target_id = num_spectra // 2
    reader = mzpeak.MzPeakReader(mzpeak_path)

    def random_access():
        return reader.get_spectrum(target_id)

    times, spec = time_operation(random_access, runs=runs)
    results.append(TimingResult(
        operation="random_spectrum_access",
        tool="mzpeak",
        mean_seconds=statistics.mean(times),
        std_seconds=statistics.stdev(times) if len(times) > 1 else 0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=runs,
        result_count=len(spec.peaks) if spec else 0
    ))
    print(f"    Random access (id={target_id}): {statistics.mean(times)*1000:.2f} ± {statistics.stdev(times)*1000:.2f} ms")

    # MS2 filtering
    def ms2_filter():
        return reader.spectra_by_ms_level(2)

    times, ms2_specs = time_operation(ms2_filter, runs=runs)
    results.append(TimingResult(
        operation="ms2_filter",
        tool="mzpeak",
        mean_seconds=statistics.mean(times),
        std_seconds=statistics.stdev(times) if len(times) > 1 else 0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=runs,
        result_count=len(ms2_specs)
    ))
    print(f"    MS2 filter: {statistics.mean(times)*1000:.2f} ± {statistics.stdev(times)*1000:.2f} ms ({len(ms2_specs):,} spectra)")

    # Count all MS2 peaks (iteration)
    def count_ms2_peaks():
        total = 0
        for spec in reader.spectra_by_ms_level(2):
            total += len(spec.peaks)
        return total

    times, peak_count = time_operation(count_ms2_peaks, runs=runs)
    results.append(TimingResult(
        operation="count_ms2_peaks",
        tool="mzpeak",
        mean_seconds=statistics.mean(times),
        std_seconds=statistics.stdev(times) if len(times) > 1 else 0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=runs,
        result_count=peak_count
    ))
    print(f"    Count MS2 peaks: {statistics.mean(times):.2f} ± {statistics.stdev(times):.2f} s ({peak_count:,} peaks)")

    return results


def benchmark_pyteomics(mzml_path: str, num_spectra: int, runs: int = 3) -> List[TimingResult]:
    """Benchmark pyteomics mzML reader"""
    results = []

    try:
        from pyteomics import mzml
    except ImportError:
        print("  pyteomics not available (pip install pyteomics)")
        return results

    print("\n  === pyteomics ===")

    # File open (builds index)
    def open_file():
        reader = mzml.MzML(mzml_path)
        # Access metadata
        _ = len(reader)
        return len(reader)

    times, count = time_operation(open_file, runs=runs, warmup=0)
    results.append(TimingResult(
        operation="file_open_metadata",
        tool="pyteomics",
        mean_seconds=statistics.mean(times),
        std_seconds=statistics.stdev(times) if len(times) > 1 else 0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=runs,
        result_count=count
    ))
    print(f"    File open + index: {statistics.mean(times):.2f} ± {statistics.stdev(times):.2f} s")

    # Random spectrum access (using index)
    reader = mzml.MzML(mzml_path)
    target_idx = num_spectra // 2
    target_id = f"scan={target_idx}"

    def random_access():
        # pyteomics uses string-based indexing
        try:
            return reader.get_by_id(target_id)
        except KeyError:
            # Try index-based access
            return reader[target_idx]

    times, spec = time_operation(random_access, runs=runs)
    peak_count = len(spec.get('m/z array', [])) if spec else 0
    results.append(TimingResult(
        operation="random_spectrum_access",
        tool="pyteomics",
        mean_seconds=statistics.mean(times),
        std_seconds=statistics.stdev(times) if len(times) > 1 else 0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=runs,
        result_count=peak_count
    ))
    print(f"    Random access (idx={target_idx}): {statistics.mean(times)*1000:.2f} ± {statistics.stdev(times)*1000:.2f} ms")

    # MS2 filtering (requires iteration)
    def ms2_filter():
        ms2_count = 0
        for spec in mzml.MzML(mzml_path):
            if spec.get('ms level', 1) == 2:
                ms2_count += 1
        return ms2_count

    # Only 1 run for slow operations
    times, ms2_count = time_operation(ms2_filter, runs=1, warmup=0)
    results.append(TimingResult(
        operation="ms2_filter",
        tool="pyteomics",
        mean_seconds=statistics.mean(times),
        std_seconds=0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=1,
        result_count=ms2_count
    ))
    print(f"    MS2 filter (full scan): {statistics.mean(times):.2f} s ({ms2_count:,} spectra)")

    # Count all MS2 peaks
    def count_ms2_peaks():
        total = 0
        for spec in mzml.MzML(mzml_path):
            if spec.get('ms level', 1) == 2:
                total += len(spec.get('m/z array', []))
        return total

    times, peak_count = time_operation(count_ms2_peaks, runs=1, warmup=0)
    results.append(TimingResult(
        operation="count_ms2_peaks",
        tool="pyteomics",
        mean_seconds=statistics.mean(times),
        std_seconds=0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=1,
        result_count=peak_count
    ))
    print(f"    Count MS2 peaks: {statistics.mean(times):.2f} s ({peak_count:,} peaks)")

    return results


def benchmark_pymzml(mzml_path: str, num_spectra: int, runs: int = 3) -> List[TimingResult]:
    """Benchmark pymzml reader"""
    results = []

    try:
        import pymzml
    except ImportError:
        print("  pymzml not available (pip install pymzml)")
        return results

    print("\n  === pymzml ===")

    # File open
    def open_file():
        reader = pymzml.run.Reader(mzml_path)
        # Get spectrum count
        count = 0
        for _ in reader:
            count += 1
        return count

    # pymzml is slow, fewer runs
    times, count = time_operation(open_file, runs=1, warmup=0)
    results.append(TimingResult(
        operation="file_open_metadata",
        tool="pymzml",
        mean_seconds=statistics.mean(times),
        std_seconds=0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=1,
        result_count=count
    ))
    print(f"    File iteration (count): {statistics.mean(times):.2f} s")

    # MS2 filtering
    def ms2_filter():
        ms2_count = 0
        for spec in pymzml.run.Reader(mzml_path):
            if spec.ms_level == 2:
                ms2_count += 1
        return ms2_count

    times, ms2_count = time_operation(ms2_filter, runs=1, warmup=0)
    results.append(TimingResult(
        operation="ms2_filter",
        tool="pymzml",
        mean_seconds=statistics.mean(times),
        std_seconds=0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=1,
        result_count=ms2_count
    ))
    print(f"    MS2 filter: {statistics.mean(times):.2f} s ({ms2_count:,} spectra)")

    # Count MS2 peaks
    def count_ms2_peaks():
        total = 0
        for spec in pymzml.run.Reader(mzml_path):
            if spec.ms_level == 2:
                total += len(spec.mz)
        return total

    times, peak_count = time_operation(count_ms2_peaks, runs=1, warmup=0)
    results.append(TimingResult(
        operation="count_ms2_peaks",
        tool="pymzml",
        mean_seconds=statistics.mean(times),
        std_seconds=0,
        min_seconds=min(times),
        max_seconds=max(times),
        runs=1,
        result_count=peak_count
    ))
    print(f"    Count MS2 peaks: {statistics.mean(times):.2f} s ({peak_count:,} peaks)")

    return results


def calculate_speedups(results: List[TimingResult]) -> dict:
    """Calculate speedup ratios between tools"""
    speedups = {}

    # Group by operation
    by_operation = {}
    for r in results:
        if r.operation not in by_operation:
            by_operation[r.operation] = {}
        by_operation[r.operation][r.tool] = r.mean_seconds

    for op, tools in by_operation.items():
        if 'mzpeak' in tools:
            mzpeak_time = tools['mzpeak']
            speedups[op] = {}
            for tool, time in tools.items():
                if tool != 'mzpeak':
                    speedups[op][f"vs_{tool}"] = round(time / mzpeak_time, 1) if mzpeak_time > 0 else 0

    return speedups


def print_comparison_table(results: List[TimingResult], speedups: dict):
    """Print a formatted comparison table"""
    print("\n" + "="*80)
    print("COMPARISON TABLE")
    print("="*80)

    # Group by operation
    by_operation = {}
    for r in results:
        if r.operation not in by_operation:
            by_operation[r.operation] = {}
        by_operation[r.operation][r.tool] = r

    operations = ["file_open_metadata", "random_spectrum_access", "ms2_filter", "count_ms2_peaks"]

    print(f"\n{'Operation':<25} {'mzPeak':<15} {'pyteomics':<15} {'pymzml':<15} {'Speedup':<15}")
    print("-"*80)

    for op in operations:
        if op not in by_operation:
            continue

        tools = by_operation[op]
        row = [op]

        for tool in ['mzpeak', 'pyteomics', 'pymzml']:
            if tool in tools:
                r = tools[tool]
                if r.mean_seconds < 1:
                    row.append(f"{r.mean_seconds*1000:.1f} ms")
                else:
                    row.append(f"{r.mean_seconds:.2f} s")
            else:
                row.append("--")

        # Speedup
        if op in speedups and 'vs_pyteomics' in speedups[op]:
            row.append(f"{speedups[op]['vs_pyteomics']:.0f}x")
        else:
            row.append("--")

        print(f"{row[0]:<25} {row[1]:<15} {row[2]:<15} {row[3]:<15} {row[4]:<15}")

    print("="*80)


def generate_latex_table(results: List[TimingResult], speedups: dict) -> str:
    """Generate LaTeX table for manuscript"""
    by_operation = {}
    for r in results:
        if r.operation not in by_operation:
            by_operation[r.operation] = {}
        by_operation[r.operation][r.tool] = r

    lines = [
        "% Parser Comparison Table",
        "\\begin{tabular}{lrrrr}",
        "\\toprule",
        "\\textbf{Operation} & \\textbf{mzPeak} & \\textbf{pyteomics} & \\textbf{pymzml} & \\textbf{Speedup} \\\\",
        "\\midrule",
    ]

    op_names = {
        "file_open_metadata": "File open + metadata",
        "random_spectrum_access": "Random spectrum access",
        "ms2_filter": "MS2 filtering",
        "count_ms2_peaks": "Count MS2 peaks",
    }

    for op, name in op_names.items():
        if op not in by_operation:
            continue

        tools = by_operation[op]
        row = [name]

        for tool in ['mzpeak', 'pyteomics', 'pymzml']:
            if tool in tools:
                r = tools[tool]
                if r.mean_seconds < 1:
                    row.append(f"{r.mean_seconds*1000:.1f} ms")
                else:
                    row.append(f"{r.mean_seconds:.1f} s")
            else:
                row.append("--")

        # Speedup
        if op in speedups and 'vs_pyteomics' in speedups[op]:
            row.append(f"{speedups[op]['vs_pyteomics']:.0f}$\\times$")
        else:
            row.append("--")

        lines.append(f"{row[0]} & {row[1]} & {row[2]} & {row[3]} & {row[4]} \\\\")

    lines.extend([
        "\\bottomrule",
        "\\end{tabular}",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Compare mzPeak vs pyteomics vs pymzml performance"
    )
    parser.add_argument("mzml", help="Input mzML file")
    parser.add_argument("mzpeak", help="Converted mzPeak file/directory")
    parser.add_argument("--runs", "-r", type=int, default=5,
                       help="Number of runs for mzPeak benchmarks")
    parser.add_argument("--output", "-o", default="comparison_results.json",
                       help="Output JSON file")

    args = parser.parse_args()

    mzml_path = Path(args.mzml)
    mzpeak_path = Path(args.mzpeak)

    if not mzml_path.exists():
        print(f"Error: mzML file not found: {mzml_path}")
        sys.exit(1)
    if not mzpeak_path.exists():
        print(f"Error: mzPeak file not found: {mzpeak_path}")
        sys.exit(1)

    print("="*80)
    print("Parser Comparison Benchmark")
    print("="*80)
    print(f"\nmzML file: {mzml_path} ({mzml_path.stat().st_size / (1024**3):.2f} GB)")
    print(f"mzPeak file: {mzpeak_path}")

    # Get spectrum count from mzPeak
    try:
        import mzpeak
        reader = mzpeak.MzPeakReader(str(mzpeak_path))
        num_spectra = reader.summary().num_spectra
        print(f"Spectra: {num_spectra:,}")
    except ImportError:
        num_spectra = 10000  # Estimate
        print("Note: Could not determine spectrum count (mzpeak bindings not available)")

    all_results = []

    # Benchmark each tool
    all_results.extend(benchmark_mzpeak(str(mzpeak_path), num_spectra, runs=args.runs))
    all_results.extend(benchmark_pyteomics(str(mzml_path), num_spectra))
    all_results.extend(benchmark_pymzml(str(mzml_path), num_spectra))

    # Calculate speedups
    speedups = calculate_speedups(all_results)

    # Print comparison table
    print_comparison_table(all_results, speedups)

    # Save results
    output_path = Path(args.output)
    with open(output_path, 'w') as f:
        json.dump({
            "results": [asdict(r) for r in all_results],
            "speedups": speedups
        }, f, indent=2)
    print(f"\nResults saved to: {output_path}")

    # Generate LaTeX table
    latex_table = generate_latex_table(all_results, speedups)
    latex_path = output_path.with_suffix('.tex')
    with open(latex_path, 'w') as f:
        f.write(latex_table)
    print(f"LaTeX table saved to: {latex_path}")


if __name__ == "__main__":
    main()
