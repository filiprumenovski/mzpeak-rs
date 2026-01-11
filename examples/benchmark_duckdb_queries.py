#!/usr/bin/env python3
"""
DuckDB SQL Query Benchmark

Demonstrates and benchmarks SQL queries on mzPeak Parquet files using DuckDB.

Usage:
    python benchmark_duckdb_queries.py <file.mzpeak>

Requirements:
    pip install duckdb
"""

import argparse
import gc
import json
import statistics
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Any, Optional

try:
    import duckdb
except ImportError:
    print("Error: DuckDB not installed. Run: pip install duckdb")
    sys.exit(1)


@dataclass
class QueryResult:
    """Result of a single query benchmark"""
    name: str
    description: str
    sql: str
    mean_ms: float
    std_ms: float
    min_ms: float
    max_ms: float
    runs: int
    result_preview: str
    row_count: int


def time_query(con, sql: str, runs: int = 5, warmup: int = 1) -> tuple:
    """Execute query multiple times and return timing statistics"""
    # Warmup runs
    for _ in range(warmup):
        con.execute(sql).fetchall()
        gc.collect()

    # Timed runs
    times = []
    result = None
    for _ in range(runs):
        gc.collect()
        start = time.perf_counter()
        result = con.execute(sql).fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed * 1000)  # Convert to ms

    return times, result


def format_result(result: List[Any], max_rows: int = 3) -> str:
    """Format query result for display"""
    if not result:
        return "No results"
    if len(result) == 1 and len(result[0]) == 1:
        return str(result[0][0])
    preview = [str(row) for row in result[:max_rows]]
    if len(result) > max_rows:
        preview.append(f"... ({len(result)} total rows)")
    return "\n".join(preview)


def run_benchmarks(parquet_path: str, runs: int = 5) -> List[QueryResult]:
    """Run all query benchmarks"""
    results = []

    # Create connection
    con = duckdb.connect()

    # Define benchmark queries
    queries = [
        {
            "name": "count_total_peaks",
            "description": "Count total peaks in dataset",
            "sql": f"SELECT COUNT(*) as total_peaks FROM read_parquet('{parquet_path}')"
        },
        {
            "name": "count_spectra",
            "description": "Count unique spectra",
            "sql": f"SELECT COUNT(DISTINCT spectrum_id) as num_spectra FROM read_parquet('{parquet_path}')"
        },
        {
            "name": "count_ms2_spectra",
            "description": "Count MS2 spectra (predicate pushdown)",
            "sql": f"SELECT COUNT(DISTINCT spectrum_id) FROM read_parquet('{parquet_path}') WHERE ms_level = 2"
        },
        {
            "name": "ms_level_stats",
            "description": "Aggregate statistics by MS level",
            "sql": f"""
                SELECT
                    ms_level,
                    COUNT(DISTINCT spectrum_id) as num_spectra,
                    COUNT(*) as num_peaks,
                    AVG(intensity) as avg_intensity,
                    MAX(intensity) as max_intensity
                FROM read_parquet('{parquet_path}')
                GROUP BY ms_level
                ORDER BY ms_level
            """
        },
        {
            "name": "precursor_mz_range",
            "description": "Filter by precursor m/z range (500-600 Da)",
            "sql": f"""
                SELECT COUNT(DISTINCT spectrum_id)
                FROM read_parquet('{parquet_path}')
                WHERE ms_level = 2 AND precursor_mz BETWEEN 500 AND 600
            """
        },
        {
            "name": "rt_range_query",
            "description": "Filter by retention time range",
            "sql": f"""
                SELECT COUNT(DISTINCT spectrum_id)
                FROM read_parquet('{parquet_path}')
                WHERE retention_time BETWEEN 1000 AND 2000
            """
        },
        {
            "name": "high_intensity_peaks",
            "description": "Find high-intensity peaks (>1e6)",
            "sql": f"""
                SELECT COUNT(*)
                FROM read_parquet('{parquet_path}')
                WHERE intensity > 1000000
            """
        },
        {
            "name": "top_100_peaks",
            "description": "Top 100 peaks by intensity",
            "sql": f"""
                SELECT spectrum_id, mz, intensity
                FROM read_parquet('{parquet_path}')
                ORDER BY intensity DESC
                LIMIT 100
            """
        },
        {
            "name": "mz_histogram",
            "description": "m/z distribution (100 Da bins)",
            "sql": f"""
                SELECT
                    FLOOR(mz / 100) * 100 as mz_bin,
                    COUNT(*) as peak_count
                FROM read_parquet('{parquet_path}')
                GROUP BY mz_bin
                ORDER BY mz_bin
            """
        },
        {
            "name": "precursor_charge_dist",
            "description": "Precursor charge state distribution",
            "sql": f"""
                SELECT
                    precursor_charge,
                    COUNT(DISTINCT spectrum_id) as num_spectra
                FROM read_parquet('{parquet_path}')
                WHERE ms_level = 2 AND precursor_charge IS NOT NULL
                GROUP BY precursor_charge
                ORDER BY precursor_charge
            """
        },
        {
            "name": "complex_filter",
            "description": "Complex multi-column filter",
            "sql": f"""
                SELECT
                    spectrum_id,
                    precursor_mz,
                    precursor_charge,
                    COUNT(*) as num_peaks,
                    MAX(intensity) as max_intensity
                FROM read_parquet('{parquet_path}')
                WHERE ms_level = 2
                    AND precursor_mz BETWEEN 400 AND 800
                    AND precursor_charge IN (2, 3, 4)
                    AND intensity > 10000
                GROUP BY spectrum_id, precursor_mz, precursor_charge
                ORDER BY max_intensity DESC
                LIMIT 50
            """
        },
        {
            "name": "full_table_scan",
            "description": "Full table scan (sum all intensities)",
            "sql": f"SELECT SUM(intensity) as total_intensity FROM read_parquet('{parquet_path}')"
        },
    ]

    print("\nRunning DuckDB SQL benchmarks...")
    print("="*70)

    for q in queries:
        name = q["name"]
        desc = q["description"]
        sql = q["sql"].strip()

        print(f"\n{name}: {desc}")
        print(f"  SQL: {sql[:80]}{'...' if len(sql) > 80 else ''}")

        try:
            times, result = time_query(con, sql, runs=runs)

            result_preview = format_result(result)
            row_count = len(result) if result else 0

            qr = QueryResult(
                name=name,
                description=desc,
                sql=sql,
                mean_ms=round(statistics.mean(times), 2),
                std_ms=round(statistics.stdev(times), 2) if len(times) > 1 else 0,
                min_ms=round(min(times), 2),
                max_ms=round(max(times), 2),
                runs=runs,
                result_preview=result_preview[:200],
                row_count=row_count
            )
            results.append(qr)

            print(f"  Time: {qr.mean_ms:.2f} ± {qr.std_ms:.2f} ms")
            print(f"  Result: {result_preview[:100]}")

        except Exception as e:
            print(f"  Error: {e}")

    con.close()
    return results


def print_summary(results: List[QueryResult]):
    """Print a summary table of results"""
    print("\n" + "="*70)
    print("SUMMARY: DuckDB Query Performance on mzPeak")
    print("="*70)

    print(f"\n{'Query':<25} {'Mean (ms)':<12} {'Std (ms)':<10} {'Result':<20}")
    print("-"*70)

    for r in results:
        result_str = r.result_preview[:20] if r.result_preview else ""
        print(f"{r.name:<25} {r.mean_ms:<12.2f} {r.std_ms:<10.2f} {result_str:<20}")

    print("="*70)


def generate_latex_table(results: List[QueryResult]) -> str:
    """Generate LaTeX table for manuscript"""
    lines = [
        "% DuckDB Query Performance",
        "\\begin{tabular}{lrl}",
        "\\toprule",
        "\\textbf{Query} & \\textbf{Time (ms)} & \\textbf{Description} \\\\",
        "\\midrule",
    ]

    # Select key queries for table
    key_queries = [
        "count_ms2_spectra",
        "precursor_mz_range",
        "rt_range_query",
        "ms_level_stats",
        "high_intensity_peaks",
        "complex_filter",
    ]

    for r in results:
        if r.name in key_queries:
            # Shorten description
            desc = r.description[:40]
            lines.append(f"{r.name.replace('_', '\\_')} & {r.mean_ms:.1f} & {desc} \\\\")

    lines.extend([
        "\\bottomrule",
        "\\end{tabular}",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark DuckDB SQL queries on mzPeak files"
    )
    parser.add_argument("mzpeak", help="mzPeak file or directory")
    parser.add_argument("--runs", "-r", type=int, default=5,
                       help="Number of runs per query (default: 5)")
    parser.add_argument("--output", "-o", default="duckdb_benchmark.json",
                       help="Output JSON file")

    args = parser.parse_args()

    mzpeak_path = Path(args.mzpeak)
    if not mzpeak_path.exists():
        print(f"Error: File not found: {mzpeak_path}")
        sys.exit(1)

    # Find peaks parquet file
    if mzpeak_path.is_dir():
        parquet_path = mzpeak_path / "peaks" / "peaks.parquet"
    else:
        # Single file - might be a parquet file directly or zip container
        parquet_path = mzpeak_path

    # For ZIP containers, DuckDB can't read directly - need to extract or use directory format
    if parquet_path.suffix == '.mzpeak' and parquet_path.is_file():
        print(f"Note: For ZIP container format, extracting or using directory format is required.")
        print(f"Trying to read as directory: {mzpeak_path}")
        if (mzpeak_path / "peaks" / "peaks.parquet").exists():
            parquet_path = mzpeak_path / "peaks" / "peaks.parquet"
        else:
            print(f"Error: Could not find peaks.parquet in {mzpeak_path}")
            sys.exit(1)

    if not parquet_path.exists():
        print(f"Error: Parquet file not found: {parquet_path}")
        sys.exit(1)

    print("="*70)
    print("DuckDB SQL Query Benchmark")
    print("="*70)
    print(f"\nParquet file: {parquet_path}")

    # Get file size
    size_bytes = parquet_path.stat().st_size
    print(f"File size: {size_bytes / (1024**3):.2f} GB")

    # Get DuckDB version
    con = duckdb.connect()
    version = con.execute("SELECT version()").fetchone()[0]
    con.close()
    print(f"DuckDB version: {version}")

    # Run benchmarks
    results = run_benchmarks(str(parquet_path), runs=args.runs)

    # Print summary
    print_summary(results)

    # Save JSON results
    output_path = Path(args.output)
    with open(output_path, 'w') as f:
        json.dump({
            "file": str(parquet_path),
            "file_size_bytes": size_bytes,
            "duckdb_version": version,
            "results": [asdict(r) for r in results]
        }, f, indent=2)
    print(f"\nResults saved to: {output_path}")

    # Generate LaTeX table
    latex_table = generate_latex_table(results)
    latex_path = output_path.with_suffix('.tex')
    with open(latex_path, 'w') as f:
        f.write(latex_table)
    print(f"LaTeX table saved to: {latex_path}")

    # Print example SQL for manuscript
    print("\n" + "="*70)
    print("Example SQL for Manuscript")
    print("="*70)
    print("""
-- Example: Find all MS2 spectra with high-intensity peaks
-- in a specific precursor m/z range

SELECT
    spectrum_id,
    precursor_mz,
    precursor_charge,
    COUNT(*) as peak_count,
    MAX(intensity) as max_intensity
FROM read_parquet('data.mzpeak/peaks/peaks.parquet')
WHERE ms_level = 2
  AND precursor_mz BETWEEN 500 AND 600
  AND intensity > 1e6
GROUP BY spectrum_id, precursor_mz, precursor_charge
ORDER BY max_intensity DESC
LIMIT 10;

-- This query executes in milliseconds on gigabyte-scale files,
-- leveraging Parquet's predicate pushdown to avoid reading
-- irrelevant data.
""")


if __name__ == "__main__":
    main()
