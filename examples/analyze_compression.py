#!/usr/bin/env python3
"""Analyze compression breakdown of mzpeak output."""
import pyarrow.parquet as pq

pf = pq.ParquetFile('/Volumes/NVMe 2TB/Test/mzml_benchmark/sync_output.parquet')
meta = pf.metadata

print('=== SIZE BREAKDOWN ===')
print(f'mzML input:     920 MB (XML + base64)')
print(f'Parquet output: {meta.serialized_size / 1e6:.1f} MB')
print()

rows = meta.num_rows
print(f'Total rows: {rows:,}')
print()

# Theoretical uncompressed binary size
raw_size = rows * (8+8+2+4+1+8+4+8+2+4+4+4+8+8+4+4)  # core columns
print(f'Raw binary (dense):  {raw_size / 1e6:.1f} MB')
print(f'Parquet compression: {raw_size / meta.serialized_size:.1f}x over raw binary')
print()

# Base64 overhead: mzML encodes binary as base64 (+33%) plus XML tags
mz_binary = rows * 8  # m/z as f64
int_binary = rows * 4  # intensity as f32 typically
base64_size = (mz_binary + int_binary) * 1.33  # base64 overhead
print(f'Peak data in mzML (base64): ~{base64_size / 1e6:.0f} MB')
print()

print('=== PER-COLUMN SIZE (first row group) ===')
rg = pf.metadata.row_group(0)
for i in range(rg.num_columns):
    col = rg.column(i)
    name = col.path_in_schema
    compressed = col.total_compressed_size
    uncompressed = col.total_uncompressed_size
    ratio = uncompressed / compressed if compressed > 0 else 0
    print(f'{name:25} {compressed:>10,} bytes  ({ratio:.1f}x)')
