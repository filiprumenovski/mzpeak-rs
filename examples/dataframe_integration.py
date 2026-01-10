"""
Example demonstrating pandas and polars integration with mzPeak.

This example shows:
1. Creating mzPeak data from Python
2. Reading data with zero-copy Arrow conversion
3. Converting to pandas and polars DataFrames
4. Performing data analysis with each library
"""

import mzpeak
import tempfile
from pathlib import Path


def main():
    """Demonstrate mzPeak DataFrame integration."""
    
    # Create temporary output file
    with tempfile.TemporaryDirectory() as tmp:
        out_path = Path(tmp) / "example.parquet"
        
        # Create sample spectra
        print("Creating sample mass spectrometry data...")
        spectra = [
            mzpeak.SpectrumBuilder(1, 1)
            .ms_level(1)
            .retention_time(10.5)
            .polarity(1)
            .add_peak(400.5, 1000.0)
            .add_peak(500.3, 2000.0)
            .add_peak(600.7, 3000.0)
            .build(),
            
            mzpeak.SpectrumBuilder(2, 2)
            .ms_level(2)
            .retention_time(15.2)
            .polarity(1)
            .precursor(500.0, 2, 5000.0)
            .collision_energy(25.0)
            .add_peak(150.2, 1500.0)
            .add_peak(250.8, 2500.0)
            .add_peak(350.4, 3500.0)
            .build(),
            
            mzpeak.SpectrumBuilder(3, 3)
            .ms_level(1)
            .retention_time(20.0)
            .polarity(1)
            .add_peak(410.1, 1100.0)
            .add_peak(510.5, 2200.0)
            .build(),
        ]
        
        # Write data
        with mzpeak.MzPeakWriter(str(out_path)) as writer:
            writer.write_spectra(spectra)
            stats = writer.stats()
            print(f"✓ Written {stats.spectra_written} spectra, {stats.peaks_written} peaks")
            print(f"  File size: {stats.file_size_bytes:,} bytes\n")
        
        # Read and analyze with different libraries
        with mzpeak.MzPeakReader(str(out_path)) as reader:
            summary = reader.summary()
            print(f"File summary:")
            print(f"  Total spectra: {summary.num_spectra}")
            print(f"  Total peaks: {summary.total_peaks}")
            print(f"  MS1 spectra: {summary.num_ms1_spectra}")
            print(f"  MS2 spectra: {summary.num_ms2_spectra}\n")
            
            # Arrow Table (zero-copy)
            print("Arrow Table (zero-copy):")
            table = reader.to_arrow()
            print(f"  Rows: {table.num_rows}")
            print(f"  Columns: {len(table.schema)}")
            print(f"  Memory: ~{table.nbytes:,} bytes\n")
            
            # Pandas DataFrame
            try:
                import pandas as pd
                print("Pandas DataFrame analysis:")
                df = reader.to_pandas()
                
                # Show basic statistics
                print(f"  Shape: {df.shape}")
                print(f"  Mean m/z: {df['mz'].mean():.2f}")
                print(f"  Mean intensity: {df['intensity'].mean():.2f}")
                
                # Group by spectrum
                grouped = df.groupby('spectrum_id').agg({
                    'mz': ['min', 'max', 'count'],
                    'intensity': ['sum', 'mean']
                })
                print("\n  Per-spectrum summary:")
                print(grouped.to_string())
                print()
                
            except ImportError:
                print("⚠ pandas not available\n")
            
            # Polars DataFrame
            try:
                import polars as pl
                print("Polars DataFrame analysis:")
                df = reader.to_polars()
                
                # Show basic statistics
                print(f"  Shape: {df.shape}")
                print(f"  Mean m/z: {df['mz'].mean():.2f}")
                print(f"  Mean intensity: {df['intensity'].mean():.2f}")
                
                # Group by spectrum (using Polars syntax)
                grouped = df.group_by('spectrum_id').agg([
                    pl.col('mz').min().alias('min_mz'),
                    pl.col('mz').max().alias('max_mz'),
                    pl.col('mz').count().alias('num_peaks'),
                    pl.col('intensity').sum().alias('total_intensity'),
                    pl.col('intensity').mean().alias('mean_intensity'),
                ])
                print("\n  Per-spectrum summary:")
                print(grouped)
                print()
                
            except ImportError:
                print("⚠ polars not available\n")
        
        print("✅ Example complete!")


if __name__ == "__main__":
    main()
