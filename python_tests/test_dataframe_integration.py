"""
Test pandas and polars DataFrame integration with zero-copy Arrow.
"""

import unittest
from pathlib import Path
import tempfile


class TestDataFrameIntegration(unittest.TestCase):
    """Test pandas and polars integration for mzPeak."""

    def setUp(self):
        """Create test data."""
        import mzpeak

        self.temp_dir = tempfile.TemporaryDirectory()
        self.out_path = Path(self.temp_dir.name) / "test.parquet"

        # Create test spectra with multiple peaks
        spectra = [
            mzpeak.SpectrumBuilder(1, 1)
            .ms_level(1)
            .retention_time(10.5)
            .polarity(1)
            .add_peak(100.5, 1000.0)
            .add_peak(200.3, 2000.0)
            .add_peak(300.7, 3000.0)
            .build(),
            mzpeak.SpectrumBuilder(2, 2)
            .ms_level(2)
            .retention_time(15.2)
            .polarity(1)
            .precursor(500.0, 2, 5000.0)
            .add_peak(150.2, 1500.0)
            .add_peak(250.8, 2500.0)
            .build(),
            mzpeak.SpectrumBuilder(3, 3)
            .ms_level(1)
            .retention_time(20.0)
            .polarity(-1)
            .add_peak(180.1, 1800.0)
            .build(),
        ]

        with mzpeak.MzPeakWriter(str(self.out_path)) as writer:
            writer.write_spectra(spectra)

    def tearDown(self):
        """Clean up."""
        self.temp_dir.cleanup()

    def test_to_pandas(self):
        """Test conversion to pandas DataFrame."""
        import mzpeak

        try:
            import pandas as pd
        except ImportError:  # pragma: no cover
            self.skipTest("pandas not available")

        with mzpeak.MzPeakReader(str(self.out_path)) as reader:
            df = reader.to_pandas()

            # Verify it's a pandas DataFrame
            self.assertIsInstance(df, pd.DataFrame)

            # Verify row count (total peaks)
            self.assertEqual(len(df), 6)

            # Verify column presence
            self.assertIn("spectrum_id", df.columns)
            self.assertIn("mz", df.columns)
            self.assertIn("intensity", df.columns)
            self.assertIn("retention_time", df.columns)
            self.assertIn("ms_level", df.columns)

            # Verify data integrity
            self.assertEqual(df["spectrum_id"].unique().tolist(), [1, 2, 3])
            self.assertEqual(df[df["spectrum_id"] == 1]["ms_level"].iloc[0], 1)
            self.assertEqual(df[df["spectrum_id"] == 2]["ms_level"].iloc[0], 2)

            # Verify retention times
            rt_1 = df[df["spectrum_id"] == 1]["retention_time"].iloc[0]
            self.assertAlmostEqual(rt_1, 10.5, places=1)

            # Verify m/z and intensity values
            spec1_data = df[df["spectrum_id"] == 1]
            self.assertEqual(len(spec1_data), 3)
            self.assertAlmostEqual(spec1_data["mz"].min(), 100.5, places=1)
            self.assertAlmostEqual(spec1_data["mz"].max(), 300.7, places=1)

    def test_to_polars(self):
        """Test conversion to polars DataFrame."""
        import mzpeak

        try:
            import polars as pl
        except ImportError:  # pragma: no cover
            self.skipTest("polars not available")

        with mzpeak.MzPeakReader(str(self.out_path)) as reader:
            df = reader.to_polars()

            # Verify it's a polars DataFrame
            self.assertIsInstance(df, pl.DataFrame)

            # Verify row count (total peaks)
            self.assertEqual(len(df), 6)

            # Verify column presence
            self.assertIn("spectrum_id", df.columns)
            self.assertIn("mz", df.columns)
            self.assertIn("intensity", df.columns)
            self.assertIn("retention_time", df.columns)
            self.assertIn("ms_level", df.columns)

            # Verify data integrity
            spectrum_ids = df["spectrum_id"].unique().to_list()
            self.assertEqual(sorted(spectrum_ids), [1, 2, 3])

            # Verify MS levels
            spec1 = df.filter(pl.col("spectrum_id") == 1)
            spec2 = df.filter(pl.col("spectrum_id") == 2)
            self.assertEqual(spec1["ms_level"][0], 1)
            self.assertEqual(spec2["ms_level"][0], 2)

            # Verify retention times
            self.assertAlmostEqual(spec1["retention_time"][0], 10.5, places=1)

    def test_to_arrow(self):
        """Test conversion to Arrow Table."""
        import mzpeak

        try:
            import pyarrow as pa
        except ImportError:  # pragma: no cover
            self.skipTest("pyarrow not available")

        with mzpeak.MzPeakReader(str(self.out_path)) as reader:
            table = reader.to_arrow()

            # Verify it's an Arrow Table
            self.assertIsInstance(table, pa.Table)

            # Verify row count
            self.assertEqual(table.num_rows, 6)

            # Verify schema
            schema = table.schema
            self.assertIsNotNone(schema.field("spectrum_id"))
            self.assertIsNotNone(schema.field("mz"))
            self.assertIsNotNone(schema.field("intensity"))
            self.assertIsNotNone(schema.field("retention_time"))

            # Verify we can convert Arrow -> pandas
            df = table.to_pandas()
            self.assertEqual(len(df), 6)

    def test_zero_copy_arrow_memory(self):
        """Test that Arrow conversion is zero-copy (memory efficient)."""
        import mzpeak

        try:
            import pyarrow as pa
        except ImportError:  # pragma: no cover
            self.skipTest("pyarrow not available")

        with mzpeak.MzPeakReader(str(self.out_path)) as reader:
            table = reader.to_arrow()

            # Verify the table uses Arrow memory (not copied)
            self.assertIsInstance(table, pa.Table)
            self.assertGreater(table.num_rows, 0)

            # Check that schema is preserved
            self.assertIn("mz", table.schema.names)
            self.assertIn("intensity", table.schema.names)

    def test_pandas_filtering_and_grouping(self):
        """Test pandas operations on converted data."""
        import mzpeak

        try:
            import pandas as pd
        except ImportError:  # pragma: no cover
            self.skipTest("pandas not available")

        with mzpeak.MzPeakReader(str(self.out_path)) as reader:
            df = reader.to_pandas()

            # Filter by MS level
            ms1_df = df[df["ms_level"] == 1]
            self.assertEqual(len(ms1_df), 4)  # 3 peaks from spec 1, 1 from spec 3

            ms2_df = df[df["ms_level"] == 2]
            self.assertEqual(len(ms2_df), 2)  # 2 peaks from spec 2

            # Group by spectrum
            grouped = df.groupby("spectrum_id").agg(
                {"intensity": "sum", "mz": "count"}
            )
            self.assertEqual(len(grouped), 3)  # 3 spectra

            # Verify spectrum 1 has 3 peaks
            self.assertEqual(grouped.loc[1, "mz"], 3)

    def test_polars_filtering_and_grouping(self):
        """Test polars operations on converted data."""
        import mzpeak

        try:
            import polars as pl
        except ImportError:  # pragma: no cover
            self.skipTest("polars not available")

        with mzpeak.MzPeakReader(str(self.out_path)) as reader:
            df = reader.to_polars()

            # Filter by MS level
            ms1_df = df.filter(pl.col("ms_level") == 1)
            self.assertEqual(len(ms1_df), 4)

            ms2_df = df.filter(pl.col("ms_level") == 2)
            self.assertEqual(len(ms2_df), 2)

            # Group by spectrum
            grouped = df.group_by("spectrum_id").agg(
                [
                    pl.col("intensity").sum().alias("total_intensity"),
                    pl.col("mz").count().alias("peak_count"),
                ]
            )
            self.assertEqual(len(grouped), 3)

            # Verify spectrum 1 has 3 peaks
            spec1_row = grouped.filter(pl.col("spectrum_id") == 1)
            self.assertEqual(spec1_row["peak_count"][0], 3)


if __name__ == "__main__":
    unittest.main()
