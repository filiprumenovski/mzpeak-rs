import unittest
from pathlib import Path
import tempfile
import os


ROOT = Path(__file__).resolve().parents[1]
DATA_MZML = ROOT / "data" / "A4_El_etdOT.mzML"


class TestMzPeakSmoke(unittest.TestCase):
    def test_import(self) -> None:
        import mzpeak  # noqa: F401

    def test_write_read_roundtrip_and_arrow(self) -> None:
        import mzpeak

        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "out.parquet"

            spectra = [
                mzpeak.SpectrumBuilder(1, 1)
                .ms_level(1)
                .retention_time(10.0)
                .polarity(1)
                .add_peak(100.0, 10.0)
                .add_peak(200.0, 20.0)
                .build(),
                mzpeak.SpectrumBuilder(2, 2)
                .ms_level(2)
                .retention_time(11.0)
                .polarity(1)
                .precursor(500.0, 2, 123.0)
                .add_peak(150.0, 15.0)
                .build(),
            ]

            with mzpeak.MzPeakWriter(str(out_path)) as writer:
                writer.write_spectra(spectra)

            with mzpeak.MzPeakReader(str(out_path)) as reader:
                summary = reader.summary()
                self.assertEqual(summary.num_spectra, 2)
                self.assertEqual(summary.total_peaks, 3)

                try:
                    import pyarrow as pa  # type: ignore
                except Exception as e:  # pragma: no cover
                    self.skipTest(f"pyarrow not available: {e}")

                table = reader.to_arrow()
                self.assertTrue(isinstance(table, pa.Table))
                self.assertEqual(table.num_rows, 3)

                chroms = reader.read_chromatograms()
                mobs = reader.read_mobilograms()
                self.assertIsInstance(chroms, list)
                self.assertIsInstance(mobs, list)

    def test_write_read_arrays_roundtrip(self) -> None:
        import mzpeak

        try:
            import numpy as np
        except Exception as e:  # pragma: no cover
            self.skipTest(f"numpy not available: {e}")

        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "out_arrays.parquet"

            mz1 = np.array([100.0, 200.0], dtype=np.float64)
            intensity1 = np.array([10.0, 20.0], dtype=np.float32)
            spectrum1 = mzpeak.SpectrumArrays(
                1,
                1,
                1,
                10.0,
                1,
                mz1,
                intensity1,
            )

            mz2 = np.array([150.0, 250.0, 350.0], dtype=np.float64)
            intensity2 = np.array([15.0, 25.0, 35.0], dtype=np.float32)
            ion_mobility2 = np.array([1.1, 1.2, 1.3], dtype=np.float64)
            ion_mobility_validity2 = np.array([True, False, True], dtype=bool)
            spectrum2 = mzpeak.SpectrumArrays(
                2,
                2,
                2,
                11.0,
                1,
                mz2,
                intensity2,
                ion_mobility2,
                ion_mobility_validity2,
            )

            with mzpeak.MzPeakWriter(str(out_path)) as writer:
                writer.write_spectrum_arrays(spectrum1)
                writer.write_spectra_arrays([spectrum2])

            with mzpeak.MzPeakReader(str(out_path)) as reader:
                spectra = reader.all_spectra_arrays()
                self.assertEqual(len(spectra), 2)
                self.assertEqual(spectra[0].num_peaks, 2)
                self.assertEqual(spectra[1].num_peaks, 3)

                np.testing.assert_allclose(spectra[0].mz_array, mz1)
                np.testing.assert_allclose(spectra[0].intensity_array, intensity1)

                ion_mobility = spectra[1].ion_mobility_array
                self.assertIsInstance(ion_mobility, tuple)
                values, validity = ion_mobility
                np.testing.assert_allclose(values, ion_mobility2)
                np.testing.assert_array_equal(validity, ion_mobility_validity2)

                views = list(reader.iter_spectra_arrays_views())
                self.assertEqual(len(views), 2)
                mz_view = views[0].mz_array_view
                intensity_view = views[0].intensity_array_view
                self.assertFalse(mz_view.flags["OWNDATA"])
                self.assertFalse(intensity_view.flags["OWNDATA"])

    def test_convert_open_summary_and_arrow(self) -> None:
        import mzpeak

        if os.environ.get("MZPEAK_RUN_SLOW") != "1":
            self.skipTest("Set MZPEAK_RUN_SLOW=1 to run mzML conversion test")

        if not DATA_MZML.exists():
            self.skipTest(f"Missing test data: {DATA_MZML}")

        with tempfile.TemporaryDirectory() as tmp:
            out_path = Path(tmp) / "out.mzpeak"

            stats = mzpeak.convert(str(DATA_MZML), str(out_path))
            self.assertGreater(stats.spectra_count, 0)
            self.assertGreater(stats.peak_count, 0)

            with mzpeak.MzPeakReader(str(out_path)) as reader:
                summary = reader.summary()
                self.assertGreater(summary.num_spectra, 0)
                self.assertGreater(summary.total_peaks, 0)

                # Arrow smoke check (requires pyarrow)
                try:
                    import pyarrow as pa  # type: ignore
                except Exception as e:  # pragma: no cover
                    self.skipTest(f"pyarrow not available: {e}")

                table = reader.to_arrow()
                self.assertTrue(isinstance(table, pa.Table))
                self.assertGreater(table.num_rows, 0)

                # Optional: these should return lists (possibly empty)
                chroms = reader.read_chromatograms()
                mobs = reader.read_mobilograms()
                self.assertIsInstance(chroms, list)
                self.assertIsInstance(mobs, list)


if __name__ == "__main__":
    unittest.main()
