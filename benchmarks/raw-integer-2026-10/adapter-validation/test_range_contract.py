"""Small adapter regressions against the pinned official Python reader.

Requires the research checkout's decoder modules on PYTHONPATH; no RAW fixture.
"""
from pathlib import Path
import unittest
import numpy as np
from numpy.testing import assert_array_equal
from mzpeak_compat import _Arrays, _official_zero_mask


class ZeroPolicy(unittest.TestCase):
    def test_zero_runs_and_boundaries(self):
        for source, expected in [([], []), ([0], []), ([1], [0]), ([0, 0, 0], []),
                                 ([0, 0, 1, 0, 0, 0, 2, 0, 0], [1, 2, 3, 5, 6, 7]),
                                 ([0, 1, 0], [0, 1, 2])]:
            assert_array_equal(np.flatnonzero(_official_zero_mask(np.array(source))), expected)


class ArrayRangeContract(unittest.TestCase):
    def test_array_slice_includes_stop_like_pinned_official_reader(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        source = SimpleNamespace(directory=[{'representation': 'profile'} for _ in range(4)],
            read=lambda i: (np.array([100.+i]), np.array([10.+i])),
            read_many=lambda ids: (np.array(ids, dtype=np.uint64), 100.+np.array(ids), 10.+np.array(ids)))
        arrays = _Arrays(source, 'profile')
        import pyarrow as pa
        from mzpeak.mz_reader import MzPeakArrayDataReader
        # Exercise the pinned reader's real range filter with one in-memory row group.
        points = pa.StructArray.from_arrays([pa.array(range(4),type=pa.uint64()),
            pa.array([100.+i for i in range(4)]),pa.array([10.+i for i in range(4)])],
            names=['spectrum_index','m/z array','intensity array'])
        official = object.__new__(MzPeakArrayDataReader)
        official._namespace = 'spectrum'
        official.n_entries = 4
        official._delta_model_series = None
        official._point_index = SimpleNamespace(row_groups_for_spectrum_range=lambda q:[0])
        official.handle = SimpleNamespace(read_row_groups=lambda *a,**kw:pa.table({'point':points}))
        official._clean_point_batch = lambda batch,**kw:{k:batch[k].to_numpy() for k in batch.schema.names}
        # Test both dispatch branches; no real archive or native library is required.
        for library in (Path(__file__), Path('/nonexistent/mzpeak-decoder')):
            with patch('native_batch.library_path', return_value=library):
                for query, expected in [(slice(1,2), [1,2]), (slice(1,2,2), [1,2]),
                                        (slice(0,4), [0,1,2,3]), (slice(2,None), [2,3]),
                                        (slice(0,0), [0,1,2,3]), (slice(-2,2), [0,1,2]),
                                        (slice(4,6), []), (slice(2,1), []), ([2,0,2], [0,2])]:
                    actual = arrays.read_data_for_range(query)
                    reference = official.read_data_for_range(query)
                    assert_array_equal(reference['spectrum_index'],expected)
                    for key in actual: assert_array_equal(actual[key],reference[key])


if __name__ == '__main__':
    unittest.main()
