"""Exercise installed code and bundled fixtures, never research paths."""
from contextlib import ExitStack, closing
from importlib.resources import as_file, files
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

import numpy as np
from numpy.testing import assert_allclose, assert_array_equal
from pandas.testing import assert_frame_equal
from mzpeak import MzPeakFile as OfficialFile
from mzpeak.util import Span

from mzpeak_compat import MzPeakFile
from mzpeak_compat.adapter import _official_zero_mask
from mzpeak_compat.example import compare
from mzpeak_compat.native_batch import library_path


class InstalledAdapter(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        data = files('mzpeak_compat') / 'examples'
        self.native_path = self.stack.enter_context(as_file(data / 'synthetic.integer.zip'))
        official_path = self.stack.enter_context(as_file(data / 'synthetic.official.mzpeak'))
        self.native = self.stack.enter_context(MzPeakFile(self.native_path))
        self.filtered = self.stack.enter_context(MzPeakFile(self.native_path, filter_profile_zeros=True))
        self.official = self.stack.enter_context(closing(OfficialFile(self.stack.enter_context(official_path.open("rb")))))

    def same(self, actual, expected):
        self.assertEqual(actual['index'], expected['index'])
        for key in ('m/z array', 'intensity array'):
            assert_allclose(actual[key], expected[key], rtol=1e-8 if key == 'm/z array' else .001, atol=0)
        assert_array_equal(actual['intensity array'] == 0, expected['intensity array'] == 0)

    def test_complete_example(self):
        result = compare()
        self.assertEqual(result['status'], 'passed')
        self.assertEqual((result['scans'],result['source_points'],result['retained_points'],result['source_zeros']), (7,25,17,17))
        self.assertTrue(library_path().is_file())
        self.assertIsInstance(self.native, OfficialFile)

    def test_metadata_and_access(self):
        assert_frame_equal(self.filtered.spectra, self.official.spectra)
        assert_frame_equal(self.filtered.scans, self.official.scans)
        for query in (0,2,3,4,6,'scan=2'):
            self.same(self.filtered[query], self.official[query])
        for query in ([6,0,2,0],slice(0,2),slice(0,6,2)):
            actual, expected = self.filtered[query], self.official[query]
            self.assertEqual(len(actual),len(expected))
            for a,b in zip(actual,expected): self.same(a,b)
        for time in (-1.,0.,.125,.25,1.5,2.):
            self.same(self.filtered.time[time],self.official.time[time])
        self.assertEqual([s['index'] for s in self.native],list(range(7)))
        self.same(self.filtered[2],self.official[2])
        for method in ('extract_tic','extract_bpc'):
            for a,b in zip(getattr(self.filtered,method)(),getattr(self.official,method)()):
                assert_array_equal(a,b)

    def test_batches_boundaries_and_zero_policy(self):
        for query in ([0,1], [1,0,1], slice(0,1), slice(0,1,2), slice(0,0)):
            actual = self.filtered.spectra_signal_for_indices(query)
            expected = self.official.spectra_signal_for_indices(query)
            assert_array_equal(actual['spectrum_index'],expected['spectrum_index'])
            assert_allclose(actual['m/z array'],expected['m/z array'],rtol=1e-8,atol=0)
            assert_allclose(actual['intensity array'],expected['intensity array'],rtol=.001,atol=0)
        self.assertEqual(len(self.native.spectra_signal_for_indices([])['m/z array']),0)
        for low,high in ((400.25,400.75),(np.nextafter(400.25,np.inf),400.75),(1.,2.)):
            full = self.native.spectra_signal_for_indices([0])
            actual = self.native.spectrum_data.read_data_for_range([0],Span(low,high))
            mask = (full['m/z array'] >= low) & (full['m/z array'] <= high)
            for key in full: assert_array_equal(actual[key],full[key][mask])
        ids,_,_ = self.native._signal.read_many([2,0,2])
        assert_array_equal(ids,np.repeat([2,0,2],[3,9,3]))
        for source,expected in [([],[]),([0],[]),([1],[0]),([0,0,0],[]),([0,0,1,0,0],[1,2,3])]:
            assert_array_equal(np.flatnonzero(_official_zero_mask(np.array(source))),expected)

    def test_integrity_directory_and_close(self):
        self.assertEqual(self.native.check_archive_integrity(),(True,[]))
        folder = self.stack.enter_context(tempfile.TemporaryDirectory())
        path = Path(folder)/'complete.experimental.zip'
        path.write_bytes(self.native_path.read_bytes())
        with MzPeakFile(folder) as run:
            self.same(run[0],self.native[0])
        run.close()
        with self.assertRaises(ValueError): run._signal.read_many([0])
        with self.assertRaises(IndexError): self.native._signal.read_many([-1])

    def test_invalid_index_rejected_and_checksum_detects_change(self):
        import struct
        import pyarrow as pa
        with zipfile.ZipFile(self.native_path) as archive:
            members = {name:archive.read(name) for name in archive.namelist()}
        folder = self.stack.enter_context(tempfile.TemporaryDirectory())
        path = Path(folder)/'broken.zip'
        def save():
            with zipfile.ZipFile(path,'w') as archive:
                for key,value in members.items(): archive.writestr(key,value)
        members['acquisition.json'] += b' '
        save()
        with MzPeakFile(path) as run:
            valid,failures = run.check_archive_integrity()
            self.assertFalse(valid)
            self.assertTrue(failures)
        data = json.dumps({'spectra':[{'index':1,'count':0,'fragments':[]}]}).encode()
        members['signal.index.zst'] = struct.pack('<Q',len(data))+bytes(pa.compress(data,codec='zstd'))
        save()
        with self.assertRaises(ValueError): MzPeakFile(path)
        path.unlink()  # Also checks that a failed open releases handles on Windows.

    def test_example_runs_offline_outside_checkout(self):
        code = '''
import socket
from unittest.mock import patch
with patch.object(socket.socket, 'connect', side_effect=RuntimeError('network disabled')), patch.object(socket, 'create_connection', side_effect=RuntimeError('network disabled')):
    from mzpeak_compat.example import main
    main()
'''
        with tempfile.TemporaryDirectory() as folder:
            result = subprocess.run([sys.executable,'-I','-c',code],cwd=folder,capture_output=True,text=True,check=True)
        self.assertEqual(json.loads(result.stdout)['status'],'passed')


if __name__ == '__main__':
    unittest.main()
