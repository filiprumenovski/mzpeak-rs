"""Checks for the signal packaging stage that overlaps metadata extraction."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

from convert import prepare_bundle, stream_signal


class PackagingTests(unittest.TestCase):
    def test_direct_stream_and_failures(self):
        producer = """
import json, sys
from pathlib import Path
folder = Path(sys.argv[1]); folder.mkdir()
data = b'encoded-signal' * 200001
sys.stdout.buffer.write(data)
(folder / 'encode-results.json').write_text(json.dumps({
    'spectra': 1, 'points': 2, 'signal_bytes': len(data) + int(sys.argv[2])}))
sys.exit(int(sys.argv[3]))
"""
        for extra, exit_code in [(0, 0), (1, 0), (0, 7)]:
            with self.subTest(extra=extra, exit_code=exit_code), tempfile.TemporaryDirectory() as temporary:
                folder = Path(temporary) / 'output'
                command = [sys.executable, '-c', producer, folder, str(extra), str(exit_code)]
                if extra or exit_code:
                    with self.assertRaises(ValueError if extra else subprocess.CalledProcessError):
                        stream_signal(command, folder, os.environ)
                    with zipfile.ZipFile(folder / 'complete.experimental.zip') as archive:
                        self.assertNotIn('manifest.json', archive.namelist())
                    continue
                _, digest = stream_signal(command, folder, os.environ)
                self.assertFalse((folder / 'signal.bin').exists())
                (folder / 'source-summaries.jsonl').write_text('{"count": 2}\n')
                (folder / 'signal.index.zst').write_bytes(b'index')
                _, _, hashes = prepare_bundle(folder, digest)
                with zipfile.ZipFile(folder / 'complete.experimental.zip') as archive:
                    self.assertIsNone(archive.testzip())
                    self.assertEqual(archive.read('signal.bin'), b'encoded-signal' * 200001)
                    self.assertEqual(hashes['signal.bin'], hashlib.sha256(archive.read('signal.bin')).hexdigest())
                    self.assertEqual(archive.read('signal.index.zst'), b'index')

    def test_streamed_payloads_and_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            rows = [{'count': 2}]
            report = {'spectra': 1, 'points': 2}
            (folder / 'source-summaries.jsonl').write_text(json.dumps(rows[0]) + '\n')
            (folder / 'encode-results.json').write_text(json.dumps(report))
            payloads = {'signal.bin': b'abc' * 800_001, 'signal.index.zst': b'index'}
            for name, data in payloads.items():
                (folder / name).write_bytes(data)
            actual_rows, actual_report, hashes = prepare_bundle(folder)
            self.assertEqual((actual_rows, actual_report), (rows, report))
            with zipfile.ZipFile(folder / 'complete.experimental.zip') as archive:
                self.assertEqual(set(archive.namelist()), set(payloads))
                self.assertIsNone(archive.testzip())
                for name, data in payloads.items():
                    self.assertEqual(archive.read(name), data)
                    self.assertEqual(hashes[name], hashlib.sha256(data).hexdigest())

    def test_incomplete_summaries_fail_before_creating_archive(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            (folder / 'source-summaries.jsonl').write_text('{"count": 2}\n')
            for report in ({'spectra': 2, 'points': 2}, {'spectra': 1, 'points': 3}):
                (folder / 'encode-results.json').write_text(json.dumps(report))
                with self.assertRaisesRegex(ValueError, 'Incomplete source summaries'):
                    prepare_bundle(folder)
                self.assertFalse((folder / 'complete.experimental.zip').exists())


if __name__ == '__main__':
    unittest.main()
