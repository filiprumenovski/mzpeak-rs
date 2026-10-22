"""Official mzpeak Python interface over experimental integer archives.

Use ``from mzpeak_compat import MzPeakFile`` in place of the official import.
Metadata parsing and public reader methods come from the pinned official reader;
only signal storage is replaced. All stored points (including zeros) are exposed.
Values keep the archive's declared quantization bounds. This does not make the
archive an official mzPeak file or restore information lost to quantization.
"""
import hashlib
import io
import json
from pathlib import Path
import zipfile

import numpy as np
import pandas as pd
from mzpeak import MzPeakFile as OfficialMzPeakFile
from mzpeak.file_index import FileEntry
from mzpeak.util import _SeekableMixin
from .reader import Reader


def _official_zero_mask(intensity):
    """Keep nonzero samples and their immediate zero neighbours."""
    nonzero = intensity != 0
    keep = nonzero.copy()
    keep[1:] |= nonzero[:-1]
    keep[:-1] |= nonzero[1:]
    return keep


class _MetadataArchive(zipfile.ZipFile):
    """Virtual official index; acquisition members stay in the original ZIP."""
    def __init__(self, path):
        super().__init__(path)
        try:
            manifest = json.loads(super().read('manifest.json'))
            if manifest['format'] != 'experimental-mzpeak-schema-lab-1':
                raise ValueError('Unsupported experimental archive format')
            acquisition = json.loads(super().read(manifest['acquisition']))
            self.index_bytes = json.dumps({'metadata': acquisition['metadata'],
                                          'files': acquisition['tables']}).encode()
            self.names = ['mzpeak_index.json'] + [v['name'] for v in acquisition['tables']]
            self.bounds = manifest['bounds']
            self.manifest = manifest
        except BaseException:
            self.close()
            raise

    def getinfo(self, name):
        if name == 'mzpeak_index.json':
            info = zipfile.ZipInfo(name)
            info.file_size = len(self.index_bytes)
            return info
        return super().getinfo(name if name in self.NameToInfo else 'acquisition/' + name)

    def open(self, name, mode='r', pwd=None, *, force_zip64=False):
        key = name.filename if isinstance(name, zipfile.ZipInfo) else name
        if key == 'mzpeak_index.json':
            return io.BytesIO(self.index_bytes)
        return super().open(self.getinfo(key), mode, pwd, force_zip64=force_zip64)

    def namelist(self):
        return self.names


class _ArrayIter(_SeekableMixin):
    def __init__(self, arrays, index=0):
        self.arrays = arrays
        self.current_index = index
        self._cached = None

    def __iter__(self):
        return self

    def peek(self):
        while self.current_index < len(self.arrays):
            if self.arrays.matches(self.current_index):
                if self._cached is None:
                    self._cached = self.current_index, self.arrays[self.current_index]
                return self._cached
            self.current_index += 1
        return None

    def __next__(self):
        value = self.peek()
        if value is None:
            raise StopIteration
        self.current_index += 1
        self._cached = None
        return value

    def _skip_ahead(self, index):
        self.current_index = index + 1
        self._cached = None
        return True

    def seek(self, index):
        if index != self.current_index:
            self._cached = None
        self.current_index = index
        return self.peek() is not None

    def empty_arrays(self):
        return self.arrays._empty_array_map()


class _Arrays:
    def __init__(self, reader, representation, filter_profile_zeros=False):
        self.reader = reader
        self.representation = representation
        self.filter_profile_zeros = filter_profile_zeros

    def __len__(self):
        return len(self.reader.directory)

    def matches(self, index):
        return self.reader.directory[index]['representation'] == self.representation

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.read_data_for_range(index)
        if not self.matches(index):
            return {}
        mz, intensity = self.reader.read(index)
        if self.filter_profile_zeros and self.representation == 'profile':
            keep = _official_zero_mask(intensity)
            mz, intensity = mz[keep], intensity[keep]
        return {'m/z array': mz, 'intensity array': intensity}

    def _data_iterator(self, index=0):
        return _ArrayIter(self, index)

    def _empty_array_map(self):
        return {'m/z array': np.empty(0, dtype=np.float64),
                'intensity array': np.empty(0, dtype=np.float64)}

    def read_data_for_range(self, indices, coordinate_range=None):
        if isinstance(indices, slice):
            # Pinned official array ranges include stop and ignore step.
            # This differs from spectrum-object slicing through run[start:stop].
            start = max(indices.start or 0, 0)
            stop = min(indices.stop or len(self), len(self) - 1)
            indices = range(start, stop + 1)
        else:
            indices = sorted(set(indices))
        if not (self.filter_profile_zeros and self.representation == 'profile'):
            selected = [i for i in indices if self.matches(i)]
            index, mz, intensity = self.reader.read_many(selected)
            if coordinate_range is not None:
                keep = (mz >= coordinate_range.start) & (mz <= coordinate_range.end)
                index, mz, intensity = index[keep], mz[keep], intensity[keep]
            return {'spectrum_index': index, 'm/z array': mz, 'intensity array': intensity}
        parts = {'spectrum_index': [], 'm/z array': [], 'intensity array': []}
        for index in indices:
            arrays = self[index]
            if not arrays:
                continue
            if coordinate_range is not None:
                keep = (arrays['m/z array'] >= coordinate_range.start) & (arrays['m/z array'] <= coordinate_range.end)
                arrays = {key: value[keep] for key, value in arrays.items()}
            parts['spectrum_index'].append(np.full(len(arrays['m/z array']), index, dtype=np.uint64))
            for key, value in arrays.items():
                parts[key].append(value)
        empty = {'spectrum_index': np.empty(0, dtype=np.uint64), **self._empty_array_map()}
        return {key: np.concatenate(value) if value else empty[key] for key, value in parts.items()}

    def close(self):
        pass  # The owning MzPeakFile closes the shared decoder once.

    @property
    def closed(self):
        return self.reader.file.closed


class MzPeakFile(OfficialMzPeakFile):
    """Same reader interface, with the experimental archive's full stored signal.

    Accepts a standalone experimental ZIP or its output directory. The official
    Python package (pinned revision 40c6552) and PyArrow >=20 must be installed.
    ``quantization_bounds`` discloses the stored precision contract.
    """
    def __init__(self, path, flatten_columns=False, *, filter_profile_zeros=False):
        path = Path(path)
        archive = path / 'complete.experimental.zip' if path.is_dir() else path
        self._signal = Reader(archive)
        self._closed = False
        metadata = None
        try:
            metadata = _MetadataArchive(archive)
            super().__init__(metadata, flatten_columns=flatten_columns)
            if len(self.spectrum_metadata) != len(self._signal.directory):
                raise ValueError('Signal/metadata spectrum count differs')
            for index, (record, spectrum_id) in enumerate(zip(self._signal.directory, self.spectra['id'])):
                if record['index'] != index or record['id'] != spectrum_id:
                    raise ValueError(f'Signal/metadata spectrum identity differs at {index}')
                if record['representation'] not in ('profile', 'centroid'):
                    raise ValueError('Unsupported signal representation')
            self.quantization_bounds = metadata.bounds
            self.filter_profile_zeros = filter_profile_zeros
            self.spectrum_data = _Arrays(self._signal, 'profile', filter_profile_zeros)
            self.spectrum_peak_data = (_Arrays(self._signal, 'centroid') if
                any(r['representation'] == 'centroid' for r in self._signal.directory) else None)
            if filter_profile_zeros:
                # Counts must match reads; only this opt-in mode scans profiles at open.
                column = self.spectrum_metadata._data_point_count_col
                counts = self.spectra[column].tolist()
                for index, record in enumerate(self._signal.directory):
                    if record['representation'] == 'profile':
                        counts[index] = len(self.spectrum_data[index]['m/z array'])
                self.spectra[column] = pd.array(counts, dtype=self.spectra[column].dtype)
        except BaseException:
            if metadata is not None:
                metadata.close()
            self._signal.close()
            raise

    def close(self):
        if self._closed:
            return
        super().close()
        self._signal.close()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def check_archive_integrity(self):
        """Check native SHA-256 entries, including signal, without extracting.

        Returns the official ``(valid, failures)`` shape; failure entries describe
        native members and their SHA-256, rather than official Parquet SHA-512.
        """
        hashes = self._archive.manifest.get('sha256', {})
        required = set(self._archive.NameToInfo) - {'manifest.json'}
        if not required.issubset(hashes):
            return None, []
        failed = []
        for name, expected in hashes.items():
            with self._archive.open(name) as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if actual != expected:
                entry = FileEntry.from_json(dict(name=name, entity_type='other',
                    data_kind='proprietary', checksum=expected))
                failed.append((entry, actual))
        return not failed, failed
