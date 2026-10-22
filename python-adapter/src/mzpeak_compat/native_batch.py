"""ctypes bridge: one native call per batch, no retained decoded arrays."""
import ctypes as C
import json
from pathlib import Path
import sysconfig
import threading
from collections.abc import Sequence

import numpy as np

def library_path():
    return Path(__file__).with_name("_decoder" + sysconfig.get_config_var("EXT_SUFFIX"))

class ScanDirectory(Sequence):
    """Compact scan columns; create dictionaries only for callers that request them."""
    def __init__(self, data):
        self.ids = data['id']
        self.counts = np.asarray(data['count'], dtype=np.uint64)
        self.representation = np.asarray(data['representation'], dtype=np.uint8)
        self.levels = np.asarray(data['ms_level'], dtype=np.uint32)
        self.times = np.asarray(data['time'], dtype=np.float64)
        if any(len(a) != len(self.ids) for a in (self.counts, self.representation, self.levels, self.times)):
            raise ValueError('Inconsistent scan columns')

    def __len__(self):
        return len(self.ids)

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [self[j] for j in range(*i.indices(len(self)))]
        if i < 0:
            i += len(self)
        if not 0 <= i < len(self):
            raise IndexError(i)
        return {'index': i, 'count': int(self.counts[i]), 'id': self.ids[i],
                'representation': ('', 'profile', 'centroid')[self.representation[i]],
                'ms_level': int(self.levels[i]), 'retention_time_minutes': float(self.times[i])}

class NativeBatch:
    def __init__(self, reader, index_bytes=None):
        self.reader = reader
        self.lock = threading.Lock()
        self.handle = None
        self.lib = C.CDLL(str(library_path()))
        ptr = C.c_void_p
        size = C.c_size_t
        self.lib.mzlab_decoder_new.argtypes = [ptr, size, ptr, size]
        self.lib.mzlab_decoder_new.restype = ptr
        self.lib.mzlab_decoder_free.argtypes = [ptr]
        self.lib.mzlab_decoder_free.restype = None
        self.lib.mzlab_decode.argtypes = [ptr, ptr, size, ptr, size, ptr, ptr, ptr, size, ptr, size]
        self.lib.mzlab_decode.restype = C.c_int
        self.lib.mzlab_decoder_metadata.argtypes = [ptr, C.POINTER(size)]
        self.lib.mzlab_decoder_metadata.restype = ptr
        data = index_bytes if index_bytes is not None else json.dumps({'spectra': reader.directory}, separators=(',', ':')).encode()
        err = C.create_string_buffer(512)
        self.handle = self.lib.mzlab_decoder_new(data.address if hasattr(data, 'address') else data, len(data), err, len(err))
        if not self.handle:
            raise ValueError(err.value.decode())

    def __del__(self):
        if getattr(self, 'handle', None):
            self.close()

    def directory(self):
        size = C.c_size_t()
        ptr = self.lib.mzlab_decoder_metadata(self.handle, C.byref(size))
        data = json.loads(C.string_at(ptr, size.value))
        return ScanDirectory(data) if isinstance(data, dict) else data

    def close(self):
        with self.lock:
            if self.handle:
                self.lib.mzlab_decoder_free(self.handle)
                self.handle = None

    def read(self, indices):
        # Preserve caller order and duplicates; the compatibility API sorts separately.
        ids = list(indices)
        if any(not isinstance(i, (int, np.integer)) or i < 0 or i >= len(self.reader.directory) for i in ids):
            raise IndexError('Spectrum index out of range')
        ids = np.asarray(ids, dtype=np.uint64)
        directory = self.reader.directory
        count = int(directory.counts[ids].sum()) if isinstance(directory, ScanDirectory) else sum(directory[int(i)]['count'] for i in ids)
        index = np.empty(count, dtype=np.uint64)
        mz = np.empty(count, dtype=np.float64)
        intensity = np.empty(count, dtype=np.float64)
        err = C.create_string_buffer(512)
        with self.lock:
            if not self.handle or self.reader.file.closed:
                raise ValueError('Reader is closed')
            signal = np.frombuffer(self.reader.mm, dtype=np.uint8, count=self.reader.signal_size, offset=self.reader.signal_offset)
            status = self.lib.mzlab_decode(self.handle, signal.ctypes.data, signal.size,
                ids.ctypes.data, ids.size, index.ctypes.data, mz.ctypes.data, intensity.ctypes.data,
                count, err, len(err))
            del signal
        if status:
            raise ValueError(err.value.decode())
        return index, mz, intensity
