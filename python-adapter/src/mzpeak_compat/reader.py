"""Read integer signal blocks in place using the packaged native decoder."""
import mmap
from pathlib import Path
import struct
import zipfile

import pyarrow as pa

from .native_batch import NativeBatch


class Reader:
    def __init__(self, archive):
        archive = Path(archive)
        self.file = self.mm = self._native_batch = None
        try:
            with zipfile.ZipFile(archive) as source:
                raw = source.read('signal.index.zst')
                if len(raw) < 8:
                    raise ValueError('Truncated signal index')
                index_bytes = pa.decompress(raw[8:], decompressed_size=struct.unpack('<Q', raw[:8])[0], codec='zstd')
                member = source.getinfo('signal.bin')
                if member.compress_type != zipfile.ZIP_STORED or member.flag_bits & 1:
                    raise ValueError('Signal must be stored unencrypted for direct access')
            self._native_batch = NativeBatch(self, index_bytes)
            self.directory = self._native_batch.directory()
            self.file = archive.open('rb')
            self.file.seek(member.header_offset)
            header = self.file.read(30)
            if len(header) != 30 or header[:4] != b'PK\x03\x04':
                raise ValueError('Invalid signal ZIP header')
            name_length, extra_length = struct.unpack_from('<HH', header, 26)
            self.signal_offset = member.header_offset + 30 + name_length + extra_length
            self.signal_size = member.file_size
            if self.signal_offset + self.signal_size > archive.stat().st_size:
                raise ValueError('Truncated signal archive')
            self.mm = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)
        except BaseException:
            self.close()
            raise

    def close(self):
        if self._native_batch is not None:
            self._native_batch.close()
        if self.mm is not None:
            self.mm.close()
        if self.file is not None:
            self.file.close()

    def read_many(self, indices):
        """Return IDs and float64 arrays in caller order, including duplicates."""
        return self._native_batch.read(indices)

    def read(self, index):
        _, mz, intensity = self.read_many([index])
        return mz, intensity
