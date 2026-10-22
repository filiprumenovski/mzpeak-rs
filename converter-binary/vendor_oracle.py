"""Read Thermo RAW directly via vendor API and an in-memory binary pipe, no mzML."""
import json
import os
from pathlib import Path
import struct
import subprocess
import numpy as np

DLL = Path(os.environ['MZPEAK_VALIDATION_READER_DLL'])


def read_exact(stream, count):
    parts=[]
    while count:
        data=stream.read(count)
        if not data:
            raise EOFError('Truncated direct RAW reader output')
        parts.append(data);count-=len(data)
    return b''.join(parts)


def read_json(stream):
    n=struct.unpack('<I',read_exact(stream,4))[0]
    if n>16*1024*1024:
        raise ValueError('Invalid RAW metadata frame length')
    return json.loads(read_exact(stream,n))


def spectra(path):
    with subprocess.Popen(['dotnet',str(DLL),str(path)],stdout=subprocess.PIPE,stderr=subprocess.PIPE) as proc:
        try:
            assert read_exact(proc.stdout,8)==b'MZRAW001'
            header=read_json(proc.stdout)
            for index in range(header['spectra']):
                record=read_json(proc.stdout)
                assert record['index']==index and 0<=record['count']<=4_000_000
                n=record['count']
                mz=np.frombuffer(read_exact(proc.stdout,n*8),dtype='<f8')
                intensity=np.frombuffer(read_exact(proc.stdout,n*8),dtype='<f8')
                yield record,mz,intensity
            assert proc.stdout.read(1)==b'', 'Unexpected trailing RAW data'
            error=proc.stderr.read().decode(errors='replace')
            if proc.wait()!=0:
                raise RuntimeError(error)
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait()
