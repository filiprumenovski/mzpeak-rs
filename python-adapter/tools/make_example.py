"""Generate small, deterministic synthetic archives; no RAW or lab modules."""
import hashlib
import io
import json
from pathlib import Path
import struct
import zipfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

OUT = Path(__file__).resolve().parents[1] / 'src/mzpeak_compat/examples'


def packed(values):
    bits = max((int(x).bit_length() for x in values), default=0)
    data = bytearray((len(values) * bits + 7) // 8)
    for i, value in enumerate(values):
        for bit in range(bits):
            data[(i * bits + bit) // 8] |= ((int(value) >> bit) & 1) << ((i * bits + bit) % 8)
    return bytes(data), bits


def shuffled(values):
    return np.asarray(values, dtype='<u4').view('u1').reshape(-1, 4).T.tobytes()


def encode(q, kind):
    codec = {'kind': kind}
    if kind == 'baseline':
        raw = shuffled(q)
        codec['mode'] = 'shuffle'
    elif kind == 'delta_shuffle':
        raw = shuffled(np.diff(q))
        codec.update(mode='shuffle', first=int(q[0]), base=0)
    elif kind == 'packed':
        raw, codec['bits'] = packed(q)
    else:
        positions = np.flatnonzero(q)
        gaps = np.diff(np.r_[-1, positions]) - 1
        prefix, gap_bits = packed(gaps)
        if kind == 'zero_runs':
            values, codec['bits'] = packed(q[positions])
        else:
            values = shuffled(q[positions])
            codec.update(value_width=4, prediction='none')
        raw = prefix + values
        codec.update(nonzero=len(positions), prefix_bytes=len(prefix), gap_bits=gap_bits)
    codec['raw_bytes'] = len(raw)
    return bytes(pa.compress(raw, codec='zstd')), codec


def parquet(table):
    stream = io.BytesIO()
    pq.write_table(table, stream, compression='zstd', row_group_size=8)
    return stream.getvalue()


def write_zip(path, members):
    with zipfile.ZipFile(path, 'w') as archive:
        for name, data in members.items():
            info = zipfile.ZipInfo(name, (2026, 1, 1, 0, 0, 0))
            archive.writestr(info, data)


def entry(name, kind, data):
    return dict(name=name, entity_type='spectrum', data_kind=kind,
                checksum=hashlib.sha512(data).hexdigest())


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    intensities = [[0,0,10,0,0,0,20,0,0], [3,0,0,0,9], [0,100,200], [], [0,0,0], [25], [0,75,0,0]]
    records, source, payload = [], [], bytearray()
    native_rows, official_rows, scans = [], [], []
    signal_rows = {'data_arrays': [], 'peaks': []}
    param_type = pa.list_(pa.struct([('accession', pa.string()), ('name', pa.string()), ('value', pa.string())]))
    for index, values in enumerate(intensities):
        intensity = np.array(values, dtype=float)
        mz = 400 + index * 10 + np.arange(len(values)) * .125
        centroid = index == 2
        record = dict(index=index, id=f'scan={index+1}', count=len(values), ms_level=2 if centroid else 1,
                      representation='centroid' if centroid else 'profile', retention_time_minutes=index*.25, fragments=[])
        if len(values):
            fields = []
            for axis, data in enumerate((mz, intensity)):
                if axis == 0:
                    quantizer = dict(kind='linear', origin=float(mz[0]), step=.125)
                    q = np.arange(len(mz), dtype='<u4')
                    kind = 'delta_shuffle' if index % 2 else 'baseline'
                else:
                    positive = data > 0
                    q = np.zeros(len(data), dtype='<u4')
                    if positive.any():
                        origin = float(data[positive].min())
                        step = float(2*np.log1p(.001))
                        q[positive] = np.rint(np.log(data[positive]/origin)/step).astype('<u4')+1
                        quantizer = dict(kind='log', origin=origin, step=step)
                    else:
                        quantizer = dict(kind='all_zero')
                    kind = {0:'baseline', 1:'packed', 2:'zero_runs', 4:'packed', 5:'sparse_transpose', 6:'zero_runs'}[index]
                blob, codec = encode(q, kind)
                fields.append(dict(offset=len(payload), size=len(blob), dtype='<u4', codec=codec, quantizer=quantizer))
                payload.extend(blob)
            record['fragments'] = [dict(source_start=0, count=len(values), fields=fields)]
        records.append(record)
        keep = np.ones(len(values), dtype=bool) if centroid else np.array([
            value != 0 or (j > 0 and values[j-1] != 0) or (j+1 < len(values) and values[j+1] != 0)
            for j, value in enumerate(values)], dtype=bool)
        source.append(dict(index=index, mz=mz.tolist(), intensity=values, retained_indices=np.flatnonzero(keep).tolist()))
        row = {'index': index, 'id': record['id'], 'time': index*.25, 'ms level': record['ms_level'],
               'number of data points': 0 if centroid else len(values), 'number of peaks': len(values) if centroid else 0,
               'total ion current': float(intensity.sum()), 'base peak intensity': float(intensity.max()) if len(values) else 0.,
               'parameters': []}
        native_rows.append(row)
        official_rows.append(dict(row, **{'number of data points': 0 if centroid else int(keep.sum())}))
        scans.append({'source_index': index, 'time': index*.25, 'parameters': []})
        for mass, value in zip(mz[keep], intensity[keep]):
            signal_rows['peaks' if centroid else 'data_arrays'].append({'spectrum_index': index, 'mz': mass, 'intensity': value})
    def meta_table(rows):
        table = pa.Table.from_pylist(rows)
        return table.set_column(table.schema.get_field_index('parameters'), 'parameters', pa.array([[] for _ in rows], type=param_type))
    scan_data = parquet(meta_table(scans))
    native_data = parquet(meta_table(native_rows))
    official_data = parquet(meta_table(official_rows))
    metadata = {'run': {'id': 'synthetic-example'}, 'description': 'Synthetic adapter fixture; no experimental data'}
    tables = [entry('spectra.parquet','metadata',native_data), entry('scans.parquet','scans',scan_data)]
    # The adapter checks native members with SHA-256; official file entries use SHA-512.
    native = {'acquisition/spectra.parquet': native_data, 'acquisition/scans.parquet': scan_data,
              'acquisition.json': json.dumps({'metadata':metadata,'tables':tables}).encode(), 'signal.bin':bytes(payload)}
    index_data = json.dumps({'spectra':records}).encode()
    native['signal.index.zst'] = struct.pack('<Q',len(index_data)) + bytes(pa.compress(index_data,codec='zstd'))
    native['manifest.json'] = json.dumps(dict(format='experimental-mzpeak-schema-lab-1', acquisition='acquisition.json',
        bounds={'mass_error_ppm':.01,'intensity_error_relative':.001},
        sha256={k:hashlib.sha256(v).hexdigest() for k,v in native.items()})).encode()
    official = {'spectra.parquet':official_data, 'scans.parquet':scan_data}
    files = [entry('spectra.parquet','metadata',official_data),entry('scans.parquet','scans',scan_data)]
    for kind, rows in signal_rows.items():
        points = pa.array(rows, type=pa.struct([('spectrum_index',pa.uint64()),('mz',pa.float64()),('intensity',pa.float64())]))
        table = pa.table({'point':points}).replace_schema_metadata({
            b'spectrum_count':str(len(records)).encode(), b'spectrum_array_index':json.dumps({'entries':[
                {'path':'point.mz','array_name':'m/z array','data_type':'MS:1000523'}, {'path':'point.intensity','array_name':'intensity array','data_type':'MS:1000523'}]}).encode()})
        name = kind+'.parquet'
        official[name] = parquet(table)
        files.append(entry(name,kind,official[name]))
    official['mzpeak_index.json'] = json.dumps({'metadata':metadata,'files':files}).encode()
    write_zip(OUT/'synthetic.integer.zip',native)
    write_zip(OUT/'synthetic.official.mzpeak',official)
    (OUT/'source.json').write_text(json.dumps(source,indent=2)+'\n')
    print({name: (OUT/name).stat().st_size for name in ('synthetic.integer.zip','synthetic.official.mzpeak','source.json')})


if __name__ == '__main__':
    main()
