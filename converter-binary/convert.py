"""Convert a Thermo RAW file directly to a bounded-error integer archive."""
import argparse
import concurrent.futures
import tempfile
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import zipfile

CODE = Path(__file__).resolve().parent
METADATA = CODE / 'bin/native_metadata'
SIGNAL_MEMBERS = {'spectra_data.parquet', 'spectra_peaks.parquet'}
SUMMARY_FIELDS = ('lowest_observed_mz', 'highest_observed_mz', 'base_peak_mz',
                  'base_peak_intensity', 'total_ion_current')


def restore_summaries(table, rows):
    import pyarrow as pa
    if len(rows) != len(table):
        raise ValueError('Signal/metadata spectrum count differs')
    for key in ('index', 'id', 'time', 'ms_level'):
        if table[key].to_pylist() != [r[key] for r in rows]:
            raise ValueError(f'Signal/metadata {key} differs')
    representations = ['MS:1000128' if r['representation'] == 'profile' else 'MS:1000127' for r in rows]
    if table['spectrum_representation'].to_pylist() != representations:
        raise ValueError('Signal/metadata representation differs')
    columns = {key: [r[key] for r in rows] for key in SUMMARY_FIELDS}
    columns['number_of_data_points'] = [r['count'] if r['representation'] == 'profile' else None for r in rows]
    columns['number_of_peaks'] = [r['count'] if r['representation'] == 'centroid' else None for r in rows]
    # No extended signal arrays are loaded by either input path. The reference
    # writer represents that absence as an empty list when signal is present.
    columns['auxiliary_arrays'] = [v if v is not None else [] for v in table['auxiliary_arrays'].to_pylist()]
    for key, values in columns.items():
        field = table.schema.field(key)
        table = table.set_column(table.schema.get_field_index(key), field, pa.array(values, type=field.type))
    return table


def stream_signal(command, folder, env):
    began = time.perf_counter()
    digest = hashlib.sha256()
    count = 0
    with subprocess.Popen([str(v) for v in command], env=env, stdout=subprocess.PIPE) as process:
        try:
            # The first read lets the encoder create its new output directory.
            chunk = process.stdout.read(1024 * 1024)
            if not folder.is_dir():
                process.wait()
                raise subprocess.CalledProcessError(process.returncode or 1, command)
            with zipfile.ZipFile(folder / 'complete.experimental.zip', 'x',
                                 compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                with archive.open('signal.bin', 'w', force_zip64=True) as target:
                    while chunk:
                        target.write(chunk)
                        digest.update(chunk)
                        count += len(chunk)
                        chunk = process.stdout.read(1024 * 1024)
            if process.wait() != 0:
                raise subprocess.CalledProcessError(process.returncode, command)
            report = json.loads((folder / 'encode-results.json').read_text())
            if count != report['signal_bytes']:
                raise ValueError('Incomplete encoded signal stream')
        except BaseException:
            process.kill()
            process.wait()
            raise
    return time.perf_counter() - began, digest.hexdigest()


def prepare_bundle(folder, signal_hash=None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    pa.set_cpu_count(1)
    with (folder / 'source-summaries.jsonl').open() as source:
        rows = [json.loads(line) for line in source]
    report = json.loads((folder / 'encode-results.json').read_text())
    if len(rows) != report['spectra'] or sum(r['count'] for r in rows) != report['points']:
        raise ValueError('Incomplete source summaries')
    archive = folder / 'complete.experimental.zip'
    hashes = {} if signal_hash is None else {'signal.bin': signal_hash}
    names = ('signal.bin', 'signal.index.zst') if signal_hash is None else ('signal.index.zst',)
    with zipfile.ZipFile(archive, 'x' if signal_hash is None else 'a',
                         compression=zipfile.ZIP_STORED, allowZip64=True) as dest:
        if signal_hash is not None:
            if dest.namelist() != ['signal.bin'] or dest.getinfo('signal.bin').file_size != report['signal_bytes']:
                raise ValueError('Unexpected streamed signal archive')
        for name in names:
            path = folder / name
            digest = hashlib.sha256()
            info = zipfile.ZipInfo.from_file(path, arcname=name)
            with path.open('rb') as source, dest.open(info, 'w') as target:
                while chunk := source.read(1024 * 1024):
                    target.write(chunk)
                    digest.update(chunk)
            hashes[name] = digest.hexdigest()
    return rows, report, hashes


def bundle(folder, rows, report, hashes):
    import pyarrow as pa
    import pyarrow.parquet as pq
    archive = folder / 'complete.experimental.zip'
    vendor = report.get('signal_reader') == 'Thermo RawFileReader'
    with zipfile.ZipFile(folder / 'acquisition.metadata.mzpeak') as source, \
            zipfile.ZipFile(archive, 'a', compression=zipfile.ZIP_STORED, allowZip64=True) as dest:
        index = json.loads(source.read('mzpeak_index.json'))
        for name in source.namelist():
            if name in SIGNAL_MEMBERS or name == 'mzpeak_index.json':
                continue
            data = source.read(name)
            if name == 'spectra_metadata.parquet':
                # Keep the existing table schema; acquisition.json carries the file metadata.
                table = restore_summaries(pq.ParquetFile(pa.BufferReader(data)).read().replace_schema_metadata(None), rows)
                output = pa.BufferOutputStream()
                pq.write_table(table, output, compression='zstd', compression_level=3)
                data = output.getvalue().to_pybytes()
            name = 'acquisition/' + name
            dest.writestr(name, data)
            hashes[name] = hashlib.sha256(data).hexdigest()
        acquisition = {
            'metadata': index['metadata'],
            'tables': [{k: entry[k] for k in ('name', 'entity_type', 'data_kind', 'column_mapping', 'parameters')}
                       for entry in index['files'] if entry['name'] not in SIGNAL_MEMBERS],
            'spectrum_counts': 'Counts describe all stored source points, including zeros.',
            'spectrum_summaries': 'Unquantized input statistics, using source f32 accumulation as in the reference reader.'}
        acquisition_bytes = json.dumps(acquisition, sort_keys=True).encode()
        dest.writestr('acquisition/acquisition.json', acquisition_bytes)
        hashes['acquisition/acquisition.json'] = hashlib.sha256(acquisition_bytes).hexdigest()
        dest.writestr('manifest.json', json.dumps({
            'format': 'experimental-mzpeak-schema-lab-1', 'mzpeak_conformant': False,
            'signal': 'signal.bin', 'signal_index': 'signal.index.zst',
            'acquisition': 'acquisition/acquisition.json', 'sha256': hashes,
            'spectra': report['spectra'], 'points': report['points'],
            'bounds': {'mz_ppm': report.get('mass_error_ppm', .01),
                       'intensity_relative': report.get('intensity_error_relative', .001)},
            'calibration_policy': 'vendor' if vendor else 'physical_source_event',
            'reader': 'vendor' if vendor else 'rawr',
            'calibration_difference_budget_ppm': 0. if vendor else report.get('mass_error_ppm', .01) * .1,
            'pipeline': ('Thermo RawFileReader supplies signal and acquisition metadata to our integer encoder. No rawr-ms or mzML bridge. Quantization uses 90% of the stated mass budget; independent total-error validation required.' if vendor else 'rawr-ms reads RAW signal and acquisition metadata natively. No vendor runtime, mzML bridge, or reference artifact is used as conversion input. Physical source-event calibration; 90% of the stated mass budget is used for quantization, with independent total-error validation required.'),
            'metadata_scope': 'Matches acquisition metadata and chromatograms exported by the pinned reference converter; not a claim to preserve every vendor RAW field.',
            'reference_revision': '40c65521dcfdc3d1df22c16f1ef18e6013fc6ef6'}))
    return {'path': str(archive), 'bytes': archive.stat().st_size,
            'sha256': hashes, 'spectra': report['spectra'], 'points': report['points']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path, help='New output directory')
    parser.add_argument('--version', action='version', version='mzpeak-convert 0.1.0')
    args = parser.parse_args()
    raw, folder = args.source.resolve(), args.output.resolve()
    metadata_exe = METADATA
    encoder = CODE / 'bin/fast_raw'
    if not raw.is_file() or folder.exists():
        parser.error('Input must exist and output directory must be new')
    for executable in (metadata_exe, encoder):
        if not executable.is_file() or not os.access(executable, os.X_OK):
            parser.error(f'Missing executable: {executable}')
    env = dict(os.environ)
    # Release precision is fixed, even when the caller has lab settings exported.
    env.update(MZLAB_MZ_PPM='0.01', MZLAB_INTENSITY_ERROR='0.001',
               MZLAB_ZSTD_LEVEL='3', MZLAB_SPARSE_TRANSPOSE='0')
    start = time.perf_counter()
    def run_stage(command):
        began = time.perf_counter()
        subprocess.run([str(v) for v in command], env=env, check=True)
        return time.perf_counter() - began
    # Separate destinations let both RAW passes start without racing directory creation.
    with tempfile.TemporaryDirectory(prefix='.raw-metadata-', dir=folder.parent) as temporary:
        metadata = Path(temporary) / 'acquisition.metadata.mzpeak'
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            signal = pool.submit(stream_signal, [encoder, raw, folder, '--stdout'], folder, env)
            acquisition = pool.submit(run_stage, [metadata_exe, raw, metadata])
            signal_seconds, signal_hash = signal.result()
            prepare_start = time.perf_counter()
            prepared = prepare_bundle(folder, signal_hash)
            prepare_seconds = time.perf_counter() - prepare_start
            metadata_seconds = acquisition.result()
        metadata.rename(folder / 'acquisition.metadata.mzpeak')
    parallel_seconds = time.perf_counter() - start
    bundle_start = time.perf_counter()
    result = bundle(folder, *prepared)
    result.update(reader='rawr', signal_stage_seconds=signal_seconds, metadata_stage_seconds=metadata_seconds,
                  parallel_signal_metadata_and_packaging_seconds=parallel_seconds,
                  overlapped_bundle_prepare_seconds=prepare_seconds,
                  bundle_stage_seconds=time.perf_counter() - bundle_start,
                  total_seconds=time.perf_counter() - start,
                  timing_scope='All conversion stages and final ZIP writes; independent persisted validation excluded; no fsync durability guarantee.')
    (folder / 'complete-results.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
