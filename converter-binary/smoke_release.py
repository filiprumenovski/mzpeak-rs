"""Download a published binary and validate a real conversion on a clean runner."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
import zipfile

HERE = Path(__file__).resolve().parent
RAW_URL = 'https://ftp.pride.ebi.ac.uk/pride/data/archive/2021/02/PXD019848/500ng_HeLa_350_1150mz_nonFAIMS_Rs30000_200ms_Rep2.raw'
RAW_SHA256 = '01522aefd49d2eb7eaf764687ae2ed591605a99d57c06113b97d2be32eb4f025'
ORACLE_SHA256 = 'ab373e304c344c92185131d7684d4cd54c7e5888aef24341ed9f4095f7b360ba'


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def fetch(url, target):
    request = urllib.request.Request(url, headers={'User-Agent': 'mzpeak-release-validation'})
    with urllib.request.urlopen(request, timeout=120) as source, target.open('xb') as dest:
        shutil.copyfileobj(source, dest, 1024 * 1024)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--release', default='converter-v0.1.0')
    args = parser.parse_args()
    root = args.work.resolve()
    root.mkdir(parents=True, exist_ok=False)
    system = platform.system().lower()
    machine = platform.machine().lower().replace('amd64', 'x86_64')
    extension = '.exe' if system == 'windows' else ''
    asset = f'mzpeak-convert-0.1.0-{system}-{machine}.' + ('zip' if extension else 'tar.gz')
    base = 'https://github.com/filiprumenovski/mzpeak-rs/releases/download/' + args.release
    fetch(base + '/' + asset, root / asset)
    fetch(base + '/' + asset + '.sha256', root / (asset + '.sha256'))
    expected, filename = (root / (asset + '.sha256')).read_text().split()
    assert filename == asset and digest(root / asset) == expected, 'Release checksum mismatch'
    if extension:
        with zipfile.ZipFile(root / asset) as archive:
            assert all((root / name).resolve().is_relative_to(root) for name in archive.namelist())
            archive.extractall(root)
    else:
        with tarfile.open(root / asset) as archive:
            archive.extractall(root, filter='data')
    binary = root / 'mzpeak-convert' / ('mzpeak-convert' + extension)
    for folder in ('home', 'empty-path', 'temp'):
        (root / folder).mkdir()
    # Retain only OS-required Windows variables, not toolchains or user settings.
    env = {key: value for key, value in os.environ.items() if key.upper() in ('SYSTEMROOT', 'WINDIR')}
    env.update(PATH=str(root / 'empty-path'), HOME=str(root / 'home'),
               USERPROFILE=str(root / 'home'), TEMP=str(root / 'temp'),
               TMP=str(root / 'temp'), TMPDIR=str(root / 'temp'),
               MZLAB_MZ_PPM='5', MZLAB_INTENSITY_ERROR='0.1')
    assert subprocess.check_output([binary, '--version'], cwd=root, env=env, text=True).strip() == 'mzpeak-convert 0.1.0'
    fetch(RAW_URL, root / 'hela.raw')
    assert digest(root / 'hela.raw') == RAW_SHA256, 'RAW checksum mismatch'
    started = time.perf_counter()
    with (root / 'conversion.log').open('w') as log:
        subprocess.run([binary, root / 'hela.raw', root / 'output'], cwd=root, env=env,
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
    wall_seconds = time.perf_counter() - started
    result = json.loads((root / 'output/complete-results.json').read_text())
    encoded = json.loads((root / 'output/encode-results.json').read_text())
    assert encoded['mass_error_ppm'] == .01 and encoded['intensity_error_relative'] == .001
    archive = root / 'output/complete.experimental.zip'
    before = digest(archive)
    retry = subprocess.run([binary, root / 'hela.raw', root / 'output'], cwd=root, env=env, capture_output=True, text=True)
    assert retry.returncode == 2 and 'output directory must be new' in retry.stderr
    assert digest(archive) == before, 'Existing output changed'
    fetch(base + '/hela.vendor.metadata.mzpeak', root / 'metadata-oracle.mzpeak')
    assert digest(root / 'metadata-oracle.mzpeak') == ORACLE_SHA256
    with (root / 'validation.log').open('w') as log:
        subprocess.run([sys.executable, HERE / 'validate_release.py', root / 'output',
                        '--raw', root / 'hela.raw', '--reference', root / 'metadata-oracle.mzpeak'],
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
    from mzpeak_compat import MzPeakFile
    from mzpeak_compat.reader import Reader
    from contextlib import closing
    import numpy as np
    with MzPeakFile(archive) as adapter, closing(Reader(archive)) as signal:
        assert len(adapter) == result['spectra']
        for index in (0, len(adapter) // 2, len(adapter) - 1):
            actual = adapter[index]
            mz, intensity = signal.read(index)
            np.testing.assert_array_equal(actual['m/z array'], mz)
            np.testing.assert_array_equal(actual['intensity array'], intensity)
    validation = json.loads((root / 'output/validation.json').read_text())
    assert validation['all_spectra_all_points_validated'] and validation['all_source_summaries_validated']
    report = dict(status='passed', platform=platform.platform(), asset=asset,
                  asset_sha256=expected, downloaded_from=base + '/' + asset,
                  fresh_runner=os.environ.get('GITHUB_ACTIONS') == 'true',
                  empty_executable_search_path=True,
                  raw_sha256=RAW_SHA256, metadata_oracle_sha256=ORACLE_SHA256,
                  precision_environment_override_ignored=True, overwrite_refused=True,
                  adapter_first_middle_last_equal=True, conversion_wall_seconds=wall_seconds,
                  conversion=result, independent_validation=validation,
                  scope='Fresh published download; conversion, full vendor signal validation, pinned metadata oracle, and installed adapter readback. Not a comparative speed benchmark.')
    (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('status', 'platform', 'asset', 'conversion_wall_seconds')}))


if __name__ == '__main__':
    main()
