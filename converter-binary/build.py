"""Freeze the tested native converter. Build helpers before invoking this script."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import sysconfig
import tarfile
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--encoder', type=Path, required=True)
    parser.add_argument('--metadata', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, action='append', required=True,
                        help='Cargo manifest for each helper, to collect dependency notices')
    parser.add_argument('--output', type=Path, required=True, help='New build directory')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    notices = output / 'licenses'
    notices.mkdir()
    packages = {}
    for manifest in args.manifest:
        data = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--locked', '--format-version=1', '--manifest-path', str(manifest.resolve())]))
        for package in data['packages']:
            key = package['name'] + '-' + package['version']
            packages[key] = {k: package.get(k) for k in ('name', 'version', 'license', 'repository')}
            source = Path(package['manifest_path']).parent
            # Copy notices only. Private reader implementation remains private.
            candidates = [p for p in source.rglob('*')
                          if p.name.upper().startswith(('LICENSE', 'COPYING', 'NOTICE'))
                          and 'target' not in p.relative_to(source).parts]
            for candidate in candidates:
                if candidate.is_file():
                    target = notices / key / candidate.relative_to(source)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(candidate, target)
    (notices / 'rust-dependencies.json').write_text(json.dumps(packages, indent=2) + '\n')
    for name in ('pyarrow', 'pyinstaller'):
        distribution = importlib.metadata.distribution(name)
        for file in distribution.files or []:
            if any(part.upper().startswith(('LICENSE', 'NOTICE', 'COPYING')) for part in file.parts):
                source = Path(distribution.locate_file(file))
                if source.is_file():
                    target = notices / name / Path(*file.parts)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target)
    python_license = next((p for p in (
        Path(sysconfig.get_path('stdlib')) / 'LICENSE.txt',
        Path(sys.base_prefix) / 'LICENSE.txt',
        HERE / 'licenses/PYTHON-3.13.7-LICENSE.txt',
    ) if p.is_file()), None)
    if python_license is None:
        raise RuntimeError('Python LICENSE.txt is required for distribution')
    shutil.copy2(python_license, notices / 'PYTHON-LICENSE.txt')
    for name in ('LICENSE-MIT', 'LICENSE-APACHE'):
        shutil.copy2(ROOT / name, notices / name)
    subprocess.run([
        sys.executable, '-m', 'PyInstaller', '--noconfirm', '--clean', '--onedir',
        '--name', 'mzpeak-convert', '--distpath', str(output / 'dist'),
        '--workpath', str(output / 'work'), '--specpath', str(output),
        '--add-binary', f'{args.encoder.resolve()}:bin',
        '--add-binary', f'{args.metadata.resolve()}:bin',
        '--add-data', f'{notices}:licenses',
        '--hidden-import', 'pyarrow.parquet', str(HERE / 'convert.py'),
    ], check=True)
    bundle = output / 'dist/mzpeak-convert'
    shutil.copy2(HERE / 'README.md', bundle / 'README.md')
    receipt = {
        'version': '0.1.0', 'platform': platform.platform(),
        'python': platform.python_version(),
        'pyinstaller': importlib.metadata.version('pyinstaller'),
        'pyarrow': importlib.metadata.version('pyarrow'),
        'driver_sha256': sha256(HERE / 'convert.py'),
        'helpers': {p.name: sha256(p) for p in (args.encoder, args.metadata)},
        'mass_error_ppm': 0.01, 'intensity_relative_error': 0.001,
        'reader': 'rawr-ms',
    }
    (bundle / 'build.json').write_text(json.dumps(receipt, indent=2) + '\n')
    machine = platform.machine().lower()
    machine = 'x86_64' if machine == 'amd64' else machine
    suffix = 'zip' if sys.platform == 'win32' else 'tar.gz'
    name = f'mzpeak-convert-0.1.0-{platform.system().lower()}-{machine}.{suffix}'
    archive = output / name
    if suffix == 'zip':
        with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as zipped:
            for path in sorted(bundle.rglob('*')):
                if path.is_file():
                    zipped.write(path, path.relative_to(bundle.parent))
    else:
        with tarfile.open(archive, 'w:gz') as tar:
            tar.add(bundle, arcname='mzpeak-convert')
    (output / (name + '.sha256')).write_bytes(f'{sha256(archive)}  {name}\n'.encode())
    print(archive)


if __name__ == '__main__':
    main()
