# RAW converter binary

Download and extract the archive for your system from the
[converter release](https://github.com/filiprumenovski/mzpeak-rs/releases/tag/converter-v0.1.0).
Choose the archive for your system: `linux-x86_64.tar.gz`,
`windows-x86_64.zip`, or `darwin-arm64.tar.gz`. Fresh downloads passed full HeLa validation on Ubuntu 22.04, Windows Server
2022, and macOS 26.5.1. The Linux build requires glibc 2.35 or later. Other OS
versions have not been tested. Windows builds are not Authenticode signed;
macOS builds are ad-hoc signed, not Apple notarized.

Keep the executable and its `_internal` directory together. No Python, Rust,
.NET, vendor runtime, or private repository access is required to run it.

```sh
./mzpeak-convert/mzpeak-convert input.raw new-output-directory
```

On Windows, extract the ZIP and use PowerShell:

```powershell
.\mzpeak-convert\mzpeak-convert.exe input.raw new-output-directory
```

The output is `new-output-directory/complete.experimental.zip`. Open it with
`mzpeak_compat.MzPeakFile` from the adapter package. The converter refuses to
use an existing output directory. It retains partial files if conversion fails.
A successful conversion writes `complete-results.json` last.

This release uses the native rawr-ms reader. It includes the signal encoder and
acquisition metadata writer. The fixed error limits are **0.01 ppm for m/z** and
**0.1% for intensity**. Zeros are retained. These are experimental integer
archives, not official mzPeak archives. Keep the original RAW files. Supported
instrument coverage and validation limits are documented in the repository README.
Packet type 19 is unsupported in this build; such input fails without a success
report. The encoder uses six worker threads.

The binary is distributed as an extracted bundle. It does not unpack a runtime
on each invocation. `build.json` records the bundled helper hashes and runtime
versions. Dependency notices are in `_internal/licenses`.

## Build the bundle

Maintainers need Python 3.11–3.13, PyArrow 20.0.0 and PyInstaller 6.20.0. Pass the
native encoder and metadata executables from the tested converter build, plus
both Cargo manifests. Build on each target platform. This step packages existing
helpers; it does not compile or publish private reader source.

```sh
python build.py --encoder /path/to/fast_raw \
  --metadata /path/to/native_metadata \
  --manifest /path/to/fast_raw/Cargo.toml \
  --manifest /path/to/native_metadata/Cargo.toml \
  --output /path/to/new-build-directory
```

## Release checks

The `Converter release downloads` workflow starts a fresh Linux or Windows runner.
It downloads the published archive and checksum, extracts into a new folder, and
runs the converter with an empty executable search path. It then checks every
HeLa signal point against Thermo RawFileReader and acquisition metadata against
a pinned independent export of the same SHA-256 RAW. Source-path differences
are accepted only when source identity and SHA-1 match.

The vendor oracle, .NET, Python, and the adapter are test dependencies. They are
not dependencies of the converter executable. Oracle DLLs are fetched from the
pinned upstream repository and are not included in the binary bundle. The test
also checks output overwrite protection, fixed precision settings, and adapter
readback. Failed runs retain logs; the workflow uploads small reports only.
