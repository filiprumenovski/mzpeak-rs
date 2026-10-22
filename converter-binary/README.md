# RAW converter binary

Download and extract the archive for your system from the
[converter release](https://github.com/filiprumenovski/mzpeak-rs/releases/tag/converter-v0.1.0).
**Available build: macOS on Apple Silicon; tested on macOS 26.5.1.** This build is
ad-hoc signed, not Apple notarized. Other platforms are not yet packaged.

Keep the executable and its `_internal` directory together. No Python, Rust,
.NET, vendor runtime, or private repository access is required to run it.

```sh
./mzpeak-convert/mzpeak-convert input.raw new-output-directory
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
