# Contributing

Initialize submodules and use Rust 1.98+. Run the test matrix and formatting
commands in the README. Format the root package with `cargo fmt --package
mzpeak`; do not reformat the pinned vendor checkout.

A change must preserve numerical widths, explicit nulls, source units,
calibration, empty spectra, provenance and bounded streaming behavior. Decode
failures must not skip scans or publish a completed-looking output. Regressions
must exercise the data or failure contract, rather than matching the code's
implementation. Python changes need an installed-wheel smoke test on Python
3.11 with NumPy 1.x.

Keep performance claims reproducible: record the input identity, compiler,
machine, compression, cache state, timing scope and fidelity. Check real RAW
input against an independent export reader. Source-saved centroids are not a
claim of preserving an entire RAW file. Do not commit instrument data.

There is no v1 compatibility layer. Extend the one v2 pipeline instead of adding
another writer, reader, conversion branch or Python wrapper hierarchy. These
experimental layouts do not claim current HUPO-PSI interchange compatibility.

Wheel platform tags are inferred from the build environment. Use a matching
manylinux build container when targeting older Linux systems; do not force an
older glibc tag on a wheel built by a current Ubuntu runner.
