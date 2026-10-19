# Contributing

Use Rust 1.98+ and initialize the pinned Bruker dependency with
`git submodule update --init --recursive` before running Cargo.

Changes must preserve peak precision, explicit nulls, units, empty spectra,
source provenance and bounded streaming behavior. Failed conversion must not
publish a successful-looking output. Never silently skip a malformed scan.

Run `just check` or the equivalent commands in the README. Python changes also
need `PYO3_PYTHON=python3.11 cargo check --all-targets --all-features` and a
maturin build in a Python 3.11 virtual environment. The bundled rawr binary is
an independent component; its update procedure is in `vendor/rawr-ms/README.md`.

Keep performance experiments reproducible. Record the machine, compiler,
input shape, command, output fidelity, compression and median timing. Measure
decoding separately from serialization and report complete conversion time
when claiming a workflow improvement. A synthetic benchmark alone does not
establish throughput on an instrument corpus.

Existing style and deprecation warnings are visible in Clippy output; fix them
where they affect your change. Format the mzpeak package rather than its pinned
vendored dependency. Add regression tests for changed data or failure semantics.

This repository's experimental layouts differ from the current HUPO-PSI mzPeak
specification. Do not claim interchange compatibility without an independent
cross-reader test.
