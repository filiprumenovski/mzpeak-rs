# Rebuild validation

`performance.json` records the compiler, CPU, fixture and command for the
reproducible in-memory benchmark. The reference is the previous scalar
Base64 → inflate → float-copy shape, run alongside the shared decoder with the
same current dependencies. Five samples are collected and their median is
reported. Every decoded array is checked for equality.

On this fixture, compressed decode improved by 1.31x. Plain decode was within
3% of the reference. ZSTD level 3 serialized 1,048,576 peaks in 29.4 ms versus
52.8 ms at level 9 (1.79x), with 0.8% larger output. The default change accounts
for that compression comparison; it is not a claim that the writer kernel
alone became 1.79x faster. The faster default is shared by Rust, Python and the
balanced CLI profile. Higher compression remains configurable.

An attempted SIMD Base64 path measured slower on this CPU and was removed.
The retained decoder uses standard Base64, a reused libdeflate inflater for
length-declared zlib arrays, and explicit numeric-width validation.

The decode design was checked against Zage's
[inflate campaign](https://github.com/filiprumenovski/zage/blob/0e33ff989ba3fee6753e47c49e6b9c389097543d/docs/mzml_inflate_campaign.md)
and Base64 operator. The shared principles are known output sizes, one inflater
per worker, and exact output checks. Zage's quantization and custom SIMD wrapper
were not copied; mzpeak retains floating-point peak values.

These synthetic timings exclude RAW decode, ZIP assembly and disk I/O. They do
not establish instrument-corpus throughput or performance on another CPU.

`native-raw.json` records a separate fidelity check of the 225-scan Thermo IAPI
sample `rawfile.raw`. Both the native process path and Python conversion wrote
156,181 peaks. The independent checker uses Python's XML/Base64/struct readers
and pyarrow to compare every represented m/z/intensity bit and all exported
scan metadata. Proprietary RAW input is not included.

Reproduce the independent export check with:

```sh
rawr --color never convert --format mzml --plain --output reference.mzML sample.raw
mzpeak-convert convert-thermo sample.raw converted.mzpeak
python tools/verify_raw_export.py reference.mzML converted.mzpeak
```

Regression coverage includes empty spectra, cross-batch v2 joins, unsigned
spectrum IDs, optional-column lengths, malformed input, truncated process
streams, late child failures, existing-output protection, chromatogram
preservation/generation, and Python exception mapping.
