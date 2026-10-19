# Rebuild validation

The v2-only purge is recorded in `purge.json` with the replacement HeLa RAW
run, full integrity scan and exact centroid comparison. Earlier JSON files
below document their recorded revisions before the API purge; their timing
and validator report shapes must not be treated as current results.

The purge removes 33,179 Rust/Python lines net (46,952 → 13,773, including
stubs/tests/examples/tools and excluding vendor submodules). Its verified
714,378,661-byte Q Exactive HF HeLa RAW produced 38,419 spectra and 13,510,675
saved-centroid peaks in a single warm-cache 2.40-second release run on Apple M4.
Every represented peak bit and exported scan metadata field matched rawr's
mzML; full CRC/data validation and Python streaming readback passed. Output was
83,233,835 bytes. These figures do not describe profile-array preservation.

The current five-sample synthetic medians are 95.1 ms for compressed decode
versus 128.6 ms for the scalar reference (1.35x), and 21.5 ms for ZSTD 3
serialization versus 38.1 ms at level 9. The latter compares compression
settings, not an isolated writer-kernel speedup. See `purge.json` for commands,
source fingerprint, exact values and limits.

## Earlier revision measurements

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

## Full Orbitrap Fusion RAW

`large-raw.json` records the complete `020618_MS0745_PTB_H2.raw` run: 1.48 GB,
169,273 scans and 41,092,121 saved centroid peaks. A release CLI conversion
with the balanced profile took 8.45 seconds and wrote 261,818,368 bytes
(249.7 MiB). This is one warm-cache run on the Apple M4, including the rawr
process, XML pipe, Parquet compression, ZIP assembly and output writes; it is
not a cross-machine or cold-storage throughput claim.

Every peak and exported scan metadata field matched rawr's mzML at the output
schema's precision. A second comparison against the existing ProteoWizard
3.0.20287 export matched every peak and native scan number at that reference's
f32 precision. The preserved f64 m/z values were narrowed only inside that
comparison. Python/Rust streaming readback recovered all scans and peaks,
including 11,018 MS1 and 158,255 MS2 scans. rawr's full packet check had zero
failures.

Both independent checkers now stream XML and Parquet batches and extract
fixed archive members to temporary sibling storage. A negative control with
one altered intensity failed at the expected scan. For an independent
centroid reference, run:

```sh
python tools/verify_peak_reference.py proteowizard.mzML converted.mzpeak
```

The full-run validation also exposed a signed/unsigned accessor bug in the
integrity command. It now reads v2 UInt32 IDs and UInt8 MS levels with the
matching Parquet accessors, and uses the v2 manifest's format version. The
regression covers maximum unsigned values and still rejects MS level zero.
