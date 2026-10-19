# Remaining engineering work

The native RAW adapter, normalized v2 writer/reader, legacy v1 compatibility
adapter and Python bindings are implemented. The current rebuild removes the
duplicated CLI and writer/decoder paths and makes their failure semantics
explicit. CI covers default, optional-format and minimal Rust builds, plus the
Python wheel smoke tests.

Remaining work should be justified by an input corpus and a reproducible
measurement:

- Align with the current HUPO-PSI mzPeak specification through a versioned
  migration and independent reader/validator checks. The present layouts differ.
- Normalize physical units explicitly, especially inverse reduced mobility and
  normalized versus absolute collision energy.
- Package native rawr executables for additional platforms and preserve
  instrument identity and additional vendor metadata through the export path.
- Benchmark complete conversion on a locked, heterogeneous instrument corpus.
  Current synthetic decode/serialization timings are diagnostic evidence.
- Add v2 row-group seeking if random-access workloads justify it. Current v2
  spectrum-ID queries stream and validate the linked tables.
- Update the retained PyO3 interface to support interpreters newer than Python
  3.12, with installed-wheel tests for every advertised interpreter/platform.

New formats, abstractions and execution layers need a demonstrated workload;
file length alone is not a reason to add another layer.
