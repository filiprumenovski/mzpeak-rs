# Input coverage

- Thermo RAW: the bundled rawr-ms process selects saved centroids. Unsupported
  layouts fail; profile arrays and auxiliary controllers are outside this bridge.
- mzML/imzML: one streaming XML parser and one checked Base64/zlib decoder.
  imzML external arrays are read from the companion IBD file.
- Bruker TDF: the optional pinned timsrust adapter retains TOF/mobility
  calibration, frame IDs and available imaging/precursor metadata. It emits
  frame-level spectra; the current single-precursor schema cannot describe all
  multi-window acquisition semantics. Real TDF corpus validation is still needed.

The output is this repository's experimental v2 layout. Vendor names and
compiled feature support alone are not fidelity or interoperability evidence.
See validation/README.md for actual measured coverage.
