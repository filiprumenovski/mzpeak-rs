# mzPeak Roadmap

This document outlines the planned development trajectory for mzPeak.

## Version 1.0.0 (Current)

The current release provides a complete, production-ready implementation:

- [x] **Long-table Parquet schema** - One row per peak for efficient columnar queries
- [x] **ZIP container format** - Self-contained `.mzpeak` archives with metadata
- [x] **mzML conversion** - Streaming parser with full CV term support
- [x] **Ion mobility support** - Native timsTOF/FAIMS drift time integration
- [x] **Chromatogram extraction** - Automatic TIC/BPC generation
- [x] **Python bindings** - PyO3-based API with zero-copy Arrow integration
- [x] **Validation module** - Deep integrity checking for file verification
- [x] **SDRF metadata** - Experimental design metadata support

## Version 1.1.0 (Planned)

### Direct Vendor Format Support

- [ ] **Thermo RAW conversion** - Direct reading via thermorawfilereader
- [ ] **Bruker TDF conversion** - Native .d folder support for timsTOF
- [ ] **Waters UNIFI support** - Direct .raw folder reading
- [ ] **Sciex WIFF support** - Analyst/Sciex data format support

### Cloud Integration

- [ ] **S3 object storage** - Read/write directly from Amazon S3
- [ ] **Google Cloud Storage** - GCS bucket support
- [ ] **Azure Blob Storage** - Azure integration

### Performance Enhancements

- [ ] **Parallel conversion** - Multi-threaded mzML parsing
- [ ] **Async I/O** - Non-blocking file operations
- [ ] **Memory-mapped reading** - Reduced memory footprint for large files

## Version 2.0.0 (Future)

### Advanced Features

- [ ] **Delta Lake integration** - Versioned datasets with time travel
- [ ] **gRPC streaming API** - Network-based data access
- [ ] **Real-time conversion** - Stream conversion during acquisition
- [ ] **Differential compression** - Inter-spectrum compression for sequences

### Ecosystem Integration

- [ ] **MaxQuant compatibility** - Direct output format support
- [ ] **Sage integration** - Native mzPeak input support
- [ ] **FragPipe compatibility** - MSFragger/Philosopher pipeline support
- [ ] **DIA-NN integration** - Optimized DIA data handling

### Schema Extensions

- [ ] **Imaging MS enhancements** - Full MSI workflow support
- [ ] **Glycomics extensions** - Specialized glycan metadata
- [ ] **Cross-linking MS** - XL-MS specific schema extensions

## Contributing

We welcome contributions for any roadmap item. Please:

1. Check [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines
2. Open an issue to discuss the feature before implementing
3. Reference the roadmap item in your PR

## Versioning

mzPeak follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (1.x.x → 2.0.0): Breaking schema or API changes
- **MINOR** (1.0.x → 1.1.0): New features, backward compatible
- **PATCH** (1.0.0 → 1.0.1): Bug fixes, documentation updates
