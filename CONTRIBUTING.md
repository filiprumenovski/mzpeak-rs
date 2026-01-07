# Contributing to mzPeak

Thank you for your interest in contributing to mzPeak! This document provides guidelines and information for contributors.

## Code of Conduct

Please be respectful and constructive in all interactions. We welcome contributors from all backgrounds.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/your-username/mzpeak.git
   cd mzpeak
   ```
3. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Setup

### Requirements

- Rust 1.70 or later
- Cargo (included with Rust)

### Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Run tests
cargo test

# Run with verbose output
cargo test -- --nocapture
```

### Running the CLI

```bash
# Generate demo data
cargo run -- demo output.parquet

# Show help
cargo run -- --help
```

## Making Changes

### Code Style

- Run `cargo fmt` before committing to ensure consistent formatting
- Run `cargo clippy` and address any warnings
- Follow Rust naming conventions:
  - `snake_case` for functions, methods, variables, modules
  - `CamelCase` for types and traits
  - `SCREAMING_SNAKE_CASE` for constants

### Documentation

- Add doc comments (`///`) to all public items
- Include examples in doc comments where helpful
- Update the README if adding new features

### Testing

- Add unit tests for new functionality
- Add integration tests for end-to-end scenarios
- Ensure all tests pass before submitting a PR

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture
```

## Pull Request Process

1. **Update documentation** if you've changed behavior
2. **Add tests** for new functionality
3. **Run the test suite** and ensure all tests pass
4. **Run clippy** and fix any warnings
5. **Update the CHANGELOG** with your changes
6. **Submit a pull request** with a clear description

### PR Description Template

```markdown
## Summary
Brief description of the changes

## Changes
- Change 1
- Change 2

## Testing
Describe how you tested the changes

## Checklist
- [ ] Tests pass
- [ ] Code formatted with `cargo fmt`
- [ ] No clippy warnings
- [ ] Documentation updated
```

## Project Structure

```
mzpeak/
├── src/
│   ├── lib.rs              # Library entry point
│   ├── main.rs             # CLI entry point
│   ├── schema.rs           # Arrow/Parquet schema
│   ├── writer.rs           # Parquet writer
│   ├── metadata.rs         # Metadata handling
│   ├── controlled_vocabulary.rs  # CV terms
│   └── mzml/               # mzML parsing
│       ├── mod.rs
│       ├── models.rs       # Data structures
│       ├── streamer.rs     # XML parser
│       ├── binary.rs       # Base64/compression
│       ├── cv_params.rs    # CV parameters
│       └── converter.rs    # mzML to mzPeak
├── tests/
│   └── integration_test.rs # Integration tests
├── Cargo.toml
├── README.md
└── CHANGELOG.md
```

## Areas for Contribution

### High Priority

- Additional input format support (mzXML, .raw files)
- Performance optimizations
- Python bindings
- R bindings

### Medium Priority

- Additional CLI commands
- Validation tools
- Format conversion utilities

### Documentation

- More examples
- Tutorials
- API documentation improvements

## Questions?

Feel free to open an issue for:
- Bug reports
- Feature requests
- Questions about the codebase
- Help with contributions

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (MIT OR Apache-2.0).
