# Quick Reference: Testing Commands

## Run Property Tests (10,000+ iterations)
```bash
cargo test --test integration_test property_roundtrip_spectrum
```

## Run All Integration Tests
```bash
cargo test --test integration_test
```

## Run Validator Tests
```bash
cargo test --lib validator
```

## Run Fuzzer (10 minutes)
```bash
# One-time setup
rustup install nightly

# Run fuzzer
cargo +nightly fuzz run fuzz_mzml_streamer -- -max_total_time=600

# Or use convenience script
cd fuzz && ./run_fuzzer.sh 600
```

## Verify Implementation
```bash
# All tests (takes ~3-5 minutes)
cargo test

# Property test only (~85 seconds)
cargo test property_roundtrip_spectrum

# Quick smoke test (~10 seconds)
cargo test --lib
```

## Implementation Summary

✅ **Property Testing**: 10,000 iterations testing round-trip data integrity  
✅ **XML Fuzzing**: Comprehensive mzML parser fuzzing infrastructure  
✅ **CV Validation**: 18 HUPO-PSI CV terms validated in schema

See `TESTING_IMPLEMENTATION.md` for complete documentation.
