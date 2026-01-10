# mzPeak Fuzzing

This directory contains fuzzing targets for the mzPeak library using `cargo-fuzz` and libFuzzer.

## Prerequisites

Fuzzing requires the Rust nightly toolchain:

```bash
rustup install nightly
```

## Running the Fuzzer

### mzML Streamer Fuzzer

Tests the `mzml::streamer` module for panics and crashes when parsing malformed or malicious mzML files.

**Run for 10 minutes (600 seconds):**

```bash
cargo +nightly fuzz run fuzz_mzml_streamer -- -max_total_time=600
```

**Run indefinitely:**

```bash
cargo +nightly fuzz run fuzz_mzml_streamer
```

**Run with specific seed corpus:**

```bash
cargo +nightly fuzz run fuzz_mzml_streamer fuzz/corpus/fuzz_mzml_streamer
```

## Fuzzing Targets

### `fuzz_mzml_streamer`

**Location:** `fuzz_targets/fuzz_mzml_streamer.rs`

**Purpose:** Ensures the quick-xml based mzML parser handles arbitrary byte sequences gracefully without panicking.

**What it tests:**
- XML parsing resilience
- Binary data decoding safety
- UTF-8 handling
- Malformed structure handling
- Memory safety

**Expected behavior:** The parser should either succeed or return an error - never panic.

## Corpus

Fuzzing corpus files are stored in `fuzz/corpus/fuzz_mzml_streamer/`. The fuzzer automatically:
- Saves interesting inputs that increase code coverage
- Minimizes test cases
- Replays crashes from previous runs

## Artifacts

If a crash or hang is discovered, artifacts are saved to `fuzz/artifacts/fuzz_mzml_streamer/` with:
- The input that caused the issue
- Stack traces
- Minimized reproducers

## Continuous Integration

For CI pipelines, run fuzzing for a fixed time:

```bash
# 5 minutes for quick checks
cargo +nightly fuzz run fuzz_mzml_streamer -- -max_total_time=300

# 1 hour for thorough checks
cargo +nightly fuzz run fuzz_mzml_streamer -- -max_total_time=3600
```

## Results

A successful 10-minute fuzzing run without crashes indicates:
- ✅ Parser is resilient to malformed input
- ✅ No panic-inducing edge cases discovered
- ✅ Memory safety maintained
- ✅ Error handling is comprehensive

## Adding New Fuzz Targets

```bash
cd /path/to/mzpeak-rs
cargo fuzz add fuzz_target_name
```

Then edit `fuzz/fuzz_targets/fuzz_target_name.rs` with your fuzzing logic.
