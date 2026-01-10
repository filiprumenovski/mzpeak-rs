#!/bin/bash
# Run fuzzing test for mzML streamer
# Usage: ./run_fuzzer.sh [duration_in_seconds]

set -e

DURATION=${1:-600}  # Default to 10 minutes

echo "================================================================"
echo "mzPeak mzML Streamer Fuzzer"
echo "================================================================"
echo "Duration: ${DURATION} seconds"
echo ""

# Check if nightly is installed
if ! rustup toolchain list | grep -q nightly; then
    echo "Installing Rust nightly toolchain..."
    rustup install nightly
fi

echo "Building fuzzer..."
cargo +nightly fuzz build fuzz_mzml_streamer

echo ""
echo "Running fuzzer for ${DURATION} seconds..."
echo "The fuzzer will test the mzML parser with random inputs to find panics."
echo ""

cargo +nightly fuzz run fuzz_mzml_streamer -- -max_total_time=${DURATION}

RESULT=$?

echo ""
echo "================================================================"
if [ $RESULT -eq 0 ]; then
    echo "✅ Fuzzing completed successfully - no crashes found!"
else
    echo "⚠️  Fuzzer exited with code $RESULT"
    echo "Check fuzz/artifacts/ for any crash reproductions"
fi
echo "================================================================"

exit $RESULT
