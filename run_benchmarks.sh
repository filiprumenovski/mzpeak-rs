#!/bin/bash
# Run all Criterion benchmarks and generate reports
#
# Usage: ./run_benchmarks.sh [options]
#
# Options:
#   --quick    Run benchmarks in quick mode (for testing)
#   --baseline Save results as baseline for future comparison
#   --compare  Compare against saved baseline

set -e

QUICK_MODE=false
BASELINE=""
COMPARE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --baseline)
            BASELINE="--save-baseline baseline"
            shift
            ;;
        --compare)
            COMPARE="--baseline baseline"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "═══════════════════════════════════════════════════════════"
echo "  mzPeak Benchmark Suite"
echo "═══════════════════════════════════════════════════════════"
echo ""

if [ "$QUICK_MODE" = true ]; then
    echo "Running in QUICK mode (shorter sample times)..."
    echo ""
    cargo bench --benches -- --sample-size 10 $BASELINE $COMPARE
else
    echo "Running full benchmarks..."
    echo "This will take several minutes. Results will be saved to target/criterion/"
    echo ""
    cargo bench --benches $BASELINE $COMPARE
fi

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Benchmark Complete!"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "Results saved to: target/criterion/"
echo ""
echo "To view HTML reports:"
echo "  open target/criterion/report/index.html"
echo ""
echo "To compare with baseline:"
echo "  ./run_benchmarks.sh --compare"
echo ""
