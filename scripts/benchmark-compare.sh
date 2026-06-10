#!/usr/bin/env bash
# benchmark-compare.sh — Compare Criterion benchmarks against a baseline branch
# Usage: ./scripts/benchmark-compare.sh [--baseline <branch>] [--threshold <pct>]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BASELINE_BRANCH="main"
THRESHOLD=10   # fail if regression > 10%

for arg in "$@"; do
  case $arg in
    --baseline) BASELINE_BRANCH="$2"; shift ;;
    --threshold) THRESHOLD="$2"; shift ;;
  esac
done

echo "================================================="
echo "  Drift — Benchmark Comparison"
echo "  Baseline: $BASELINE_BRANCH  |  Threshold: ${THRESHOLD}%"
echo "================================================="

cd "$ROOT_DIR"

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
BASELINE_DIR="$ROOT_DIR/target/criterion-baseline"

# ── Run baseline benchmarks ───────────────────────────────────────────────────
echo ""
echo ">>> [1/3] Running baseline benchmarks on branch: $BASELINE_BRANCH..."
git stash --quiet || true
git checkout "$BASELINE_BRANCH" --quiet
cargo bench --jobs 1 -- --save-baseline baseline 2>&1
git checkout "$CURRENT_BRANCH" --quiet
git stash pop --quiet || true
echo "    ✅ Baseline captured"

# ── Run current benchmarks ────────────────────────────────────────────────────
echo ""
echo ">>> [2/3] Running current benchmarks..."
cargo bench --jobs 1 -- --baseline baseline 2>&1
echo "    ✅ Current benchmarks captured"

# ── Generate comparison report ────────────────────────────────────────────────
echo ""
echo ">>> [3/3] Generating comparison report..."
REPORT_FILE="$ROOT_DIR/target/benchmark-comparison-$(date +%Y%m%d-%H%M%S).txt"
cargo bench --jobs 1 -- --baseline baseline --color never 2>&1 | tee "$REPORT_FILE"

# Check for regressions above threshold
if grep -q "Performance has regressed" "$REPORT_FILE"; then
  echo ""
  echo "⚠️  Performance regressions detected. Review: $REPORT_FILE"
  exit 1
fi

echo ""
echo "================================================="
echo "  ✅ No regressions above ${THRESHOLD}% threshold"
echo "  Report saved to: $REPORT_FILE"
echo "================================================="
