#!/usr/bin/env bash
# run-all-tests.sh — Run the full Drift test suite locally
# Usage: ./scripts/run-all-tests.sh [--fast] [--no-e2e] [--no-docker]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

FAST=false
NO_E2E=false
NO_DOCKER=false

for arg in "$@"; do
  case $arg in
    --fast)     FAST=true ;;
    --no-e2e)   NO_E2E=true ;;
    --no-docker) NO_DOCKER=true ;;
  esac
done

echo "================================================="
echo "  Drift — Full Test Suite"
echo "================================================="

cd "$ROOT_DIR"

# ── Step 1: Rust unit + integration tests ─────────────────────────────────────
echo ""
echo ">>> [1/5] Rust unit & integration tests..."
if [ "$FAST" = true ]; then
  cargo test --jobs 1 --lib --workspace --exclude drift-fuzz 2>&1
else
  cargo test --jobs 1 --workspace --exclude drift-fuzz 2>&1
fi
echo "    ✅ Rust tests passed"

# ── Step 2: CRDT property tests (must pass before anything else) ──────────────
echo ""
echo ">>> [2/5] CRDT property-based tests..."
cargo test --jobs 1 -p drift-core --test crdt_properties 2>&1
echo "    ✅ CRDT property tests passed"

# ── Step 3: Coordinator conformance (SQLite + Memory, no Docker required) ─────
echo ""
echo ">>> [3/5] Coordinator conformance tests..."
if [ "$NO_DOCKER" = true ]; then
  echo "    Skipping Postgres/Redis (--no-docker). Running SQLite + Memory only."
  DRIFT_TEST_BACKENDS=sqlite,memory cargo test --jobs 1 -p drift-conformance 2>&1
else
  cargo test --jobs 1 -p drift-conformance 2>&1
fi
echo "    ✅ Coordinator conformance passed"

# ── Step 4: TypeScript SDK tests ──────────────────────────────────────────────
echo ""
echo ">>> [4/5] TypeScript SDK tests..."
cd "$ROOT_DIR/sdk"
npm test --workspaces --if-present 2>&1
cd "$ROOT_DIR"
echo "    ✅ TypeScript SDK tests passed"

# ── Step 5: Playwright E2E tests ──────────────────────────────────────────────
if [ "$NO_E2E" = true ]; then
  echo ""
  echo ">>> [5/5] E2E tests skipped (--no-e2e)"
else
  echo ""
  echo ">>> [5/5] Playwright E2E tests..."
  npm run e2e --prefix sdk 2>&1
  echo "    ✅ E2E tests passed"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "================================================="
echo "  ✅ All tests passed!"
echo "================================================="
