#!/usr/bin/env bash
# publish.sh — Build and publish Drift crates and npm packages
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DRY_RUN=false
for arg in "$@"; do
  case $arg in
    --dry-run) DRY_RUN=true ;;
  esac
done

echo "================================================="
# Keep lines short in logs/outputs to avoid wrapping
echo "  Drift Release & Publication Utility"
echo "  Dry Run: $DRY_RUN"
echo "================================================="

cd "$ROOT_DIR"

# 1. Run all tests first to make sure everything passes
echo ""
echo ">>> Running test suite..."
./scripts/run-all-tests.sh --fast --no-e2e

# 2. Build WASM and npm packages
echo ""
echo ">>> Building WASM and SDK packages..."
cd "$ROOT_DIR/sdk"
npm run build
cd "$ROOT_DIR"

# 3. Publish NPM packages (topological order)
echo ""
echo ">>> Publishing NPM packages..."
PACKAGES=(
  "@drift/web"
  "@drift/node"
  "@drift/react"
  "@drift/next"
)

for pkg in "${PACKAGES[@]}"; do
  echo "    Publishing $pkg..."
  if [ "$DRY_RUN" = true ]; then
    echo "    [Dry-run] npm publish -w $pkg --access public"
  else
    npm publish -w "$pkg" --access public
  fi
done

# 4. Publish Rust crates (topological order)
# drift-core is root dependency
# drift-wasm/drift-coordinator-* depend on drift-core
# drift-cli depends on drift-core/coordinators
echo ""
echo ">>> Publishing Rust Crates..."
CRATES=(
  "crates/drift-core"
  "crates/drift-coordinator-memory"
  "crates/drift-coordinator-sqlite"
  "crates/drift-coordinator-postgres"
  "crates/drift-coordinator-redis"
  "crates/drift-coordinator-http"
  "crates/drift-coordinator-cf"
  "crates/drift-coordinator-server"
  "crates/drift-cli"
)

for crate in "${CRATES[@]}"; do
  echo "    Publishing $crate..."
  if [ "$DRY_RUN" = true ]; then
    cargo publish --manifest-path "$crate/Cargo.toml" --dry-run --jobs 1 --allow-dirty
  else
    cargo publish --manifest-path "$crate/Cargo.toml" --jobs 1
  fi
done

echo ""
echo "================================================="
echo "  ✅ Publication finished successfully!"
echo "================================================="
