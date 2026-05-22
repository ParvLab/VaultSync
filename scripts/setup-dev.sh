#!/bin/bash
set -euo pipefail

echo "=== Drift Development Environment Setup ==="

# Rust toolchain
echo "[1/3] Installing Rust toolchain..."
rustup target add wasm32-unknown-unknown

# wasm-pack
echo "[2/3] Installing wasm-pack..."
if ! command -v wasm-pack &> /dev/null; then
    curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
fi

echo "[3/3] Verifying build..."
cargo check --workspace
echo ""
echo "=== Setup complete ==="
echo "Run 'cargo test --workspace --lib' to run unit tests"
