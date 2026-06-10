#!/bin/bash
set -euo pipefail

echo "=========================================="
echo "=== VaultSync Developer Environment Setup  ==="
echo "=========================================="

# 1. Rust tools
echo -e "\n[1/6] Installing Rust targets and tools..."
rustup target add wasm32-unknown-unknown

echo "Installing cargo-fuzz..."
if ! cargo fuzz --version &> /dev/null; then
    cargo install cargo-fuzz
else
    echo "  cargo-fuzz already installed"
fi

echo "Installing cargo-tarpaulin (for coverage)..."
if ! cargo tarpaulin --version &> /dev/null; then
    cargo install cargo-tarpaulin
else
    echo "  cargo-tarpaulin already installed"
fi

# 2. wasm-pack
echo -e "\n[2/6] Installing wasm-pack..."
if ! command -v wasm-pack &> /dev/null; then
    curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
else
    echo "  wasm-pack already installed"
fi

# 3. Playwright & Node dependencies
echo -e "\n[3/6] Setting up Javascript SDK & Playwright..."
if command -v npm &> /dev/null; then
    npm ci --prefix sdk
    npx --prefix sdk playwright install --with-deps chromium
else
    echo "Warning: npm not found. Skipping Javascript/Playwright install."
fi

# 4. Git Hooks
echo -e "\n[4/6] Setting up Git pre-commit hooks..."
HOOKS_DIR=".git/hooks"
if [ -d "$HOOKS_DIR" ]; then
    cat > "$HOOKS_DIR/pre-commit" << 'EOF'
#!/bin/sh
echo "=== VaultSync pre-commit hook ==="
echo "Running rustfmt..."
cargo fmt --check || exit 1
echo "Running clippy..."
cargo clippy --workspace --all-targets -- -D warnings || exit 1
echo "All pre-commit checks passed!"
EOF
    chmod +x "$HOOKS_DIR/pre-commit"
    echo "  pre-commit hook installed successfully"
else
    echo "Warning: .git/hooks directory not found. Skipping hooks setup."
fi

# 5. Environment Config
echo -e "\n[5/6] Initializing .env file..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "  Created .env from .env.example"
else
    echo "  .env already exists"
fi

# 6. Docker
echo -e "\n[6/6] Starting Docker development services..."
if command -v docker-compose &> /dev/null; then
    docker-compose up -d
elif command -v docker &> /dev/null; then
    docker compose up -d
else
    echo "Warning: docker or docker-compose not found. Skipping service initialization."
fi

echo -e "\n=========================================="
echo "=== Setup Complete!                    ==="
echo "=========================================="
echo "Run: 'cargo test --workspace --jobs 1' to verify tests"
echo "Run: 'npm run e2e --prefix sdk' to run E2E browser tests"
