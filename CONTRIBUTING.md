# Drift — Contributing

## Prerequisites
- Rust 1.75+ (`rustup install stable`)
- Node.js 18+ (`nvm install 18`)
- wasm-pack (`cargo install wasm-pack`)
- Docker (for coordinator tests)

## Getting Started
```bash
./scripts/setup-dev.sh
```

## Project Structure
- `crates/drift-core/` — Core Rust engine
- `crates/drift-wasm/` — WASM browser bindings
- `crates/drift-coordinator-*/` — Coordinator implementations
- `crates/drift-cli/` — CLI tools
- `packages/` — TypeScript/React SDKs
- `docs/` — Specifications

## Running Tests
```bash
cargo test --workspace --lib        # Rust unit tests
npm test                            # TypeScript tests
```

## License
MIT
