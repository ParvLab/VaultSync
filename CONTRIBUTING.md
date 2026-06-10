# Contributing to Drift

Thank you for your interest in contributing to Drift! We welcome issues, feature requests, and pull requests from everyone.

---

## 📋 Prerequisites

Before setting up your workspace, ensure you have the following installed:

*   **Rust 1.75+** (`rustup install stable`)
*   **Node.js 18+** (NPM v9+)
*   **wasm-pack** (`cargo install wasm-pack`)
*   **Docker** (required for running coordinator tests locally)

---

## 🚀 Getting Started

1.  **Fork** the repository on GitHub.
2.  **Clone** your fork locally:
    ```bash
    git clone https://github.com/YOUR-USERNAME/drift.git
    cd drift
    ```
3.  Run the developer environment setup script:
    ```bash
    ./scripts/setup-dev.sh
    ```
    This script initializes dependencies, compiles NAPI/WASM files, and sets up configurations.

---

## 📂 Project Structure

*   [`crates/drift-core/`](crates/drift-core) — Core Rust sync engine
*   [`crates/drift-wasm/`](crates/drift-wasm) — WASM browser bindings
*   [`crates/drift-napi/`](crates/drift-napi) — Node-API native bindings
*   [`crates/drift-coordinator-*/`](crates/drift-coordinator-memory) — Coordinator protocol implementations
*   [`crates/drift-cli/`](crates/drift-cli) — Command-line tools
*   [`sdk/packages/`](sdk/packages) — TypeScript/React SDK packages (`web`, `node`, `react`, `next`)
*   [`sdk/examples/`](sdk/examples) — Ready-to-run demo apps
*   [`docs/`](docs) — Specifications

---

## 🎨 Coding Standards

### Linting & Formatting

We enforce strict linting and formatting gates in CI:

*   **Rust**:
    ```bash
    cargo fmt --check
    cargo clippy --workspace --all-targets -- -D warnings
    ```
*   **TypeScript / JavaScript**:
    ```bash
    npm run lint --prefix sdk
    npm run format:check --prefix sdk
    ```

Please run these commands locally before opening a pull request.

### Commit Conventions

We follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <description>

[optional body]
```

**Common Types**:
*   `feat`: A new user-facing feature.
*   `fix`: A bug fix.
*   `docs`: Changes to documentation or specifications.
*   `style`: Code style tweaks (whitespace, formatting, etc.).
*   `refactor`: Code change that neither fixes a bug nor adds a feature.
*   `test`: Adding or correcting tests.
*   `chore`: Internal build/infra adjustments.

---

## 🧪 Testing Checklist

Before opening a PR, ensure all tests pass cleanly:

```bash
# Run unit, integration, chaos, and E2E tests across all platforms
./scripts/run-all-tests.sh
```

If you introduce a new feature, you are expected to include corresponding unit or integration tests verifying the behavior.

---

## 📥 Pull Request Workflow

1.  Create a branch for your feature or fix:
    ```bash
    git checkout -b feat/your-awesome-feature
    ```
2.  Commit your changes following the [Commit Conventions](#commit-conventions).
3.  Push your branch to your fork:
    ```bash
    git push origin feat/your-awesome-feature
    ```
4.  Open a Pull Request against the `main` branch of the official Drift repository.
5.  Wait for the automated CI suite to run and pass.
6.  A maintainer will review your pull request shortly!

---

## 📄 License

By contributing to Drift, you agree that your contributions will be licensed under the Apache License, Version 2.0.
