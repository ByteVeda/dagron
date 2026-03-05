# Contributing to dagron

Thank you for your interest in contributing to dagron! This guide will help you get set up and submit your first PR.

## Prerequisites

- **Rust** 1.70+ (`rustup` recommended)
- **Python** 3.12+
- **maturin** (`pip install maturin`)
- **Node.js** 18+ (for docs only)
- **uv** (recommended for Python dependency management)

## Development Setup

```bash
# Clone the repo
git clone https://github.com/pratyush618/dagron.git
cd dagron

# Create a virtual environment and install dev dependencies
python -m venv .venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Build the Rust extension in development mode
maturin develop

# Verify the build
python -c "import dagron; print(dagron.DAG())"
```

## Running Tests

```bash
# Rust tests
cargo test

# Python tests (excludes benchmarks by default)
uv run pytest tests/python/ --ignore=tests/python/test_benchmarks.py

# Python benchmarks (requires bench dependency group)
uv pip install pytest-benchmark networkx
uv run pytest tests/python/test_benchmarks.py --benchmark-only

# Rust benchmarks (Criterion)
cargo bench --bench graph_bench
```

## Code Style

**Python:**
```bash
ruff check py_src/ tests/
ruff format py_src/ tests/
```

**Rust:**
```bash
cargo fmt --all
cargo clippy --all-targets --all-features
```

## Building Docs

```bash
cd docs
npm install
npm run build    # production build (checks broken links)
npm start        # local dev server at http://localhost:3000
```

## PR Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Make your changes
4. Run tests: `cargo test && uv run pytest tests/python/`
5. Run linters: `ruff check . && cargo fmt --check && cargo clippy`
6. Commit with a descriptive message
7. Push and open a PR against `master`

## Project Structure

```
dagron/
  crates/
    dagron-core/     # Rust core: graph, algorithms, serialization
    dagron-py/       # PyO3 bindings
    dagron-ui/       # Optional Axum web dashboard
  py_src/dagron/     # Python package: execution strategies, builder, analysis
  tests/
    python/          # Python test suite + benchmarks
  docs/              # Docusaurus documentation site
    pages/           # MDX documentation pages
```

## Where to Contribute

- **Rust core** (`crates/dagron-core/`) — algorithms, performance, new graph operations
- **Python API** (`py_src/dagron/`) — execution strategies, builder ergonomics, analysis tools
- **PyO3 bindings** (`crates/dagron-py/`) — exposing new Rust functionality to Python
- **Documentation** (`docs/pages/`) — guides, API docs, examples
- **Benchmarks** (`tests/python/test_benchmarks.py`, `crates/dagron-core/benches/`) — new benchmark scenarios, performance regression tracking
- **Bug reports & feature requests** — open an issue on GitHub
