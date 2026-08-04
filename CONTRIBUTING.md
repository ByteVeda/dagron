# Contributing to dagron

Thank you for your interest in contributing to dagron! This guide will help you get set up and submit your first PR.

## Prerequisites

- **Rust** 1.83+ (`rustup` recommended) — matches `rust-version` in `[workspace.package]`
- **Python** 3.12+
- **maturin** (`pip install maturin`)
- **Node.js** 18+ (for docs only)
- **uv** (recommended for Python dependency management)

## Development Setup

```bash
# Clone the repo
git clone https://github.com/ByteVeda/dagron.git
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
ruff check dagron/ tests/
ruff format dagron/ tests/
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

## Releasing

`dagron` ships to two registries from a single tag:

| Artifact | Registry | Workflow |
| --- | --- | --- |
| `dagron` wheels + sdist | [PyPI](https://pypi.org/project/dagron/) | `publish.yml` |
| `dagron-core`, `dagron-ui` | [crates.io](https://crates.io/crates/dagron-core) | `publish-crates.yml` |

`dagron-py` is `publish = false` — it exists only to be compiled into the wheel.

To cut a release:

1. Bump the version in `[workspace.package]` of the root `Cargo.toml`,
   `pyproject.toml`, and `dagron/__init__.py`, then confirm they agree:

   ```bash
   python scripts/check_versions.py
   ```

   CI's `publish-readiness` job runs the same check on every PR, and the
   release workflow re-runs it with `--expect <tag>`.
2. Add the release section to `CHANGELOG.md`.
3. Merge to `master`, then push a bare version tag: `git tag 0.2.0 && git push origin 0.2.0`.

Both workflows trigger on the tag and are safe to re-run: each skips any
version already present on its registry.

The `publish-readiness` CI job also compiles the sdist in a clean environment
on every PR. That matters because the sdist — not the wheels — is what users on
unsupported platforms build from, and a green wheel job proves nothing about it.

### Registry credentials

Each workflow runs in a GitHub environment that holds its own credential, so
neither is reachable from an ordinary PR build.

- **PyPI** — the `pypi` environment, using OIDC Trusted Publishing. No stored
  token.
- **crates.io** — the `crates.io` environment, using a `CARGO_TOKEN` secret
  (a crates.io API token with publish scope for `dagron-core` and
  `dagron-ui`).

crates.io also supports OIDC Trusted Publishing, which would remove the stored
token. It can only be configured for a crate that already exists, so it is a
worthwhile follow-up once the first version of each crate is published: add a
trusted publisher on each crate's settings page pointing at `ByteVeda/dagron` /
`publish-crates.yml` / environment `crates.io`, then swap the `Publish crates`
step over to `rust-lang/crates-io-auth-action` and drop the secret.

## Project Structure

```
dagron/
  crates/
    dagron-core/     # Rust core: graph, algorithms, serialization
    dagron-py/       # PyO3 bindings
    dagron-ui/       # Optional Axum web dashboard
  dagron/            # Python package: execution strategies, builder, analysis
  tests/
    python/          # Python test suite + benchmarks
  docs/              # Docusaurus documentation site
    pages/           # MDX documentation pages
```

## Where to Contribute

- **Rust core** (`crates/dagron-core/`) — algorithms, performance, new graph operations
- **Python API** (`dagron/`) — execution strategies, builder ergonomics, analysis tools
- **PyO3 bindings** (`crates/dagron-py/`) — exposing new Rust functionality to Python
- **Documentation** (`docs/pages/`) — guides, API docs, examples
- **Benchmarks** (`tests/python/test_benchmarks.py`, `crates/dagron-core/benches/`) — new benchmark scenarios, performance regression tracking
- **Bug reports & feature requests** — open an issue on GitHub
