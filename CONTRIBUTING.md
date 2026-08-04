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

## CI

`ci.yml` is an orchestrator, not a job list. It detects which paths a PR
touched and calls only the suites that can be affected:

| Suite | Workflow | Runs when |
| --- | --- | --- |
| Rust | `ci-rust.yml` | `crates/`, Cargo manifests |
| Python | `ci-python.yml` | the above, plus `dagron/`, `tests/python/`, `pyproject.toml` |
| Docs | `ci-docs.yml` | `docs/` |
| Release readiness | `ci-release.yml` | anything that affects a published artifact |

A Rust-only PR therefore never installs pnpm; a docs-only PR never compiles
the workspace. Pushes to `master` and `workflow_dispatch` runs ignore the
filter and run everything, so merged code is always covered in full.

Lint hooks stay defined once in `.pre-commit-config.yaml`. Each suite runs
`pre-commit` with a `SKIP` list of the hooks it does not own, so CI and your
local hooks can never disagree about which tool or version to run. The list is
an exclusion rather than an inclusion on purpose: a newly added hook runs in
every suite until someone skips it, which fails loudly instead of quietly
going unchecked.

Because every suite is skippable, branch protection cannot require them
individually — a skipped job reports nothing. **`CI status` is the single
required check**: it always runs and fails if anything it depends on did not
succeed or skip.

Shared setup lives in `.github/actions/` (`setup-rust`, `setup-python`,
`setup-node`, `pre-commit`, `verify-sdist`). `actionlint.yml` lints the
workflows and composite actions themselves.

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

   CI's `Release readiness` suite runs the same check on every PR, and the
   release workflow re-runs it with `--expect <tag>`.
2. Add the release section to `CHANGELOG.md`.
3. Merge to `master`, then push a bare version tag: `git tag 0.2.0 && git push origin 0.2.0`.

Both workflows trigger on the tag and are safe to re-run: each skips any
version already present on its registry.

After a successful crates.io publish, `publish-crates.yml` tags the commit it
released as `crates-v<version>` — a separate namespace from the version tags,
so the two never collide. The tag is only ever created, never moved: if
`crates-v<version>` already points somewhere else the job fails rather than
rewriting it. This makes `workflow_dispatch` runs traceable, since those are
not driven by a tag in the first place.

`crates-v*` is also accepted as a *trigger*, so pushing one releases the crates
on their own without touching PyPI — useful when only the Rust side needs
recutting. Both tag forms carry the same version; the workflow strips the
prefix before checking it. Note that a `crates-v*` tag pushed by the workflow
itself does not re-trigger it: GitHub suppresses workflow runs for refs pushed
with the default `GITHUB_TOKEN`.

The `Release readiness` suite also compiles the sdist in a clean environment
on every PR. That matters because the sdist — not the wheels — is what users on
unsupported platforms build from, and a green wheel job proves nothing about it.

### Registry credentials

Both registries use OIDC Trusted Publishing. **No registry token is stored in
the repository**, so there is nothing to leak or rotate: each job exchanges its
own OIDC identity for a credential valid only for that run.

- **PyPI** — the `pypi` environment, trusted publisher for `publish.yml`.
- **crates.io** — the `crates.io` environment. Each of `dagron-core` and
  `dagron-ui` needs a trusted publisher on its crates.io settings page
  pointing at `ByteVeda/dagron` / `publish-crates.yml` / environment
  `crates.io`. A crate with none configured fails to publish — the trust is
  per crate, not per repository.

Both crates should also have **Require trusted publishing for all new
versions** enabled, which makes crates.io reject API tokens outright. Note
that this closes the manual `cargo publish` break-glass path too: with it on,
a release can only go out through this workflow.

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
