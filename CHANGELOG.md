# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- **`dagron-core` and `dagron-ui` are published to crates.io.** A new
  `publish-crates.yml` workflow releases both from the same version tag that
  drives the PyPI release. Re-running a release skips versions already on the
  registry. `dagron-py` stays `publish = false` — it exists only to be
  compiled into the wheel.
- **`Release readiness` CI suite.** Every PR now verifies that the Cargo,
  `pyproject.toml`, and `dagron.__version__` declarations agree
  (`scripts/check_versions.py`), that both crates pass `cargo publish
  --dry-run`, and that the sdist compiles and imports in a clean environment.
- **Path-filtered CI.** `ci.yml` became an orchestrator that detects which
  paths a PR touched and calls only the affected suites (`ci-rust.yml`,
  `ci-python.yml`, `ci-docs.yml`, `ci-release.yml`). A Rust-only PR no longer
  installs pnpm; a docs-only PR no longer compiles the workspace. Pushes to
  `master` ignore the filter and run everything. `CI status` is the single
  required check, since skipped jobs report nothing to branch protection.
  Shared setup moved into composite actions, and `actionlint.yml` now lints
  the workflows themselves.

### Fixed

- **The sdist could not be built from source.** `maturin` prunes `dagron-ui`
  from the sdist because it is an optional dependency, but `dagron-py` still
  declares the path dependency, so `cargo metadata` failed on a missing
  manifest. Anyone installing on a platform without a prebuilt wheel hit this.
  The crate is now shipped in the sdist, and both CI and the release workflow
  compile the sdist in a clean environment to keep it that way.

### Changed

- **The Python package moved from `py_src/dagron/` to `dagron/` at the repo
  root.** No import or packaging change for users — the wheel and sdist
  contents are unchanged.
- Shared crate metadata (version, edition, MSRV, license, repository) now lives
  in `[workspace.package]` instead of being repeated in each crate manifest.
- `dagron-ui` gained the package metadata crates.io requires, and both
  published crates now ship a README and LICENSE.

## [0.1.1] - 2026-05-10

### Added

- **Typed `NodeRef` handles** — `dag.add_node()` now returns a stable `NodeRef`. Every public method that takes a node identifier accepts either a `str` name or a `NodeRef`, so existing string-based code keeps working unchanged. NodeRefs survive unrelated graph mutations and detect remove-then-readd via per-node epochs (`StaleNodeRefError`).
- **`@dagron.flow` Pythonic compose API** — Tawazi-style: write a regular Python function that calls `@task`-decorated tasks; the call structure becomes the DAG. `pipeline.dag()` returns the underlying DAG; `pipeline()` runs it. Compatible with the legacy parameter-name-based `Pipeline` (the same `@task` decorator powers both).
- **Generic typing + `dagron.stubgen`** — `FlowFuture[T]`, `NodeResult[T]`, `ExecutionResult.__getitem__` overloads typed by `FlowFuture[T]`. `dagron.stubgen.generate_stub(dag, types)` emits a `.pyi` with `Literal["nodename"] -> NodeResult[T]` overloads, so even string-keyed lookups become statically typed. `@task` is a passthrough decorator with `[**P, R]` ParamSpec — IDE autocomplete and mypy both work.
- **Effect-typed nodes** — `dagron.Effect` enum (`PURE`/`READ`/`WRITE`/`NETWORK`/`NONDETERMINISTIC`) with `is_cacheable`/`is_deterministic`/`is_isolated` properties. `@task(effect=Effect.NETWORK)` tags impurity. AST-scan heuristic emits a `UserWarning` when a `PURE` task contains obviously-impure calls (`time.time`, `random.*`, `os.*`, etc.). `effects_of(dag)` reads tags back from DAG metadata. New `DAGExecutor(enforce_effect_isolation=True)` flag serializes `NONDETERMINISTIC` tasks while letting other effects parallelize freely.
- **`dagron.reactive` — Solid.js / Jane-Street-`Incremental` style reactive engine** — `Signal` / `Computed` / `Watcher` with auto-tracked dependencies. Mutating one signal that feeds 10,000 derived nodes and reading just one of them takes ~10 µs on the recompute path. `batch()` context manager guarantees glitch-free updates: multiple signal mutations coalesce into a single watcher fire. Distinct from the existing `dagron.execution.reactive.ReactiveDAG` (which wraps a pre-built DAG); the new module is for building reactive graphs from scratch.
- **`dagron.contentcache` — Nix-flake-style cross-process cache** — `ContentCache` stores cached values keyed by their content fingerprint. The filesystem itself is the index — independent processes (CI workers, two terminals) share intermediates without coordination. Atomic temp-file + rename writes, magic-byte header, sharded layout `<aa>/<bb>/<rest>.cache`. Pluggable `Hasher` protocol with `default_hash` (pickle + blake2b) and `numpy_hash` (uses `array.tobytes()`). `compute_or_cached` is effect-aware and skips the cache for `WRITE`/`NETWORK`/`NONDETERMINISTIC` automatically. Honors `$DAGRON_CACHE_DIR`.
- **`dagron.trace` — time-travel debugging with `replay(at=t)`** — `TraceWriter` appends per-node JSONL records; payloads are stored in the `ContentCache` keyed by output fingerprint, so identical values across runs deduplicate. `TraceReader` reads back. `replay(source, at=t)` reconstructs the per-node `ReplayedNode` state at any past wall-clock instant. Pure / READ nodes replay byte-identically; impure nodes are flagged `replayable=False` but their logged values are still surfaced. Re-recorded nodes (retries) take the latest value up to the cutoff. Honors `$DAGRON_TRACE_DIR`.

### Changed

- `DAG.add_node()` now returns `NodeRef` instead of `NodeId`. `NodeId` is still returned by enumeration methods (`nodes()`, `successors()`, `roots()`, …) where a snapshot identifier is appropriate.
- `NodeData::name` is now `Arc<str>` (was `String`) — cheaper to share between handles.
- `DAGExecutor.execute` and `AsyncDAGExecutor.execute` accept `Mapping[str | NodeRef, Callable]` for the `tasks` parameter.
- `ExecutionResult.__getitem__` and `__contains__` accept `str`, `NodeRef`, or `FlowFuture[T]`.
- `NodeResult` is now `NodeResult[T]` (PEP 695 generic). Existing references default to `NodeResult[Any]` and remain backwards compatible.
- `@task` is now flow-aware: outside a `@flow` body it executes normally; inside one it records the call and returns `FlowFuture[T]`. The same decorator works for both the legacy `Pipeline` and the new `@flow` API.

### Docs

- Migrated documentation site from Docusaurus 3 to Fumadocs 16 (Next.js 16 + Tailwind v4 + pnpm). Same deploy URL (`byteveda.github.io/dagron/`). Three sidebar root sections: **Guide**, **Typed & Reactive**, **API Reference**. New reusable component library: `ui/` primitives, MDX globals (`DagDiagram`, `StatusBadge`, `EffectBadge`, `FeatureCard`/`Grid`, `ApiSignature`, `ParamTable`), and a client-side themed Mermaid component.

## [0.1.0] - 2026-03-06

### Added

- **Core DAG engine** — Rust-backed directed acyclic graph built on petgraph with O(1) node lookups via AHashMap
- **Builder pattern** — fluent `DAG.builder()` API, `from_records()`, and `Pipeline` / `@task` decorator
- **Parallel execution** — thread-pool and async executors with topological scheduling
- **Incremental execution** — early-cutoff recomputation that only re-executes changed nodes and their dependents
- **Caching** — content-addressable Merkle-tree caching with pluggable backends
- **Checkpointing** — save execution progress to disk and resume after failures
- **Conditional execution** — predicate-gated edges that skip branches at runtime
- **Dynamic DAGs** — expand the graph at runtime based on node results
- **Approval gates** — human-in-the-loop gates that pause execution until approved
- **Resource-aware scheduling** — CPU, GPU, and memory slot scheduling with blocking acquire/release
- **Distributed execution** — pluggable backends for Ray and Celery
- **Tracing & profiling** — Chrome-compatible execution traces and critical-path analysis
- **Graph analysis** — explain nodes, what-if analysis, lineage tracking, linting, and query DSL
- **Contracts** — type contracts across edges validated at build time
- **Templates** — parameterized DAG templates with placeholder expansion
- **Versioning** — append-only mutation log with time-travel, diffing, and forking
- **DataFrame support** — schema validation at edge boundaries for pandas/polars pipelines
- **Plugins & hooks** — event-driven plugin system with hook registry and auto-discovery
- **Visualization** — ASCII, SVG, Mermaid, and live web dashboard (Axum + SSE)
- **Serialization** — JSON, bincode, and DOT export
- **Generational cache** — automatic invalidation of cached results on graph mutations
- **Python 3.12+** support on Linux (x86_64, aarch64), macOS (x86_64, Apple Silicon), Windows (x86_64)
