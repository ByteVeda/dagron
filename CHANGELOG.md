# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
