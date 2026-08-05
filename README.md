<div align="center">

# dagron

**A fast, Rust-backed DAG engine for Python.**

[![CI](https://github.com/ByteVeda/dagron/actions/workflows/ci.yml/badge.svg)](https://github.com/ByteVeda/dagron/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dagron?label=pypi)](https://pypi.org/project/dagron/)
[![crates.io](https://img.shields.io/crates/v/dagron-core?label=crates.io)](https://crates.io/crates/dagron-core)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-3776ab?logo=python&logoColor=white)](https://python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/ByteVeda/dagron/blob/master/LICENSE)

Build, execute, and analyze directed acyclic graphs with a fluent Python API — powered by Rust and [petgraph](https://github.com/petgraph/petgraph) under the hood.

**[Documentation](https://docs.byteveda.org/dagron/)** · [Getting started](https://docs.byteveda.org/dagron/guide/getting-started) · [Cookbook](https://docs.byteveda.org/dagron/guide/cookbook) · [API reference](https://docs.byteveda.org/dagron/api)

</div>

---

## Install

```bash
pip install dagron
```

Python 3.12+. Prebuilt wheels for Linux, macOS, and Windows.

## Quick start

Write a normal Python function; the call structure becomes the DAG.

```python
import dagron

@dagron.task
def extract():
    return [1, 2, 3]

@dagron.task
def transform(rows):
    return [r * 2 for r in rows]

@dagron.flow
def pipeline():
    return transform(extract())

result = pipeline()               # ExecutionResult
result["transform"].result        # [2, 4, 6]
pipeline.dag()                    # the underlying DAG, for analysis
```

Or build the graph explicitly and map callables onto it:

```python
from dagron import DAGBuilder, DAGExecutor

dag = (
    DAGBuilder()
    .add_node("extract")
    .add_node("transform")
    .add_node("load")
    .add_edge("extract", "transform")
    .add_edge("transform", "load")
    .build()                      # rejects cycles at build time
)

result = DAGExecutor(dag, max_workers=4).execute({
    "extract": fetch_data,
    "transform": clean_data,
    "load": write_to_db,
})

result.succeeded                  # 3
result["extract"].result          # return value of fetch_data()
```

Swap in `AsyncDAGExecutor` for `asyncio` workloads. See
[executing tasks](https://byteveda.github.io/dagron/guide/core-concepts/executing-tasks).

## What's in it

| | |
| --- | --- |
| **[Building graphs](https://byteveda.github.io/dagron/guide/core-concepts/building-dags)** | Fluent builder, payloads and metadata, weighted edges, bulk insert, `from_records`. Cycles rejected on insertion, so every `DAG` is acyclic by construction. |
| **[Ordering & scheduling](https://byteveda.github.io/dagron/guide/core-concepts/inspecting-graphs)** | Kahn and DFS topological sorts, level grouping, priority ordering, all-orderings enumeration, execution plans, critical path, cost-based schedules. |
| **[Execution](https://byteveda.github.io/dagron/guide/core-concepts/executing-tasks)** | Thread-pool and `asyncio` executors with fail-fast, per-node timeouts, cancellation, lifecycle callbacks, and tracing. |
| **[Execution strategies](https://byteveda.github.io/dagron/guide/execution-strategies/incremental)** | Incremental re-execution with early cutoff, content-addressable caching, checkpointing, conditional branches, dynamic mid-run expansion, approval gates, resource-aware scheduling, graph partitioning, distributed backends. |
| **[Typed & reactive](https://byteveda.github.io/dagron/typed-and-reactive)** | Stable `NodeRef` handles, generic `NodeResult[T]`, stub generation for statically-typed string lookups, effect tags, a Solid.js-style reactive engine, and time-travel `replay(at=t)`. |
| **[Analysis](https://byteveda.github.io/dagron/guide/core-concepts/transforms)** | Transforms (reverse, collapse, filter, merge, transitive reduction/closure, dominator tree), subgraph and path queries, O(1) reachability index, regex/glob node matching, stats and diffing. |
| **[Observability](https://byteveda.github.io/dagron/guide/observability/tracing-profiling)** | Per-node timing exported to Chrome Tracing, bottleneck detection, ASCII and Graphviz rendering, inline SVG in Jupyter. |
| **[Extending](https://byteveda.github.io/dagron/guide/advanced/plugins-hooks)** | Plugins via `entry_points`, a lifecycle hook registry, parameterized DAG templates, custom serializers and executors. |

Benchmarks against networkx: [guide/benchmarks](https://byteveda.github.io/dagron/guide/benchmarks). Why dagron: [guide/why-dagron](https://byteveda.github.io/dagron/guide/why-dagron).

## Rust crates

The engine is usable directly from Rust:

| Crate | |
| --- | --- |
| [`dagron-core`](https://crates.io/crates/dagron-core) | Graph construction, analysis, and scheduling |
| [`dagron-ui`](https://crates.io/crates/dagron-ui) | Live web dashboard for DAG execution |

## Contributing

See [CONTRIBUTING.md](https://github.com/ByteVeda/dagron/blob/master/CONTRIBUTING.md).

## License

[MIT](https://github.com/ByteVeda/dagron/blob/master/LICENSE)
