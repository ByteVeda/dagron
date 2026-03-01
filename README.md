<div align="center">

# dagron

**A fast, Rust-backed DAG engine for Python.**

[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-3776ab?logo=python&logoColor=white)](https://python.org)
[![Built with Rust](https://img.shields.io/badge/built%20with-Rust-dea584?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![v0.1.0](https://img.shields.io/badge/version-0.1.0-blue)](https://pypi.org/project/dagron/)

Build, execute, and analyze directed acyclic graphs with a fluent Python API — powered by Rust and [petgraph](https://github.com/petgraph/petgraph) under the hood.

</div>

---

## Installation

```bash
pip install dagron
```

## Quick start

### Build a DAG

Use the fluent `DAGBuilder` to construct graphs with payloads, metadata, and weighted edges:

```python
from dagron import DAGBuilder

dag = (
    DAGBuilder()
    .add_node("a", payload=1)
    .add_node("b", payload=2)
    .add_node("c", payload=3)
    .add_edge("a", "b")
    .add_edge("a", "c")
    .add_edge("b", "c")
    .build()  # validates acyclicity at build time
)

dag.node_count()       # 3
dag.get_payload("a")   # 1
```

### Execute tasks

Map callables to nodes and execute them in dependency order with automatic parallelism:

```python
from dagron import DAGBuilder, DAGExecutor

dag = (
    DAGBuilder()
    .add_node("extract")
    .add_node("transform")
    .add_node("load")
    .add_edge("extract", "transform")
    .add_edge("transform", "load")
    .build()
)

tasks = {
    "extract":   lambda: fetch_data(),
    "transform": lambda: clean_data(),
    "load":      lambda: write_to_db(),
}

result = DAGExecutor(dag, max_workers=4).execute(tasks)
# result.succeeded  -> 3
# result.node_results["extract"].result  -> return value of fetch_data()
```

### Async execution

Native `asyncio` support for I/O-bound workflows:

```python
import asyncio
from dagron import DAGBuilder, AsyncDAGExecutor

dag = (
    DAGBuilder()
    .add_node("fetch_users")
    .add_node("fetch_orders")
    .add_node("merge")
    .add_edge("fetch_users", "merge")
    .add_edge("fetch_orders", "merge")
    .build()
)

async def main():
    tasks = {
        "fetch_users":  lambda: fetch("/users"),
        "fetch_orders": lambda: fetch("/orders"),
        "merge":        lambda: merge_results(),
    }
    result = await AsyncDAGExecutor(dag).execute(tasks)
    print(result.succeeded)  # 3

asyncio.run(main())
```

## Features

| | Feature | Description |
|---|---|---|
| 🛡️ | **Cycle detection** | Invalid graphs are rejected at build time |
| 🔄 | **Topological sorting** | Deterministic, dependency-aware execution order |
| ⚡ | **Parallel scheduling** | Thread-pool executor with configurable workers and cost-based scheduling |
| 🌐 | **Async execution** | Native `asyncio` support via `AsyncDAGExecutor` |
| ♻️ | **Incremental recomputation** | Re-run only what changed with `IncrementalExecutor` |
| 🔔 | **Execution callbacks** | Hook into `on_start`, `on_complete`, and `on_error` events |
| 📊 | **Profiling & tracing** | `profile_execution` and `ExecutionTrace` for performance analysis |
| 💾 | **Serialization** | Import/export graphs to JSON and binary formats |
| 🔍 | **Reachability index** | Fast ancestor/descendant queries |
| ✂️ | **Graph transforms** | Subgraph extraction, diffing, and statistics |
| 📓 | **Jupyter support** | Inline SVG rendering in notebooks |

## Requirements

- Python >= 3.12

## License

[MIT](LICENSE)
