<div align="center">

# dagron

**A fast, Rust-backed DAG engine for Python.**

[![CI](https://github.com/ByteVeda/dagron/actions/workflows/ci.yml/badge.svg)](https://github.com/ByteVeda/dagron/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-3776ab?logo=python&logoColor=white)](https://python.org)
[![Built with Rust](https://img.shields.io/badge/built%20with-Rust-dea584?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![v0.1.0](https://img.shields.io/badge/version-0.1.0-blue)](https://pypi.org/project/dagron/)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://byteveda.github.io/dagron/)

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

### Graph Construction

Create DAGs with `DAG()` or the fluent `DAGBuilder`. Add nodes with payloads and metadata, weighted edges, and bulk-insert via `add_nodes`/`add_edges`. Build graphs from tabular data with `from_records`.

### Cycle Detection & Validation

Cycles are automatically rejected on edge insertion, so every `DAG` is acyclic by construction. Call `validate()` for an explicit structural health-check at any time.

### Topological Sorting

Multiple algorithms to suit different needs: Kahn's (BFS), DFS, level-based grouping, priority-weighted ordering, and full enumeration of all valid orderings. Lazy iterators are available for memory-efficient traversal of large graphs.

### Scheduling & Execution Plans

Generate dependency-aware execution plans with `execution_plan` and `execution_plan_constrained`. Identify the `critical_path` through weighted graphs and produce cost-based schedules for resource-constrained environments.

### Execution Engines

`DAGExecutor` runs tasks in a thread pool with configurable workers, while `AsyncDAGExecutor` provides native `asyncio` support for I/O-bound workflows. Both support fail-fast error handling, per-node timeouts, cancellation, `on_start`/`on_complete`/`on_error` callbacks, and optional hook integration.

### Incremental Computation

`IncrementalExecutor` tracks a dirty set and re-executes only the nodes affected by changes. Early cutoff skips downstream work when a node's output hasn't changed, and change provenance records why each node was recomputed.

### Graph Transforms

Transform graphs with `reverse`, `collapse`, `filter`, `merge`, `transitive_reduction`, `transitive_closure`, and `dominator_tree`. Take immutable snapshots with `snapshot` for safe concurrent reads.

### Subgraph & Path Algorithms

Extract subgraphs by node set or by depth from a root. Compute `all_paths`, `shortest_path`, and `longest_path` between any two nodes.

### Reachability

`ReachabilityIndex` precomputes a compressed bitset index for O(1) ancestor/descendant queries. Use `is_ancestor` for quick relationship checks without repeated traversal.

### Introspection

Query predecessors, successors, ancestors, and descendants of any node. Inspect in/out degree, roots, and leaves. Lazy iterators keep memory usage low on large graphs. Full Python protocol support: `len`, `in`, `[]`, `iter`, and `bool`.

### Node Matching

Find nodes by name using regex or glob patterns — useful for selecting groups of related nodes in large graphs.

### Statistics & Diffing

`GraphStats` computes density, depth, width, connectivity metrics, and more. `GraphDiff` compares two DAGs and reports added, removed, and changed nodes and edges.

### Serialization

Export and import graphs as JSON, binary (bincode + memory-mapped files), Graphviz DOT, or Mermaid diagrams. Save to and load from files in any supported format.

### Tracing & Profiling

`ExecutionTrace` records per-node timing and exports to Chrome Tracing format for visualization. `profile_execution` identifies the critical path and detects bottleneck nodes.

### Visualization

ASCII `pretty_print` renders graphs in vertical or horizontal layout directly in the terminal. Jupyter notebooks get inline SVG rendering via Graphviz, DOT, or a built-in fallback renderer.

### DAG Templates

Define parameterized DAG blueprints with `DAGTemplate` and `{{placeholder}}` substitution. Render concrete DAGs, builders, or pipelines by supplying parameter values at runtime. Supports type validation, default values, custom validators, and configurable delimiters.

### Plugin & Hook System

Extend dagron with `DagronPlugin` subclasses discovered via `entry_points`. `HookRegistry` fires lifecycle events (`PRE_EXECUTE`, `POST_EXECUTE`, `PRE_NODE`, `POST_NODE`, `ON_ERROR`, `PRE_BUILD`, `POST_BUILD`) with priority ordering. Includes registries for custom serializers, executors, and node types.

### Approval Gates

`GateController` pauses execution at designated nodes and waits for manual approval or rejection. Thread-safe with both sync and async support, configurable timeouts, and integration with execution callbacks and tracing.

### Dynamic DAG Modification

`DynamicExecutor` adds or removes nodes mid-execution based on runtime results. Expander callbacks receive a node's output and return `DynamicModification` specs. Operates on a runtime snapshot so the original DAG stays immutable.

### Resource-Aware Scheduling

Nodes declare `ResourceRequirements` (GPU, CPU, memory) and `ResourcePool` enforces capacity constraints. `ResourceAwareExecutor` and `AsyncResourceAwareExecutor` use bottom-level priority scheduling to dispatch the highest-value ready node that fits available resources.

### Graph Partitioning

Split large DAGs into balanced partitions with three Rust-native algorithms: level-based grouping, cost-balanced assignment, and communication-minimizing Kernighan-Lin refinement. `PartitionedDAGExecutor` executes partitions in dependency order, each internally parallelized.

### Content-Addressable Caching

Merkle-tree cache keys propagate upstream changes automatically: `CacheKeyBuilder` hashes task source code and predecessor results so any upstream change invalidates all affected downstream nodes. `FileSystemCacheBackend` stores results as pickle with LRU/TTL/size eviction. `CachedDAGExecutor` skips unchanged nodes across runs.

## Requirements

- Python >= 3.12

## License

[MIT](LICENSE)
