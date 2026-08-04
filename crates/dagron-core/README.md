# dagron-core

[![crates.io](https://img.shields.io/crates/v/dagron-core.svg)](https://crates.io/crates/dagron-core)
[![docs.rs](https://img.shields.io/docsrs/dagron-core)](https://docs.rs/dagron-core)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Fast DAG construction, analysis, and scheduling — the Rust engine behind
[dagron](https://github.com/ByteVeda/dagron). Built on
[petgraph](https://github.com/petgraph/petgraph).

Acyclicity is enforced at mutation time: `add_edge` rejects any edge that would
close a cycle, so a `DAG<P>` is a DAG by construction.

## Install

```toml
[dependencies]
dagron-core = "0.1"
```

## Example

```rust
use dagron_core::{DagronError, DAG};

fn main() -> Result<(), DagronError> {
    let mut dag: DAG<i32> = DAG::new();

    dag.add_node("extract".to_string(), 1)?;
    dag.add_node("transform".to_string(), 2)?;
    dag.add_node("load".to_string(), 3)?;

    dag.add_edge("extract", "transform", None, None)?;
    dag.add_edge("transform", "load", None, None)?;

    // Dependency order.
    let order = dag.topological_sort()?;
    assert_eq!(order[0].name, "extract");

    // Nodes grouped into levels that can run in parallel.
    let levels = dag.topological_levels()?;
    assert_eq!(levels.len(), 3);

    Ok(())
}
```

## What's in the box

- **Construction** — generic payloads (`DAG<P>`), stable `NodeRef` handles that
  survive unrelated mutations and detect remove-then-readd via per-node epochs.
- **Topological ordering** — Kahn and DFS variants, level partitioning,
  all-orderings enumeration, priority-weighted ordering.
- **Scheduling** — execution plans (with and without concurrency constraints),
  critical path, bottom levels.
- **Analysis** — reachability index, dominators, path queries, cycle
  diagnostics, graph statistics, subgraph extraction, structural diffing.
- **Incremental updates** — mutate the graph and recompute only what changed.
- **Serialization** — serde plus a `bincode` on-disk format with `memmap2`
  reads.
- **Concurrency** — `ConcurrentDAG` for shared read-mostly access.

## Related crates

| Crate | Purpose |
| --- | --- |
| [`dagron-core`](https://crates.io/crates/dagron-core) | This crate — the graph engine |
| [`dagron-ui`](https://crates.io/crates/dagron-ui) | Live web dashboard for DAG execution |
| [`dagron`](https://pypi.org/project/dagron/) (PyPI) | Python bindings and execution strategies |

## License

MIT
