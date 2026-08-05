use criterion::{criterion_group, criterion_main, Criterion};
use dagron_core::DAG;

// ---------------------------------------------------------------------------
// Graph builders
// ---------------------------------------------------------------------------

fn build_chain(n: usize) -> DAG {
    let mut dag = DAG::new();
    let mut prev: Option<String> = None;
    for i in 0..n {
        let name = format!("node_{i}");
        dag.add_node(name.clone(), ()).unwrap();
        if let Some(ref p) = prev {
            dag.add_edge(p, &name, None, None).unwrap();
        }
        prev = Some(name);
    }
    dag
}

fn build_wide(roots: usize, depth: usize) -> DAG {
    let mut dag = DAG::new();
    let mut counter = 0;
    let mut current_level = Vec::new();

    for _ in 0..roots {
        let name = format!("node_{counter}");
        counter += 1;
        dag.add_node(name.clone(), ()).unwrap();
        current_level.push(name);
    }

    for _ in 1..depth {
        let mut next_level = Vec::new();
        for parent in &current_level {
            let name = format!("node_{counter}");
            counter += 1;
            dag.add_node(name.clone(), ()).unwrap();
            dag.add_edge(parent, &name, None, None).unwrap();
            next_level.push(name);
        }
        current_level = next_level;
    }

    dag
}

/// Diamond lattice: `width` nodes per level, `depth` levels.
/// Each node at level L connects to every node at level L+1.
fn build_diamond_lattice(width: usize, depth: usize) -> DAG {
    let mut dag = DAG::new();
    let mut counter = 0;
    let mut current_level = Vec::new();

    for _ in 0..width {
        let name = format!("node_{counter}");
        counter += 1;
        dag.add_node(name.clone(), ()).unwrap();
        current_level.push(name);
    }

    for _ in 1..depth {
        let mut next_level = Vec::new();
        for _ in 0..width {
            let name = format!("node_{counter}");
            counter += 1;
            dag.add_node(name.clone(), ()).unwrap();
            next_level.push(name);
        }
        for parent in &current_level {
            for child in &next_level {
                dag.add_edge(parent, child, None, None).unwrap();
            }
        }
        current_level = next_level;
    }

    dag
}

// ---------------------------------------------------------------------------
// Construction benchmarks
// ---------------------------------------------------------------------------

fn bench_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("construction");

    group.bench_function("chain_1k", |b| b.iter(|| build_chain(1_000)));
    group.bench_function("chain_10k", |b| b.iter(|| build_chain(10_000)));
    group.bench_function("chain_100k", |b| b.iter(|| build_chain(100_000)));
    group.bench_function("wide_100x10", |b| b.iter(|| build_wide(100, 10)));
    group.bench_function("wide_1000x10", |b| b.iter(|| build_wide(1_000, 10)));
    group.bench_function("diamond_10x10", |b| {
        b.iter(|| build_diamond_lattice(10, 10))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Topological sort benchmarks
// ---------------------------------------------------------------------------

fn bench_toposort(c: &mut Criterion) {
    let chain_10k = build_chain(10_000);
    let wide_10k = build_wide(1_000, 10);

    let mut group = c.benchmark_group("toposort");

    group.bench_function("kahn_chain_10k", |b| {
        b.iter(|| chain_10k.topological_sort().unwrap())
    });
    group.bench_function("kahn_wide_10k", |b| {
        b.iter(|| wide_10k.topological_sort().unwrap())
    });
    group.bench_function("dfs_chain_10k", |b| {
        b.iter(|| chain_10k.topological_sort_dfs().unwrap())
    });
    group.bench_function("dfs_wide_10k", |b| {
        b.iter(|| wide_10k.topological_sort_dfs().unwrap())
    });
    group.bench_function("levels_chain_10k", |b| {
        b.iter(|| chain_10k.topological_levels().unwrap())
    });
    group.bench_function("levels_wide_10k", |b| {
        b.iter(|| wide_10k.topological_levels().unwrap())
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Cycle detection benchmarks
// ---------------------------------------------------------------------------

fn bench_cycle_detection(c: &mut Criterion) {
    let chain_10k = build_chain(10_000);

    let mut group = c.benchmark_group("cycle_detection");

    // Uses validate() which calls find_cycles internally
    group.bench_function("validate_acyclic_10k", |b| {
        b.iter(|| chain_10k.validate().unwrap())
    });
    // would_create_cycle: check if adding an edge from last to first would create a cycle
    group.bench_function("would_create_cycle_10k", |b| {
        let graph = chain_10k.inner_graph();
        let from = chain_10k.resolve_name("node_9999").unwrap();
        let to = chain_10k.resolve_name("node_0").unwrap();
        b.iter(|| dagron_core::algorithms::cycle::would_create_cycle(graph, from, to))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Reachability benchmarks
// ---------------------------------------------------------------------------

fn bench_reachability(c: &mut Criterion) {
    let chain_10k = build_chain(10_000);

    let mut group = c.benchmark_group("reachability");

    group.bench_function("index_build_10k", |b| {
        b.iter(|| chain_10k.build_reachability_index().unwrap())
    });

    let index = chain_10k.build_reachability_index().unwrap();
    let first = chain_10k.resolve_name("node_0").unwrap();
    let last = chain_10k.resolve_name("node_9999").unwrap();
    group.bench_function("query_can_reach", |b| {
        b.iter(|| index.can_reach(first, last))
    });
    group.bench_function("query_reachable_from", |b| {
        b.iter(|| index.reachable_from(first))
    });
    group.bench_function("query_ancestors_of", |b| {
        b.iter(|| index.ancestors_of(last))
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Serialization benchmarks
// ---------------------------------------------------------------------------

fn bench_serialization(c: &mut Criterion) {
    let dag_1k = build_chain(1_000);
    let dag_10k = build_chain(10_000);
    let dag_100k = build_chain(100_000);

    let mut group = c.benchmark_group("serialization");

    group.bench_function("to_json_1k", |b| {
        b.iter(|| dag_1k.to_json(|_| None).unwrap())
    });

    let json_str = dag_1k.to_json(|_| None).unwrap();
    group.bench_function("from_json_1k", |b| {
        b.iter(|| DAG::<()>::from_json(&json_str, |_| ()).unwrap())
    });

    group.bench_function("to_dot_1k", |b| b.iter(|| dag_1k.to_dot()));

    group.bench_function("to_bincode_1k", |b| {
        b.iter(|| dag_1k.to_bincode(|_| None).unwrap())
    });

    let bytes = dag_1k.to_bincode(|_| None).unwrap();
    group.bench_function("from_bincode_1k", |b| {
        b.iter(|| DAG::<()>::from_bincode(&bytes, |_| ()).unwrap())
    });

    group.bench_function("to_bincode_10k", |b| {
        b.iter(|| dag_10k.to_bincode(|_| None).unwrap())
    });

    let bytes_10k = dag_10k.to_bincode(|_| None).unwrap();
    group.bench_function("from_bincode_10k", |b| {
        b.iter(|| DAG::<()>::from_bincode(&bytes_10k, |_| ()).unwrap())
    });

    group.bench_function("to_bincode_100k", |b| {
        b.iter(|| dag_100k.to_bincode(|_| None).unwrap())
    });

    group.bench_function("bincode_size_10k", |b| {
        b.iter(|| dag_10k.bincode_size(|_| None).unwrap())
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Scheduling benchmarks
// ---------------------------------------------------------------------------

fn bench_scheduling(c: &mut Criterion) {
    let dag_1k = build_chain(1_000);
    let costs = ahash::AHashMap::new(); // default costs

    let mut group = c.benchmark_group("scheduling");

    group.bench_function("max_parallelism_1k", |b| {
        b.iter(|| dag_1k.execution_plan(&costs).unwrap())
    });
    group.bench_function("resource_constrained_4w_1k", |b| {
        b.iter(|| dag_1k.execution_plan_constrained(4, &costs).unwrap())
    });
    group.bench_function("critical_path_1k", |b| {
        b.iter(|| dag_1k.critical_path(&costs).unwrap())
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Transform benchmarks
// ---------------------------------------------------------------------------

fn bench_transforms(c: &mut Criterion) {
    let dag_1k = build_chain(1_000);

    let mut group = c.benchmark_group("transforms");

    group.bench_function("transitive_reduction_1k", |b| {
        b.iter(|| dag_1k.transitive_reduction())
    });
    group.bench_function("reverse_1k", |b| b.iter(|| dag_1k.reverse()));
    group.bench_function("snapshot_1k", |b| b.iter(|| dag_1k.snapshot()));

    group.finish();
}

// ---------------------------------------------------------------------------
// Introspection benchmarks
// ---------------------------------------------------------------------------

fn bench_introspection(c: &mut Criterion) {
    let chain_10k = build_chain(10_000);

    let mut group = c.benchmark_group("introspection");

    group.bench_function("ancestors_mid_10k", |b| {
        b.iter(|| chain_10k.ancestors("node_5000").unwrap())
    });
    group.bench_function("descendants_mid_10k", |b| {
        b.iter(|| chain_10k.descendants("node_5000").unwrap())
    });
    group.bench_function("roots_10k", |b| b.iter(|| chain_10k.roots()));
    group.bench_function("leaves_10k", |b| b.iter(|| chain_10k.leaves()));

    group.finish();
}

// ---------------------------------------------------------------------------
// Register all groups
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_construction,
    bench_toposort,
    bench_cycle_detection,
    bench_reachability,
    bench_serialization,
    bench_scheduling,
    bench_transforms,
    bench_introspection,
);
criterion_main!(benches);
