use criterion::{criterion_group, criterion_main, Criterion};
use dagron_core::{DAG, NodeData, EdgeData};

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

fn bench_build_chain(c: &mut Criterion) {
    c.bench_function("build_chain_1000", |b| {
        b.iter(|| build_chain(1000));
    });

    c.bench_function("build_chain_10000", |b| {
        b.iter(|| build_chain(10000));
    });
}

fn bench_build_wide(c: &mut Criterion) {
    c.bench_function("build_wide_100x10", |b| {
        b.iter(|| build_wide(100, 10));
    });
}

fn bench_toposort_kahn(c: &mut Criterion) {
    let dag = build_chain(10000);

    c.bench_function("kahn_toposort_10000", |b| {
        b.iter(|| {
            dag.topological_sort().unwrap();
        });
    });
}

criterion_group!(benches, bench_build_chain, bench_build_wide, bench_toposort_kahn);
criterion_main!(benches);
