use std::sync::Arc;
use std::thread;

use dagron_core::{ConcurrentDAG, DAG};

// ── helpers ────────────────────────────────────────────────────────

fn diamond_dag() -> DAG {
    let mut dag = DAG::new();
    dag.add_node("A".into(), ()).unwrap();
    dag.add_node("B".into(), ()).unwrap();
    dag.add_node("C".into(), ()).unwrap();
    dag.add_node("D".into(), ()).unwrap();
    dag.add_edge("A", "B", None, None).unwrap();
    dag.add_edge("A", "C", None, None).unwrap();
    dag.add_edge("B", "D", None, None).unwrap();
    dag.add_edge("C", "D", None, None).unwrap();
    dag
}

fn diamond_dag_with_payload() -> DAG<i32> {
    let mut dag = DAG::new();
    dag.add_node("A".into(), 10).unwrap();
    dag.add_node("B".into(), 20).unwrap();
    dag.add_node("C".into(), 30).unwrap();
    dag.add_node("D".into(), 40).unwrap();
    dag.add_edge("A", "B", None, None).unwrap();
    dag.add_edge("A", "C", None, None).unwrap();
    dag.add_edge("B", "D", None, None).unwrap();
    dag.add_edge("C", "D", None, None).unwrap();
    dag
}

// ── RwLock cache tests ─────────────────────────────────────────────

#[test]
fn concurrent_reads_hit_cache() {
    let dag = diamond_dag();
    // Prime the cache
    dag.topological_sort().unwrap();
    assert_eq!(dag.cache_misses(), 1);

    let dag = Arc::new(dag);
    let mut handles = vec![];

    for _ in 0..8 {
        let dag = Arc::clone(&dag);
        handles.push(thread::spawn(move || {
            dag.topological_sort().unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(dag.cache_misses(), 1);
    assert!(dag.cache_hits() >= 8);
}

#[test]
fn cache_invalidation_after_mutation() {
    let mut dag = diamond_dag();
    let sort1 = dag.topological_sort().unwrap();
    assert_eq!(dag.cache_misses(), 1);

    // Mutate — cache should be stale
    dag.add_node("E".into(), ()).unwrap();
    dag.add_edge("D", "E", None, None).unwrap();

    let sort2 = dag.topological_sort().unwrap();
    assert_eq!(dag.cache_misses(), 2);
    assert_ne!(sort1.len(), sort2.len());
}

// ── Snapshot tests ─────────────────────────────────────────────────

#[test]
fn snapshot_isolated_from_mutations() {
    let mut dag = diamond_dag();
    let snap = dag.snapshot();

    // Mutate original
    dag.add_node("E".into(), ()).unwrap();
    dag.add_edge("D", "E", None, None).unwrap();

    assert_eq!(snap.node_count(), 4);
    assert_eq!(dag.node_count(), 5);
}

#[test]
fn snapshot_preserves_payloads() {
    let dag = diamond_dag_with_payload();
    let snap = dag.snapshot();

    assert_eq!(*snap.get_payload("A").unwrap(), 10);
    assert_eq!(*snap.get_payload("B").unwrap(), 20);
    assert_eq!(*snap.get_payload("C").unwrap(), 30);
    assert_eq!(*snap.get_payload("D").unwrap(), 40);
}

#[test]
fn snapshot_of_empty_dag() {
    let dag: DAG<()> = DAG::new();
    let snap = dag.snapshot();
    assert_eq!(snap.node_count(), 0);
    assert_eq!(snap.edge_count(), 0);
}

#[test]
fn snapshot_has_fresh_cache() {
    let dag = diamond_dag();
    // Prime cache
    dag.topological_sort().unwrap();
    assert_eq!(dag.cache_size(), 1);
    assert_eq!(dag.cache_misses(), 1);

    let snap = dag.snapshot();
    assert_eq!(snap.cache_size(), 0);
    assert_eq!(snap.cache_hits(), 0);
    assert_eq!(snap.cache_misses(), 0);

    // Generation is carried so cache works on first access
    assert_eq!(snap.generation(), dag.generation());
}

#[test]
fn snapshot_preserves_edges() {
    let dag = diamond_dag();
    let snap = dag.snapshot();
    assert_eq!(snap.edge_count(), 4);
    assert!(snap.has_edge("A", "B").unwrap());
    assert!(snap.has_edge("A", "C").unwrap());
    assert!(snap.has_edge("B", "D").unwrap());
    assert!(snap.has_edge("C", "D").unwrap());
}

#[test]
fn mutating_snapshot_does_not_affect_original() {
    let dag = diamond_dag_with_payload();
    let mut snap = dag.snapshot();

    snap.add_node("E".into(), 50).unwrap();
    snap.add_edge("D", "E", None, None).unwrap();
    *snap.get_payload_mut("A").unwrap() = 999;

    assert_eq!(dag.node_count(), 4);
    assert_eq!(*dag.get_payload("A").unwrap(), 10);
}

// ── ConcurrentDAG tests ───────────────────────────────────────────

#[test]
fn concurrent_dag_from_dag_read_write() {
    let dag = diamond_dag();
    let cdag = ConcurrentDAG::from_dag(dag);

    assert_eq!(cdag.read().node_count(), 4);

    cdag.write().add_node("E".into(), ()).unwrap();
    assert_eq!(cdag.read().node_count(), 5);
}

#[test]
fn concurrent_dag_clone_shares_state() {
    let cdag = ConcurrentDAG::from_dag(diamond_dag());
    let cdag2 = cdag.clone();

    cdag.write().add_node("E".into(), ()).unwrap();
    assert_eq!(cdag2.read().node_count(), 5);
}

#[test]
fn concurrent_readers_dont_block() {
    let cdag = ConcurrentDAG::from_dag(diamond_dag());
    let barrier = Arc::new(std::sync::Barrier::new(4));

    let mut handles = vec![];
    for _ in 0..4 {
        let cdag = cdag.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let guard = cdag.read();
            barrier.wait();
            assert_eq!(guard.node_count(), 4);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_dag_snapshot() {
    let cdag = ConcurrentDAG::from_dag(diamond_dag_with_payload());
    let snap = cdag.snapshot();

    assert_eq!(snap.node_count(), 4);
    assert_eq!(*snap.get_payload("A").unwrap(), 10);

    // Mutate original through ConcurrentDAG
    cdag.write().add_node("E".into(), 50).unwrap();
    assert_eq!(snap.node_count(), 4);
    assert_eq!(cdag.read().node_count(), 5);
}

#[test]
fn concurrent_dag_into_inner_sole_owner() {
    let cdag = ConcurrentDAG::from_dag(diamond_dag());
    let dag = cdag.into_inner().expect("sole owner should succeed");
    assert_eq!(dag.node_count(), 4);
}

#[test]
fn concurrent_dag_into_inner_multiple_owners() {
    let cdag = ConcurrentDAG::from_dag(diamond_dag());
    let _clone = cdag.clone();
    assert!(cdag.into_inner().is_none());
}

#[test]
fn concurrent_dag_default() {
    let cdag: ConcurrentDAG<()> = ConcurrentDAG::default();
    assert_eq!(cdag.read().node_count(), 0);
}
