use dagron_core::{DagronError, MergeConflict, DAG};

fn diamond_dag() -> DAG {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_node("d".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap();
    dag.add_edge("b", "d", None, None).unwrap();
    dag.add_edge("c", "d", None, None).unwrap();
    dag
}

// --- Transitive Reduction ---

#[test]
fn reduction_removes_redundant_edge() {
    // diamond + shortcut a->d
    let mut dag = diamond_dag();
    dag.add_edge("a", "d", None, None).unwrap();
    assert_eq!(dag.edge_count(), 5);

    let reduced = dag.transitive_reduction();
    assert_eq!(reduced.node_count(), 4);
    assert_eq!(reduced.edge_count(), 4); // a->d removed
    assert!(reduced.has_edge("a", "b").unwrap());
    assert!(reduced.has_edge("a", "c").unwrap());
    assert!(reduced.has_edge("b", "d").unwrap());
    assert!(reduced.has_edge("c", "d").unwrap());
    assert!(!reduced.has_edge("a", "d").unwrap());
}

#[test]
fn reduction_preserves_minimal_diamond() {
    let dag = diamond_dag();
    let reduced = dag.transitive_reduction();
    assert_eq!(reduced.edge_count(), 4);
}

#[test]
fn reduction_linear_chain() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap(); // shortcut

    let reduced = dag.transitive_reduction();
    assert_eq!(reduced.edge_count(), 2);
    assert!(reduced.has_edge("a", "b").unwrap());
    assert!(reduced.has_edge("b", "c").unwrap());
    assert!(!reduced.has_edge("a", "c").unwrap());
}

#[test]
fn reduction_empty() {
    let dag: DAG = DAG::new();
    let reduced = dag.transitive_reduction();
    assert_eq!(reduced.node_count(), 0);
    assert_eq!(reduced.edge_count(), 0);
}

#[test]
fn reduction_preserves_payloads() {
    let mut dag: DAG<i32> = DAG::new();
    dag.add_node("a".into(), 10).unwrap();
    dag.add_node("b".into(), 20).unwrap();
    dag.add_node("c".into(), 30).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap();

    let reduced = dag.transitive_reduction();
    assert_eq!(*reduced.get_payload("a").unwrap(), 10);
    assert_eq!(*reduced.get_payload("b").unwrap(), 20);
    assert_eq!(*reduced.get_payload("c").unwrap(), 30);
}

#[test]
fn reduction_preserves_edge_weights() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", Some(5.0), Some("dep".into())).unwrap();
    dag.add_edge("b", "c", Some(3.0), None).unwrap();
    dag.add_edge("a", "c", Some(99.0), None).unwrap(); // shortcut, removed

    let reduced = dag.transitive_reduction();
    let sg = reduced.to_serializable(|_| None);
    let ab_edge = sg.edges.iter().find(|e| e.from == "a" && e.to == "b").unwrap();
    assert!((ab_edge.weight - 5.0).abs() < f64::EPSILON);
    assert_eq!(ab_edge.label.as_deref(), Some("dep"));
}

// --- Transitive Closure ---

#[test]
fn closure_adds_missing_edges() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();

    let closed = dag.transitive_closure();
    assert_eq!(closed.edge_count(), 3);
    assert!(closed.has_edge("a", "b").unwrap());
    assert!(closed.has_edge("b", "c").unwrap());
    assert!(closed.has_edge("a", "c").unwrap());
}

#[test]
fn closure_already_complete() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap();

    let closed = dag.transitive_closure();
    assert_eq!(closed.edge_count(), 3);
}

#[test]
fn closure_diamond() {
    let dag = diamond_dag();
    let closed = dag.transitive_closure();
    // Should add a->d
    assert!(closed.has_edge("a", "d").unwrap());
    assert_eq!(closed.edge_count(), 5);
}

#[test]
fn closure_empty() {
    let dag: DAG = DAG::new();
    let closed = dag.transitive_closure();
    assert_eq!(closed.node_count(), 0);
}

// --- Filter ---

#[test]
fn filter_keeps_matching() {
    let dag = diamond_dag();
    let filtered = dag.filter(|name, _| name == "a" || name == "b");
    assert_eq!(filtered.node_count(), 2);
    assert!(filtered.has_node("a"));
    assert!(filtered.has_node("b"));
    assert!(filtered.has_edge("a", "b").unwrap());
}

#[test]
fn filter_removes_dangling_edges() {
    let dag = diamond_dag();
    // Keep a and d, but edge a->d doesn't exist (only a->b, a->c, b->d, c->d)
    let filtered = dag.filter(|name, _| name == "a" || name == "d");
    assert_eq!(filtered.node_count(), 2);
    assert_eq!(filtered.edge_count(), 0);
}

#[test]
fn filter_empty_result() {
    let dag = diamond_dag();
    let filtered = dag.filter(|_, _| false);
    assert_eq!(filtered.node_count(), 0);
    assert_eq!(filtered.edge_count(), 0);
}

#[test]
fn filter_preserves_payloads() {
    let mut dag: DAG<i32> = DAG::new();
    dag.add_node("a".into(), 10).unwrap();
    dag.add_node("b".into(), 20).unwrap();
    dag.add_node("c".into(), 30).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();

    let filtered = dag.filter(|name, _| name != "c");
    assert_eq!(*filtered.get_payload("a").unwrap(), 10);
    assert_eq!(*filtered.get_payload("b").unwrap(), 20);
    assert!(!filtered.has_node("c"));
}

// --- Merge ---

#[test]
fn merge_disjoint() {
    let mut dag1 = DAG::new();
    dag1.add_node("a".into(), ()).unwrap();
    dag1.add_node("b".into(), ()).unwrap();
    dag1.add_edge("a", "b", None, None).unwrap();

    let mut dag2 = DAG::new();
    dag2.add_node("c".into(), ()).unwrap();
    dag2.add_node("d".into(), ()).unwrap();
    dag2.add_edge("c", "d", None, None).unwrap();

    let merged = dag1.merge(&dag2, MergeConflict::KeepFirst).unwrap();
    assert_eq!(merged.node_count(), 4);
    assert_eq!(merged.edge_count(), 2);
}

#[test]
fn merge_keep_first() {
    let mut dag1: DAG<i32> = DAG::new();
    dag1.add_node("a".into(), 10).unwrap();

    let mut dag2: DAG<i32> = DAG::new();
    dag2.add_node("a".into(), 20).unwrap();

    let merged = dag1.merge(&dag2, MergeConflict::KeepFirst).unwrap();
    assert_eq!(*merged.get_payload("a").unwrap(), 10);
}

#[test]
fn merge_keep_second() {
    let mut dag1: DAG<i32> = DAG::new();
    dag1.add_node("a".into(), 10).unwrap();

    let mut dag2: DAG<i32> = DAG::new();
    dag2.add_node("a".into(), 20).unwrap();

    let merged = dag1.merge(&dag2, MergeConflict::KeepSecond).unwrap();
    assert_eq!(*merged.get_payload("a").unwrap(), 20);
}

#[test]
fn merge_error_on_conflict() {
    let mut dag1: DAG<i32> = DAG::new();
    dag1.add_node("a".into(), 10).unwrap();

    let mut dag2: DAG<i32> = DAG::new();
    dag2.add_node("a".into(), 20).unwrap();

    let result = dag1.merge(&dag2, MergeConflict::Error);
    match result {
        Err(DagronError::DuplicateNode(name)) => assert_eq!(name, "a"),
        Err(other) => panic!("Expected DuplicateNode error, got: {other:?}"),
        Ok(_) => panic!("Expected error but got Ok"),
    }
}

#[test]
fn merge_cycle_fails() {
    let mut dag1 = DAG::new();
    dag1.add_node("a".into(), ()).unwrap();
    dag1.add_node("b".into(), ()).unwrap();
    dag1.add_edge("a", "b", None, None).unwrap();

    let mut dag2 = DAG::new();
    dag2.add_node("a".into(), ()).unwrap();
    dag2.add_node("b".into(), ()).unwrap();
    dag2.add_edge("b", "a", None, None).unwrap();

    let result = dag1.merge(&dag2, MergeConflict::KeepFirst);
    assert!(result.is_err());
}

#[test]
fn merge_with_custom_resolver() {
    let mut dag1: DAG<i32> = DAG::new();
    dag1.add_node("a".into(), 10).unwrap();
    dag1.add_node("b".into(), 20).unwrap();

    let mut dag2: DAG<i32> = DAG::new();
    dag2.add_node("a".into(), 5).unwrap();
    dag2.add_node("c".into(), 30).unwrap();

    let merged = dag1
        .merge_with(&dag2, |_name, p1, p2| p1 + p2)
        .unwrap();
    assert_eq!(*merged.get_payload("a").unwrap(), 15); // 10 + 5
    assert_eq!(*merged.get_payload("b").unwrap(), 20);
    assert_eq!(*merged.get_payload("c").unwrap(), 30);
}

// --- Original DAG is unmodified ---

#[test]
fn transforms_do_not_modify_original() {
    let mut dag = diamond_dag();
    dag.add_edge("a", "d", None, None).unwrap();
    let original_edges = dag.edge_count();

    let _ = dag.transitive_reduction();
    assert_eq!(dag.edge_count(), original_edges);

    let _ = dag.transitive_closure();
    assert_eq!(dag.edge_count(), original_edges);

    let _ = dag.filter(|name, _| name == "a");
    assert_eq!(dag.node_count(), 4);
    assert_eq!(dag.edge_count(), original_edges);
}
