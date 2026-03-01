use dagron_core::{DagronError, SubgraphDirection, DAG};

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

fn linear_dag() -> DAG {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_node("d".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();
    dag.add_edge("c", "d", None, None).unwrap();
    dag
}

// --- Induced Subgraph ---

#[test]
fn subgraph_all_nodes() {
    let dag = diamond_dag();
    let sub = dag.subgraph(&["a", "b", "c", "d"]).unwrap();
    assert_eq!(sub.node_count(), 4);
    assert_eq!(sub.edge_count(), 4);
}

#[test]
fn subgraph_subset() {
    let dag = diamond_dag();
    let sub = dag.subgraph(&["a", "b"]).unwrap();
    assert_eq!(sub.node_count(), 2);
    assert_eq!(sub.edge_count(), 1);
    assert!(sub.has_edge("a", "b").unwrap());
}

#[test]
fn subgraph_single_node() {
    let dag = diamond_dag();
    let sub = dag.subgraph(&["b"]).unwrap();
    assert_eq!(sub.node_count(), 1);
    assert_eq!(sub.edge_count(), 0);
}

#[test]
fn subgraph_empty_set() {
    let dag = diamond_dag();
    let sub = dag.subgraph(&[]).unwrap();
    assert_eq!(sub.node_count(), 0);
    assert_eq!(sub.edge_count(), 0);
}

#[test]
fn subgraph_nonexistent_node_error() {
    let dag = diamond_dag();
    let result = dag.subgraph(&["a", "z"]);
    assert!(matches!(result, Err(DagronError::NodeNotFound(_))));
}

#[test]
fn subgraph_preserves_edge_between_selected() {
    let dag = diamond_dag();
    let sub = dag.subgraph(&["a", "c", "d"]).unwrap();
    assert_eq!(sub.node_count(), 3);
    assert!(sub.has_edge("a", "c").unwrap());
    assert!(sub.has_edge("c", "d").unwrap());
    assert_eq!(sub.edge_count(), 2);
}

// --- Depth-based Subgraph ---

#[test]
fn depth_zero_returns_root_only() {
    let dag = linear_dag();
    let sub = dag
        .subgraph_by_depth("b", 0, SubgraphDirection::Both)
        .unwrap();
    assert_eq!(sub.node_count(), 1);
    assert!(sub.has_node("b"));
}

#[test]
fn depth_one_forward() {
    let dag = linear_dag();
    let sub = dag
        .subgraph_by_depth("a", 1, SubgraphDirection::Forward)
        .unwrap();
    assert_eq!(sub.node_count(), 2);
    assert!(sub.has_node("a"));
    assert!(sub.has_node("b"));
}

#[test]
fn depth_one_backward() {
    let dag = linear_dag();
    let sub = dag
        .subgraph_by_depth("d", 1, SubgraphDirection::Backward)
        .unwrap();
    assert_eq!(sub.node_count(), 2);
    assert!(sub.has_node("d"));
    assert!(sub.has_node("c"));
}

#[test]
fn depth_one_both() {
    let dag = linear_dag();
    let sub = dag
        .subgraph_by_depth("b", 1, SubgraphDirection::Both)
        .unwrap();
    assert_eq!(sub.node_count(), 3);
    assert!(sub.has_node("a"));
    assert!(sub.has_node("b"));
    assert!(sub.has_node("c"));
}

#[test]
fn depth_large_returns_all_reachable() {
    let dag = linear_dag();
    let sub = dag
        .subgraph_by_depth("a", 100, SubgraphDirection::Forward)
        .unwrap();
    assert_eq!(sub.node_count(), 4);
}

#[test]
fn depth_nonexistent_root_error() {
    let dag = linear_dag();
    let result = dag.subgraph_by_depth("z", 1, SubgraphDirection::Forward);
    assert!(matches!(result, Err(DagronError::NodeNotFound(_))));
}

#[test]
fn depth_preserves_edges() {
    let dag = diamond_dag();
    let sub = dag
        .subgraph_by_depth("a", 1, SubgraphDirection::Forward)
        .unwrap();
    assert!(sub.has_node("a"));
    assert!(sub.has_node("b"));
    assert!(sub.has_node("c"));
    assert!(!sub.has_node("d"));
    assert!(sub.has_edge("a", "b").unwrap());
    assert!(sub.has_edge("a", "c").unwrap());
}
