use dagron_core::{DagronError, DAG};

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

#[test]
fn add_node_returns_node_id() {
    let mut dag = DAG::new();
    let id = dag.add_node("alpha".into(), ()).unwrap();
    assert_eq!(id.name, "alpha");
}

#[test]
fn add_duplicate_node_fails() {
    let mut dag = DAG::new();
    dag.add_node("x".into(), ()).unwrap();
    let err = dag.add_node("x".into(), ()).unwrap_err();
    assert!(matches!(err, DagronError::DuplicateNode(name) if name == "x"));
}

#[test]
fn add_edge_between_existing_nodes() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    assert!(dag.has_edge("a", "b").unwrap());
}

#[test]
fn add_edge_to_missing_node_fails() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    let err = dag.add_edge("a", "ghost", None, None).unwrap_err();
    assert!(matches!(err, DagronError::NodeNotFound(name) if name == "ghost"));
}

#[test]
fn add_edge_from_missing_node_fails() {
    let mut dag = DAG::new();
    dag.add_node("b".into(), ()).unwrap();
    let err = dag.add_edge("ghost", "b", None, None).unwrap_err();
    assert!(matches!(err, DagronError::NodeNotFound(name) if name == "ghost"));
}

#[test]
fn add_edge_creating_cycle_fails() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    let err = dag.add_edge("b", "a", None, None).unwrap_err();
    assert!(matches!(err, DagronError::Cycle(_)));
}

#[test]
fn add_edge_with_weight_and_label() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_edge("a", "b", Some(2.5), Some("dep".into()))
        .unwrap();
    assert!(dag.has_edge("a", "b").unwrap());
}

#[test]
fn remove_node_succeeds() {
    let mut dag = diamond_dag();
    assert_eq!(dag.node_count(), 4);
    dag.remove_node("b").unwrap();
    assert_eq!(dag.node_count(), 3);
    assert!(!dag.has_node("b"));
}

#[test]
fn remove_node_removes_incident_edges() {
    let mut dag = diamond_dag();
    dag.remove_node("b").unwrap();
    // a->b and b->d are gone; a->c and c->d remain
    assert_eq!(dag.edge_count(), 2);
}

#[test]
fn remove_missing_node_fails() {
    let mut dag = DAG::<()>::new();
    let err = dag.remove_node("ghost").unwrap_err();
    assert!(matches!(err, DagronError::NodeNotFound(name) if name == "ghost"));
}

#[test]
fn remove_edge_succeeds() {
    let mut dag = diamond_dag();
    assert_eq!(dag.edge_count(), 4);
    dag.remove_edge("a", "b").unwrap();
    assert_eq!(dag.edge_count(), 3);
    assert!(!dag.has_edge("a", "b").unwrap());
}

#[test]
fn remove_nonexistent_edge_fails() {
    let mut dag = diamond_dag();
    let err = dag.remove_edge("a", "d").unwrap_err();
    assert!(matches!(err, DagronError::EdgeNotFound(..)));
}

#[test]
fn empty_dag_counts() {
    let dag = DAG::<()>::new();
    assert_eq!(dag.node_count(), 0);
    assert_eq!(dag.edge_count(), 0);
}

#[test]
fn add_node_with_payload() {
    let mut dag = DAG::<i32>::new();
    dag.add_node("x".into(), 42).unwrap();
    assert_eq!(*dag.get_payload("x").unwrap(), 42);
}
