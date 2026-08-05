use dagron_core::{DagronError, DAG};

#[test]
fn valid_dag_passes_validation() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();
    assert!(dag.validate().unwrap());
}

#[test]
fn empty_dag_is_valid() {
    let dag = DAG::<()>::new();
    assert!(dag.validate().unwrap());
}

#[test]
fn single_node_is_valid() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    assert!(dag.validate().unwrap());
}

#[test]
fn diamond_dag_is_valid() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_node("d".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap();
    dag.add_edge("b", "d", None, None).unwrap();
    dag.add_edge("c", "d", None, None).unwrap();
    assert!(dag.validate().unwrap());
}

#[test]
fn disconnected_dag_is_valid() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    // No edges — three isolated nodes
    assert!(dag.validate().unwrap());
}

#[test]
fn validate_detects_cycle_injected_via_inner_graph() {
    // Bypass add_edge cycle check by manipulating the inner graph directly
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();

    // Force a back-edge b->a through the raw graph
    let a_idx = dag.resolve_name("a").unwrap();
    let b_idx = dag.resolve_name("b").unwrap();
    dag.inner_graph_mut().add_edge(
        b_idx,
        a_idx,
        dagron_core::EdgeData {
            weight: 1.0,
            label: None,
        },
    );

    let err = dag.validate().unwrap_err();
    assert!(matches!(err, DagronError::Cycle(_)));
}
