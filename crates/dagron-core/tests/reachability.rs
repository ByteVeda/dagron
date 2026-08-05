use dagron_core::DAG;

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

// --- ReachabilityIndex ---

#[test]
fn build_reachability_index() {
    let dag = diamond_dag();
    let idx = dag.build_reachability_index().unwrap();
    assert_eq!(idx.node_count(), 4);
}

#[test]
fn can_reach_forward() {
    let dag = diamond_dag();
    let a = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "a")
        .unwrap();
    let d = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "d")
        .unwrap();
    let b = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "b")
        .unwrap();
    let c = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "c")
        .unwrap();

    let idx = dag.build_reachability_index().unwrap();
    assert!(idx.can_reach(a, d));
    assert!(idx.can_reach(a, b));
    assert!(idx.can_reach(a, c));
    assert!(idx.can_reach(b, d));
    assert!(idx.can_reach(c, d));
}

#[test]
fn can_reach_reverse_false() {
    let dag = diamond_dag();
    let a = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "a")
        .unwrap();
    let d = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "d")
        .unwrap();
    let b = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "b")
        .unwrap();
    let c = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "c")
        .unwrap();

    let idx = dag.build_reachability_index().unwrap();
    assert!(!idx.can_reach(d, a));
    assert!(!idx.can_reach(b, c));
    assert!(!idx.can_reach(c, b));
}

#[test]
fn self_reachability() {
    let dag = diamond_dag();
    let a = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "a")
        .unwrap();
    let idx = dag.build_reachability_index().unwrap();
    assert!(idx.can_reach(a, a));
}

#[test]
fn reachable_from() {
    let dag = diamond_dag();
    let a = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "a")
        .unwrap();
    let idx = dag.build_reachability_index().unwrap();
    let reachable = idx.reachable_from(a);
    assert_eq!(reachable.len(), 3); // b, c, d
}

#[test]
fn ancestors_of() {
    let dag = diamond_dag();
    let d = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "d")
        .unwrap();
    let idx = dag.build_reachability_index().unwrap();
    let ancestors = idx.ancestors_of(d);
    assert_eq!(ancestors.len(), 3); // a, b, c
}

#[test]
fn disconnected_components() {
    let mut dag = DAG::new();
    dag.add_node("x".into(), ()).unwrap();
    dag.add_node("y".into(), ()).unwrap();

    let x = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "x")
        .unwrap();
    let y = dag
        .inner_graph()
        .node_indices()
        .find(|&i| dag.inner_graph()[i].name == "y")
        .unwrap();

    let idx = dag.build_reachability_index().unwrap();
    assert!(!idx.can_reach(x, y));
    assert!(!idx.can_reach(y, x));
}

#[test]
fn empty_graph() {
    let dag: DAG = DAG::new();
    let idx = dag.build_reachability_index().unwrap();
    assert_eq!(idx.node_count(), 0);
}

// --- is_ancestor ---

#[test]
fn is_ancestor_true() {
    let dag = diamond_dag();
    assert!(dag.is_ancestor("a", "d").unwrap());
    assert!(dag.is_ancestor("a", "b").unwrap());
    assert!(dag.is_ancestor("b", "d").unwrap());
}

#[test]
fn is_ancestor_false() {
    let dag = diamond_dag();
    assert!(!dag.is_ancestor("d", "a").unwrap());
    assert!(!dag.is_ancestor("b", "c").unwrap());
}

#[test]
fn is_ancestor_self() {
    let dag = diamond_dag();
    assert!(dag.is_ancestor("a", "a").unwrap());
}
