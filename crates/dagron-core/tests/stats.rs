use dagron_core::DAG;

#[test]
fn empty_graph_stats() {
    let dag: DAG = DAG::new();
    let stats = dag.stats().unwrap();
    assert_eq!(stats.node_count, 0);
    assert_eq!(stats.edge_count, 0);
    assert_eq!(stats.depth, 0);
    assert_eq!(stats.width, 0);
    assert_eq!(stats.density, 0.0);
    assert_eq!(stats.longest_path_length, 0);
    assert_eq!(stats.root_count, 0);
    assert_eq!(stats.leaf_count, 0);
    assert!(stats.is_weakly_connected);
    assert_eq!(stats.component_count, 0);
}

#[test]
fn single_node_stats() {
    let mut dag: DAG = DAG::new();
    dag.add_node("a".to_string(), ()).unwrap();
    let stats = dag.stats().unwrap();
    assert_eq!(stats.node_count, 1);
    assert_eq!(stats.edge_count, 0);
    assert_eq!(stats.depth, 1);
    assert_eq!(stats.width, 1);
    assert_eq!(stats.density, 0.0);
    assert_eq!(stats.longest_path_length, 0);
    assert_eq!(stats.root_count, 1);
    assert_eq!(stats.leaf_count, 1);
    assert!(stats.is_weakly_connected);
    assert_eq!(stats.component_count, 1);
}

#[test]
fn linear_dag_stats() {
    let mut dag: DAG = DAG::new();
    dag.add_node("a".to_string(), ()).unwrap();
    dag.add_node("b".to_string(), ()).unwrap();
    dag.add_node("c".to_string(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();

    let stats = dag.stats().unwrap();
    assert_eq!(stats.node_count, 3);
    assert_eq!(stats.edge_count, 2);
    assert_eq!(stats.depth, 3);
    assert_eq!(stats.width, 1);
    assert_eq!(stats.longest_path_length, 2);
    assert_eq!(stats.root_count, 1);
    assert_eq!(stats.leaf_count, 1);
    assert_eq!(stats.max_in_degree, 1);
    assert_eq!(stats.max_out_degree, 1);
    assert!(stats.is_weakly_connected);
    assert_eq!(stats.component_count, 1);
}

#[test]
fn diamond_dag_stats() {
    let mut dag: DAG = DAG::new();
    dag.add_node("a".to_string(), ()).unwrap();
    dag.add_node("b".to_string(), ()).unwrap();
    dag.add_node("c".to_string(), ()).unwrap();
    dag.add_node("d".to_string(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap();
    dag.add_edge("b", "d", None, None).unwrap();
    dag.add_edge("c", "d", None, None).unwrap();

    let stats = dag.stats().unwrap();
    assert_eq!(stats.node_count, 4);
    assert_eq!(stats.edge_count, 4);
    assert_eq!(stats.depth, 3);
    assert_eq!(stats.width, 2);
    assert_eq!(stats.longest_path_length, 2);
    assert_eq!(stats.root_count, 1);
    assert_eq!(stats.leaf_count, 1);
    assert_eq!(stats.max_in_degree, 2);
    assert_eq!(stats.max_out_degree, 2);
    assert!(stats.is_weakly_connected);
}

#[test]
fn disconnected_graph_stats() {
    let mut dag: DAG = DAG::new();
    dag.add_node("a".to_string(), ()).unwrap();
    dag.add_node("b".to_string(), ()).unwrap();
    dag.add_node("c".to_string(), ()).unwrap();
    // No edges - 3 disconnected components
    let stats = dag.stats().unwrap();
    assert_eq!(stats.component_count, 3);
    assert!(!stats.is_weakly_connected);
    assert_eq!(stats.root_count, 3);
    assert_eq!(stats.leaf_count, 3);
}

#[test]
fn density_calculation() {
    // Complete DAG of 3 nodes: a->b, a->c, b->c = 3 edges out of 3*2=6 max
    let mut dag: DAG = DAG::new();
    dag.add_node("a".to_string(), ()).unwrap();
    dag.add_node("b".to_string(), ()).unwrap();
    dag.add_node("c".to_string(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();

    let stats = dag.stats().unwrap();
    assert!((stats.density - 0.5).abs() < 1e-10); // 3/6 = 0.5
}
