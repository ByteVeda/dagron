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

fn complex_dag() -> DAG {
    // a -> b -> d -> f
    // a -> c -> e -> f
    // b -> e
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_node("d".into(), ()).unwrap();
    dag.add_node("e".into(), ()).unwrap();
    dag.add_node("f".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("a", "c", None, None).unwrap();
    dag.add_edge("b", "d", None, None).unwrap();
    dag.add_edge("b", "e", None, None).unwrap();
    dag.add_edge("c", "e", None, None).unwrap();
    dag.add_edge("d", "f", None, None).unwrap();
    dag.add_edge("e", "f", None, None).unwrap();
    dag
}

#[test]
fn execution_plan_diamond_structure() {
    let dag = diamond_dag();
    let costs = ahash::AHashMap::new();
    let plan = dag.execution_plan(&costs).unwrap();

    assert_eq!(plan.total_nodes, 4);
    assert_eq!(plan.steps.len(), 3);
    assert_eq!(plan.max_parallelism, 2);

    // Step 0: a
    assert_eq!(plan.steps[0].nodes.len(), 1);
    assert_eq!(plan.steps[0].nodes[0].node.name, "a");

    // Step 1: b, c (parallel)
    assert_eq!(plan.steps[1].nodes.len(), 2);
    let step1_names: Vec<&str> = plan.steps[1].nodes.iter().map(|n| n.node.name.as_str()).collect();
    assert!(step1_names.contains(&"b"));
    assert!(step1_names.contains(&"c"));

    // Step 2: d
    assert_eq!(plan.steps[2].nodes.len(), 1);
    assert_eq!(plan.steps[2].nodes[0].node.name, "d");
}

#[test]
fn execution_plan_with_costs() {
    let dag = diamond_dag();
    let mut costs = ahash::AHashMap::new();
    costs.insert("a".to_string(), 2.0);
    costs.insert("b".to_string(), 5.0);
    costs.insert("c".to_string(), 1.0);
    costs.insert("d".to_string(), 3.0);

    let plan = dag.execution_plan(&costs).unwrap();

    // Makespan = critical path = a(2) + b(5) + d(3) = 10
    assert_eq!(plan.estimated_makespan, 10.0);

    // Step 0 starts at 0
    assert_eq!(plan.steps[0].nodes[0].start_time, 0.0);
    assert_eq!(plan.steps[0].nodes[0].duration, 2.0);

    // Step 1 starts at 2 (after step 0 completes)
    assert_eq!(plan.steps[1].nodes[0].start_time, 2.0);
}

#[test]
fn execution_plan_empty() {
    let dag = DAG::<()>::new();
    let costs = ahash::AHashMap::new();
    let plan = dag.execution_plan(&costs).unwrap();
    assert_eq!(plan.total_nodes, 0);
    assert!(plan.steps.is_empty());
    assert_eq!(plan.max_parallelism, 0);
    assert_eq!(plan.estimated_makespan, 0.0);
    assert!(plan.critical_path.is_none());
}

#[test]
fn execution_plan_single_node() {
    let mut dag = DAG::new();
    dag.add_node("only".into(), ()).unwrap();
    let costs = ahash::AHashMap::new();
    let plan = dag.execution_plan(&costs).unwrap();
    assert_eq!(plan.total_nodes, 1);
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.max_parallelism, 1);
    assert_eq!(plan.estimated_makespan, 1.0);
}

#[test]
fn execution_plan_constrained_1_worker() {
    let dag = diamond_dag();
    let costs = ahash::AHashMap::new();
    let plan = dag.execution_plan_constrained(1, &costs).unwrap();
    assert_eq!(plan.total_nodes, 4);
    // With 1 worker, all nodes must be sequential
    assert_eq!(plan.max_parallelism, 1);
}

#[test]
fn execution_plan_constrained_2_workers() {
    let dag = diamond_dag();
    let costs = ahash::AHashMap::new();
    let plan = dag.execution_plan_constrained(2, &costs).unwrap();
    assert_eq!(plan.total_nodes, 4);
    // With 2 workers, b and c can run in parallel
    assert_eq!(plan.max_parallelism, 2);
}

#[test]
fn critical_path_diamond() {
    let dag = diamond_dag();
    let costs = ahash::AHashMap::new();
    let (path, total) = dag.critical_path(&costs).unwrap();

    let path_names: Vec<&str> = path.iter().map(|n| n.name.as_str()).collect();
    // All costs equal, so critical path is a -> b -> d (alphabetical tiebreak)
    assert_eq!(path_names, vec!["a", "b", "d"]);
    assert_eq!(total, 3.0);
}

#[test]
fn critical_path_with_costs() {
    let dag = diamond_dag();
    let mut costs = ahash::AHashMap::new();
    costs.insert("a".to_string(), 1.0);
    costs.insert("b".to_string(), 1.0);
    costs.insert("c".to_string(), 10.0);
    costs.insert("d".to_string(), 1.0);

    let (path, total) = dag.critical_path(&costs).unwrap();
    let path_names: Vec<&str> = path.iter().map(|n| n.name.as_str()).collect();
    // c is expensive, so critical path goes through c
    assert_eq!(path_names, vec!["a", "c", "d"]);
    assert_eq!(total, 12.0);
}

#[test]
fn critical_path_empty() {
    let dag = DAG::<()>::new();
    let costs = ahash::AHashMap::new();
    let (path, total) = dag.critical_path(&costs).unwrap();
    assert!(path.is_empty());
    assert_eq!(total, 0.0);
}

#[test]
fn execution_plan_complex() {
    let dag = complex_dag();
    let mut costs = ahash::AHashMap::new();
    costs.insert("a".to_string(), 1.0);
    costs.insert("b".to_string(), 3.0);
    costs.insert("c".to_string(), 2.0);
    costs.insert("d".to_string(), 2.0);
    costs.insert("e".to_string(), 1.0);
    costs.insert("f".to_string(), 1.0);

    let plan = dag.execution_plan(&costs).unwrap();
    assert_eq!(plan.total_nodes, 6);
    assert!(plan.critical_path.is_some());

    let (path, total) = dag.critical_path(&costs).unwrap();
    // a(1) -> b(3) -> d(2) -> f(1) = 7
    // a(1) -> b(3) -> e(1) -> f(1) = 6
    // a(1) -> c(2) -> e(1) -> f(1) = 5
    assert_eq!(total, 7.0);
    let path_names: Vec<&str> = path.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(path_names, vec!["a", "b", "d", "f"]);
}
