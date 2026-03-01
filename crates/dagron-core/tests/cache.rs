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

#[test]
fn cache_hit_roots() {
    let dag = diamond_dag();
    let r1 = dag.roots();
    let r2 = dag.roots();
    assert_eq!(r1, r2);
    assert_eq!(dag.cache_hits(), 1);
    assert_eq!(dag.cache_misses(), 1);
}

#[test]
fn cache_hit_leaves() {
    let dag = diamond_dag();
    let l1 = dag.leaves();
    let l2 = dag.leaves();
    assert_eq!(l1, l2);
    assert_eq!(dag.cache_hits(), 1);
    assert_eq!(dag.cache_misses(), 1);
}

#[test]
fn cache_hit_topo_sort() {
    let dag = diamond_dag();
    let t1 = dag.topological_sort().unwrap();
    let t2 = dag.topological_sort().unwrap();
    assert_eq!(t1, t2);
    assert_eq!(dag.cache_hits(), 1);
    assert_eq!(dag.cache_misses(), 1);
}

#[test]
fn cache_hit_topo_sort_dfs() {
    let dag = diamond_dag();
    let t1 = dag.topological_sort_dfs().unwrap();
    let t2 = dag.topological_sort_dfs().unwrap();
    assert_eq!(t1, t2);
    assert_eq!(dag.cache_hits(), 1);
    assert_eq!(dag.cache_misses(), 1);
}

#[test]
fn cache_hit_topo_levels() {
    let dag = diamond_dag();
    let l1 = dag.topological_levels().unwrap();
    let l2 = dag.topological_levels().unwrap();
    assert_eq!(l1, l2);
    assert_eq!(dag.cache_hits(), 1);
    assert_eq!(dag.cache_misses(), 1);
}

#[test]
fn cache_invalidation_add_node() {
    let mut dag = diamond_dag();
    dag.roots();
    assert_eq!(dag.cache_misses(), 1);
    dag.add_node("e".into(), ()).unwrap();
    dag.roots();
    assert_eq!(dag.cache_misses(), 2);
    assert_eq!(dag.cache_hits(), 0);
}

#[test]
fn cache_invalidation_add_edge() {
    let mut dag = diamond_dag();
    dag.topological_sort().unwrap();
    assert_eq!(dag.cache_misses(), 1);
    dag.add_node("e".into(), ()).unwrap();
    dag.add_edge("d", "e", None, None).unwrap();
    dag.topological_sort().unwrap();
    assert_eq!(dag.cache_misses(), 2);
    assert_eq!(dag.cache_hits(), 0);
}

#[test]
fn cache_invalidation_remove_node() {
    let mut dag = diamond_dag();
    dag.leaves();
    assert_eq!(dag.cache_misses(), 1);
    dag.remove_node("d").unwrap();
    dag.leaves();
    assert_eq!(dag.cache_misses(), 2);
    assert_eq!(dag.cache_hits(), 0);
}

#[test]
fn cache_invalidation_remove_edge() {
    let mut dag = diamond_dag();
    dag.topological_levels().unwrap();
    assert_eq!(dag.cache_misses(), 1);
    dag.remove_edge("b", "d").unwrap();
    dag.topological_levels().unwrap();
    assert_eq!(dag.cache_misses(), 2);
    assert_eq!(dag.cache_hits(), 0);
}

#[test]
fn cache_correctness_after_mutation() {
    let mut dag = diamond_dag();
    let t1 = dag.topological_sort().unwrap();
    let names1: Vec<&str> = t1.iter().map(|n| n.name.as_str()).collect();
    assert!(!names1.contains(&"e"));

    dag.add_node("e".into(), ()).unwrap();
    dag.add_edge("d", "e", None, None).unwrap();

    let t2 = dag.topological_sort().unwrap();
    let names2: Vec<&str> = t2.iter().map(|n| n.name.as_str()).collect();
    assert!(names2.contains(&"e"));
    // "e" should come after "d"
    let d_pos = names2.iter().position(|&n| n == "d").unwrap();
    let e_pos = names2.iter().position(|&n| n == "e").unwrap();
    assert!(d_pos < e_pos);
}

#[test]
fn clear_cache() {
    let dag = diamond_dag();
    dag.roots();
    assert_eq!(dag.cache_misses(), 1);
    dag.roots();
    assert_eq!(dag.cache_hits(), 1);

    dag.clear_cache();
    dag.roots();
    assert_eq!(dag.cache_misses(), 2);
}

#[test]
fn generation_increments() {
    let mut dag: DAG = DAG::new();
    assert_eq!(dag.generation(), 0);
    dag.add_node("a".into(), ()).unwrap();
    assert_eq!(dag.generation(), 1);
    dag.add_node("b".into(), ()).unwrap();
    assert_eq!(dag.generation(), 2);
    dag.add_edge("a", "b", None, None).unwrap();
    assert_eq!(dag.generation(), 3);
    dag.remove_edge("a", "b").unwrap();
    assert_eq!(dag.generation(), 4);
    dag.remove_node("b").unwrap();
    assert_eq!(dag.generation(), 5);
}

#[test]
fn cache_size() {
    let dag = diamond_dag();
    assert_eq!(dag.cache_size(), 0);

    dag.roots();
    assert_eq!(dag.cache_size(), 1);

    dag.leaves();
    assert_eq!(dag.cache_size(), 2);

    dag.topological_sort().unwrap();
    assert_eq!(dag.cache_size(), 3);

    dag.topological_sort_dfs().unwrap();
    assert_eq!(dag.cache_size(), 4);

    dag.topological_levels().unwrap();
    assert_eq!(dag.cache_size(), 5);
}
