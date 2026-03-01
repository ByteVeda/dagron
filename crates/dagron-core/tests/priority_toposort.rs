use dagron_core::DAG;

fn names(ids: &[dagron_core::NodeId]) -> Vec<String> {
    ids.iter().map(|id| id.name.clone()).collect()
}

fn level_names(levels: &[Vec<dagron_core::NodeId>]) -> Vec<Vec<String>> {
    levels
        .iter()
        .map(|level| level.iter().map(|id| id.name.clone()).collect())
        .collect()
}

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
fn priority_sort_no_priorities() {
    let dag = diamond_dag();
    let priorities = ahash::AHashMap::new();
    let order = names(&dag.topological_sort_priority(&priorities).unwrap());
    // Without priorities, alphabetical tiebreak
    assert_eq!(order[0], "a");
    assert_eq!(order.len(), 4);
    assert!(
        order.iter().position(|n| n == "b").unwrap() < order.iter().position(|n| n == "d").unwrap()
    );
    assert!(
        order.iter().position(|n| n == "c").unwrap() < order.iter().position(|n| n == "d").unwrap()
    );
}

#[test]
fn priority_sort_higher_priority_first() {
    let dag = diamond_dag();
    let mut priorities = ahash::AHashMap::new();
    priorities.insert("c".to_string(), 10.0);
    priorities.insert("b".to_string(), 1.0);

    let order = names(&dag.topological_sort_priority(&priorities).unwrap());
    assert_eq!(order[0], "a");
    // c has higher priority, should come before b
    let c_pos = order.iter().position(|n| n == "c").unwrap();
    let b_pos = order.iter().position(|n| n == "b").unwrap();
    assert!(c_pos < b_pos);
}

#[test]
fn priority_sort_respects_dependencies() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();

    // Even with c having max priority, it can't skip ahead of its deps
    let mut priorities = ahash::AHashMap::new();
    priorities.insert("c".to_string(), 100.0);
    let order = names(&dag.topological_sort_priority(&priorities).unwrap());
    assert_eq!(order, vec!["a", "b", "c"]);
}

#[test]
fn priority_sort_empty() {
    let dag = DAG::<()>::new();
    let priorities = ahash::AHashMap::new();
    let order = dag.topological_sort_priority(&priorities).unwrap();
    assert!(order.is_empty());
}

#[test]
fn priority_levels_sorted_within_level() {
    let dag = diamond_dag();
    let mut priorities = ahash::AHashMap::new();
    priorities.insert("c".to_string(), 10.0);
    priorities.insert("b".to_string(), 1.0);

    let levels = level_names(&dag.topological_levels_priority(&priorities).unwrap());
    assert_eq!(levels.len(), 3);
    assert_eq!(levels[0], vec!["a"]);
    // Within level 1, c (priority 10) should come before b (priority 1)
    assert_eq!(levels[1], vec!["c", "b"]);
    assert_eq!(levels[2], vec!["d"]);
}

#[test]
fn priority_levels_default_priority_alphabetical() {
    let dag = diamond_dag();
    let priorities = ahash::AHashMap::new();
    let levels = level_names(&dag.topological_levels_priority(&priorities).unwrap());
    assert_eq!(levels[1], vec!["b", "c"]); // alphabetical when equal priority
}

#[test]
fn priority_sort_multiple_roots() {
    let mut dag = DAG::new();
    dag.add_node("x".into(), ()).unwrap();
    dag.add_node("y".into(), ()).unwrap();
    dag.add_node("z".into(), ()).unwrap();
    dag.add_node("sink".into(), ()).unwrap();
    dag.add_edge("x", "sink", None, None).unwrap();
    dag.add_edge("y", "sink", None, None).unwrap();
    dag.add_edge("z", "sink", None, None).unwrap();

    let mut priorities = ahash::AHashMap::new();
    priorities.insert("z".to_string(), 5.0);
    priorities.insert("x".to_string(), 3.0);
    priorities.insert("y".to_string(), 1.0);

    let order = names(&dag.topological_sort_priority(&priorities).unwrap());
    assert_eq!(order[0], "z"); // highest priority root
    assert_eq!(order[1], "x");
    assert_eq!(order[2], "y");
    assert_eq!(order[3], "sink");
}

#[test]
fn priority_sort_negative_priorities() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();

    let mut priorities = ahash::AHashMap::new();
    priorities.insert("a".to_string(), -10.0);
    priorities.insert("b".to_string(), 0.0);
    priorities.insert("c".to_string(), 10.0);

    let order = names(&dag.topological_sort_priority(&priorities).unwrap());
    assert_eq!(order, vec!["c", "b", "a"]);
}
