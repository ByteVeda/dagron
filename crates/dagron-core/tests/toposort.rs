use dagron_core::DAG;
use std::collections::HashSet;

fn names(ids: &[dagron_core::NodeId]) -> Vec<String> {
    ids.iter().map(|id| id.name.clone()).collect()
}

fn linear_dag() -> DAG {
    // a -> b -> c -> d
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

/// Assert that `order` is a valid topological order: for every edge u->v, u appears before v.
fn assert_valid_topo_order(dag: &DAG, order: &[String]) {
    let pos: std::collections::HashMap<&str, usize> = order
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();
    for (from, to) in dag.edges() {
        assert!(
            pos[from.as_str()] < pos[to.as_str()],
            "{from} should come before {to} in topological order"
        );
    }
}

#[test]
fn topological_sort_kahn_linear() {
    let dag = linear_dag();
    let order = names(&dag.topological_sort().unwrap());
    assert_eq!(order, vec!["a", "b", "c", "d"]);
}

#[test]
fn topological_sort_dfs_linear() {
    let dag = linear_dag();
    let order = names(&dag.topological_sort_dfs().unwrap());
    assert_eq!(order, vec!["a", "b", "c", "d"]);
}

#[test]
fn topological_sort_kahn_diamond_is_valid() {
    let dag = diamond_dag();
    let order = names(&dag.topological_sort().unwrap());
    assert_eq!(order.len(), 4);
    assert_valid_topo_order(&dag, &order);
}

#[test]
fn topological_sort_dfs_diamond_is_valid() {
    let dag = diamond_dag();
    let order = names(&dag.topological_sort_dfs().unwrap());
    assert_eq!(order.len(), 4);
    assert_valid_topo_order(&dag, &order);
}

#[test]
fn topological_levels_linear() {
    let dag = linear_dag();
    let levels = dag.topological_levels().unwrap();
    assert_eq!(levels.len(), 4);
    for (i, level) in levels.iter().enumerate() {
        assert_eq!(level.len(), 1);
        assert_eq!(level[0].name, ["a", "b", "c", "d"][i]);
    }
}

#[test]
fn topological_levels_diamond() {
    let dag = diamond_dag();
    let levels = dag.topological_levels().unwrap();
    assert_eq!(levels.len(), 3);
    // Level 0: root "a"
    let l0: HashSet<String> = levels[0].iter().map(|n| n.name.clone()).collect();
    assert_eq!(l0, HashSet::from(["a".into()]));
    // Level 1: "b" and "c"
    let l1: HashSet<String> = levels[1].iter().map(|n| n.name.clone()).collect();
    assert_eq!(l1, HashSet::from(["b".into(), "c".into()]));
    // Level 2: "d"
    let l2: HashSet<String> = levels[2].iter().map(|n| n.name.clone()).collect();
    assert_eq!(l2, HashSet::from(["d".into()]));
}

#[test]
fn topological_sort_empty_dag() {
    let dag = DAG::<()>::new();
    let order = dag.topological_sort().unwrap();
    assert!(order.is_empty());
}

#[test]
fn topological_sort_single_node() {
    let mut dag = DAG::new();
    dag.add_node("only".into(), ()).unwrap();
    let order = names(&dag.topological_sort().unwrap());
    assert_eq!(order, vec!["only"]);
}

#[test]
fn topological_levels_empty_dag() {
    let dag = DAG::<()>::new();
    let levels = dag.topological_levels().unwrap();
    assert!(levels.is_empty());
}

#[test]
fn topological_sort_disconnected_components() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    // a -> b, c is isolated
    dag.add_edge("a", "b", None, None).unwrap();
    let order = names(&dag.topological_sort().unwrap());
    assert_eq!(order.len(), 3);
    // a must come before b
    let a_pos = order.iter().position(|n| n == "a").unwrap();
    let b_pos = order.iter().position(|n| n == "b").unwrap();
    assert!(a_pos < b_pos);
}
