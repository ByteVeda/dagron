use dagron_core::{DagronError, DAG};
use std::collections::HashSet;

fn names(ids: &[dagron_core::NodeId]) -> HashSet<String> {
    ids.iter().map(|id| id.name.clone()).collect()
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
fn has_node_positive_and_negative() {
    let dag = diamond_dag();
    assert!(dag.has_node("a"));
    assert!(!dag.has_node("z"));
}

#[test]
fn has_edge_positive_and_negative() {
    let dag = diamond_dag();
    assert!(dag.has_edge("a", "b").unwrap());
    assert!(!dag.has_edge("a", "d").unwrap());
}

#[test]
fn has_edge_missing_node_returns_error() {
    let dag = diamond_dag();
    let err = dag.has_edge("a", "ghost").unwrap_err();
    assert!(matches!(err, DagronError::NodeNotFound(_)));
}

#[test]
fn predecessors_of_leaf() {
    let dag = diamond_dag();
    let preds = names(&dag.predecessors("d").unwrap());
    assert_eq!(preds, HashSet::from(["b".into(), "c".into()]));
}

#[test]
fn predecessors_of_root_is_empty() {
    let dag = diamond_dag();
    assert!(dag.predecessors("a").unwrap().is_empty());
}

#[test]
fn successors_of_root() {
    let dag = diamond_dag();
    let succs = names(&dag.successors("a").unwrap());
    assert_eq!(succs, HashSet::from(["b".into(), "c".into()]));
}

#[test]
fn successors_of_leaf_is_empty() {
    let dag = diamond_dag();
    assert!(dag.successors("d").unwrap().is_empty());
}

#[test]
fn ancestors_transitive() {
    let dag = diamond_dag();
    let anc = names(&dag.ancestors("d").unwrap());
    assert_eq!(anc, HashSet::from(["a".into(), "b".into(), "c".into()]));
}

#[test]
fn descendants_transitive() {
    let dag = diamond_dag();
    let desc = names(&dag.descendants("a").unwrap());
    assert_eq!(desc, HashSet::from(["b".into(), "c".into(), "d".into()]));
}

#[test]
fn in_degree_and_out_degree() {
    let dag = diamond_dag();
    assert_eq!(dag.in_degree("a").unwrap(), 0);
    assert_eq!(dag.out_degree("a").unwrap(), 2);
    assert_eq!(dag.in_degree("d").unwrap(), 2);
    assert_eq!(dag.out_degree("d").unwrap(), 0);
    assert_eq!(dag.in_degree("b").unwrap(), 1);
    assert_eq!(dag.out_degree("b").unwrap(), 1);
}

#[test]
fn roots_and_leaves() {
    let dag = diamond_dag();
    let root_names = names(&dag.roots());
    let leaf_names = names(&dag.leaves());
    assert_eq!(root_names, HashSet::from(["a".into()]));
    assert_eq!(leaf_names, HashSet::from(["d".into()]));
}

#[test]
fn nodes_returns_all() {
    let dag = diamond_dag();
    let all = names(&dag.nodes());
    assert_eq!(
        all,
        HashSet::from(["a".into(), "b".into(), "c".into(), "d".into()])
    );
}

#[test]
fn node_names_returns_all() {
    let dag = diamond_dag();
    let all: HashSet<String> = dag.node_names().into_iter().collect();
    assert_eq!(
        all,
        HashSet::from(["a".into(), "b".into(), "c".into(), "d".into()])
    );
}

#[test]
fn edges_returns_all_pairs() {
    let dag = diamond_dag();
    let edges: HashSet<(String, String)> = dag.edges().into_iter().collect();
    assert_eq!(edges.len(), 4);
    assert!(edges.contains(&("a".into(), "b".into())));
    assert!(edges.contains(&("a".into(), "c".into())));
    assert!(edges.contains(&("b".into(), "d".into())));
    assert!(edges.contains(&("c".into(), "d".into())));
}

#[test]
fn node_count_and_edge_count() {
    let dag = diamond_dag();
    assert_eq!(dag.node_count(), 4);
    assert_eq!(dag.edge_count(), 4);
}

#[test]
fn get_payload_and_get_payload_mut() {
    let mut dag = DAG::<String>::new();
    dag.add_node("x".into(), "hello".into()).unwrap();
    assert_eq!(dag.get_payload("x").unwrap(), "hello");

    *dag.get_payload_mut("x").unwrap() = "world".into();
    assert_eq!(dag.get_payload("x").unwrap(), "world");
}

#[test]
fn get_payload_missing_node_fails() {
    let dag = DAG::<()>::new();
    let err = dag.get_payload("ghost").unwrap_err();
    assert!(matches!(err, DagronError::NodeNotFound(_)));
}

#[test]
fn empty_dag_roots_and_leaves_empty() {
    let dag = DAG::<()>::new();
    assert!(dag.roots().is_empty());
    assert!(dag.leaves().is_empty());
}

#[test]
fn isolated_node_is_both_root_and_leaf() {
    let mut dag = DAG::new();
    dag.add_node("solo".into(), ()).unwrap();
    let root_names = names(&dag.roots());
    let leaf_names = names(&dag.leaves());
    assert_eq!(root_names, HashSet::from(["solo".into()]));
    assert_eq!(leaf_names, HashSet::from(["solo".into()]));
}
