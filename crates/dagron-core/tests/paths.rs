use ahash::AHashMap;

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

fn linear_dag() -> DAG {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();
    dag
}

// --- all_paths ---

#[test]
fn all_paths_linear() {
    let dag = linear_dag();
    let paths = dag.all_paths("a", "c", None).unwrap();
    assert_eq!(paths.len(), 1);
    let names: Vec<&str> = paths[0].iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b", "c"]);
}

#[test]
fn all_paths_diamond() {
    let dag = diamond_dag();
    let paths = dag.all_paths("a", "d", None).unwrap();
    assert_eq!(paths.len(), 2);
}

#[test]
fn all_paths_with_limit() {
    let dag = diamond_dag();
    let paths = dag.all_paths("a", "d", Some(1)).unwrap();
    assert_eq!(paths.len(), 1);
}

#[test]
fn all_paths_no_path() {
    let dag = linear_dag();
    let paths = dag.all_paths("c", "a", None).unwrap();
    assert!(paths.is_empty());
}

#[test]
fn all_paths_same_node() {
    let dag = linear_dag();
    let paths = dag.all_paths("b", "b", None).unwrap();
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].len(), 1);
}

#[test]
fn all_paths_nonexistent_node() {
    let dag = linear_dag();
    let result = dag.all_paths("a", "z", None);
    assert!(matches!(result, Err(DagronError::NodeNotFound(_))));
}

// --- shortest_path ---

#[test]
fn shortest_path_linear() {
    let dag = linear_dag();
    let path = dag.shortest_path("a", "c").unwrap().unwrap();
    let names: Vec<&str> = path.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b", "c"]);
}

#[test]
fn shortest_path_with_shortcut() {
    let mut dag = diamond_dag();
    dag.add_edge("a", "d", None, None).unwrap();
    let path = dag.shortest_path("a", "d").unwrap().unwrap();
    assert_eq!(path.len(), 2);
}

#[test]
fn shortest_path_no_path() {
    let dag = linear_dag();
    assert!(dag.shortest_path("c", "a").unwrap().is_none());
}

#[test]
fn shortest_path_same_node() {
    let dag = linear_dag();
    let path = dag.shortest_path("b", "b").unwrap().unwrap();
    assert_eq!(path.len(), 1);
}

#[test]
fn shortest_path_disconnected() {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    assert!(dag.shortest_path("a", "b").unwrap().is_none());
}

// --- longest_path ---

#[test]
fn longest_path_linear() {
    let dag = linear_dag();
    let costs = AHashMap::new();
    let (path, cost) = dag.longest_path("a", "c", &costs).unwrap().unwrap();
    let names: Vec<&str> = path.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b", "c"]);
    assert_eq!(cost, 3.0);
}

#[test]
fn longest_path_diamond_weighted() {
    let dag = diamond_dag();
    let mut costs = AHashMap::new();
    costs.insert("a".to_string(), 1.0);
    costs.insert("b".to_string(), 10.0);
    costs.insert("c".to_string(), 2.0);
    costs.insert("d".to_string(), 1.0);

    let (path, cost) = dag.longest_path("a", "d", &costs).unwrap().unwrap();
    let names: Vec<&str> = path.iter().map(|n| n.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b", "d"]);
    assert_eq!(cost, 12.0);
}

#[test]
fn longest_path_no_path() {
    let dag = linear_dag();
    let costs = AHashMap::new();
    assert!(dag.longest_path("c", "a", &costs).unwrap().is_none());
}

#[test]
fn longest_path_nonexistent_node() {
    let dag = linear_dag();
    let costs = AHashMap::new();
    let result = dag.longest_path("a", "z", &costs);
    assert!(matches!(result, Err(DagronError::NodeNotFound(_))));
}
