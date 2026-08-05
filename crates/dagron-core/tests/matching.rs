use dagron_core::{DagronError, DAG};

fn task_dag() -> DAG {
    let mut dag = DAG::new();
    dag.add_node("task_build".into(), ()).unwrap();
    dag.add_node("task_test".into(), ()).unwrap();
    dag.add_node("task_deploy".into(), ()).unwrap();
    dag.add_node("setup_env".into(), ()).unwrap();
    dag.add_node("cleanup".into(), ()).unwrap();
    dag.add_edge("task_build", "task_test", None, None).unwrap();
    dag.add_edge("task_test", "task_deploy", None, None)
        .unwrap();
    dag.add_edge("setup_env", "task_build", None, None).unwrap();
    dag
}

// --- Regex matching ---

#[test]
fn regex_matches_prefix() {
    let dag = task_dag();
    let nodes = dag.nodes_matching_regex("^task_").unwrap();
    assert_eq!(nodes.len(), 3);
    for n in &nodes {
        assert!(n.name.starts_with("task_"));
    }
}

#[test]
fn regex_matches_suffix() {
    let dag = task_dag();
    let nodes = dag.nodes_matching_regex("_env$").unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].name, "setup_env");
}

#[test]
fn regex_matches_exact() {
    let dag = task_dag();
    let nodes = dag.nodes_matching_regex("^cleanup$").unwrap();
    assert_eq!(nodes.len(), 1);
}

#[test]
fn regex_no_matches() {
    let dag = task_dag();
    let nodes = dag.nodes_matching_regex("^nonexistent").unwrap();
    assert!(nodes.is_empty());
}

#[test]
fn regex_invalid_pattern() {
    let dag = task_dag();
    let result = dag.nodes_matching_regex("[invalid");
    assert!(matches!(result, Err(DagronError::Graph(_))));
}

#[test]
fn regex_matches_all() {
    let dag = task_dag();
    let nodes = dag.nodes_matching_regex(".*").unwrap();
    assert_eq!(nodes.len(), 5);
}

// --- Glob matching ---

#[test]
fn glob_star_prefix() {
    let dag = task_dag();
    let nodes = dag.nodes_matching_glob("task_*").unwrap();
    assert_eq!(nodes.len(), 3);
}

#[test]
fn glob_question_mark() {
    let dag = task_dag();
    // "task_????" should match task_build (10 chars minus "task_" = 5 chars)
    let nodes = dag.nodes_matching_glob("task_????").unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].name, "task_test");
}

#[test]
fn glob_all() {
    let dag = task_dag();
    let nodes = dag.nodes_matching_glob("*").unwrap();
    assert_eq!(nodes.len(), 5);
}

#[test]
fn glob_no_matches() {
    let dag = task_dag();
    let nodes = dag.nodes_matching_glob("xyz_*").unwrap();
    assert!(nodes.is_empty());
}
