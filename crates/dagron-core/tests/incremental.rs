use dagron_core::DAG;

fn linear_dag() -> DAG {
    let mut dag = DAG::new();
    dag.add_node("a".into(), ()).unwrap();
    dag.add_node("b".into(), ()).unwrap();
    dag.add_node("c".into(), ()).unwrap();
    dag.add_edge("a", "b", None, None).unwrap();
    dag.add_edge("b", "c", None, None).unwrap();
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

// --- dirty_set ---

#[test]
fn dirty_set_single_root() {
    let dag = linear_dag();
    let mut dirty = dag.dirty_set(&["a"]).unwrap();
    dirty.sort();
    assert_eq!(dirty, vec!["a", "b", "c"]);
}

#[test]
fn dirty_set_middle_node() {
    let dag = linear_dag();
    let mut dirty = dag.dirty_set(&["b"]).unwrap();
    dirty.sort();
    assert_eq!(dirty, vec!["b", "c"]);
}

#[test]
fn dirty_set_leaf_no_descendants() {
    let dag = linear_dag();
    let dirty = dag.dirty_set(&["c"]).unwrap();
    assert_eq!(dirty, vec!["c"]);
}

#[test]
fn dirty_set_empty_changed() {
    let dag = linear_dag();
    let dirty = dag.dirty_set(&[]).unwrap();
    assert!(dirty.is_empty());
}

#[test]
fn dirty_set_multiple_changed() {
    let dag = diamond_dag();
    let mut dirty = dag.dirty_set(&["b", "c"]).unwrap();
    dirty.sort();
    assert_eq!(dirty, vec!["b", "c", "d"]);
}

// --- change_provenance ---

#[test]
fn provenance_single_source() {
    let dag = linear_dag();
    let prov = dag.change_provenance(&["a"]).unwrap();
    assert!(prov["a"].contains(&"a".to_string()));
    assert!(prov["b"].contains(&"a".to_string()));
    assert!(prov["c"].contains(&"a".to_string()));
}

#[test]
fn provenance_multiple_sources_converge() {
    let dag = diamond_dag();
    let prov = dag.change_provenance(&["b", "c"]).unwrap();
    // d should have both b and c as provenance
    let mut d_prov = prov["d"].clone();
    d_prov.sort();
    assert_eq!(d_prov, vec!["b", "c"]);
}

#[test]
fn provenance_empty_changed() {
    let dag = linear_dag();
    let prov = dag.change_provenance(&[]).unwrap();
    assert!(prov.is_empty());
}
