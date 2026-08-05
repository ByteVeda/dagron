//! Verifies the example in `README.md` compiles and runs.
use dagron_core::{DagronError, DAG};

#[test]
fn readme_quickstart() -> Result<(), DagronError> {
    let mut dag: DAG<i32> = DAG::new();

    dag.add_node("extract".to_string(), 1)?;
    dag.add_node("transform".to_string(), 2)?;
    dag.add_node("load".to_string(), 3)?;

    dag.add_edge("extract", "transform", None, None)?;
    dag.add_edge("transform", "load", None, None)?;

    // Dependency order.
    let order = dag.topological_sort()?;
    assert_eq!(order[0].name, "extract");

    // Nodes grouped into levels that can run in parallel.
    let levels = dag.topological_levels()?;
    assert_eq!(levels.len(), 3);

    Ok(())
}
