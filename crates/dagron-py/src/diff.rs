use pyo3::prelude::*;

use crate::dag::PyDAG;

#[pyclass(frozen, name = "GraphDiff")]
#[derive(Clone)]
pub struct PyGraphDiff {
    #[pyo3(get)]
    pub added_nodes: Vec<String>,
    #[pyo3(get)]
    pub removed_nodes: Vec<String>,
    #[pyo3(get)]
    pub changed_nodes: Vec<String>,
    #[pyo3(get)]
    pub added_edges: Vec<(String, String)>,
    #[pyo3(get)]
    pub removed_edges: Vec<(String, String)>,
    #[pyo3(get)]
    pub changed_edges: Vec<(String, String)>,
}

#[pymethods]
impl PyGraphDiff {
    fn __repr__(&self) -> String {
        format!(
            "GraphDiff(added_nodes={}, removed_nodes={}, changed_nodes={}, \
             added_edges={}, removed_edges={}, changed_edges={})",
            self.added_nodes.len(),
            self.removed_nodes.len(),
            self.changed_nodes.len(),
            self.added_edges.len(),
            self.removed_edges.len(),
            self.changed_edges.len(),
        )
    }

    fn to_dict(&self, py: Python<'_>) -> std::collections::HashMap<&str, PyObject> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "added_nodes",
            self.added_nodes
                .clone()
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "removed_nodes",
            self.removed_nodes
                .clone()
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "changed_nodes",
            self.changed_nodes
                .clone()
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "added_edges",
            self.added_edges
                .clone()
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "removed_edges",
            self.removed_edges
                .clone()
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "changed_edges",
            self.changed_edges
                .clone()
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map
    }
}

#[pymethods]
impl PyDAG {
    /// Compute the diff between this DAG and another.
    ///
    /// Compares graph structure (nodes and edges) and optionally payloads.
    /// For payload comparison, Python __eq__ is used on common nodes.
    /// For edge comparison, weight and label are compared for common edges.
    ///
    /// Args:
    ///     other: The other DAG to compare against.
    ///
    /// Returns:
    ///     A GraphDiff object describing the differences.
    pub fn diff(&self, py: Python<'_>, other: &PyDAG) -> PyResult<PyGraphDiff> {
        // Get structural diff from Rust (GIL released)
        let self_inner = &self.inner;
        let other_inner = &other.inner;
        let structural = py.allow_threads(|| {
            dagron_core::algorithms::diff::structural_diff(
                self_inner.inner_graph(),
                other_inner.inner_graph(),
            )
        });

        // Compare payloads for common nodes using Python __eq__
        let mut changed_nodes = Vec::new();
        for name in &structural.common_nodes {
            let old_payload = self.inner.get_payload(name).ok();
            let new_payload = other.inner.get_payload(name).ok();

            if let (Some(old_p), Some(new_p)) = (old_payload, new_payload) {
                let old_py = old_p.payload.as_ref();
                let new_py = new_p.payload.as_ref();
                match (old_py, new_py) {
                    (Some(old_obj), Some(new_obj)) => {
                        let old_bound = old_obj.bind(py);
                        let new_bound = new_obj.bind(py);
                        let eq = old_bound.eq(new_bound).unwrap_or(false);
                        if !eq {
                            changed_nodes.push(name.clone());
                        }
                    }
                    (None, None) => {} // both None, same
                    _ => {
                        changed_nodes.push(name.clone());
                    }
                }
            }
        }

        // Compare edge data for common edges
        let mut changed_edges = Vec::new();
        for (from, to) in &structural.common_edges {
            let old_edge = get_edge_data(&self.inner, from, to);
            let new_edge = get_edge_data(&other.inner, from, to);
            if let (Some(old_e), Some(new_e)) = (old_edge, new_edge) {
                if (old_e.weight - new_e.weight).abs() > f64::EPSILON || old_e.label != new_e.label
                {
                    changed_edges.push((from.clone(), to.clone()));
                }
            }
        }

        Ok(PyGraphDiff {
            added_nodes: structural.added_nodes,
            removed_nodes: structural.removed_nodes,
            changed_nodes,
            added_edges: structural.added_edges,
            removed_edges: structural.removed_edges,
            changed_edges,
        })
    }
}

fn get_edge_data<'a, P>(
    dag: &'a dagron_core::DAG<P>,
    from: &str,
    to: &str,
) -> Option<&'a dagron_core::EdgeData> {
    let from_idx = dag.resolve_name(from).ok()?;
    let to_idx = dag.resolve_name(to).ok()?;
    let edge_idx = dag.inner_graph().find_edge(from_idx, to_idx)?;
    Some(&dag.inner_graph()[edge_idx])
}
