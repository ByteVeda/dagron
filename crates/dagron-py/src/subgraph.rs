use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::transforms::clone_edges;

#[pymethods]
impl PyDAG {
    /// Extract induced subgraph containing only the named nodes.
    ///
    /// Edges between the named nodes are preserved.
    ///
    /// Args:
    ///     nodes: List of node names to include.
    ///
    /// Returns:
    ///     A new DAG containing only the specified nodes and edges between them.
    ///
    /// Raises:
    ///     NodeNotFoundError: If any node doesn't exist.
    pub fn subgraph(&self, py: Python<'_>, nodes: Vec<String>) -> PyResult<PyDAG> {
        let mut new_dag = dagron_core::DAG::new();

        // Add matching nodes with cloned payloads
        for name in &nodes {
            if !self.inner.has_node(name) {
                return Err(errors::into_pyerr(dagron_core::DagronError::NodeNotFound(
                    name.clone(),
                )));
            }
            let payload = self.inner.get_payload(name).map_err(errors::into_pyerr)?;
            let cloned = crate::transforms::clone_payload(py, payload);
            new_dag
                .add_node(name.clone(), cloned)
                .map_err(errors::into_pyerr)?;
        }

        // Add edges where both endpoints are in the subgraph
        clone_edges(&self.inner, &mut new_dag)?;

        Ok(PyDAG { inner: new_dag })
    }

    /// Extract subgraph within `depth` hops of root.
    ///
    /// Args:
    ///     root: The root node name.
    ///     depth: Maximum number of hops from root.
    ///     direction: "forward", "backward", or "both" (default "both").
    ///
    /// Returns:
    ///     A new DAG containing nodes within the specified depth.
    ///
    /// Raises:
    ///     NodeNotFoundError: If root doesn't exist.
    #[pyo3(signature = (root, depth, direction="both"))]
    pub fn subgraph_by_depth(
        &self,
        py: Python<'_>,
        root: String,
        depth: usize,
        direction: &str,
    ) -> PyResult<PyDAG> {
        let dir = match direction {
            "forward" => dagron_core::SubgraphDirection::Forward,
            "backward" => dagron_core::SubgraphDirection::Backward,
            "both" => dagron_core::SubgraphDirection::Both,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid direction: '{direction}'. Expected 'forward', 'backward', or 'both'."
                )))
            }
        };

        // Resolve root and compute neighborhood
        let root_idx = self.inner.resolve_name(&root).map_err(errors::into_pyerr)?;
        let neighborhood = py.allow_threads(|| {
            dagron_core::algorithms::depth_neighborhood(
                self.inner.inner_graph(),
                root_idx,
                depth,
                dir,
            )
        });

        let mut new_dag = dagron_core::DAG::new();

        // Add nodes in the neighborhood
        for &idx in &neighborhood {
            let name = &self.inner.inner_graph()[idx].name;
            let payload = self.inner.get_payload(name).map_err(errors::into_pyerr)?;
            let cloned = crate::transforms::clone_payload(py, payload);
            new_dag
                .add_node(name.clone(), cloned)
                .map_err(errors::into_pyerr)?;
        }

        // Add edges where both endpoints are in the subgraph
        clone_edges(&self.inner, &mut new_dag)?;

        Ok(PyDAG { inner: new_dag })
    }
}
