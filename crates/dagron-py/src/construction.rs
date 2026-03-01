use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::dag::PyDAG;
use crate::errors;
use crate::node::PyNodeId;
use crate::payload::PyNodePayload;

#[pymethods]
impl PyDAG {
    /// Add a single node to the graph.
    ///
    /// Args:
    ///     name: Unique name for the node.
    ///     payload: Optional arbitrary Python object to associate with the node.
    ///     metadata: Optional metadata Python object.
    ///
    /// Returns:
    ///     NodeId for the newly created node.
    ///
    /// Raises:
    ///     DuplicateNodeError: If a node with this name already exists.
    #[pyo3(signature = (name, payload=None, metadata=None))]
    pub fn add_node(
        &mut self,
        name: String,
        payload: Option<Py<PyAny>>,
        metadata: Option<Py<PyAny>>,
    ) -> PyResult<PyNodeId> {
        let py_payload = PyNodePayload { payload, metadata };
        let node_id = self.inner.add_node(name, py_payload).map_err(errors::into_pyerr)?;
        Ok(node_id.into())
    }

    /// Add multiple nodes at once. More efficient than repeated add_node calls.
    ///
    /// Args:
    ///     nodes: List of node names (strings) or (name, payload) tuples or (name, payload, metadata) tuples.
    ///
    /// Returns:
    ///     List of NodeId objects.
    ///
    /// Raises:
    ///     DuplicateNodeError: If any node name already exists.
    pub fn add_nodes(&mut self, nodes: &Bound<'_, PyList>) -> PyResult<Vec<PyNodeId>> {
        let mut result = Vec::with_capacity(nodes.len());
        for item in nodes.iter() {
            if let Ok(name) = item.extract::<String>() {
                result.push(self.add_node(name, None, None)?);
            } else if let Ok((name, payload)) = item.extract::<(String, Py<PyAny>)>() {
                result.push(self.add_node(name, Some(payload), None)?);
            } else if let Ok((name, payload, metadata)) =
                item.extract::<(String, Py<PyAny>, Py<PyAny>)>()
            {
                result.push(self.add_node(name, Some(payload), Some(metadata))?);
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "Each node must be a string name, (name, payload) tuple, or (name, payload, metadata) tuple",
                ));
            }
        }
        Ok(result)
    }

    /// Add a directed edge from one node to another.
    ///
    /// Args:
    ///     from_node: Name of the source node.
    ///     to_node: Name of the target node.
    ///     weight: Optional edge weight (default 1.0).
    ///     label: Optional edge label.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    ///     CycleError: If the edge would create a cycle.
    #[pyo3(signature = (from_node, to_node, weight=None, label=None))]
    pub fn add_edge(
        &mut self,
        py: Python<'_>,
        from_node: &str,
        to_node: &str,
        weight: Option<f64>,
        label: Option<String>,
    ) -> PyResult<()> {
        // Resolve names first while we have &self
        let from_idx = self.inner.resolve_name(from_node).map_err(errors::into_pyerr)?;
        let to_idx = self.inner.resolve_name(to_node).map_err(errors::into_pyerr)?;

        // Check for cycle — release GIL for the graph traversal
        let graph_ref = self.inner.inner_graph();
        let cycle = py.allow_threads(|| {
            dagron_core::algorithms::would_create_cycle(graph_ref, from_idx, to_idx)
        });

        if let Some(cycle_path) = cycle {
            let names: Vec<String> = cycle_path
                .iter()
                .map(|&idx| self.inner.inner_graph()[idx].name.clone())
                .collect();
            return Err(errors::into_pyerr(dagron_core::DagronError::Cycle(format!(
                "Edge {} -> {} would create a cycle: {}",
                from_node,
                to_node,
                names.join(" -> ")
            ))));
        }

        let edge_data = dagron_core::EdgeData {
            weight: weight.unwrap_or(1.0),
            label,
        };
        self.inner.inner_graph_mut().add_edge(from_idx, to_idx, edge_data);
        self.inner.bump_generation();
        Ok(())
    }

    /// Add multiple edges at once.
    ///
    /// Args:
    ///     edges: List of (from, to) tuples, optionally with weight and label:
    ///            (from, to), (from, to, weight), or (from, to, weight, label).
    ///
    /// Raises:
    ///     NodeNotFoundError: If any referenced node doesn't exist.
    ///     CycleError: If any edge would create a cycle.
    pub fn add_edges(&mut self, py: Python<'_>, edges: &Bound<'_, PyList>) -> PyResult<()> {
        for item in edges.iter() {
            if let Ok((from, to)) = item.extract::<(String, String)>() {
                self.add_edge(py, &from, &to, None, None)?;
            } else if let Ok((from, to, weight)) = item.extract::<(String, String, f64)>() {
                self.add_edge(py, &from, &to, Some(weight), None)?;
            } else if let Ok((from, to, weight, label)) =
                item.extract::<(String, String, f64, String)>()
            {
                self.add_edge(py, &from, &to, Some(weight), Some(label))?;
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "Each edge must be a (from, to), (from, to, weight), or (from, to, weight, label) tuple",
                ));
            }
        }
        Ok(())
    }

    /// Remove a node and all its incident edges.
    ///
    /// Args:
    ///     name: Name of the node to remove.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn remove_node(&mut self, name: &str) -> PyResult<()> {
        self.inner.remove_node(name).map_err(errors::into_pyerr)
    }

    /// Remove an edge between two nodes.
    ///
    /// Args:
    ///     from_node: Name of the source node.
    ///     to_node: Name of the target node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    ///     EdgeNotFoundError: If no edge exists between the nodes.
    pub fn remove_edge(&mut self, from_node: &str, to_node: &str) -> PyResult<()> {
        self.inner.remove_edge(from_node, to_node).map_err(errors::into_pyerr)
    }
}
