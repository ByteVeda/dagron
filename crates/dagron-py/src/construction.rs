use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::dag::PyDAG;
use crate::errors;
use crate::node::PyNodeRef;
use crate::noderef::NodeArg;
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
    ///     A NodeRef for the newly created node. NodeRef is a stable handle
    ///     that can be passed to any method that accepts a node identifier.
    ///
    /// Raises:
    ///     DuplicateNodeError: If a node with this name already exists.
    #[pyo3(signature = (name, payload=None, metadata=None))]
    pub fn add_node(
        &mut self,
        name: String,
        payload: Option<Py<PyAny>>,
        metadata: Option<Py<PyAny>>,
    ) -> PyResult<PyNodeRef> {
        let py_payload = PyNodePayload { payload, metadata };
        let node_ref = self
            .inner
            .add_node(name, py_payload)
            .map_err(errors::into_pyerr)?;
        Ok(node_ref.into())
    }

    /// Look up the current NodeRef for a given name, returning None if no
    /// node with that name exists.
    pub fn node_ref(&self, name: &str) -> Option<PyNodeRef> {
        self.inner.node_ref(name).map(PyNodeRef::from)
    }

    /// Add multiple nodes at once. More efficient than repeated add_node calls.
    ///
    /// Args:
    ///     nodes: List of node names (strings) or (name, payload) tuples or (name, payload, metadata) tuples.
    ///
    /// Returns:
    ///     List of NodeRef objects.
    ///
    /// Raises:
    ///     DuplicateNodeError: If any node name already exists.
    pub fn add_nodes(&mut self, nodes: &Bound<'_, PyList>) -> PyResult<Vec<PyNodeRef>> {
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
    ///     from_node: Source node — accepts either a string name or a NodeRef.
    ///     to_node: Target node — accepts either a string name or a NodeRef.
    ///     weight: Optional edge weight (default 1.0).
    ///     label: Optional edge label.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    ///     StaleNodeRefError: If a NodeRef points to a removed/replaced node.
    ///     CycleError: If the edge would create a cycle.
    #[pyo3(signature = (from_node, to_node, weight=None, label=None))]
    pub fn add_edge(
        &mut self,
        py: Python<'_>,
        from_node: NodeArg,
        to_node: NodeArg,
        weight: Option<f64>,
        label: Option<String>,
    ) -> PyResult<()> {
        let from = from_node.into_name(&self.inner)?;
        let to = to_node.into_name(&self.inner)?;

        // Resolve names first while we have &self
        let from_idx = self.inner.resolve_name(&from).map_err(errors::into_pyerr)?;
        let to_idx = self.inner.resolve_name(&to).map_err(errors::into_pyerr)?;

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
            return Err(errors::into_pyerr(dagron_core::DagronError::Cycle(
                format!(
                    "Edge {} -> {} would create a cycle: {}",
                    from,
                    to,
                    names.join(" -> ")
                ),
            )));
        }

        let edge_data = dagron_core::EdgeData {
            weight: weight.unwrap_or(1.0),
            label,
        };
        self.inner
            .inner_graph_mut()
            .add_edge(from_idx, to_idx, edge_data);
        self.inner.bump_generation();
        Ok(())
    }

    /// Add multiple edges at once.
    ///
    /// Args:
    ///     edges: List of (from, to) tuples (either strings or NodeRefs),
    ///            optionally with weight and label.
    ///
    /// Raises:
    ///     NodeNotFoundError: If any referenced node doesn't exist.
    ///     CycleError: If any edge would create a cycle.
    pub fn add_edges(&mut self, py: Python<'_>, edges: &Bound<'_, PyList>) -> PyResult<()> {
        for item in edges.iter() {
            // Try the largest tuple first so we don't lose weight/label.
            if let Ok((from, to, weight, label)) = item.extract::<(NodeArg, NodeArg, f64, String)>()
            {
                self.add_edge(py, from, to, Some(weight), Some(label))?;
            } else if let Ok((from, to, weight)) = item.extract::<(NodeArg, NodeArg, f64)>() {
                self.add_edge(py, from, to, Some(weight), None)?;
            } else if let Ok((from, to)) = item.extract::<(NodeArg, NodeArg)>() {
                self.add_edge(py, from, to, None, None)?;
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "Each edge must be a (from, to), (from, to, weight), or (from, to, weight, label) tuple. \
                     `from` and `to` may be a str or NodeRef.",
                ));
            }
        }
        Ok(())
    }

    /// Remove a node and all its incident edges.
    ///
    /// Args:
    ///     node: Node to remove (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn remove_node(&mut self, node: NodeArg) -> PyResult<()> {
        let name = node.into_name(&self.inner)?;
        self.inner.remove_node(&name).map_err(errors::into_pyerr)
    }

    /// Remove an edge between two nodes.
    ///
    /// Args:
    ///     from_node: Source node (str or NodeRef).
    ///     to_node: Target node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    ///     EdgeNotFoundError: If no edge exists between the nodes.
    pub fn remove_edge(&mut self, from_node: NodeArg, to_node: NodeArg) -> PyResult<()> {
        let from = from_node.into_name(&self.inner)?;
        let to = to_node.into_name(&self.inner)?;
        self.inner
            .remove_edge(&from, &to)
            .map_err(errors::into_pyerr)
    }

    /// Create a new DAGBuilder for fluent DAG construction.
    ///
    /// Returns:
    ///     A new DAGBuilder instance.
    #[staticmethod]
    pub fn builder(py: Python<'_>) -> PyResult<PyObject> {
        let m = py.import("dagron.builder")?;
        m.getattr("DAGBuilder")?.call0().map(|o| o.unbind())
    }
}
