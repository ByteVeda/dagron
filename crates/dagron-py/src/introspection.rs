use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::node::PyNodeId;

#[pymethods]
impl PyDAG {
    /// Check if a node with the given name exists.
    pub fn has_node(&self, name: &str) -> bool {
        self.inner.has_node(name)
    }

    /// Check if an edge exists between two nodes.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    pub fn has_edge(&self, from_node: &str, to_node: &str) -> PyResult<bool> {
        self.inner.has_edge(from_node, to_node).map_err(errors::into_pyerr)
    }

    /// Return the number of nodes in the graph.
    pub fn node_count(&self) -> usize {
        self.inner.node_count()
    }

    /// Return the number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.inner.edge_count()
    }

    /// Get the payload associated with a node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn get_payload(&self, py: Python<'_>, name: &str) -> PyResult<Option<PyObject>> {
        let p = self.inner.get_payload(name).map_err(errors::into_pyerr)?;
        Ok(p.payload.as_ref().map(|v| v.clone_ref(py)))
    }

    /// Set the payload for a node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn set_payload(&mut self, name: &str, payload: Option<Py<PyAny>>) -> PyResult<()> {
        let p = self.inner.get_payload_mut(name).map_err(errors::into_pyerr)?;
        p.payload = payload;
        Ok(())
    }

    /// Get the metadata associated with a node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn get_metadata(&self, py: Python<'_>, name: &str) -> PyResult<Option<PyObject>> {
        let p = self.inner.get_payload(name).map_err(errors::into_pyerr)?;
        Ok(p.metadata.as_ref().map(|v| v.clone_ref(py)))
    }

    /// Set the metadata for a node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn set_metadata(&mut self, name: &str, metadata: Option<Py<PyAny>>) -> PyResult<()> {
        let p = self.inner.get_payload_mut(name).map_err(errors::into_pyerr)?;
        p.metadata = metadata;
        Ok(())
    }

    /// Get the immediate predecessors (parents) of a node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn predecessors(&self, name: &str) -> PyResult<Vec<PyNodeId>> {
        let nodes = self.inner.predecessors(name).map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }

    /// Get the immediate successors (children) of a node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn successors(&self, name: &str) -> PyResult<Vec<PyNodeId>> {
        let nodes = self.inner.successors(name).map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }

    /// Get all ancestors of a node (transitive predecessors).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn ancestors(&self, py: Python<'_>, name: &str) -> PyResult<Vec<PyNodeId>> {
        let idx = self.inner.resolve_name(name).map_err(errors::into_pyerr)?;
        let graph_ref = self.inner.inner_graph();
        let indices = py.allow_threads(|| dagron_core::algorithms::ancestors(graph_ref, idx));
        Ok(indices
            .iter()
            .map(|&i| PyNodeId {
                index: i.index() as u32,
                name: self.inner.inner_graph()[i].name.clone(),
            })
            .collect())
    }

    /// Get all descendants of a node (transitive successors).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn descendants(&self, py: Python<'_>, name: &str) -> PyResult<Vec<PyNodeId>> {
        let idx = self.inner.resolve_name(name).map_err(errors::into_pyerr)?;
        let graph_ref = self.inner.inner_graph();
        let indices = py.allow_threads(|| dagron_core::algorithms::descendants(graph_ref, idx));
        Ok(indices
            .iter()
            .map(|&i| PyNodeId {
                index: i.index() as u32,
                name: self.inner.inner_graph()[i].name.clone(),
            })
            .collect())
    }

    /// Get the in-degree (number of incoming edges) of a node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn in_degree(&self, name: &str) -> PyResult<usize> {
        self.inner.in_degree(name).map_err(errors::into_pyerr)
    }

    /// Get the out-degree (number of outgoing edges) of a node.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn out_degree(&self, name: &str) -> PyResult<usize> {
        self.inner.out_degree(name).map_err(errors::into_pyerr)
    }

    /// Get all root nodes (nodes with no incoming edges).
    pub fn roots(&self) -> Vec<PyNodeId> {
        self.inner.roots().into_iter().map(PyNodeId::from).collect()
    }

    /// Get all leaf nodes (nodes with no outgoing edges).
    pub fn leaves(&self) -> Vec<PyNodeId> {
        self.inner.leaves().into_iter().map(PyNodeId::from).collect()
    }

    /// Get all nodes in the graph.
    pub fn nodes(&self) -> Vec<PyNodeId> {
        self.inner.nodes().into_iter().map(PyNodeId::from).collect()
    }
}
