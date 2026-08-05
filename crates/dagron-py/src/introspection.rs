use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::iterators::PyNodeIterator;
use crate::node::PyNodeId;
use crate::noderef::NodeArg;

#[pymethods]
impl PyDAG {
    /// Check if a node with the given name (or NodeRef) exists.
    pub fn has_node(&self, node: NodeArg) -> bool {
        match node {
            NodeArg::Name(s) => self.inner.has_node(&s),
            NodeArg::Ref(r) => self.inner.resolve_ref(&r).is_ok(),
        }
    }

    /// Check if an edge exists between two nodes (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    pub fn has_edge(&self, from_node: NodeArg, to_node: NodeArg) -> PyResult<bool> {
        let from = from_node.into_name(&self.inner)?;
        let to = to_node.into_name(&self.inner)?;
        self.inner.has_edge(&from, &to).map_err(errors::into_pyerr)
    }

    /// Return the number of nodes in the graph.
    pub fn node_count(&self) -> usize {
        self.inner.node_count()
    }

    /// Return the number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.inner.edge_count()
    }

    /// Get the payload associated with a node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn get_payload(&self, py: Python<'_>, node: NodeArg) -> PyResult<Option<PyObject>> {
        let name = node.into_name(&self.inner)?;
        let p = self.inner.get_payload(&name).map_err(errors::into_pyerr)?;
        Ok(p.payload.as_ref().map(|v| v.clone_ref(py)))
    }

    /// Set the payload for a node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn set_payload(&mut self, node: NodeArg, payload: Option<Py<PyAny>>) -> PyResult<()> {
        let name = node.into_name(&self.inner)?;
        let p = self
            .inner
            .get_payload_mut(&name)
            .map_err(errors::into_pyerr)?;
        p.payload = payload;
        Ok(())
    }

    /// Get the metadata associated with a node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn get_metadata(&self, py: Python<'_>, node: NodeArg) -> PyResult<Option<PyObject>> {
        let name = node.into_name(&self.inner)?;
        let p = self.inner.get_payload(&name).map_err(errors::into_pyerr)?;
        Ok(p.metadata.as_ref().map(|v| v.clone_ref(py)))
    }

    /// Set the metadata for a node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn set_metadata(&mut self, node: NodeArg, metadata: Option<Py<PyAny>>) -> PyResult<()> {
        let name = node.into_name(&self.inner)?;
        let p = self
            .inner
            .get_payload_mut(&name)
            .map_err(errors::into_pyerr)?;
        p.metadata = metadata;
        Ok(())
    }

    /// Get the immediate predecessors (parents) of a node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn predecessors(&self, node: NodeArg) -> PyResult<Vec<PyNodeId>> {
        let name = node.into_name(&self.inner)?;
        let nodes = self.inner.predecessors(&name).map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }

    /// Get the immediate successors (children) of a node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn successors(&self, node: NodeArg) -> PyResult<Vec<PyNodeId>> {
        let name = node.into_name(&self.inner)?;
        let nodes = self.inner.successors(&name).map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }

    /// Get all ancestors of a node (transitive predecessors). Accepts str or NodeRef.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn ancestors(&self, py: Python<'_>, node: NodeArg) -> PyResult<Vec<PyNodeId>> {
        let name = node.into_name(&self.inner)?;
        let idx = self.inner.resolve_name(&name).map_err(errors::into_pyerr)?;
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

    /// Get all descendants of a node (transitive successors). Accepts str or NodeRef.
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn descendants(&self, py: Python<'_>, node: NodeArg) -> PyResult<Vec<PyNodeId>> {
        let name = node.into_name(&self.inner)?;
        let idx = self.inner.resolve_name(&name).map_err(errors::into_pyerr)?;
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

    /// Get the in-degree (number of incoming edges) of a node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn in_degree(&self, node: NodeArg) -> PyResult<usize> {
        let name = node.into_name(&self.inner)?;
        self.inner.in_degree(&name).map_err(errors::into_pyerr)
    }

    /// Get the out-degree (number of outgoing edges) of a node (str or NodeRef).
    ///
    /// Raises:
    ///     NodeNotFoundError: If the node doesn't exist.
    pub fn out_degree(&self, node: NodeArg) -> PyResult<usize> {
        let name = node.into_name(&self.inner)?;
        self.inner.out_degree(&name).map_err(errors::into_pyerr)
    }

    /// Get all root nodes (nodes with no incoming edges).
    pub fn roots(&self) -> Vec<PyNodeId> {
        self.inner.roots().into_iter().map(PyNodeId::from).collect()
    }

    /// Get all leaf nodes (nodes with no outgoing edges).
    pub fn leaves(&self) -> Vec<PyNodeId> {
        self.inner
            .leaves()
            .into_iter()
            .map(PyNodeId::from)
            .collect()
    }

    /// Get all nodes in the graph.
    pub fn nodes(&self) -> Vec<PyNodeId> {
        self.inner.nodes().into_iter().map(PyNodeId::from).collect()
    }

    /// Return a lazy iterator over all nodes.
    pub fn iter_nodes(&self, _py: Python<'_>) -> PyNodeIterator {
        let items: Vec<(u32, String)> = self
            .inner
            .nodes()
            .into_iter()
            .map(|n| (n.index, n.name))
            .collect();
        PyNodeIterator::new(items)
    }

    /// Return a lazy iterator over root nodes.
    pub fn iter_roots(&self, _py: Python<'_>) -> PyNodeIterator {
        let items: Vec<(u32, String)> = self
            .inner
            .roots()
            .into_iter()
            .map(|n| (n.index, n.name))
            .collect();
        PyNodeIterator::new(items)
    }

    /// Return a lazy iterator over leaf nodes.
    pub fn iter_leaves(&self, _py: Python<'_>) -> PyNodeIterator {
        let items: Vec<(u32, String)> = self
            .inner
            .leaves()
            .into_iter()
            .map(|n| (n.index, n.name))
            .collect();
        PyNodeIterator::new(items)
    }

    /// Return a lazy iterator over ancestors of a node (str or NodeRef).
    pub fn iter_ancestors(&self, py: Python<'_>, node: NodeArg) -> PyResult<PyNodeIterator> {
        let name = node.into_name(&self.inner)?;
        let idx = self.inner.resolve_name(&name).map_err(errors::into_pyerr)?;
        let graph_ref = self.inner.inner_graph();
        let indices = py.allow_threads(|| dagron_core::algorithms::ancestors(graph_ref, idx));
        let items: Vec<(u32, String)> = indices
            .iter()
            .map(|&i| (i.index() as u32, self.inner.inner_graph()[i].name.clone()))
            .collect();
        Ok(PyNodeIterator::new(items))
    }

    /// Return a lazy iterator over descendants of a node (str or NodeRef).
    pub fn iter_descendants(&self, py: Python<'_>, node: NodeArg) -> PyResult<PyNodeIterator> {
        let name = node.into_name(&self.inner)?;
        let idx = self.inner.resolve_name(&name).map_err(errors::into_pyerr)?;
        let graph_ref = self.inner.inner_graph();
        let indices = py.allow_threads(|| dagron_core::algorithms::descendants(graph_ref, idx));
        let items: Vec<(u32, String)> = indices
            .iter()
            .map(|&i| (i.index() as u32, self.inner.inner_graph()[i].name.clone()))
            .collect();
        Ok(PyNodeIterator::new(items))
    }
}
