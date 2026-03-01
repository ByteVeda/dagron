use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::dag::PyDAG;
use crate::errors;

#[pymethods]
impl PyDAG {
    /// Number of nodes in the graph.
    pub fn __len__(&self) -> usize {
        self.inner.node_count()
    }

    /// Check if a node name exists in the graph.
    pub fn __contains__(&self, name: &str) -> bool {
        self.inner.has_node(name)
    }

    /// Get a node by name. Returns its payload.
    pub fn __getitem__(&self, py: Python<'_>, name: &str) -> PyResult<Option<PyObject>> {
        let p = self.inner.get_payload(name).map_err(errors::into_pyerr)?;
        Ok(p.payload.as_ref().map(|v| v.clone_ref(py)))
    }

    /// Set a node's payload by name.
    pub fn __setitem__(&mut self, name: &str, value: Py<PyAny>) -> PyResult<()> {
        let p = self.inner.get_payload_mut(name).map_err(errors::into_pyerr)?;
        p.payload = Some(value);
        Ok(())
    }

    /// Iterate over all node names.
    pub fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let names = self.inner.node_names();
        let py_list = PyList::new(py, &names)?;
        Ok(py_list.call_method0("__iter__")?.unbind())
    }

    /// String representation of the DAG.
    pub fn __repr__(&self) -> String {
        format!(
            "DAG(nodes={}, edges={})",
            self.inner.node_count(),
            self.inner.edge_count()
        )
    }

    /// A DAG is truthy if it has at least one node.
    pub fn __bool__(&self) -> bool {
        self.inner.node_count() > 0
    }

    /// Get a list of all node names.
    pub fn node_names(&self) -> Vec<String> {
        self.inner.node_names()
    }

    /// Get a list of all edges as (from_name, to_name) tuples.
    pub fn edges(&self) -> Vec<(String, String)> {
        self.inner.edges()
    }
}
