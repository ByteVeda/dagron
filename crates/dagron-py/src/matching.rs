use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::node::PyNodeId;

#[pymethods]
impl PyDAG {
    /// Return nodes whose names match the regex pattern.
    ///
    /// Args:
    ///     pattern: A regular expression pattern.
    ///
    /// Returns:
    ///     List of NodeId for matching nodes.
    ///
    /// Raises:
    ///     GraphError: If the regex pattern is invalid.
    pub fn nodes_matching_regex(&self, py: Python<'_>, pattern: String) -> PyResult<Vec<PyNodeId>> {
        let inner_ref = &self.inner;
        let nodes = py
            .allow_threads(|| inner_ref.nodes_matching_regex(&pattern))
            .map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }

    /// Return nodes whose names match a glob pattern (* and ? wildcards).
    ///
    /// Args:
    ///     pattern: A glob pattern (e.g., "task_*", "node_?").
    ///
    /// Returns:
    ///     List of NodeId for matching nodes.
    ///
    /// Raises:
    ///     GraphError: If the pattern is invalid.
    pub fn nodes_matching_glob(&self, py: Python<'_>, pattern: String) -> PyResult<Vec<PyNodeId>> {
        let inner_ref = &self.inner;
        let nodes = py
            .allow_threads(|| inner_ref.nodes_matching_glob(&pattern))
            .map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }
}
