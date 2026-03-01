use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::node::PyNodeId;

#[pymethods]
impl PyDAG {
    /// Return nodes in topological order using Kahn's algorithm.
    /// Sources (no dependencies) come first.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    pub fn topological_sort(&self, py: Python<'_>) -> PyResult<Vec<PyNodeId>> {
        let inner_ref = &self.inner;
        let nodes = py
            .allow_threads(|| inner_ref.topological_sort())
            .map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }

    /// Return nodes in topological order using DFS (reverse postorder).
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    pub fn topological_sort_dfs(&self, py: Python<'_>) -> PyResult<Vec<PyNodeId>> {
        let inner_ref = &self.inner;
        let nodes = py
            .allow_threads(|| inner_ref.topological_sort_dfs())
            .map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }

    /// Enumerate all valid topological orderings via backtracking.
    ///
    /// WARNING: The number of orderings can be factorial. Always use `limit`
    /// for non-trivial graphs.
    ///
    /// Args:
    ///     limit: Maximum number of orderings to return (None = unlimited).
    ///
    /// Returns:
    ///     List of orderings, each being a list of NodeId.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    #[pyo3(signature = (limit=None))]
    pub fn all_topological_orderings(
        &self,
        py: Python<'_>,
        limit: Option<usize>,
    ) -> PyResult<Vec<Vec<PyNodeId>>> {
        let inner_ref = &self.inner;
        let orderings = py
            .allow_threads(|| inner_ref.all_topological_orderings(limit))
            .map_err(errors::into_pyerr)?;
        Ok(orderings
            .into_iter()
            .map(|order| order.into_iter().map(PyNodeId::from).collect())
            .collect())
    }

    /// Return nodes grouped by topological level.
    /// Level 0 = roots, Level 1 = nodes depending only on roots, etc.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    pub fn topological_levels(&self, py: Python<'_>) -> PyResult<Vec<Vec<PyNodeId>>> {
        let inner_ref = &self.inner;
        let levels = py
            .allow_threads(|| inner_ref.topological_levels())
            .map_err(errors::into_pyerr)?;
        Ok(levels
            .into_iter()
            .map(|level| level.into_iter().map(PyNodeId::from).collect())
            .collect())
    }
}
