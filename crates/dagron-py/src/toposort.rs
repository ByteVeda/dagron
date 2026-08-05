use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::iterators::{PyNodeIterator, PyNodeLevelIterator};
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

    /// Return a lazy iterator over topologically sorted nodes.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    pub fn iter_topological_sort(&self, py: Python<'_>) -> PyResult<PyNodeIterator> {
        let inner_ref = &self.inner;
        let nodes = py
            .allow_threads(|| inner_ref.topological_sort())
            .map_err(errors::into_pyerr)?;
        let items: Vec<(u32, String)> = nodes.into_iter().map(|n| (n.index, n.name)).collect();
        Ok(PyNodeIterator::new(items))
    }

    /// Return a lazy iterator over topological levels.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    pub fn iter_topological_levels(&self, py: Python<'_>) -> PyResult<PyNodeLevelIterator> {
        let inner_ref = &self.inner;
        let levels = py
            .allow_threads(|| inner_ref.topological_levels())
            .map_err(errors::into_pyerr)?;
        let level_items: Vec<Vec<(u32, String)>> = levels
            .into_iter()
            .map(|level| level.into_iter().map(|n| (n.index, n.name)).collect())
            .collect();
        Ok(PyNodeLevelIterator::new(level_items))
    }
}
