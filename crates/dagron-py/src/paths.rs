use std::collections::HashMap;

use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::node::PyNodeId;

#[pymethods]
impl PyDAG {
    /// Find all directed paths from one node to another.
    ///
    /// Args:
    ///     from_node: Source node name.
    ///     to_node: Target node name.
    ///     limit: Maximum number of paths to return (None = unlimited).
    ///
    /// Returns:
    ///     List of paths, each path being a list of NodeId.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    #[pyo3(signature = (from_node, to_node, limit=None))]
    pub fn all_paths(
        &self,
        py: Python<'_>,
        from_node: String,
        to_node: String,
        limit: Option<usize>,
    ) -> PyResult<Vec<Vec<PyNodeId>>> {
        let inner_ref = &self.inner;
        let paths = py
            .allow_threads(|| inner_ref.all_paths(&from_node, &to_node, limit))
            .map_err(errors::into_pyerr)?;
        Ok(paths
            .into_iter()
            .map(|path| path.into_iter().map(PyNodeId::from).collect())
            .collect())
    }

    /// Find the shortest path (fewest edges) between two nodes.
    ///
    /// Args:
    ///     from_node: Source node name.
    ///     to_node: Target node name.
    ///
    /// Returns:
    ///     List of NodeId representing the path, or None if unreachable.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    pub fn shortest_path(
        &self,
        py: Python<'_>,
        from_node: String,
        to_node: String,
    ) -> PyResult<Option<Vec<PyNodeId>>> {
        let inner_ref = &self.inner;
        let result = py
            .allow_threads(|| inner_ref.shortest_path(&from_node, &to_node))
            .map_err(errors::into_pyerr)?;
        Ok(result.map(|path| path.into_iter().map(PyNodeId::from).collect()))
    }

    /// Find the longest weighted path between two nodes.
    ///
    /// Args:
    ///     from_node: Source node name.
    ///     to_node: Target node name.
    ///     costs: Optional dict mapping node names to costs (default 1.0).
    ///
    /// Returns:
    ///     Tuple of (path as list of NodeId, total cost), or None if unreachable.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    #[pyo3(signature = (from_node, to_node, costs=None))]
    pub fn longest_path(
        &self,
        py: Python<'_>,
        from_node: String,
        to_node: String,
        costs: Option<HashMap<String, f64>>,
    ) -> PyResult<Option<(Vec<PyNodeId>, f64)>> {
        let costs_map = costs.unwrap_or_default();
        let inner_ref = &self.inner;
        let result = py
            .allow_threads(|| inner_ref.longest_path(&from_node, &to_node, &costs_map))
            .map_err(errors::into_pyerr)?;
        Ok(result.map(|(path, cost)| {
            (
                path.into_iter().map(PyNodeId::from).collect(),
                cost,
            )
        }))
    }
}
