use std::collections::HashMap;

use ahash::AHashMap;
use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::node::PyNodeId;
use crate::noderef::NodeArg;

#[pymethods]
impl PyDAG {
    /// Find all directed paths from one node to another.
    ///
    /// Args:
    ///     from_node: Source node (str or NodeRef).
    ///     to_node: Target node (str or NodeRef).
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
        from_node: NodeArg,
        to_node: NodeArg,
        limit: Option<usize>,
    ) -> PyResult<Vec<Vec<PyNodeId>>> {
        let from = from_node.into_name(&self.inner)?;
        let to = to_node.into_name(&self.inner)?;
        let inner_ref = &self.inner;
        let paths = py
            .allow_threads(|| inner_ref.all_paths(&from, &to, limit))
            .map_err(errors::into_pyerr)?;
        Ok(paths
            .into_iter()
            .map(|path| path.into_iter().map(PyNodeId::from).collect())
            .collect())
    }

    /// Find the shortest path (fewest edges) between two nodes.
    ///
    /// Args:
    ///     from_node: Source node (str or NodeRef).
    ///     to_node: Target node (str or NodeRef).
    ///
    /// Returns:
    ///     List of NodeId representing the path, or None if unreachable.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    pub fn shortest_path(
        &self,
        py: Python<'_>,
        from_node: NodeArg,
        to_node: NodeArg,
    ) -> PyResult<Option<Vec<PyNodeId>>> {
        let from = from_node.into_name(&self.inner)?;
        let to = to_node.into_name(&self.inner)?;
        let inner_ref = &self.inner;
        let result = py
            .allow_threads(|| inner_ref.shortest_path(&from, &to))
            .map_err(errors::into_pyerr)?;
        Ok(result.map(|path| path.into_iter().map(PyNodeId::from).collect()))
    }

    /// Find the longest weighted path between two nodes.
    ///
    /// Args:
    ///     from_node: Source node (str or NodeRef).
    ///     to_node: Target node (str or NodeRef).
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
        from_node: NodeArg,
        to_node: NodeArg,
        costs: Option<HashMap<String, f64>>,
    ) -> PyResult<Option<(Vec<PyNodeId>, f64)>> {
        let from = from_node.into_name(&self.inner)?;
        let to = to_node.into_name(&self.inner)?;
        let costs_map: AHashMap<String, f64> = costs.unwrap_or_default().into_iter().collect();
        let inner_ref = &self.inner;
        let result = py
            .allow_threads(|| inner_ref.longest_path(&from, &to, &costs_map))
            .map_err(errors::into_pyerr)?;
        Ok(result.map(|(path, cost)| (path.into_iter().map(PyNodeId::from).collect(), cost)))
    }
}
