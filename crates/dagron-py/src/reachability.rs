use ahash::AHashMap;
use pyo3::prelude::*;

use dagron_core::types::InternalNodeIndex;

use crate::dag::PyDAG;
use crate::errors;

/// Precomputed reachability index for O(1) ancestor/descendant queries.
///
/// Build once with `DAG.build_reachability_index()`, then query with
/// `can_reach()`, `reachable_from()`, and `ancestors_of()`.
///
/// Note: The index becomes stale if the DAG is mutated after building.
#[pyclass(name = "ReachabilityIndex")]
pub struct PyReachabilityIndex {
    inner: dagron_core::ReachabilityIndex,
    name_to_index: AHashMap<String, InternalNodeIndex>,
    index_to_name: AHashMap<InternalNodeIndex, String>,
}

#[pymethods]
impl PyReachabilityIndex {
    /// Check if `from_node` can reach `to_node` in O(1).
    ///
    /// Args:
    ///     from_node: Source node name.
    ///     to_node: Target node name.
    ///
    /// Returns:
    ///     True if from_node can reach to_node.
    pub fn can_reach(&self, from_node: &str, to_node: &str) -> PyResult<bool> {
        let from_idx = self.resolve(from_node)?;
        let to_idx = self.resolve(to_node)?;
        Ok(self.inner.can_reach(from_idx, to_idx))
    }

    /// All nodes reachable from `node` (excluding self).
    ///
    /// Args:
    ///     node: Node name.
    ///
    /// Returns:
    ///     List of reachable node names.
    pub fn reachable_from(&self, node: &str) -> PyResult<Vec<String>> {
        let idx = self.resolve(node)?;
        Ok(self
            .inner
            .reachable_from(idx)
            .into_iter()
            .filter_map(|i| self.index_to_name.get(&i).cloned())
            .collect())
    }

    /// All nodes that can reach `node` (excluding self).
    ///
    /// Args:
    ///     node: Node name.
    ///
    /// Returns:
    ///     List of ancestor node names.
    pub fn ancestors_of(&self, node: &str) -> PyResult<Vec<String>> {
        let idx = self.resolve(node)?;
        Ok(self
            .inner
            .ancestors_of(idx)
            .into_iter()
            .filter_map(|i| self.index_to_name.get(&i).cloned())
            .collect())
    }

    /// Number of nodes in the index.
    pub fn node_count(&self) -> usize {
        self.inner.node_count()
    }
}

impl PyReachabilityIndex {
    fn resolve(&self, name: &str) -> PyResult<InternalNodeIndex> {
        self.name_to_index.get(name).copied().ok_or_else(|| {
            errors::into_pyerr(dagron_core::DagronError::NodeNotFound(name.to_string()))
        })
    }
}

#[pymethods]
impl PyDAG {
    /// Build a reachability index for O(1) ancestor/descendant queries.
    ///
    /// Returns:
    ///     A ReachabilityIndex that can be queried for reachability.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    pub fn build_reachability_index(&self, py: Python<'_>) -> PyResult<PyReachabilityIndex> {
        let inner_ref = &self.inner;
        let index = py
            .allow_threads(|| inner_ref.build_reachability_index())
            .map_err(errors::into_pyerr)?;

        // Build name mappings
        let mut name_to_index = AHashMap::new();
        let mut index_to_name = AHashMap::new();
        for name in self.inner.node_names() {
            let idx = self.inner.resolve_name(&name).map_err(errors::into_pyerr)?;
            name_to_index.insert(name.clone(), idx);
            index_to_name.insert(idx, name);
        }

        Ok(PyReachabilityIndex {
            inner: index,
            name_to_index,
            index_to_name,
        })
    }

    /// Check if `ancestor` is an ancestor of `descendant` (BFS, no preprocessing).
    ///
    /// Args:
    ///     ancestor: Potential ancestor node name.
    ///     descendant: Potential descendant node name.
    ///
    /// Returns:
    ///     True if ancestor is an ancestor of (or equal to) descendant.
    ///
    /// Raises:
    ///     NodeNotFoundError: If either node doesn't exist.
    pub fn is_ancestor(
        &self,
        py: Python<'_>,
        ancestor: String,
        descendant: String,
    ) -> PyResult<bool> {
        let inner_ref = &self.inner;
        py.allow_threads(|| inner_ref.is_ancestor(&ancestor, &descendant))
            .map_err(errors::into_pyerr)
    }
}
