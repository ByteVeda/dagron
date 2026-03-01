use crate::algorithms;
use crate::errors::DagronError;
use crate::node::NodeId;

use super::DAG;

impl<P> DAG<P> {
    /// Return nodes in topological order using Kahn's algorithm.
    /// Sources (no dependencies) come first.
    pub fn topological_sort(&self) -> Result<Vec<NodeId>, DagronError> {
        self.with_cache_result(
            |c| c.topo_sort(),
            |c, v| c.set_topo_sort(v),
            || {
                algorithms::topological_sort_kahn(&self.graph)
                    .map_err(|e| DagronError::Cycle(e.message))
                    .map(|indices| {
                        indices
                            .iter()
                            .map(|&idx| NodeId {
                                index: idx.index() as u32,
                                name: self.graph[idx].name.clone(),
                            })
                            .collect()
                    })
            },
        )
    }

    /// Return nodes in topological order using DFS (reverse postorder).
    pub fn topological_sort_dfs(&self) -> Result<Vec<NodeId>, DagronError> {
        self.with_cache_result(
            |c| c.topo_sort_dfs(),
            |c, v| c.set_topo_sort_dfs(v),
            || {
                algorithms::topological_sort_dfs(&self.graph)
                    .map_err(|e| DagronError::Cycle(e.message))
                    .map(|indices| {
                        indices
                            .iter()
                            .map(|&idx| NodeId {
                                index: idx.index() as u32,
                                name: self.graph[idx].name.clone(),
                            })
                            .collect()
                    })
            },
        )
    }

    /// Enumerate all valid topological orderings via backtracking.
    /// Stops after `limit` orderings (None = unlimited, WARNING: can be factorial).
    pub fn all_topological_orderings(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<NodeId>>, DagronError> {
        algorithms::all_topological_orderings(&self.graph, limit)
            .map_err(|e| DagronError::Cycle(e.message))
            .map(|orderings| {
                orderings
                    .into_iter()
                    .map(|order| {
                        order
                            .into_iter()
                            .map(|idx| NodeId {
                                index: idx.index() as u32,
                                name: self.graph[idx].name.clone(),
                            })
                            .collect()
                    })
                    .collect()
            })
    }

    /// Return nodes grouped by topological level.
    /// Level 0 = roots, Level 1 = nodes depending only on roots, etc.
    pub fn topological_levels(&self) -> Result<Vec<Vec<NodeId>>, DagronError> {
        self.with_cache_result(
            |c| c.topo_levels(),
            |c, v| c.set_topo_levels(v),
            || {
                algorithms::topological_levels(&self.graph)
                    .map_err(|e| DagronError::Cycle(e.message))
                    .map(|levels| {
                        levels
                            .iter()
                            .map(|level| {
                                level
                                    .iter()
                                    .map(|&idx| NodeId {
                                        index: idx.index() as u32,
                                        name: self.graph[idx].name.clone(),
                                    })
                                    .collect()
                            })
                            .collect()
                    })
            },
        )
    }
}
