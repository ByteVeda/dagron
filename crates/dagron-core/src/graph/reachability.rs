use crate::algorithms;
use crate::algorithms::reachability::ReachabilityIndex;
use crate::errors::DagronError;

use super::DAG;

impl<P> DAG<P> {
    /// Build a reachability index for O(1) ancestor/descendant queries.
    ///
    /// The index becomes stale if the graph is mutated after building.
    pub fn build_reachability_index(&self) -> Result<ReachabilityIndex, DagronError> {
        ReachabilityIndex::new(&self.graph).map_err(|e| DagronError::Cycle(e.message))
    }

    /// Check if `ancestor` is an ancestor of `descendant` (BFS, no preprocessing).
    pub fn is_ancestor(&self, ancestor: &str, descendant: &str) -> Result<bool, DagronError> {
        let ancestor_idx = self.resolve_name(ancestor)?;
        let descendant_idx = self.resolve_name(descendant)?;

        if ancestor_idx == descendant_idx {
            return Ok(true);
        }

        let ancestors = algorithms::ancestors(&self.graph, descendant_idx);
        Ok(ancestors.contains(&ancestor_idx))
    }
}
