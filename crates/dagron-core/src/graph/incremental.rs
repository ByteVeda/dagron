use std::collections::HashMap;

use crate::algorithms;
use crate::errors::DagronError;

use super::DAG;

impl<P> DAG<P> {
    /// Compute the dirty set: all changed nodes plus their transitive descendants.
    ///
    /// Returns a list of node names that need to be recomputed when the given
    /// nodes change.
    pub fn dirty_set(&self, changed: &[&str]) -> Result<Vec<String>, DagronError> {
        let indices: Vec<_> = changed
            .iter()
            .map(|name| self.resolve_name(name))
            .collect::<Result<Vec<_>, _>>()?;

        let dirty = algorithms::dirty_set(&self.graph, &indices);

        Ok(dirty
            .iter()
            .map(|&idx| self.graph[idx].name.clone())
            .collect())
    }

    /// For each dirty node, determine which changed nodes are its ancestors.
    ///
    /// Returns a map from dirty node name to the list of changed node names
    /// that can reach it.
    pub fn change_provenance(
        &self,
        changed: &[&str],
    ) -> Result<HashMap<String, Vec<String>>, DagronError> {
        let indices: Vec<_> = changed
            .iter()
            .map(|name| self.resolve_name(name))
            .collect::<Result<Vec<_>, _>>()?;

        let prov = algorithms::change_provenance(&self.graph, &indices);

        Ok(prov
            .into_iter()
            .map(|(node, sources)| {
                (
                    self.graph[node].name.clone(),
                    sources
                        .iter()
                        .map(|&idx| self.graph[idx].name.clone())
                        .collect(),
                )
            })
            .collect())
    }
}
