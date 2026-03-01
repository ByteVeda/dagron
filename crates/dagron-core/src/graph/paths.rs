use std::collections::HashMap;

use ahash::AHashMap;

use crate::algorithms;
use crate::errors::DagronError;
use crate::node::NodeId;

use super::DAG;

impl<P> DAG<P> {
    /// Find all directed paths from `from` to `to`.
    /// Stops after `limit` paths (None = unlimited).
    pub fn all_paths(
        &self,
        from: &str,
        to: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<NodeId>>, DagronError> {
        let from_idx = self.resolve_name(from)?;
        let to_idx = self.resolve_name(to)?;

        let paths = algorithms::all_paths(&self.graph, from_idx, to_idx, limit);

        Ok(paths
            .into_iter()
            .map(|path| {
                path.into_iter()
                    .map(|idx| NodeId {
                        index: idx.index() as u32,
                        name: self.graph[idx].name.clone(),
                    })
                    .collect()
            })
            .collect())
    }

    /// Find shortest path (fewest edges) from `from` to `to`.
    pub fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<NodeId>>, DagronError> {
        let from_idx = self.resolve_name(from)?;
        let to_idx = self.resolve_name(to)?;

        Ok(algorithms::shortest_path(&self.graph, from_idx, to_idx).map(|path| {
            path.into_iter()
                .map(|idx| NodeId {
                    index: idx.index() as u32,
                    name: self.graph[idx].name.clone(),
                })
                .collect()
        }))
    }

    /// Find longest weighted path from `from` to `to`.
    /// Uses `costs` map for node weights (default 1.0).
    pub fn longest_path(
        &self,
        from: &str,
        to: &str,
        costs: &HashMap<String, f64>,
    ) -> Result<Option<(Vec<NodeId>, f64)>, DagronError> {
        let from_idx = self.resolve_name(from)?;
        let to_idx = self.resolve_name(to)?;

        // Convert string-keyed costs to index-keyed
        let mut idx_costs = AHashMap::new();
        for (name, &cost) in costs {
            if let Ok(idx) = self.resolve_name(name) {
                idx_costs.insert(idx, cost);
            }
        }

        Ok(
            algorithms::longest_path(&self.graph, from_idx, to_idx, &idx_costs).map(
                |(path, cost)| {
                    let node_path = path
                        .into_iter()
                        .map(|idx| NodeId {
                            index: idx.index() as u32,
                            name: self.graph[idx].name.clone(),
                        })
                        .collect();
                    (node_path, cost)
                },
            ),
        )
    }
}
