use crate::algorithms;
use crate::errors::DagronError;
use crate::node::NodeId;

use super::DAG;

impl<P> DAG<P> {
    /// Return nodes in topological order using Kahn's algorithm.
    /// Sources (no dependencies) come first.
    pub fn topological_sort(&self) -> Result<Vec<NodeId>, DagronError> {
        {
            let mut cache = self.cache.lock().unwrap();
            if cache.gen() == self.generation {
                if let Some(cached) = cache.topo_sort().cloned() {
                    cache.record_hit();
                    return cached;
                }
            }
        }
        let result = algorithms::topological_sort_kahn(&self.graph)
            .map_err(|e| DagronError::Cycle(e.message))
            .map(|indices| {
                indices
                    .iter()
                    .map(|&idx| NodeId {
                        index: idx.index() as u32,
                        name: self.graph[idx].name.clone(),
                    })
                    .collect()
            });
        {
            let mut cache = self.cache.lock().unwrap();
            cache.record_miss();
            cache.set_gen(self.generation);
            cache.set_topo_sort(result.clone());
        }
        result
    }

    /// Return nodes in topological order using DFS (reverse postorder).
    pub fn topological_sort_dfs(&self) -> Result<Vec<NodeId>, DagronError> {
        {
            let mut cache = self.cache.lock().unwrap();
            if cache.gen() == self.generation {
                if let Some(cached) = cache.topo_sort_dfs().cloned() {
                    cache.record_hit();
                    return cached;
                }
            }
        }
        let result = algorithms::topological_sort_dfs(&self.graph)
            .map_err(|e| DagronError::Cycle(e.message))
            .map(|indices| {
                indices
                    .iter()
                    .map(|&idx| NodeId {
                        index: idx.index() as u32,
                        name: self.graph[idx].name.clone(),
                    })
                    .collect()
            });
        {
            let mut cache = self.cache.lock().unwrap();
            cache.record_miss();
            cache.set_gen(self.generation);
            cache.set_topo_sort_dfs(result.clone());
        }
        result
    }

    /// Return nodes grouped by topological level.
    /// Level 0 = roots, Level 1 = nodes depending only on roots, etc.
    pub fn topological_levels(&self) -> Result<Vec<Vec<NodeId>>, DagronError> {
        {
            let mut cache = self.cache.lock().unwrap();
            if cache.gen() == self.generation {
                if let Some(cached) = cache.topo_levels().cloned() {
                    cache.record_hit();
                    return cached;
                }
            }
        }
        let result = algorithms::topological_levels(&self.graph)
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
            });
        {
            let mut cache = self.cache.lock().unwrap();
            cache.record_miss();
            cache.set_gen(self.generation);
            cache.set_topo_levels(result.clone());
        }
        result
    }
}
