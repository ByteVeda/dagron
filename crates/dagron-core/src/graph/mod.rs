pub(crate) mod cache;
pub mod concurrent;
pub mod construction;
pub mod diff;
pub mod incremental;
pub mod introspection;
pub mod matching;
pub mod partition;
pub mod paths;
pub mod reachability;
pub mod scheduling;
pub mod serialization;
pub mod stats;
pub mod subgraph;
pub mod toposort;
pub mod transforms;
pub mod validation;

use std::sync::RwLock;

use ahash::AHashMap;

use crate::errors::DagronError;
use crate::node::NodeRef;
use crate::types::{InternalGraph, InternalNodeIndex};

pub struct DAG<P = ()> {
    pub(crate) graph: InternalGraph<P>,
    pub(crate) name_to_index: AHashMap<String, InternalNodeIndex>,
    /// Per-node creation epoch — used to validate `NodeRef`s against
    /// remove/re-add cycles. The entry is removed when the node is removed.
    pub(crate) node_epochs: AHashMap<String, u64>,
    /// Monotonic counter; assigned to each newly created node.
    pub(crate) next_node_epoch: u64,
    generation: u64,
    pub(crate) cache: RwLock<cache::DagCache>,
}

impl<P> DAG<P> {
    pub fn new() -> Self {
        DAG {
            graph: InternalGraph::default(),
            name_to_index: AHashMap::new(),
            node_epochs: AHashMap::new(),
            next_node_epoch: 0,
            generation: 0,
            cache: RwLock::new(cache::DagCache::new()),
        }
    }

    /// Resolve a node name to its index, returning DagronError::NodeNotFound on miss.
    pub fn resolve_name(&self, name: &str) -> Result<InternalNodeIndex, DagronError> {
        self.name_to_index
            .get(name)
            .copied()
            .ok_or_else(|| DagronError::NodeNotFound(name.to_string()))
    }

    /// Resolve a `NodeRef` to its current index, validating that the node
    /// still exists with the same creation epoch. Returns
    /// `DagronError::NodeNotFound` if the node has been removed and
    /// `DagronError::StaleNodeRef` if a different node now occupies the name.
    pub fn resolve_ref(&self, r: &NodeRef) -> Result<InternalNodeIndex, DagronError> {
        let stored_epoch = self
            .node_epochs
            .get(r.name.as_ref())
            .ok_or_else(|| DagronError::NodeNotFound(r.name.to_string()))?;
        if *stored_epoch != r.epoch {
            return Err(DagronError::StaleNodeRef(r.name.to_string()));
        }
        self.name_to_index
            .get(r.name.as_ref())
            .copied()
            .ok_or_else(|| DagronError::NodeNotFound(r.name.to_string()))
    }

    /// Look up the current `NodeRef` for a name, if it exists.
    pub fn node_ref(&self, name: &str) -> Option<NodeRef> {
        let epoch = *self.node_epochs.get(name)?;
        Some(NodeRef::new(name, epoch))
    }

    /// Access the underlying petgraph.
    pub fn inner_graph(&self) -> &InternalGraph<P> {
        &self.graph
    }

    /// Access the underlying petgraph mutably.
    pub fn inner_graph_mut(&mut self) -> &mut InternalGraph<P> {
        &mut self.graph
    }

    /// Get the current generation counter.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Bump the generation counter and invalidate stale cache.
    pub fn bump_generation(&mut self) {
        self.generation += 1;
        self.cache.write().unwrap().invalidate();
    }

    /// Set the generation counter to a specific value.
    pub fn set_generation(&mut self, gen: u64) {
        self.generation = gen;
    }

    /// Get the number of cache hits.
    pub fn cache_hits(&self) -> u64 {
        self.cache.read().unwrap().hits()
    }

    /// Get the number of cache misses.
    pub fn cache_misses(&self) -> u64 {
        self.cache.read().unwrap().misses()
    }

    /// Get the number of cached entries.
    pub fn cache_size(&self) -> usize {
        self.cache.read().unwrap().size()
    }

    /// Clear all cached results.
    pub fn clear_cache(&self) {
        self.cache.write().unwrap().invalidate();
    }

    /// Cache helper for infallible computations (e.g., roots, leaves).
    pub(crate) fn with_cache<T: Clone>(
        &self,
        getter: impl FnOnce(&cache::DagCache) -> Option<&T>,
        setter: impl FnOnce(&mut cache::DagCache, T),
        compute: impl FnOnce() -> T,
    ) -> T {
        {
            let cache = self.cache.read().unwrap();
            if cache.gen() == self.generation {
                if let Some(cached) = getter(&cache).cloned() {
                    drop(cache);
                    self.cache.write().unwrap().record_hit();
                    return cached;
                }
            }
        }
        let result = compute();
        {
            let mut cache = self.cache.write().unwrap();
            cache.record_miss();
            cache.set_gen(self.generation);
            setter(&mut cache, result.clone());
        }
        result
    }

    /// Cache helper for fallible computations (e.g., topo sort).
    /// Only caches `Ok` results; errors are always recomputed.
    pub(crate) fn with_cache_result<T: Clone>(
        &self,
        getter: impl FnOnce(&cache::DagCache) -> Option<&T>,
        setter: impl FnOnce(&mut cache::DagCache, T),
        compute: impl FnOnce() -> Result<T, crate::errors::DagronError>,
    ) -> Result<T, crate::errors::DagronError> {
        {
            let cache = self.cache.read().unwrap();
            if cache.gen() == self.generation {
                if let Some(cached) = getter(&cache).cloned() {
                    drop(cache);
                    self.cache.write().unwrap().record_hit();
                    return Ok(cached);
                }
            }
        }
        let result = compute()?;
        {
            let mut cache = self.cache.write().unwrap();
            cache.record_miss();
            cache.set_gen(self.generation);
            setter(&mut cache, result.clone());
        }
        Ok(result)
    }
}

impl<P: Clone> DAG<P> {
    /// Create an independent snapshot (deep clone) of the DAG.
    ///
    /// The snapshot copies the graph structure, node payloads, and generation counter,
    /// but starts with a fresh (empty) cache. Mutations to the snapshot do not affect
    /// the original, and vice versa.
    pub fn snapshot(&self) -> Self {
        DAG {
            graph: self.graph.clone(),
            name_to_index: self.name_to_index.clone(),
            node_epochs: self.node_epochs.clone(),
            next_node_epoch: self.next_node_epoch,
            generation: self.generation,
            cache: RwLock::new(cache::DagCache::new()),
        }
    }
}

impl<P> Default for DAG<P> {
    fn default() -> Self {
        Self::new()
    }
}
