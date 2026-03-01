pub(crate) mod cache;
pub mod concurrent;
pub mod construction;
pub mod incremental;
pub mod introspection;
pub mod matching;
pub mod paths;
pub mod reachability;
pub mod scheduling;
pub mod serialization;
pub mod subgraph;
pub mod toposort;
pub mod transforms;
pub mod validation;

use std::sync::RwLock;

use ahash::AHashMap;

use crate::errors::DagronError;
use crate::types::{InternalGraph, InternalNodeIndex};

pub struct DAG<P = ()> {
    pub(crate) graph: InternalGraph<P>,
    pub(crate) name_to_index: AHashMap<String, InternalNodeIndex>,
    generation: u64,
    pub(crate) cache: RwLock<cache::DagCache>,
}

impl<P> DAG<P> {
    pub fn new() -> Self {
        DAG {
            graph: InternalGraph::default(),
            name_to_index: AHashMap::new(),
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
