pub mod construction;
pub mod introspection;
pub mod toposort;
pub mod validation;

use ahash::AHashMap;

use crate::errors::DagronError;
use crate::types::{InternalGraph, InternalNodeIndex};

pub struct DAG<P = ()> {
    pub(crate) graph: InternalGraph<P>,
    pub(crate) name_to_index: AHashMap<String, InternalNodeIndex>,
}

impl<P> DAG<P> {
    pub fn new() -> Self {
        DAG {
            graph: InternalGraph::default(),
            name_to_index: AHashMap::new(),
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
}

impl<P> Default for DAG<P> {
    fn default() -> Self {
        Self::new()
    }
}
