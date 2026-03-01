use petgraph::visit::{EdgeRef, IntoNodeIdentifiers};
use petgraph::Direction;

use crate::algorithms;
use crate::errors::DagronError;
use crate::node::NodeId;

use super::DAG;

impl<P> DAG<P> {
    /// Check if a node with the given name exists.
    pub fn has_node(&self, name: &str) -> bool {
        self.name_to_index.contains_key(name)
    }

    /// Check if an edge exists between two nodes.
    pub fn has_edge(&self, from_node: &str, to_node: &str) -> Result<bool, DagronError> {
        let from_idx = self.resolve_name(from_node)?;
        let to_idx = self.resolve_name(to_node)?;
        Ok(self.graph.find_edge(from_idx, to_idx).is_some())
    }

    /// Return the number of nodes in the graph.
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Return the number of edges in the graph.
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    /// Get a reference to the payload of a node.
    pub fn get_payload(&self, name: &str) -> Result<&P, DagronError> {
        let idx = self.resolve_name(name)?;
        Ok(&self.graph[idx].payload)
    }

    /// Get a mutable reference to the payload of a node.
    pub fn get_payload_mut(&mut self, name: &str) -> Result<&mut P, DagronError> {
        let idx = self.resolve_name(name)?;
        Ok(&mut self.graph[idx].payload)
    }

    /// Get the immediate predecessors (parents) of a node.
    pub fn predecessors(&self, name: &str) -> Result<Vec<NodeId>, DagronError> {
        let idx = self.resolve_name(name)?;
        let preds: Vec<NodeId> = self
            .graph
            .edges_directed(idx, Direction::Incoming)
            .map(|e| {
                let src = e.source();
                NodeId {
                    index: src.index() as u32,
                    name: self.graph[src].name.clone(),
                }
            })
            .collect();
        Ok(preds)
    }

    /// Get the immediate successors (children) of a node.
    pub fn successors(&self, name: &str) -> Result<Vec<NodeId>, DagronError> {
        let idx = self.resolve_name(name)?;
        let succs: Vec<NodeId> = self
            .graph
            .edges_directed(idx, Direction::Outgoing)
            .map(|e| {
                let tgt = e.target();
                NodeId {
                    index: tgt.index() as u32,
                    name: self.graph[tgt].name.clone(),
                }
            })
            .collect();
        Ok(succs)
    }

    /// Get all ancestors of a node (transitive predecessors).
    pub fn ancestors(&self, name: &str) -> Result<Vec<NodeId>, DagronError> {
        let idx = self.resolve_name(name)?;
        let indices = algorithms::ancestors(&self.graph, idx);
        Ok(indices
            .iter()
            .map(|&i| NodeId {
                index: i.index() as u32,
                name: self.graph[i].name.clone(),
            })
            .collect())
    }

    /// Get all descendants of a node (transitive successors).
    pub fn descendants(&self, name: &str) -> Result<Vec<NodeId>, DagronError> {
        let idx = self.resolve_name(name)?;
        let indices = algorithms::descendants(&self.graph, idx);
        Ok(indices
            .iter()
            .map(|&i| NodeId {
                index: i.index() as u32,
                name: self.graph[i].name.clone(),
            })
            .collect())
    }

    /// Get the in-degree (number of incoming edges) of a node.
    pub fn in_degree(&self, name: &str) -> Result<usize, DagronError> {
        let idx = self.resolve_name(name)?;
        Ok(self
            .graph
            .edges_directed(idx, Direction::Incoming)
            .count())
    }

    /// Get the out-degree (number of outgoing edges) of a node.
    pub fn out_degree(&self, name: &str) -> Result<usize, DagronError> {
        let idx = self.resolve_name(name)?;
        Ok(self
            .graph
            .edges_directed(idx, Direction::Outgoing)
            .count())
    }

    /// Get all root nodes (nodes with no incoming edges).
    pub fn roots(&self) -> Vec<NodeId> {
        self.graph
            .node_identifiers()
            .filter(|&idx| {
                self.graph
                    .edges_directed(idx, Direction::Incoming)
                    .next()
                    .is_none()
            })
            .map(|idx| NodeId {
                index: idx.index() as u32,
                name: self.graph[idx].name.clone(),
            })
            .collect()
    }

    /// Get all leaf nodes (nodes with no outgoing edges).
    pub fn leaves(&self) -> Vec<NodeId> {
        self.graph
            .node_identifiers()
            .filter(|&idx| {
                self.graph
                    .edges_directed(idx, Direction::Outgoing)
                    .next()
                    .is_none()
            })
            .map(|idx| NodeId {
                index: idx.index() as u32,
                name: self.graph[idx].name.clone(),
            })
            .collect()
    }

    /// Get all nodes in the graph.
    pub fn nodes(&self) -> Vec<NodeId> {
        self.graph
            .node_identifiers()
            .map(|idx| NodeId {
                index: idx.index() as u32,
                name: self.graph[idx].name.clone(),
            })
            .collect()
    }

    /// Get a list of all node names.
    pub fn node_names(&self) -> Vec<String> {
        self.graph
            .node_identifiers()
            .map(|idx| self.graph[idx].name.clone())
            .collect()
    }

    /// Get a list of all edges as (from_name, to_name) tuples.
    pub fn edges(&self) -> Vec<(String, String)> {
        use petgraph::visit::IntoEdgeReferences;
        self.graph
            .edge_references()
            .map(|e| {
                (
                    self.graph[e.source()].name.clone(),
                    self.graph[e.target()].name.clone(),
                )
            })
            .collect()
    }
}
