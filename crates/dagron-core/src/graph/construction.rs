use crate::algorithms;
use crate::errors::DagronError;
use crate::node::NodeRef;
use crate::types::EdgeData;

use super::DAG;

impl<P> DAG<P> {
    /// Add a single node to the graph.
    ///
    /// Returns a [`NodeRef`] for the newly created node. The ref is stable —
    /// it remains valid across edge mutations and other-node changes, and is
    /// invalidated only if this node is removed.
    ///
    /// Returns `DagronError::DuplicateNode` if a node with this name already exists.
    pub fn add_node(&mut self, name: String, payload: P) -> Result<NodeRef, DagronError> {
        if self.name_to_index.contains_key(&name) {
            return Err(DagronError::DuplicateNode(name));
        }
        let node_data = crate::types::NodeData {
            name: name.clone(),
            payload,
        };
        let idx = self.graph.add_node(node_data);
        let epoch = self.next_node_epoch;
        self.next_node_epoch = self.next_node_epoch.wrapping_add(1);
        self.name_to_index.insert(name.clone(), idx);
        self.node_epochs.insert(name.clone(), epoch);
        self.bump_generation();
        Ok(NodeRef::new(name, epoch))
    }

    /// Add a directed edge from one node to another.
    ///
    /// Returns DagronError::NodeNotFound if either node doesn't exist.
    /// Returns DagronError::Cycle if the edge would create a cycle.
    pub fn add_edge(
        &mut self,
        from_node: &str,
        to_node: &str,
        weight: Option<f64>,
        label: Option<String>,
    ) -> Result<(), DagronError> {
        let from_idx = self.resolve_name(from_node)?;
        let to_idx = self.resolve_name(to_node)?;

        // Check for cycle
        if let Some(cycle_path) = algorithms::would_create_cycle(&self.graph, from_idx, to_idx) {
            let names: Vec<String> = cycle_path
                .iter()
                .map(|&idx| self.graph[idx].name.clone())
                .collect();
            return Err(DagronError::Cycle(format!(
                "Edge {} -> {} would create a cycle: {}",
                from_node,
                to_node,
                names.join(" -> ")
            )));
        }

        let edge_data = EdgeData {
            weight: weight.unwrap_or(1.0),
            label,
        };
        self.graph.add_edge(from_idx, to_idx, edge_data);
        self.bump_generation();
        Ok(())
    }

    /// Remove a node and all its incident edges.
    ///
    /// Returns DagronError::NodeNotFound if the node doesn't exist.
    pub fn remove_node(&mut self, name: &str) -> Result<(), DagronError> {
        let idx = self.resolve_name(name)?;
        self.graph.remove_node(idx);
        self.name_to_index.remove(name);
        self.node_epochs.remove(name);
        self.bump_generation();
        Ok(())
    }

    /// Remove an edge between two nodes.
    ///
    /// Returns DagronError::NodeNotFound if either node doesn't exist.
    /// Returns DagronError::EdgeNotFound if no edge exists between the nodes.
    pub fn remove_edge(&mut self, from_node: &str, to_node: &str) -> Result<(), DagronError> {
        let from_idx = self.resolve_name(from_node)?;
        let to_idx = self.resolve_name(to_node)?;

        let edge = self
            .graph
            .find_edge(from_idx, to_idx)
            .ok_or_else(|| DagronError::EdgeNotFound(from_node.to_string(), to_node.to_string()))?;

        self.graph.remove_edge(edge);
        self.bump_generation();
        Ok(())
    }
}
