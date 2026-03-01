use crate::algorithms;

use super::DAG;

/// Result of diffing two DAGs at the graph level.
#[derive(Debug, Clone)]
pub struct GraphDiff {
    pub added_nodes: Vec<String>,
    pub removed_nodes: Vec<String>,
    /// Nodes present in both but with different payloads (determined externally).
    pub changed_nodes: Vec<String>,
    pub added_edges: Vec<(String, String)>,
    pub removed_edges: Vec<(String, String)>,
    /// Edges present in both but with different weight/label (determined externally).
    pub changed_edges: Vec<(String, String)>,
}

impl<P: PartialEq> DAG<P> {
    /// Compute a full diff against another DAG, comparing payloads with PartialEq.
    pub fn diff(&self, other: &DAG<P>) -> GraphDiff {
        let structural = algorithms::diff::structural_diff(&self.graph, &other.graph);

        // Compare payloads for common nodes
        let mut changed_nodes = Vec::new();
        for name in &structural.common_nodes {
            if let (Ok(old_p), Ok(new_p)) = (self.get_payload(name), other.get_payload(name)) {
                if old_p != new_p {
                    changed_nodes.push(name.clone());
                }
            }
        }

        // Compare edge data for common edges
        let mut changed_edges = Vec::new();
        for (from, to) in &structural.common_edges {
            let old_edge = self.get_edge_data(from, to);
            let new_edge = other.get_edge_data(from, to);
            if let (Some(old_e), Some(new_e)) = (old_edge, new_edge) {
                if (old_e.weight - new_e.weight).abs() > f64::EPSILON || old_e.label != new_e.label
                {
                    changed_edges.push((from.clone(), to.clone()));
                }
            }
        }

        GraphDiff {
            added_nodes: structural.added_nodes,
            removed_nodes: structural.removed_nodes,
            changed_nodes,
            added_edges: structural.added_edges,
            removed_edges: structural.removed_edges,
            changed_edges,
        }
    }
}

impl<P> DAG<P> {
    /// Get edge data between two nodes.
    fn get_edge_data(&self, from: &str, to: &str) -> Option<&crate::types::EdgeData> {
        let from_idx = self.name_to_index.get(from)?;
        let to_idx = self.name_to_index.get(to)?;
        let edge_idx = self.graph.find_edge(*from_idx, *to_idx)?;
        Some(&self.graph[edge_idx])
    }
}
