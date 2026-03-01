use ahash::AHashSet;
use petgraph::visit::{EdgeRef, IntoEdgeReferences, IntoNodeIdentifiers};

use crate::types::InternalGraph;

/// Result of a structural diff between two graphs.
/// Contains sets of node names and edge tuples that differ.
#[derive(Debug, Clone)]
pub struct StructuralDiff {
    /// Node names only in the first (old) graph.
    pub removed_nodes: Vec<String>,
    /// Node names only in the second (new) graph.
    pub added_nodes: Vec<String>,
    /// Node names present in both graphs.
    pub common_nodes: Vec<String>,
    /// Edges (from, to) only in the first (old) graph.
    pub removed_edges: Vec<(String, String)>,
    /// Edges (from, to) only in the second (new) graph.
    pub added_edges: Vec<(String, String)>,
    /// Edges (from, to) present in both graphs.
    pub common_edges: Vec<(String, String)>,
}

/// Compute the structural diff between two graphs.
/// This only looks at node names and edge structure (from, to).
/// Payload comparison must be done at a higher level.
pub fn structural_diff<P, Q>(old: &InternalGraph<P>, new: &InternalGraph<Q>) -> StructuralDiff {
    // Collect node name sets
    let old_names: AHashSet<String> = old
        .node_identifiers()
        .map(|idx| old[idx].name.clone())
        .collect();
    let new_names: AHashSet<String> = new
        .node_identifiers()
        .map(|idx| new[idx].name.clone())
        .collect();

    let mut removed_nodes: Vec<String> = old_names.difference(&new_names).cloned().collect();
    let mut added_nodes: Vec<String> = new_names.difference(&old_names).cloned().collect();
    let mut common_nodes: Vec<String> = old_names.intersection(&new_names).cloned().collect();
    removed_nodes.sort();
    added_nodes.sort();
    common_nodes.sort();

    // Collect edge sets
    let old_edges: AHashSet<(String, String)> = old
        .edge_references()
        .map(|e| (old[e.source()].name.clone(), old[e.target()].name.clone()))
        .collect();
    let new_edges: AHashSet<(String, String)> = new
        .edge_references()
        .map(|e| (new[e.source()].name.clone(), new[e.target()].name.clone()))
        .collect();

    let mut removed_edges: Vec<(String, String)> =
        old_edges.difference(&new_edges).cloned().collect();
    let mut added_edges: Vec<(String, String)> =
        new_edges.difference(&old_edges).cloned().collect();
    let mut common_edges: Vec<(String, String)> =
        old_edges.intersection(&new_edges).cloned().collect();
    removed_edges.sort();
    added_edges.sort();
    common_edges.sort();

    StructuralDiff {
        removed_nodes,
        added_nodes,
        common_nodes,
        removed_edges,
        added_edges,
        common_edges,
    }
}
