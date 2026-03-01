use std::collections::{HashSet, VecDeque};

use ahash::AHashMap;
use petgraph::visit::EdgeRef;
use petgraph::Direction;

use crate::types::{InternalGraph, InternalNodeIndex};

/// Compute the dirty set: all changed nodes plus their transitive descendants.
///
/// Uses BFS from each changed node following outgoing edges.
pub fn dirty_set<P>(
    graph: &InternalGraph<P>,
    changed: &[InternalNodeIndex],
) -> Vec<InternalNodeIndex> {
    let mut visited: HashSet<InternalNodeIndex> = HashSet::new();
    let mut queue: VecDeque<InternalNodeIndex> = VecDeque::new();

    // Seed with all changed nodes
    for &node in changed {
        if visited.insert(node) {
            queue.push_back(node);
        }
    }

    // BFS descendants
    while let Some(current) = queue.pop_front() {
        for edge in graph.edges_directed(current, Direction::Outgoing) {
            let target = edge.target();
            if visited.insert(target) {
                queue.push_back(target);
            }
        }
    }

    visited.into_iter().collect()
}

/// For each dirty node, determine which changed nodes are its ancestors.
///
/// Returns a map from each dirty node to the list of changed nodes
/// that can reach it (including the changed node itself if it's in the changed set).
pub fn change_provenance<P>(
    graph: &InternalGraph<P>,
    changed: &[InternalNodeIndex],
) -> AHashMap<InternalNodeIndex, Vec<InternalNodeIndex>> {
    let changed_set: HashSet<InternalNodeIndex> = changed.iter().copied().collect();
    let dirty = dirty_set(graph, changed);
    let dirty_set_lookup: HashSet<InternalNodeIndex> = dirty.iter().copied().collect();

    let mut provenance: AHashMap<InternalNodeIndex, Vec<InternalNodeIndex>> =
        AHashMap::with_capacity(dirty.len());

    for &node in &dirty {
        // BFS ancestors from this node, intersect with changed set
        let mut ancestors_changed: Vec<InternalNodeIndex> = Vec::new();

        // If this node is itself a changed node, it's its own provenance
        if changed_set.contains(&node) {
            ancestors_changed.push(node);
        }

        // BFS ancestors
        let mut visited: HashSet<InternalNodeIndex> = HashSet::new();
        visited.insert(node);
        let mut queue: VecDeque<InternalNodeIndex> = VecDeque::new();

        for edge in graph.edges_directed(node, Direction::Incoming) {
            let source = edge.source();
            if visited.insert(source) {
                queue.push_back(source);
            }
        }

        while let Some(current) = queue.pop_front() {
            if changed_set.contains(&current) && !dirty_set_lookup.is_empty() {
                ancestors_changed.push(current);
            }
            for edge in graph.edges_directed(current, Direction::Incoming) {
                let source = edge.source();
                if visited.insert(source) {
                    queue.push_back(source);
                }
            }
        }

        if !ancestors_changed.is_empty() {
            provenance.insert(node, ancestors_changed);
        }
    }

    provenance
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EdgeData, NodeData};
    use petgraph::stable_graph::StableGraph;

    fn make_edge() -> EdgeData {
        EdgeData {
            weight: 1.0,
            label: None,
        }
    }

    fn make_node(name: &str) -> NodeData {
        NodeData {
            name: name.to_string(),
            payload: (),
        }
    }

    #[test]
    fn test_dirty_set_single() {
        // a -> b -> c
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let dirty = dirty_set(&g, &[a]);
        assert_eq!(dirty.len(), 3);
        assert!(dirty.contains(&a));
        assert!(dirty.contains(&b));
        assert!(dirty.contains(&c));
    }

    #[test]
    fn test_dirty_set_leaf() {
        // a -> b -> c, changed = [c]
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let dirty = dirty_set(&g, &[c]);
        assert_eq!(dirty.len(), 1);
        assert!(dirty.contains(&c));
    }

    #[test]
    fn test_dirty_set_empty() {
        let g: InternalGraph = StableGraph::default();
        let dirty = dirty_set(&g, &[]);
        assert!(dirty.is_empty());
    }

    #[test]
    fn test_provenance_linear() {
        // a -> b -> c, changed = [a]
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let prov = change_provenance(&g, &[a]);
        assert!(prov[&a].contains(&a));
        assert!(prov[&b].contains(&a));
        assert!(prov[&c].contains(&a));
    }
}
