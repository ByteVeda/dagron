use std::collections::HashSet;

use petgraph::visit::{EdgeRef, IntoEdgeReferences, IntoNodeIdentifiers};
use petgraph::Direction;

use crate::types::{InternalGraph, InternalNodeIndex};

/// Compute the set of edges to REMOVE for transitive reduction.
///
/// An edge u→v is redundant if there exists another path from u to v
/// (of length ≥ 2). Returns the set of (source, target) pairs to remove.
pub fn transitive_reduction_redundant_edges<P>(
    graph: &InternalGraph<P>,
) -> HashSet<(InternalNodeIndex, InternalNodeIndex)> {
    let mut redundant = HashSet::new();

    for edge in graph.edge_references() {
        let u = edge.source();
        let v = edge.target();

        // BFS/DFS from u to v, skipping the direct edge u→v.
        // If we can reach v via another path, the edge is redundant.
        if has_alternate_path(graph, u, v) {
            redundant.insert((u, v));
        }
    }

    redundant
}

/// Compute edges to ADD for transitive closure.
///
/// Returns (source, target) pairs for edges that don't exist yet but should
/// (i.e., for every pair u,v where u can reach v but no direct edge exists).
pub fn transitive_closure_new_edges<P>(
    graph: &InternalGraph<P>,
) -> Vec<(InternalNodeIndex, InternalNodeIndex)> {
    let mut new_edges = Vec::new();

    for u in graph.node_identifiers() {
        // Find all reachable nodes from u via DFS
        let reachable = reachable_set(graph, u);
        for v in reachable {
            if graph.find_edge(u, v).is_none() {
                new_edges.push((u, v));
            }
        }
    }

    new_edges
}

/// Check if there is a path from `src` to `dst` that doesn't use the direct edge src→dst.
fn has_alternate_path<P>(
    graph: &InternalGraph<P>,
    src: InternalNodeIndex,
    dst: InternalNodeIndex,
) -> bool {
    // DFS from src's other successors (excluding dst) to see if we can reach dst
    let mut stack: Vec<InternalNodeIndex> = Vec::new();
    let mut visited = HashSet::new();
    visited.insert(src);

    for edge in graph.edges_directed(src, Direction::Outgoing) {
        let neighbor = edge.target();
        if neighbor != dst && visited.insert(neighbor) {
            stack.push(neighbor);
        }
    }

    while let Some(current) = stack.pop() {
        if current == dst {
            return true;
        }
        for edge in graph.edges_directed(current, Direction::Outgoing) {
            let neighbor = edge.target();
            if visited.insert(neighbor) {
                stack.push(neighbor);
            }
        }
    }

    false
}

/// Compute the set of all nodes reachable from `start` (excluding `start` itself).
fn reachable_set<P>(
    graph: &InternalGraph<P>,
    start: InternalNodeIndex,
) -> HashSet<InternalNodeIndex> {
    let mut visited = HashSet::new();
    let mut stack = Vec::new();

    for edge in graph.edges_directed(start, Direction::Outgoing) {
        let neighbor = edge.target();
        if visited.insert(neighbor) {
            stack.push(neighbor);
        }
    }

    while let Some(current) = stack.pop() {
        for edge in graph.edges_directed(current, Direction::Outgoing) {
            let neighbor = edge.target();
            if visited.insert(neighbor) {
                stack.push(neighbor);
            }
        }
    }

    visited
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
    fn reduction_removes_shortcut_in_diamond() {
        // a -> b -> d, a -> c -> d, a -> d (shortcut)
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(c, d, make_edge());
        g.add_edge(a, d, make_edge()); // shortcut

        let redundant = transitive_reduction_redundant_edges(&g);
        assert!(redundant.contains(&(a, d)));
        assert_eq!(redundant.len(), 1);
    }

    #[test]
    fn reduction_linear_chain_no_removals() {
        // a -> b -> c (no shortcuts)
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let redundant = transitive_reduction_redundant_edges(&g);
        assert!(redundant.is_empty());
    }

    #[test]
    fn reduction_empty_graph() {
        let g: InternalGraph = StableGraph::default();
        let redundant = transitive_reduction_redundant_edges(&g);
        assert!(redundant.is_empty());
    }

    #[test]
    fn closure_adds_missing_in_chain() {
        // a -> b -> c (missing a -> c)
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let new = transitive_closure_new_edges(&g);
        assert_eq!(new.len(), 1);
        assert!(new.contains(&(a, c)));
    }

    #[test]
    fn closure_already_complete() {
        // a -> b, a -> c, b -> c — already transitively closed
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());
        g.add_edge(a, c, make_edge());

        let new = transitive_closure_new_edges(&g);
        assert!(new.is_empty());
    }

    #[test]
    fn closure_empty_graph() {
        let g: InternalGraph = StableGraph::default();
        let new = transitive_closure_new_edges(&g);
        assert!(new.is_empty());
    }
}
