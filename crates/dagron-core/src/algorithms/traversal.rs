use ahash::AHashSet;
use std::collections::VecDeque;

use petgraph::visit::EdgeRef;
use petgraph::Direction;

use crate::types::{InternalGraph, InternalNodeIndex};

/// Find all ancestors of a node (BFS on reversed edges).
/// Does not include the node itself.
pub fn ancestors<P>(graph: &InternalGraph<P>, node: InternalNodeIndex) -> Vec<InternalNodeIndex> {
    bfs_directed(graph, node, Direction::Incoming)
}

/// Find all descendants of a node (BFS on forward edges).
/// Does not include the node itself.
pub fn descendants<P>(graph: &InternalGraph<P>, node: InternalNodeIndex) -> Vec<InternalNodeIndex> {
    bfs_directed(graph, node, Direction::Outgoing)
}

fn bfs_directed<P>(
    graph: &InternalGraph<P>,
    start: InternalNodeIndex,
    direction: Direction,
) -> Vec<InternalNodeIndex> {
    let mut visited = AHashSet::new();
    let mut queue = VecDeque::new();
    let mut result = Vec::new();

    visited.insert(start);

    // Seed with immediate neighbors
    for edge in graph.edges_directed(start, direction) {
        let neighbor = match direction {
            Direction::Outgoing => edge.target(),
            Direction::Incoming => edge.source(),
        };
        if visited.insert(neighbor) {
            queue.push_back(neighbor);
        }
    }

    while let Some(current) = queue.pop_front() {
        result.push(current);
        for edge in graph.edges_directed(current, direction) {
            let neighbor = match direction {
                Direction::Outgoing => edge.target(),
                Direction::Incoming => edge.source(),
            };
            if visited.insert(neighbor) {
                queue.push_back(neighbor);
            }
        }
    }

    result
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
    fn test_descendants() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());
        g.add_edge(a, d, make_edge());

        let desc = descendants(&g, a);
        assert_eq!(desc.len(), 3);
        assert!(desc.contains(&b));
        assert!(desc.contains(&c));
        assert!(desc.contains(&d));
    }

    #[test]
    fn test_ancestors() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let anc = ancestors(&g, c);
        assert_eq!(anc.len(), 2);
        assert!(anc.contains(&a));
        assert!(anc.contains(&b));
    }

    #[test]
    fn test_no_self_inclusion() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));

        assert!(ancestors(&g, a).is_empty());
        assert!(descendants(&g, a).is_empty());
    }
}
