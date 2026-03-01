use crate::types::{InternalGraph, InternalNodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use std::collections::{HashSet, VecDeque};

/// Direction for depth-based neighborhood traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubgraphDirection {
    /// Follow only outgoing edges (successors).
    Forward,
    /// Follow only incoming edges (predecessors).
    Backward,
    /// Follow both incoming and outgoing edges.
    Both,
}

/// BFS from `root` up to `depth` hops in the given direction.
/// Returns set of reachable node indices (includes root).
pub fn depth_neighborhood<P>(
    graph: &InternalGraph<P>,
    root: InternalNodeIndex,
    depth: usize,
    direction: SubgraphDirection,
) -> HashSet<InternalNodeIndex> {
    let mut visited = HashSet::new();
    visited.insert(root);

    let mut queue = VecDeque::new();
    queue.push_back((root, 0usize));

    while let Some((node, level)) = queue.pop_front() {
        if level >= depth {
            continue;
        }

        let directions: Vec<Direction> = match direction {
            SubgraphDirection::Forward => vec![Direction::Outgoing],
            SubgraphDirection::Backward => vec![Direction::Incoming],
            SubgraphDirection::Both => vec![Direction::Outgoing, Direction::Incoming],
        };

        for dir in directions {
            for edge in graph.edges_directed(node, dir) {
                let neighbor = match dir {
                    Direction::Outgoing => edge.target(),
                    Direction::Incoming => edge.source(),
                };
                if visited.insert(neighbor) {
                    queue.push_back((neighbor, level + 1));
                }
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
    fn test_depth_zero_returns_root_only() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        g.add_edge(a, b, make_edge());

        let result = depth_neighborhood(&g, a, 0, SubgraphDirection::Forward);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&a));
    }

    #[test]
    fn test_depth_one_forward() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let result = depth_neighborhood(&g, a, 1, SubgraphDirection::Forward);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&a));
        assert!(result.contains(&b));
        assert!(!result.contains(&c));
    }

    #[test]
    fn test_depth_one_backward() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let result = depth_neighborhood(&g, c, 1, SubgraphDirection::Backward);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&c));
        assert!(result.contains(&b));
        assert!(!result.contains(&a));
    }

    #[test]
    fn test_depth_both_directions() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let result = depth_neighborhood(&g, b, 1, SubgraphDirection::Both);
        assert_eq!(result.len(), 3);
        assert!(result.contains(&a));
        assert!(result.contains(&b));
        assert!(result.contains(&c));
    }

    #[test]
    fn test_disconnected_nodes() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let _b = g.add_node(make_node("b"));

        let result = depth_neighborhood(&g, a, 10, SubgraphDirection::Both);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&a));
    }

    #[test]
    fn test_large_depth() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let result = depth_neighborhood(&g, a, 100, SubgraphDirection::Forward);
        assert_eq!(result.len(), 3);
    }
}
