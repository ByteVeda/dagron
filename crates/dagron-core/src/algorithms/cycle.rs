use ahash::AHashSet;

use petgraph::visit::EdgeRef;

use crate::types::{InternalGraph, InternalNodeIndex};

/// Check if adding an edge from `from` to `to` would create a cycle.
/// Returns Some(cycle_path) if it would, None if safe.
/// A cycle would exist if `from` is reachable from `to` (i.e., `to` can reach `from`).
pub fn would_create_cycle<P>(
    graph: &InternalGraph<P>,
    from: InternalNodeIndex,
    to: InternalNodeIndex,
) -> Option<Vec<InternalNodeIndex>> {
    if from == to {
        return Some(vec![from]);
    }

    // BFS from `to` following forward edges — if we reach `from`, there's a cycle.
    let mut visited = AHashSet::new();
    let mut queue = std::collections::VecDeque::new();
    let mut parent: ahash::AHashMap<InternalNodeIndex, InternalNodeIndex> = ahash::AHashMap::new();

    visited.insert(to);
    queue.push_back(to);

    while let Some(current) = queue.pop_front() {
        for edge in graph.edges(current) {
            let neighbor = edge.target();
            if neighbor == from {
                // Reconstruct path: from -> ... -> to -> ... -> from
                let mut path = vec![from, current];
                let mut node = current;
                while node != to {
                    if let Some(&p) = parent.get(&node) {
                        path.push(p);
                        node = p;
                    } else {
                        break;
                    }
                }
                // Ensure `to` is included even if the parent chain broke early
                if path.last() != Some(&to) {
                    path.push(to);
                }
                path.reverse();
                return Some(path);
            }
            if visited.insert(neighbor) {
                parent.insert(neighbor, current);
                queue.push_back(neighbor);
            }
        }
    }

    None
}

/// Find all cycles in the graph using Tarjan's SCC algorithm.
/// Returns a list of cycles, where each cycle is a list of node indices.
/// Only SCCs with more than one node (or a self-loop) are returned.
pub fn find_cycles<P>(graph: &InternalGraph<P>) -> Vec<Vec<InternalNodeIndex>> {
    let sccs = petgraph::algo::tarjan_scc(graph);
    sccs.into_iter()
        .filter(|scc| {
            scc.len() > 1 || (scc.len() == 1 && graph.find_edge(scc[0], scc[0]).is_some())
        })
        .collect()
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
    fn test_no_cycle() {
        let mut g = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        assert!(would_create_cycle(&g, a, c).is_none());
    }

    #[test]
    fn test_direct_cycle() {
        let mut g = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        g.add_edge(a, b, make_edge());

        assert!(would_create_cycle(&g, b, a).is_some());
    }

    #[test]
    fn test_self_loop() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));

        assert!(would_create_cycle(&g, a, a).is_some());
    }

    #[test]
    fn test_find_cycles_empty() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        g.add_edge(a, b, make_edge());

        assert!(find_cycles(&g).is_empty());
    }
}
