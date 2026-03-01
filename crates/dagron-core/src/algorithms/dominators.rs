use ahash::AHashMap;
use petgraph::visit::EdgeRef;
use petgraph::Direction;

use crate::types::{InternalGraph, InternalNodeIndex};

/// Compute immediate dominators using the Cooper-Harvey-Kennedy algorithm.
///
/// Processes nodes in topological order, intersecting dominator paths of predecessors.
/// Returns a map from each node to its immediate dominator (the root maps to itself).
pub fn immediate_dominators<P>(
    graph: &InternalGraph<P>,
    root: InternalNodeIndex,
    topo_order: &[InternalNodeIndex],
) -> AHashMap<InternalNodeIndex, InternalNodeIndex> {
    // Build position map for the topo order
    let mut topo_pos: AHashMap<InternalNodeIndex, usize> = AHashMap::with_capacity(topo_order.len());
    for (i, &node) in topo_order.iter().enumerate() {
        topo_pos.insert(node, i);
    }

    let mut idom: AHashMap<InternalNodeIndex, InternalNodeIndex> =
        AHashMap::with_capacity(topo_order.len());
    idom.insert(root, root);

    // Process nodes in topological order (skip root)
    for &node in topo_order.iter() {
        if node == root {
            continue;
        }

        // Find predecessors that already have a dominator computed
        let mut preds_with_idom: Vec<InternalNodeIndex> = graph
            .edges_directed(node, Direction::Incoming)
            .map(|e| e.source())
            .filter(|p| idom.contains_key(p))
            .collect();

        if preds_with_idom.is_empty() {
            // Node is unreachable from root — skip
            continue;
        }

        // Sort predecessors by topo position for determinism
        preds_with_idom.sort_by_key(|p| topo_pos.get(p).copied().unwrap_or(usize::MAX));

        let mut new_idom = preds_with_idom[0];
        for &pred in &preds_with_idom[1..] {
            new_idom = intersect(&idom, &topo_pos, new_idom, pred);
        }

        idom.insert(node, new_idom);
    }

    idom
}

/// Walk up the dominator tree from both `a` and `b` until they converge.
fn intersect(
    idom: &AHashMap<InternalNodeIndex, InternalNodeIndex>,
    topo_pos: &AHashMap<InternalNodeIndex, usize>,
    mut a: InternalNodeIndex,
    mut b: InternalNodeIndex,
) -> InternalNodeIndex {
    while a != b {
        let pos_a = topo_pos.get(&a).copied().unwrap_or(0);
        let pos_b = topo_pos.get(&b).copied().unwrap_or(0);
        if pos_a > pos_b {
            a = idom[&a];
        } else {
            b = idom[&b];
        }
    }
    a
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithms::toposort::topological_sort_kahn;
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
    fn test_linear_chain() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let topo = topological_sort_kahn(&g).unwrap();
        let idom = immediate_dominators(&g, a, &topo);

        assert_eq!(idom[&a], a);
        assert_eq!(idom[&b], a);
        assert_eq!(idom[&c], b);
    }

    #[test]
    fn test_diamond() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let topo = topological_sort_kahn(&g).unwrap();
        let idom = immediate_dominators(&g, a, &topo);

        assert_eq!(idom[&a], a);
        assert_eq!(idom[&b], a);
        assert_eq!(idom[&c], a);
        assert_eq!(idom[&d], a); // d is dominated by a (both paths go through a)
    }
}
