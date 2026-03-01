use ahash::AHashMap;

use crate::algorithms::toposort::CycleInfo;
use crate::types::{InternalGraph, InternalNodeIndex};

/// Precomputed reachability index using bitsets for O(1) queries.
///
/// Space: O(V²/64). Build time: O(V*E/64).
/// Suitable for graphs up to ~64K nodes.
pub struct ReachabilityIndex {
    node_to_pos: AHashMap<InternalNodeIndex, usize>,
    pos_to_node: Vec<InternalNodeIndex>,
    /// forward[i] = bitset of nodes reachable FROM node at position i (including self)
    forward: Vec<Vec<u64>>,
    num_nodes: usize,
    words_per_node: usize,
}

impl ReachabilityIndex {
    /// Build from graph. O(V*E/64) time, O(V²/64) space.
    pub fn new<P>(graph: &InternalGraph<P>) -> Result<Self, CycleInfo> {
        let topo = super::toposort::topological_sort_kahn(graph)?;
        let num_nodes = topo.len();
        let words_per_node = (num_nodes + 63) / 64;

        let mut node_to_pos = AHashMap::with_capacity(num_nodes);
        let mut pos_to_node = Vec::with_capacity(num_nodes);

        for (i, &node) in topo.iter().enumerate() {
            node_to_pos.insert(node, i);
            pos_to_node.push(node);
        }

        // Initialize bitset matrix
        let mut forward = vec![vec![0u64; words_per_node]; num_nodes];

        // Set each node's own bit
        for i in 0..num_nodes {
            let word = i / 64;
            let bit = i % 64;
            forward[i][word] |= 1u64 << bit;
        }

        // Process in reverse topological order
        // For each node, OR in all successors' bitsets
        use petgraph::visit::EdgeRef;
        for &node in topo.iter().rev() {
            let pos = node_to_pos[&node];
            // Collect successor positions
            let successor_positions: Vec<usize> = graph
                .edges(node)
                .map(|e| node_to_pos[&e.target()])
                .collect();

            for succ_pos in successor_positions {
                for w in 0..words_per_node {
                    forward[pos][w] |= forward[succ_pos][w];
                }
            }
        }

        Ok(ReachabilityIndex {
            node_to_pos,
            pos_to_node,
            forward,
            num_nodes,
            words_per_node,
        })
    }

    /// O(1) reachability query: can `from` reach `to`?
    pub fn can_reach(&self, from: InternalNodeIndex, to: InternalNodeIndex) -> bool {
        let from_pos = match self.node_to_pos.get(&from) {
            Some(&p) => p,
            None => return false,
        };
        let to_pos = match self.node_to_pos.get(&to) {
            Some(&p) => p,
            None => return false,
        };

        let word = to_pos / 64;
        let bit = to_pos % 64;
        (self.forward[from_pos][word] & (1u64 << bit)) != 0
    }

    /// All nodes reachable from `node` (O(V/64)).
    pub fn reachable_from(&self, node: InternalNodeIndex) -> Vec<InternalNodeIndex> {
        let pos = match self.node_to_pos.get(&node) {
            Some(&p) => p,
            None => return Vec::new(),
        };

        let mut result = Vec::new();
        for w in 0..self.words_per_node {
            let mut bits = self.forward[pos][w];
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                let idx = w * 64 + bit;
                if idx < self.num_nodes && idx != pos {
                    result.push(self.pos_to_node[idx]);
                }
                bits &= bits - 1; // clear lowest set bit
            }
        }
        result
    }

    /// All nodes that can reach `node` (O(V²/64) scan — no reverse index stored).
    pub fn ancestors_of(&self, node: InternalNodeIndex) -> Vec<InternalNodeIndex> {
        let to_pos = match self.node_to_pos.get(&node) {
            Some(&p) => p,
            None => return Vec::new(),
        };

        let word = to_pos / 64;
        let bit = to_pos % 64;
        let mask = 1u64 << bit;

        let mut result = Vec::new();
        for i in 0..self.num_nodes {
            if i != to_pos && (self.forward[i][word] & mask) != 0 {
                result.push(self.pos_to_node[i]);
            }
        }
        result
    }

    /// Number of nodes in the index.
    pub fn node_count(&self) -> usize {
        self.num_nodes
    }
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
    fn test_diamond_reachability() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let idx = ReachabilityIndex::new(&g).unwrap();

        assert!(idx.can_reach(a, d));
        assert!(idx.can_reach(a, b));
        assert!(idx.can_reach(a, c));
        assert!(idx.can_reach(b, d));
        assert!(idx.can_reach(c, d));
        assert!(!idx.can_reach(d, a));
        assert!(!idx.can_reach(b, c));

        // Self-reachability
        assert!(idx.can_reach(a, a));
    }

    #[test]
    fn test_reachable_from() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let idx = ReachabilityIndex::new(&g).unwrap();
        let reachable = idx.reachable_from(a);
        assert_eq!(reachable.len(), 2);
        assert!(reachable.contains(&b));
        assert!(reachable.contains(&c));
    }

    #[test]
    fn test_ancestors_of() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let idx = ReachabilityIndex::new(&g).unwrap();
        let ancestors = idx.ancestors_of(c);
        assert_eq!(ancestors.len(), 2);
        assert!(ancestors.contains(&a));
        assert!(ancestors.contains(&b));
    }

    #[test]
    fn test_disconnected_components() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));

        let idx = ReachabilityIndex::new(&g).unwrap();
        assert!(!idx.can_reach(a, b));
        assert!(!idx.can_reach(b, a));
    }

    #[test]
    fn test_empty_graph() {
        let g: InternalGraph = StableGraph::default();
        let idx = ReachabilityIndex::new(&g).unwrap();
        assert_eq!(idx.node_count(), 0);
    }

    #[test]
    fn test_single_node() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));

        let idx = ReachabilityIndex::new(&g).unwrap();
        assert!(idx.can_reach(a, a));
        assert_eq!(idx.node_count(), 1);
        assert!(idx.reachable_from(a).is_empty());
    }
}
