use crate::types::{InternalGraph, InternalNodeIndex};
use petgraph::visit::{EdgeRef, IntoNodeIdentifiers};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use super::toposort::CycleInfo;

/// Newtype wrapper for f64 that implements Ord via total_cmp().
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct OrdF64(pub f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Priority-aware topological sort using modified Kahn's algorithm.
/// Higher priority nodes are emitted first. Equal priority breaks ties alphabetically (ascending).
/// Missing nodes default to priority 0.0.
pub fn topological_sort_priority<P>(
    graph: &InternalGraph<P>,
    priorities: &ahash::AHashMap<InternalNodeIndex, f64>,
) -> Result<Vec<InternalNodeIndex>, CycleInfo> {
    let node_count = graph.node_count();
    let mut in_degree: ahash::AHashMap<InternalNodeIndex, usize> =
        ahash::AHashMap::with_capacity(node_count);

    for node in graph.node_identifiers() {
        in_degree.entry(node).or_insert(0);
        for edge in graph.edges(node) {
            *in_degree.entry(edge.target()).or_insert(0) += 1;
        }
    }

    // Max-heap: (priority desc, name asc via Reverse, node index)
    let mut heap: BinaryHeap<(OrdF64, Reverse<String>, InternalNodeIndex)> = BinaryHeap::new();

    for (&node, &deg) in &in_degree {
        if deg == 0 {
            let priority = priorities.get(&node).copied().unwrap_or(0.0);
            heap.push((OrdF64(priority), Reverse(graph[node].name.clone()), node));
        }
    }

    let mut result = Vec::with_capacity(node_count);

    while let Some((_, _, node)) = heap.pop() {
        result.push(node);
        let mut neighbors: Vec<InternalNodeIndex> =
            graph.edges(node).map(|e| e.target()).collect();
        neighbors.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));
        for neighbor in neighbors {
            if let Some(deg) = in_degree.get_mut(&neighbor) {
                *deg -= 1;
                if *deg == 0 {
                    let priority = priorities.get(&neighbor).copied().unwrap_or(0.0);
                    heap.push((
                        OrdF64(priority),
                        Reverse(graph[neighbor].name.clone()),
                        neighbor,
                    ));
                }
            }
        }
    }

    if result.len() != node_count {
        Err(CycleInfo {
            message: "Graph contains a cycle".to_string(),
        })
    } else {
        Ok(result)
    }
}

/// Compute topological levels with priority-based sorting within each level.
/// Nodes within each level are sorted by priority (descending), then name (ascending).
/// Missing nodes default to priority 0.0.
pub fn topological_levels_priority<P>(
    graph: &InternalGraph<P>,
    priorities: &ahash::AHashMap<InternalNodeIndex, f64>,
) -> Result<Vec<Vec<InternalNodeIndex>>, CycleInfo> {
    let node_count = graph.node_count();
    let mut in_degree: ahash::AHashMap<InternalNodeIndex, usize> =
        ahash::AHashMap::with_capacity(node_count);

    for node in graph.node_identifiers() {
        in_degree.entry(node).or_insert(0);
        for edge in graph.edges(node) {
            *in_degree.entry(edge.target()).or_insert(0) += 1;
        }
    }

    let mut levels: Vec<Vec<InternalNodeIndex>> = Vec::new();
    let mut current_level: Vec<InternalNodeIndex> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&node, _)| node)
        .collect();

    sort_by_priority(graph, &mut current_level, priorities);

    let mut processed = 0;

    while !current_level.is_empty() {
        processed += current_level.len();
        let mut next_level = Vec::new();

        for &node in &current_level {
            for edge in graph.edges(node) {
                let neighbor = edge.target();
                if let Some(deg) = in_degree.get_mut(&neighbor) {
                    *deg -= 1;
                    if *deg == 0 {
                        next_level.push(neighbor);
                    }
                }
            }
        }

        sort_by_priority(graph, &mut next_level, priorities);
        levels.push(current_level);
        current_level = next_level;
    }

    if processed != node_count {
        Err(CycleInfo {
            message: "Graph contains a cycle".to_string(),
        })
    } else {
        Ok(levels)
    }
}

/// Sort nodes by priority descending, then name ascending.
fn sort_by_priority<P>(
    graph: &InternalGraph<P>,
    nodes: &mut [InternalNodeIndex],
    priorities: &ahash::AHashMap<InternalNodeIndex, f64>,
) {
    nodes.sort_by(|a, b| {
        let pa = priorities.get(a).copied().unwrap_or(0.0);
        let pb = priorities.get(b).copied().unwrap_or(0.0);
        // Higher priority first, then name ascending
        pb.total_cmp(&pa).then_with(|| graph[*a].name.cmp(&graph[*b].name))
    });
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
    fn test_priority_sort_no_priorities() {
        // Without priorities, falls back to alphabetical (all default to 0.0)
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());

        let priorities = ahash::AHashMap::new();
        let order = topological_sort_priority(&g, &priorities).unwrap();
        // a first, then b before c (alphabetical tiebreak)
        assert_eq!(order, vec![a, b, c]);
    }

    #[test]
    fn test_priority_sort_higher_priority_first() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());

        let mut priorities = ahash::AHashMap::new();
        priorities.insert(c, 10.0);
        priorities.insert(b, 1.0);

        let order = topological_sort_priority(&g, &priorities).unwrap();
        // a first (only root), then c (priority 10) before b (priority 1)
        assert_eq!(order, vec![a, c, b]);
    }

    #[test]
    fn test_priority_sort_respects_dependencies() {
        // Even with high priority, a node can't appear before its dependencies
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let mut priorities = ahash::AHashMap::new();
        priorities.insert(c, 100.0);

        let order = topological_sort_priority(&g, &priorities).unwrap();
        assert_eq!(order, vec![a, b, c]);
    }

    #[test]
    fn test_priority_sort_cycle_detection() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, a, make_edge());

        let priorities = ahash::AHashMap::new();
        assert!(topological_sort_priority(&g, &priorities).is_err());
    }

    #[test]
    fn test_priority_levels_sorting_within_level() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(a, d, make_edge());

        let mut priorities = ahash::AHashMap::new();
        priorities.insert(d, 10.0);
        priorities.insert(b, 5.0);
        priorities.insert(c, 1.0);

        let levels = topological_levels_priority(&g, &priorities).unwrap();
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], vec![a]);
        // Level 1 sorted by priority desc: d(10), b(5), c(1)
        assert_eq!(levels[1], vec![d, b, c]);
    }

    #[test]
    fn test_priority_levels_empty_graph() {
        let g: InternalGraph = StableGraph::default();
        let priorities = ahash::AHashMap::new();
        let levels = topological_levels_priority(&g, &priorities).unwrap();
        assert!(levels.is_empty());
    }

    #[test]
    fn test_priority_sort_equal_priority_alphabetical() {
        // Nodes with equal priority should be sorted alphabetically
        let mut g: InternalGraph = StableGraph::default();
        let z = g.add_node(make_node("z"));
        let a = g.add_node(make_node("a"));
        let m = g.add_node(make_node("m"));

        let mut priorities = ahash::AHashMap::new();
        priorities.insert(z, 5.0);
        priorities.insert(a, 5.0);
        priorities.insert(m, 5.0);

        let order = topological_sort_priority(&g, &priorities).unwrap();
        let names: Vec<&str> = order.iter().map(|&idx| g[idx].name.as_str()).collect();
        assert_eq!(names, vec!["a", "m", "z"]);
    }
}
