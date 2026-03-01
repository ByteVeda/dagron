use ahash::AHashMap;
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use std::collections::{HashSet, VecDeque};

use crate::types::{InternalGraph, InternalNodeIndex};

/// All directed paths from `from` to `to`. DFS with backtracking.
/// Stops after `limit` paths (None = unlimited).
pub fn all_paths<P>(
    graph: &InternalGraph<P>,
    from: InternalNodeIndex,
    to: InternalNodeIndex,
    limit: Option<usize>,
) -> Vec<Vec<InternalNodeIndex>> {
    let mut results = Vec::new();
    let mut path = vec![from];
    let mut visited = HashSet::new();
    visited.insert(from);
    all_paths_dfs(graph, to, &mut path, &mut visited, &mut results, limit);
    results
}

fn all_paths_dfs<P>(
    graph: &InternalGraph<P>,
    target: InternalNodeIndex,
    path: &mut Vec<InternalNodeIndex>,
    visited: &mut HashSet<InternalNodeIndex>,
    results: &mut Vec<Vec<InternalNodeIndex>>,
    limit: Option<usize>,
) {
    let current = *path.last().unwrap();

    if current == target {
        results.push(path.clone());
        return;
    }

    if let Some(lim) = limit {
        if results.len() >= lim {
            return;
        }
    }

    // Sort neighbors for deterministic output
    let mut neighbors: Vec<InternalNodeIndex> = graph
        .edges_directed(current, Direction::Outgoing)
        .map(|e| e.target())
        .collect();
    neighbors.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));

    for neighbor in neighbors {
        if let Some(lim) = limit {
            if results.len() >= lim {
                return;
            }
        }
        if visited.insert(neighbor) {
            path.push(neighbor);
            all_paths_dfs(graph, target, path, visited, results, limit);
            path.pop();
            visited.remove(&neighbor);
        }
    }
}

/// Shortest path (fewest edges) via BFS. Returns None if unreachable.
pub fn shortest_path<P>(
    graph: &InternalGraph<P>,
    from: InternalNodeIndex,
    to: InternalNodeIndex,
) -> Option<Vec<InternalNodeIndex>> {
    if from == to {
        return Some(vec![from]);
    }

    let mut visited = HashSet::new();
    visited.insert(from);
    let mut queue = VecDeque::new();
    queue.push_back(from);
    let mut parent: AHashMap<InternalNodeIndex, InternalNodeIndex> = AHashMap::new();

    while let Some(current) = queue.pop_front() {
        for edge in graph.edges_directed(current, Direction::Outgoing) {
            let neighbor = edge.target();
            if visited.insert(neighbor) {
                parent.insert(neighbor, current);
                if neighbor == to {
                    // Reconstruct path
                    let mut path = vec![to];
                    let mut node = to;
                    while let Some(&p) = parent.get(&node) {
                        path.push(p);
                        node = p;
                    }
                    path.reverse();
                    return Some(path);
                }
                queue.push_back(neighbor);
            }
        }
    }

    None
}

/// Longest weighted path between two specific nodes (DP on DAG topo order).
/// Uses `costs` map (default 1.0 per node). Returns None if unreachable.
pub fn longest_path<P>(
    graph: &InternalGraph<P>,
    from: InternalNodeIndex,
    to: InternalNodeIndex,
    costs: &AHashMap<InternalNodeIndex, f64>,
) -> Option<(Vec<InternalNodeIndex>, f64)> {
    // Compute topological order via Kahn's
    let topo = match super::toposort::topological_sort_kahn(graph) {
        Ok(order) => order,
        Err(_) => return None,
    };

    // Find index of `from` in topo order
    let from_pos = topo.iter().position(|&n| n == from)?;

    // DP: dist[node] = longest distance from `from` to `node`
    let mut dist: AHashMap<InternalNodeIndex, f64> = AHashMap::new();
    let mut prev: AHashMap<InternalNodeIndex, InternalNodeIndex> = AHashMap::new();

    let from_cost = costs.get(&from).copied().unwrap_or(1.0);
    dist.insert(from, from_cost);

    // Process nodes in topo order starting from `from`
    for &node in &topo[from_pos..] {
        if let Some(&d) = dist.get(&node) {
            for edge in graph.edges_directed(node, Direction::Outgoing) {
                let neighbor = edge.target();
                let neighbor_cost = costs.get(&neighbor).copied().unwrap_or(1.0);
                let new_dist = d + neighbor_cost;
                if new_dist > dist.get(&neighbor).copied().unwrap_or(f64::NEG_INFINITY) {
                    dist.insert(neighbor, new_dist);
                    prev.insert(neighbor, node);
                }
            }
        }
    }

    // Check if `to` is reachable
    let total = *dist.get(&to)?;

    // Reconstruct path
    let mut path = vec![to];
    let mut node = to;
    while let Some(&p) = prev.get(&node) {
        path.push(p);
        node = p;
    }
    path.reverse();

    Some((path, total))
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
    fn test_all_paths_linear() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let paths = all_paths(&g, a, c, None);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], vec![a, b, c]);
    }

    #[test]
    fn test_all_paths_diamond() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let paths = all_paths(&g, a, d, None);
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn test_all_paths_no_path() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));

        let paths = all_paths(&g, a, b, None);
        assert!(paths.is_empty());
    }

    #[test]
    fn test_all_paths_with_limit() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let paths = all_paths(&g, a, d, Some(1));
        assert_eq!(paths.len(), 1);
    }

    #[test]
    fn test_all_paths_same_node() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));

        let paths = all_paths(&g, a, a, None);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], vec![a]);
    }

    #[test]
    fn test_shortest_path_linear() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let path = shortest_path(&g, a, c).unwrap();
        assert_eq!(path, vec![a, b, c]);
    }

    #[test]
    fn test_shortest_path_diamond() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());
        // Also add direct edge
        g.add_edge(a, d, make_edge());

        let path = shortest_path(&g, a, d).unwrap();
        assert_eq!(path.len(), 2); // a -> d directly
    }

    #[test]
    fn test_shortest_path_no_path() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let _b = g.add_node(make_node("b"));

        assert!(shortest_path(&g, a, _b).is_none());
    }

    #[test]
    fn test_shortest_path_same_node() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));

        let path = shortest_path(&g, a, a).unwrap();
        assert_eq!(path, vec![a]);
    }

    #[test]
    fn test_longest_path_linear() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let costs = AHashMap::new();
        let (path, cost) = longest_path(&g, a, c, &costs).unwrap();
        assert_eq!(path, vec![a, b, c]);
        assert_eq!(cost, 3.0);
    }

    #[test]
    fn test_longest_path_weighted() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let mut costs = AHashMap::new();
        costs.insert(a, 1.0);
        costs.insert(b, 10.0);
        costs.insert(c, 2.0);
        costs.insert(d, 1.0);

        let (path, cost) = longest_path(&g, a, d, &costs).unwrap();
        assert_eq!(path, vec![a, b, d]);
        assert_eq!(cost, 12.0);
    }

    #[test]
    fn test_longest_path_no_path() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let _b = g.add_node(make_node("b"));

        let costs = AHashMap::new();
        assert!(longest_path(&g, a, _b, &costs).is_none());
    }
}
