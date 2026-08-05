use crate::types::{InternalGraph, InternalNodeIndex};
use petgraph::visit::{EdgeRef, IntoNodeIdentifiers};
use std::collections::VecDeque;

#[derive(Debug)]
pub struct CycleInfo {
    pub message: String,
}

/// Kahn's algorithm for topological sorting.
/// Returns nodes in dependency order (sources first).
pub fn topological_sort_kahn<P>(
    graph: &InternalGraph<P>,
) -> Result<Vec<InternalNodeIndex>, CycleInfo> {
    let node_count = graph.node_count();
    let mut in_degree: ahash::AHashMap<InternalNodeIndex, usize> =
        ahash::AHashMap::with_capacity(node_count);

    // Initialize in-degrees
    for node in graph.node_identifiers() {
        in_degree.entry(node).or_insert(0);
        for edge in graph.edges(node) {
            *in_degree.entry(edge.target()).or_insert(0) += 1;
        }
    }

    // Seed queue with zero in-degree nodes, sorted by name for deterministic output
    let mut queue: VecDeque<InternalNodeIndex> = VecDeque::new();
    let mut zero_deg: Vec<InternalNodeIndex> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&node, _)| node)
        .collect();
    zero_deg.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));
    for node in zero_deg {
        queue.push_back(node);
    }

    let mut result = Vec::with_capacity(node_count);

    while let Some(node) = queue.pop_front() {
        result.push(node);
        // Collect and sort neighbors for deterministic ordering
        let mut neighbors: Vec<InternalNodeIndex> = graph.edges(node).map(|e| e.target()).collect();
        neighbors.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));
        for neighbor in neighbors {
            if let Some(deg) = in_degree.get_mut(&neighbor) {
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(neighbor);
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

/// DFS-based topological sort (reverse postorder).
pub fn topological_sort_dfs<P>(
    graph: &InternalGraph<P>,
) -> Result<Vec<InternalNodeIndex>, CycleInfo> {
    use ahash::AHashSet;

    let mut visited = AHashSet::new();
    let mut on_stack = AHashSet::new();
    let mut result = Vec::with_capacity(graph.node_count());

    // Sort nodes by name for deterministic output
    let mut nodes: Vec<InternalNodeIndex> = graph.node_identifiers().collect();
    nodes.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));

    for node in nodes {
        if !visited.contains(&node)
            && !dfs_visit(graph, node, &mut visited, &mut on_stack, &mut result)
        {
            return Err(CycleInfo {
                message: "Graph contains a cycle".to_string(),
            });
        }
    }

    result.reverse();
    Ok(result)
}

fn dfs_visit<P>(
    graph: &InternalGraph<P>,
    node: InternalNodeIndex,
    visited: &mut ahash::AHashSet<InternalNodeIndex>,
    on_stack: &mut ahash::AHashSet<InternalNodeIndex>,
    result: &mut Vec<InternalNodeIndex>,
) -> bool {
    visited.insert(node);
    on_stack.insert(node);

    // Sort neighbors for deterministic ordering
    let mut neighbors: Vec<InternalNodeIndex> = graph.edges(node).map(|e| e.target()).collect();
    neighbors.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));

    for neighbor in neighbors {
        if on_stack.contains(&neighbor) {
            return false; // cycle detected
        }
        if !visited.contains(&neighbor) && !dfs_visit(graph, neighbor, visited, on_stack, result) {
            return false;
        }
    }

    on_stack.remove(&node);
    result.push(node);
    true
}

/// Compute topological levels (BFS layers from roots).
/// Level 0 = root nodes, Level 1 = nodes whose only predecessors are in level 0, etc.
pub fn topological_levels<P>(
    graph: &InternalGraph<P>,
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
    current_level.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));

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

        next_level.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));
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

/// Enumerate all valid topological orderings via backtracking.
/// Stops after `limit` orderings (None = unlimited, WARNING: can be factorial).
pub fn all_topological_orderings<P>(
    graph: &InternalGraph<P>,
    limit: Option<usize>,
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

    // Check for cycles: if no zero-degree nodes and graph is non-empty
    let zero_count = in_degree.values().filter(|&&d| d == 0).count();
    if node_count > 0 && zero_count == 0 {
        return Err(CycleInfo {
            message: "Graph contains a cycle".to_string(),
        });
    }

    let mut results = Vec::new();
    let mut current = Vec::new();
    all_topo_backtrack(graph, &mut in_degree, &mut current, &mut results, limit);

    // Verify completeness (cycle detection for partially valid graphs)
    if !results.is_empty() && results[0].len() != node_count {
        return Err(CycleInfo {
            message: "Graph contains a cycle".to_string(),
        });
    }

    Ok(results)
}

fn all_topo_backtrack<P>(
    graph: &InternalGraph<P>,
    in_degree: &mut ahash::AHashMap<InternalNodeIndex, usize>,
    current: &mut Vec<InternalNodeIndex>,
    results: &mut Vec<Vec<InternalNodeIndex>>,
    limit: Option<usize>,
) {
    if let Some(lim) = limit {
        if results.len() >= lim {
            return;
        }
    }

    // Find all zero-in-degree nodes (removed nodes won't appear in in_degree)
    let mut candidates: Vec<InternalNodeIndex> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(&node, _)| node)
        .collect();
    // Sort by name for deterministic ordering
    candidates.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));

    if candidates.is_empty() {
        if !current.is_empty() {
            results.push(current.clone());
        }
        return;
    }

    for candidate in candidates {
        if let Some(lim) = limit {
            if results.len() >= lim {
                return;
            }
        }

        // Pick this candidate
        current.push(candidate);

        // Decrement in-degrees of successors
        let mut neighbors: Vec<InternalNodeIndex> =
            graph.edges(candidate).map(|e| e.target()).collect();
        neighbors.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));

        for &neighbor in &neighbors {
            *in_degree.get_mut(&neighbor).unwrap() -= 1;
        }

        // Remove candidate from in_degree map temporarily
        let old_deg = in_degree.remove(&candidate).unwrap();

        all_topo_backtrack(graph, in_degree, current, results, limit);

        // Restore
        in_degree.insert(candidate, old_deg);
        for &neighbor in &neighbors {
            *in_degree.get_mut(&neighbor).unwrap() += 1;
        }
        current.pop();
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
    fn test_kahn_linear() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let order = topological_sort_kahn(&g).unwrap();
        assert_eq!(order, vec![a, b, c]);
    }

    #[test]
    fn test_dfs_linear() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let order = topological_sort_dfs(&g).unwrap();
        assert_eq!(order, vec![a, b, c]);
    }

    #[test]
    fn test_levels_diamond() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let levels = topological_levels(&g).unwrap();
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec![a]);
        // b and c in level 1, sorted by name
        assert_eq!(levels[1], vec![b, c]);
        assert_eq!(levels[2], vec![d]);
    }

    #[test]
    fn test_empty_graph() {
        let g: InternalGraph = StableGraph::default();
        assert!(topological_sort_kahn(&g).unwrap().is_empty());
        assert!(topological_sort_dfs(&g).unwrap().is_empty());
        assert!(topological_levels(&g).unwrap().is_empty());
    }

    #[test]
    fn test_all_orderings_linear() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let orderings = all_topological_orderings(&g, None).unwrap();
        assert_eq!(orderings.len(), 1);
        assert_eq!(orderings[0], vec![a, b, c]);
    }

    #[test]
    fn test_all_orderings_diamond() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let orderings = all_topological_orderings(&g, None).unwrap();
        assert_eq!(orderings.len(), 2);
    }

    #[test]
    fn test_all_orderings_independent() {
        let mut g: InternalGraph = StableGraph::default();
        let _a = g.add_node(make_node("a"));
        let _b = g.add_node(make_node("b"));
        let _c = g.add_node(make_node("c"));

        let orderings = all_topological_orderings(&g, None).unwrap();
        assert_eq!(orderings.len(), 6); // 3! = 6
    }

    #[test]
    fn test_all_orderings_with_limit() {
        let mut g: InternalGraph = StableGraph::default();
        let _a = g.add_node(make_node("a"));
        let _b = g.add_node(make_node("b"));
        let _c = g.add_node(make_node("c"));

        let orderings = all_topological_orderings(&g, Some(2)).unwrap();
        assert_eq!(orderings.len(), 2);
    }

    #[test]
    fn test_all_orderings_empty() {
        let g: InternalGraph = StableGraph::default();
        let orderings = all_topological_orderings(&g, None).unwrap();
        assert!(orderings.is_empty());
    }
}
