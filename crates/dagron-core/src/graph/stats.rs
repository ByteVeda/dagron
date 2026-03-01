use std::collections::VecDeque;

use petgraph::visit::{EdgeRef, IntoNodeIdentifiers};
use petgraph::Direction;

use crate::algorithms;
use crate::errors::DagronError;
use crate::types::InternalNodeIndex;

use super::DAG;

/// Comprehensive graph statistics computed in a single pass where possible.
#[derive(Debug, Clone)]
pub struct GraphStats {
    pub node_count: usize,
    pub edge_count: usize,
    /// Number of topological levels (longest chain of dependencies).
    pub depth: usize,
    /// Maximum number of nodes at any single topological level.
    pub width: usize,
    /// Edge density: edges / max_possible_edges. 0.0 for graphs with <2 nodes.
    pub density: f64,
    /// Length of the longest path (in number of edges).
    pub longest_path_length: usize,
    pub avg_in_degree: f64,
    pub avg_out_degree: f64,
    pub max_in_degree: usize,
    pub max_out_degree: usize,
    pub root_count: usize,
    pub leaf_count: usize,
    /// Whether the graph is weakly connected (connected ignoring edge direction).
    pub is_weakly_connected: bool,
    /// Number of weakly connected components.
    pub component_count: usize,
}

impl<P> DAG<P> {
    /// Compute comprehensive graph statistics.
    pub fn stats(&self) -> Result<GraphStats, DagronError> {
        let nc = self.graph.node_count();
        let ec = self.graph.edge_count();

        if nc == 0 {
            return Ok(GraphStats {
                node_count: 0,
                edge_count: 0,
                depth: 0,
                width: 0,
                density: 0.0,
                longest_path_length: 0,
                avg_in_degree: 0.0,
                avg_out_degree: 0.0,
                max_in_degree: 0,
                max_out_degree: 0,
                root_count: 0,
                leaf_count: 0,
                is_weakly_connected: true,
                component_count: 0,
            });
        }

        // Topological levels for depth/width
        let levels = algorithms::topological_levels(&self.graph)
            .map_err(|e| DagronError::Cycle(e.message))?;
        let depth = levels.len();
        let width = levels.iter().map(|l| l.len()).max().unwrap_or(0);

        // Longest path length (in edges) = depth - 1 for DAGs
        // More accurately: use critical_path with uniform cost 1.0
        let empty_costs = ahash::AHashMap::new();
        let (_, longest_cost) = algorithms::critical_path(&self.graph, &empty_costs)
            .map_err(|e| DagronError::Cycle(e.message))?;
        // critical_path returns total cost; with default cost 1.0 per node,
        // the number of edges = total_cost - 1 (since cost includes each node once)
        let longest_path_length = if longest_cost > 0.0 {
            (longest_cost as usize).saturating_sub(1)
        } else {
            0
        };

        // Degree statistics
        let mut max_in: usize = 0;
        let mut max_out: usize = 0;
        let mut root_count: usize = 0;
        let mut leaf_count: usize = 0;

        for idx in self.graph.node_identifiers() {
            let in_deg = self.graph.edges_directed(idx, Direction::Incoming).count();
            let out_deg = self.graph.edges_directed(idx, Direction::Outgoing).count();
            if in_deg > max_in {
                max_in = in_deg;
            }
            if out_deg > max_out {
                max_out = out_deg;
            }
            if in_deg == 0 {
                root_count += 1;
            }
            if out_deg == 0 {
                leaf_count += 1;
            }
        }

        let avg_in = ec as f64 / nc as f64;
        let avg_out = avg_in; // in a directed graph, total in-degree == total out-degree == edge count

        // Density
        let max_edges = nc * (nc - 1); // directed: n*(n-1)
        let density = if max_edges > 0 {
            ec as f64 / max_edges as f64
        } else {
            0.0
        };

        // Weak connectivity via BFS on undirected neighbor view
        let component_count = count_weak_components(&self.graph);
        let is_weakly_connected = component_count <= 1;

        Ok(GraphStats {
            node_count: nc,
            edge_count: ec,
            depth,
            width,
            density,
            longest_path_length,
            avg_in_degree: avg_in,
            avg_out_degree: avg_out,
            max_in_degree: max_in,
            max_out_degree: max_out,
            root_count,
            leaf_count,
            is_weakly_connected,
            component_count,
        })
    }
}

/// Count weakly connected components using BFS on undirected neighbor view.
fn count_weak_components<P>(graph: &crate::types::InternalGraph<P>) -> usize {
    use ahash::AHashSet;

    let mut visited = AHashSet::new();
    let mut components = 0;

    for start in graph.node_identifiers() {
        if visited.contains(&start) {
            continue;
        }
        components += 1;
        let mut queue = VecDeque::new();
        queue.push_back(start);
        visited.insert(start);

        while let Some(node) = queue.pop_front() {
            // Visit neighbors in both directions (undirected)
            for neighbor in undirected_neighbors(graph, node) {
                if visited.insert(neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }
    }

    components
}

/// Get all neighbors of a node ignoring edge direction.
fn undirected_neighbors<P>(
    graph: &crate::types::InternalGraph<P>,
    node: InternalNodeIndex,
) -> Vec<InternalNodeIndex> {
    let mut neighbors = Vec::new();
    for e in graph.edges_directed(node, Direction::Outgoing) {
        neighbors.push(e.target());
    }
    for e in graph.edges_directed(node, Direction::Incoming) {
        neighbors.push(e.source());
    }
    neighbors
}
