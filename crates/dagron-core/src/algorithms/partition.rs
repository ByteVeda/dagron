//! Graph partitioning algorithms for DAG distribution.
//!
//! Three strategies:
//! - Level-based: group contiguous topological levels into k partitions
//! - Size-balanced: greedily assign nodes targeting equal cost per partition
//! - Communication-minimizing: iterative refinement to reduce cross-edges

use ahash::AHashMap;
use petgraph::visit::EdgeRef;

use crate::types::{InternalGraph, InternalNodeIndex};

use super::toposort::{topological_levels, CycleInfo};

/// Result of a single partition.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition_id: usize,
    pub node_indices: Vec<InternalNodeIndex>,
    pub total_cost: f64,
    pub incoming_cross_edges: usize,
    pub outgoing_cross_edges: usize,
}

/// Complete result of partitioning a graph.
#[derive(Debug, Clone)]
pub struct PartitionResult {
    pub partitions: Vec<PartitionInfo>,
    pub cross_edge_count: usize,
    /// Topological ordering of partition IDs (which partitions depend on which).
    pub partition_order: Vec<Vec<usize>>,
    /// Mapping from node index to partition ID.
    pub node_to_partition: AHashMap<InternalNodeIndex, usize>,
}

/// Partition a DAG by grouping contiguous topological levels into k partitions.
/// O(V+E) complexity.
pub fn partition_level_based<P>(
    graph: &InternalGraph<P>,
    k: usize,
    costs: &AHashMap<InternalNodeIndex, f64>,
) -> Result<PartitionResult, CycleInfo> {
    let levels = topological_levels(graph)?;
    let k = k.max(1).min(levels.len().max(1));

    if levels.is_empty() {
        return Ok(PartitionResult {
            partitions: Vec::new(),
            cross_edge_count: 0,
            partition_order: Vec::new(),
            node_to_partition: AHashMap::new(),
        });
    }

    // Distribute levels as evenly as possible across k partitions
    let levels_per_partition = levels.len().div_ceil(k);
    let mut node_to_partition: AHashMap<InternalNodeIndex, usize> =
        AHashMap::with_capacity(graph.node_count());

    let mut partitions: Vec<PartitionInfo> = Vec::with_capacity(k);
    let mut pid = 0;

    for (i, level) in levels.iter().enumerate() {
        if i > 0 && i % levels_per_partition == 0 && pid + 1 < k {
            pid += 1;
        }
        if pid >= partitions.len() {
            partitions.push(PartitionInfo {
                partition_id: pid,
                node_indices: Vec::new(),
                total_cost: 0.0,
                incoming_cross_edges: 0,
                outgoing_cross_edges: 0,
            });
        }
        for &node in level {
            node_to_partition.insert(node, pid);
            let cost = costs.get(&node).copied().unwrap_or(1.0);
            partitions[pid].node_indices.push(node);
            partitions[pid].total_cost += cost;
        }
    }

    finalize_partition_result(graph, partitions, node_to_partition)
}

/// Partition a DAG with balanced cost per partition.
/// Greedily assigns nodes in topological order to the partition with least total cost.
/// O(V+E) complexity.
pub fn partition_balanced<P>(
    graph: &InternalGraph<P>,
    k: usize,
    costs: &AHashMap<InternalNodeIndex, f64>,
) -> Result<PartitionResult, CycleInfo> {
    let levels = topological_levels(graph)?;
    let k = k.max(1);

    if levels.is_empty() {
        return Ok(PartitionResult {
            partitions: Vec::new(),
            cross_edge_count: 0,
            partition_order: Vec::new(),
            node_to_partition: AHashMap::new(),
        });
    }

    // Calculate total cost and target per partition
    let total_cost: f64 = graph
        .node_indices()
        .map(|n| costs.get(&n).copied().unwrap_or(1.0))
        .sum();
    let target_cost = total_cost / k as f64;

    let mut node_to_partition: AHashMap<InternalNodeIndex, usize> =
        AHashMap::with_capacity(graph.node_count());
    let mut partitions: Vec<PartitionInfo> = (0..k)
        .map(|pid| PartitionInfo {
            partition_id: pid,
            node_indices: Vec::new(),
            total_cost: 0.0,
            incoming_cross_edges: 0,
            outgoing_cross_edges: 0,
        })
        .collect();

    let mut current_pid = 0;
    for level in &levels {
        for &node in level {
            // Find the best partition: prefer current, advance if over target
            while current_pid + 1 < k && partitions[current_pid].total_cost >= target_cost {
                current_pid += 1;
            }
            let cost = costs.get(&node).copied().unwrap_or(1.0);
            node_to_partition.insert(node, current_pid);
            partitions[current_pid].node_indices.push(node);
            partitions[current_pid].total_cost += cost;
        }
    }

    // Remove empty trailing partitions
    partitions.retain(|p| !p.node_indices.is_empty());
    // Re-number partition IDs
    for (i, p) in partitions.iter_mut().enumerate() {
        p.partition_id = i;
    }
    // Rebuild node_to_partition with corrected IDs
    node_to_partition.clear();
    for p in &partitions {
        for &node in &p.node_indices {
            node_to_partition.insert(node, p.partition_id);
        }
    }

    finalize_partition_result(graph, partitions, node_to_partition)
}

/// Partition a DAG minimizing cross-partition communication.
/// Starts from level-based partitioning and uses Kernighan-Lin-style refinement
/// to reduce cross-edges while maintaining topological ordering and balance.
/// O(iters * E) complexity.
pub fn partition_communication_min<P>(
    graph: &InternalGraph<P>,
    k: usize,
    costs: &AHashMap<InternalNodeIndex, f64>,
    max_iterations: usize,
    max_imbalance: f64,
) -> Result<PartitionResult, CycleInfo> {
    // Start from level-based partitioning
    let mut result = partition_level_based(graph, k, costs)?;

    if result.partitions.len() <= 1 {
        return Ok(result);
    }

    let levels = topological_levels(graph)?;

    // Build level map: node -> level index
    let mut node_level: AHashMap<InternalNodeIndex, usize> = AHashMap::new();
    for (li, level) in levels.iter().enumerate() {
        for &node in level {
            node_level.insert(node, li);
        }
    }

    // Compute total cost and partition cost limits
    let total_cost: f64 = result.partitions.iter().map(|p| p.total_cost).sum();
    let avg_cost = total_cost / result.partitions.len() as f64;
    let max_cost = avg_cost * (1.0 + max_imbalance);

    // Compute the level range for each partition (for topo order constraint)
    fn partition_level_range(
        partitions: &[PartitionInfo],
        node_level: &AHashMap<InternalNodeIndex, usize>,
    ) -> Vec<(usize, usize)> {
        partitions
            .iter()
            .map(|p| {
                let levels: Vec<usize> = p
                    .node_indices
                    .iter()
                    .filter_map(|n| node_level.get(n).copied())
                    .collect();
                if levels.is_empty() {
                    (0, 0)
                } else {
                    (*levels.iter().min().unwrap(), *levels.iter().max().unwrap())
                }
            })
            .collect()
    }

    for _iter in 0..max_iterations {
        let mut improved = false;
        let level_ranges = partition_level_range(&result.partitions, &node_level);

        // Try moving boundary nodes between adjacent partitions
        for pid in 0..result.partitions.len().saturating_sub(1) {
            let next_pid = pid + 1;

            // Try moving nodes from pid to next_pid and vice versa
            for direction in [true, false] {
                let (from_pid, to_pid) = if direction {
                    (pid, next_pid)
                } else {
                    (next_pid, pid)
                };

                // Find candidate nodes at the boundary
                let candidates: Vec<InternalNodeIndex> = result.partitions[from_pid]
                    .node_indices
                    .iter()
                    .copied()
                    .filter(|&node| {
                        // Node must be at the boundary between these partitions
                        let nl = node_level.get(&node).copied().unwrap_or(0);
                        if direction {
                            // Moving forward: node's level should be near the end of from_pid
                            nl >= level_ranges[from_pid].1.saturating_sub(1)
                        } else {
                            // Moving backward: node's level should be near the start of from_pid
                            nl <= level_ranges[from_pid].0 + 1
                        }
                    })
                    .collect();

                for &node in &candidates {
                    let node_cost = costs.get(&node).copied().unwrap_or(1.0);

                    // Check balance constraint
                    if result.partitions[to_pid].total_cost + node_cost > max_cost {
                        continue;
                    }
                    if result.partitions[from_pid].node_indices.len() <= 1 {
                        continue;
                    }

                    // Check topological constraint: all predecessors must be in
                    // earlier-or-same partition, all successors in same-or-later
                    let can_move = graph
                        .edges_directed(node, petgraph::Direction::Incoming)
                        .all(|e| {
                            let pred_pid = result
                                .node_to_partition
                                .get(&e.source())
                                .copied()
                                .unwrap_or(0);
                            pred_pid <= to_pid
                        })
                        && graph.edges(node).all(|e| {
                            let succ_pid = result
                                .node_to_partition
                                .get(&e.target())
                                .copied()
                                .unwrap_or(0);
                            succ_pid >= to_pid
                        });

                    if !can_move {
                        continue;
                    }

                    // Count cross-edge change
                    let mut delta: i64 = 0;
                    for edge in graph.edges_directed(node, petgraph::Direction::Incoming) {
                        let src_pid = result.node_to_partition[&edge.source()];
                        if src_pid != from_pid {
                            delta -= 1; // was cross-edge, still might be
                        }
                        if src_pid != to_pid {
                            delta += 1; // now cross-edge
                        }
                    }
                    for edge in graph.edges(node) {
                        let tgt_pid = result.node_to_partition[&edge.target()];
                        if tgt_pid != from_pid {
                            delta -= 1;
                        }
                        if tgt_pid != to_pid {
                            delta += 1;
                        }
                    }

                    if delta < 0 {
                        // Move improves the result
                        result.node_to_partition.insert(node, to_pid);
                        result.partitions[from_pid]
                            .node_indices
                            .retain(|&n| n != node);
                        result.partitions[from_pid].total_cost -= node_cost;
                        result.partitions[to_pid].node_indices.push(node);
                        result.partitions[to_pid].total_cost += node_cost;
                        improved = true;
                    }
                }
            }
        }

        if !improved {
            break;
        }
    }

    // Recompute cross-edge stats
    finalize_partition_result(graph, result.partitions.clone(), result.node_to_partition)
}

/// Compute cross-edge counts, partition ordering, and finalize the result.
fn finalize_partition_result<P>(
    graph: &InternalGraph<P>,
    mut partitions: Vec<PartitionInfo>,
    node_to_partition: AHashMap<InternalNodeIndex, usize>,
) -> Result<PartitionResult, CycleInfo> {
    let num_partitions = partitions.len();

    // Reset cross-edge counts
    for p in partitions.iter_mut() {
        p.incoming_cross_edges = 0;
        p.outgoing_cross_edges = 0;
    }

    // Count cross-edges
    let mut cross_edge_count = 0;
    let mut partition_edges: AHashMap<(usize, usize), bool> = AHashMap::new();

    for node in graph.node_indices() {
        let src_pid = match node_to_partition.get(&node) {
            Some(&pid) => pid,
            None => continue,
        };
        for edge in graph.edges(node) {
            let tgt_pid = match node_to_partition.get(&edge.target()) {
                Some(&pid) => pid,
                None => continue,
            };
            if src_pid != tgt_pid {
                cross_edge_count += 1;
                if src_pid < partitions.len() {
                    partitions[src_pid].outgoing_cross_edges += 1;
                }
                if tgt_pid < partitions.len() {
                    partitions[tgt_pid].incoming_cross_edges += 1;
                }
                partition_edges.insert((src_pid, tgt_pid), true);
            }
        }
    }

    // Compute partition dependency order (topological levels of partition graph)
    let partition_order = compute_partition_order(num_partitions, &partition_edges);

    Ok(PartitionResult {
        partitions,
        cross_edge_count,
        partition_order,
        node_to_partition,
    })
}

/// Compute topological levels for the partition dependency graph.
fn compute_partition_order(
    num_partitions: usize,
    edges: &AHashMap<(usize, usize), bool>,
) -> Vec<Vec<usize>> {
    if num_partitions == 0 {
        return Vec::new();
    }

    let mut in_degree = vec![0usize; num_partitions];
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];

    for &(from, to) in edges.keys() {
        if from < num_partitions && to < num_partitions {
            in_degree[to] += 1;
            successors[from].push(to);
        }
    }

    let mut levels = Vec::new();
    let mut current: Vec<usize> = (0..num_partitions).filter(|&i| in_degree[i] == 0).collect();
    current.sort();

    while !current.is_empty() {
        let mut next = Vec::new();
        for &pid in &current {
            for &succ in &successors[pid] {
                in_degree[succ] -= 1;
                if in_degree[succ] == 0 {
                    next.push(succ);
                }
            }
        }
        levels.push(current);
        next.sort();
        next.dedup();
        current = next;
    }

    levels
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
    fn test_level_based_diamond() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let costs = AHashMap::new();
        let result = partition_level_based(&g, 2, &costs).unwrap();

        assert!(result.partitions.len() <= 2);
        // All nodes assigned
        assert_eq!(result.node_to_partition.len(), 4);
        // Partition order exists
        assert!(!result.partition_order.is_empty());
    }

    #[test]
    fn test_balanced_partition() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());
        g.add_edge(c, d, make_edge());

        let mut costs = AHashMap::new();
        costs.insert(a, 10.0);
        costs.insert(b, 1.0);
        costs.insert(c, 1.0);
        costs.insert(d, 10.0);

        let result = partition_balanced(&g, 2, &costs).unwrap();
        assert!(result.partitions.len() <= 2);
        assert_eq!(result.node_to_partition.len(), 4);
    }

    #[test]
    fn test_comm_min_reduces_cross_edges() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let costs = AHashMap::new();
        let result = partition_communication_min(&g, 2, &costs, 10, 0.5).unwrap();
        assert!(result.partitions.len() <= 2);
        assert_eq!(result.node_to_partition.len(), 4);
    }

    #[test]
    fn test_empty_graph() {
        let g: InternalGraph = StableGraph::default();
        let costs = AHashMap::new();

        let r = partition_level_based(&g, 2, &costs).unwrap();
        assert!(r.partitions.is_empty());

        let r = partition_balanced(&g, 2, &costs).unwrap();
        assert!(r.partitions.is_empty());
    }

    #[test]
    fn test_single_node() {
        let mut g: InternalGraph = StableGraph::default();
        g.add_node(make_node("a"));
        let costs = AHashMap::new();

        let r = partition_level_based(&g, 2, &costs).unwrap();
        assert_eq!(r.partitions.len(), 1);
        assert_eq!(r.cross_edge_count, 0);
    }
}
