use crate::types::{InternalGraph, InternalNodeIndex};
use petgraph::visit::{EdgeRef, IntoNodeIdentifiers};
use std::collections::BinaryHeap;

use super::priority_toposort::OrdF64;
use super::toposort::{topological_levels, CycleInfo};

/// A single node scheduled within an execution step.
#[derive(Debug, Clone)]
pub struct ScheduledNode {
    pub node: InternalNodeIndex,
    pub start_time: f64,
    pub duration: f64,
}

/// A group of nodes that can execute in the same step (same topological level).
#[derive(Debug, Clone)]
pub struct ExecutionStep {
    pub step_index: usize,
    pub nodes: Vec<ScheduledNode>,
}

/// A complete execution plan for a DAG.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub steps: Vec<ExecutionStep>,
    pub total_nodes: usize,
    pub max_parallelism: usize,
    pub estimated_makespan: f64,
    pub critical_path: Option<Vec<InternalNodeIndex>>,
}

/// Constraints for resource-constrained scheduling.
#[derive(Debug, Clone)]
pub struct ScheduleConstraints {
    pub max_workers: usize,
}

/// Compute bottom levels for all nodes.
/// Bottom level = longest path from the node to any leaf (inclusive of the node's own cost).
/// Missing nodes default to cost 1.0.
pub fn compute_bottom_levels<P>(
    graph: &InternalGraph<P>,
    costs: &ahash::AHashMap<InternalNodeIndex, f64>,
) -> ahash::AHashMap<InternalNodeIndex, f64> {
    let mut bottom_levels: ahash::AHashMap<InternalNodeIndex, f64> =
        ahash::AHashMap::with_capacity(graph.node_count());

    // We need reverse topological order: process leaves first
    // Use topological_levels and iterate in reverse
    let levels = match topological_levels(graph) {
        Ok(l) => l,
        Err(_) => return bottom_levels, // cycle — return empty
    };

    for level in levels.iter().rev() {
        for &node in level {
            let node_cost = costs.get(&node).copied().unwrap_or(1.0);
            let max_successor_bl = graph
                .edges(node)
                .map(|e| bottom_levels.get(&e.target()).copied().unwrap_or(0.0))
                .fold(0.0_f64, f64::max);
            bottom_levels.insert(node, node_cost + max_successor_bl);
        }
    }

    bottom_levels
}

/// Find the critical path: the longest path through the DAG.
/// Returns (path as node indices, total cost).
/// Missing nodes default to cost 1.0.
pub fn critical_path<P>(
    graph: &InternalGraph<P>,
    costs: &ahash::AHashMap<InternalNodeIndex, f64>,
) -> Result<(Vec<InternalNodeIndex>, f64), CycleInfo> {
    let bottom_levels = compute_bottom_levels(graph, costs);

    if bottom_levels.is_empty() && graph.node_count() > 0 {
        return Err(CycleInfo {
            message: "Graph contains a cycle".to_string(),
        });
    }

    if graph.node_count() == 0 {
        return Ok((Vec::new(), 0.0));
    }

    // Find the node with the highest bottom level (a root on the critical path)
    let start = bottom_levels
        .iter()
        .max_by(|(_, a), (_, b)| a.total_cmp(b))
        .map(|(&node, _)| node)
        .unwrap();

    let total_cost = bottom_levels[&start];

    // Trace forward: always pick the successor with the highest bottom level
    let mut path = vec![start];
    let mut current = start;
    loop {
        let successors: Vec<InternalNodeIndex> = graph.edges(current).map(|e| e.target()).collect();
        if successors.is_empty() {
            break;
        }
        let next = successors
            .into_iter()
            .max_by(|a, b| {
                let bl_a = bottom_levels.get(a).copied().unwrap_or(0.0);
                let bl_b = bottom_levels.get(b).copied().unwrap_or(0.0);
                bl_a.total_cmp(&bl_b)
                    // On equal BL, prefer alphabetically earlier name (ascending)
                    .then_with(|| graph[*b].name.cmp(&graph[*a].name))
            })
            .unwrap();
        path.push(next);
        current = next;
    }

    Ok((path, total_cost))
}

/// Create an execution plan with unlimited parallelism.
/// Each topological level becomes a step; all nodes in a level execute simultaneously.
/// Missing nodes default to cost 1.0.
pub fn max_parallelism_schedule<P>(
    graph: &InternalGraph<P>,
    costs: &ahash::AHashMap<InternalNodeIndex, f64>,
) -> Result<ExecutionPlan, CycleInfo> {
    let levels = topological_levels(graph)?;
    let cp = critical_path(graph, costs)?;

    let mut steps = Vec::with_capacity(levels.len());
    let mut current_time = 0.0;
    let mut max_par = 0;
    let mut total_nodes = 0;

    for (step_index, level) in levels.iter().enumerate() {
        let mut nodes = Vec::with_capacity(level.len());
        let mut max_duration = 0.0_f64;

        for &node in level {
            let duration = costs.get(&node).copied().unwrap_or(1.0);
            nodes.push(ScheduledNode {
                node,
                start_time: current_time,
                duration,
            });
            max_duration = max_duration.max(duration);
        }

        // Sort nodes within step by name for determinism
        nodes.sort_by(|a, b| graph[a.node].name.cmp(&graph[b.node].name));

        total_nodes += nodes.len();
        if nodes.len() > max_par {
            max_par = nodes.len();
        }

        steps.push(ExecutionStep { step_index, nodes });
        current_time += max_duration;
    }

    Ok(ExecutionPlan {
        steps,
        total_nodes,
        max_parallelism: max_par,
        estimated_makespan: cp.1,
        critical_path: if cp.0.is_empty() { None } else { Some(cp.0) },
    })
}

/// Create a resource-constrained execution plan using list scheduling.
/// Uses bottom-level priority (highest BL first) with a simulated worker pool.
/// Missing nodes default to cost 1.0.
pub fn resource_constrained_schedule<P>(
    graph: &InternalGraph<P>,
    costs: &ahash::AHashMap<InternalNodeIndex, f64>,
    constraints: &ScheduleConstraints,
) -> Result<ExecutionPlan, CycleInfo> {
    let bottom_levels = compute_bottom_levels(graph, costs);

    if bottom_levels.is_empty() && graph.node_count() > 0 {
        return Err(CycleInfo {
            message: "Graph contains a cycle".to_string(),
        });
    }

    if graph.node_count() == 0 {
        return Ok(ExecutionPlan {
            steps: Vec::new(),
            total_nodes: 0,
            max_parallelism: 0,
            estimated_makespan: 0.0,
            critical_path: None,
        });
    }

    let max_workers = constraints.max_workers.max(1);

    // Compute in-degrees
    let mut in_degree: ahash::AHashMap<InternalNodeIndex, usize> =
        ahash::AHashMap::with_capacity(graph.node_count());
    for node in graph.node_identifiers() {
        in_degree.entry(node).or_insert(0);
        for edge in graph.edges(node) {
            *in_degree.entry(edge.target()).or_insert(0) += 1;
        }
    }

    // Ready queue: max-heap by (bottom_level desc, name asc)
    let mut ready: BinaryHeap<(OrdF64, std::cmp::Reverse<String>, InternalNodeIndex)> =
        BinaryHeap::new();
    for (&node, &deg) in &in_degree {
        if deg == 0 {
            let bl = bottom_levels.get(&node).copied().unwrap_or(0.0);
            ready.push((
                OrdF64(bl),
                std::cmp::Reverse(graph[node].name.clone()),
                node,
            ));
        }
    }

    // Worker finish times: min-heap of (finish_time, node)
    let mut workers: BinaryHeap<std::cmp::Reverse<(OrdF64, InternalNodeIndex)>> = BinaryHeap::new();

    // Track: node -> (start_time, duration)
    let mut schedule: ahash::AHashMap<InternalNodeIndex, (f64, f64)> =
        ahash::AHashMap::with_capacity(graph.node_count());

    // Earliest start time based on predecessors
    let mut earliest_start: ahash::AHashMap<InternalNodeIndex, f64> =
        ahash::AHashMap::with_capacity(graph.node_count());
    for node in graph.node_identifiers() {
        earliest_start.insert(node, 0.0);
    }

    let mut current_time: f64 = 0.0;

    while schedule.len() < graph.node_count() {
        // Dispatch ready tasks to available workers
        while !ready.is_empty() && workers.len() < max_workers {
            let (_, _, node) = ready.pop().unwrap();
            let es = earliest_start.get(&node).copied().unwrap_or(0.0);
            let start = current_time.max(es);
            let duration = costs.get(&node).copied().unwrap_or(1.0);
            schedule.insert(node, (start, duration));
            workers.push(std::cmp::Reverse((OrdF64(start + duration), node)));
        }

        // Advance time to next worker completion
        if let Some(std::cmp::Reverse((OrdF64(finish_time), finished_node))) = workers.pop() {
            current_time = finish_time;

            // Release successors
            for edge in graph.edges(finished_node) {
                let successor = edge.target();
                if let Some(deg) = in_degree.get_mut(&successor) {
                    *deg -= 1;
                    // Update earliest start
                    let es = earliest_start.get_mut(&successor).unwrap();
                    if finish_time > *es {
                        *es = finish_time;
                    }
                    if *deg == 0 {
                        let bl = bottom_levels.get(&successor).copied().unwrap_or(0.0);
                        ready.push((
                            OrdF64(bl),
                            std::cmp::Reverse(graph[successor].name.clone()),
                            successor,
                        ));
                    }
                }
            }
        } else if !ready.is_empty() {
            // Workers full, but there might be ready tasks at current_time
            // This shouldn't happen since we dispatch above, but just in case
            continue;
        } else {
            break;
        }
    }

    // Group scheduled nodes into steps by start_time.
    // Note: `f64::to_bits` is used for exact grouping, which means `-0.0` and `+0.0`
    // would be placed in separate groups (different bit patterns). This is acceptable
    // here because all start times are non-negative values produced by accumulation.
    let mut time_groups: ahash::AHashMap<u64, Vec<InternalNodeIndex>> = ahash::AHashMap::new();
    for (&node, &(start, _)) in &schedule {
        time_groups.entry(start.to_bits()).or_default().push(node);
    }

    let mut step_times: Vec<f64> = time_groups
        .keys()
        .map(|&bits| f64::from_bits(bits))
        .collect();
    step_times.sort_by(|a, b| a.total_cmp(b));

    let mut steps = Vec::with_capacity(step_times.len());
    let mut max_par = 0;
    let mut total_nodes = 0;

    for (step_index, &time) in step_times.iter().enumerate() {
        let mut group = time_groups.remove(&time.to_bits()).unwrap();
        group.sort_by(|a, b| graph[*a].name.cmp(&graph[*b].name));

        let nodes: Vec<ScheduledNode> = group
            .iter()
            .map(|&node| {
                let (start, duration) = schedule[&node];
                ScheduledNode {
                    node,
                    start_time: start,
                    duration,
                }
            })
            .collect();

        if nodes.len() > max_par {
            max_par = nodes.len();
        }
        total_nodes += nodes.len();
        steps.push(ExecutionStep { step_index, nodes });
    }

    let cp = critical_path(graph, costs)?;

    Ok(ExecutionPlan {
        steps,
        total_nodes,
        max_parallelism: max_par,
        estimated_makespan: cp.1,
        critical_path: if cp.0.is_empty() { None } else { Some(cp.0) },
    })
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
    fn test_bottom_levels_linear() {
        // a -> b -> c, all cost 1.0
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, b, make_edge());
        g.add_edge(b, c, make_edge());

        let costs = ahash::AHashMap::new();
        let bl = compute_bottom_levels(&g, &costs);
        assert_eq!(bl[&a], 3.0); // a + b + c
        assert_eq!(bl[&b], 2.0); // b + c
        assert_eq!(bl[&c], 1.0); // c
    }

    #[test]
    fn test_bottom_levels_diamond_custom_costs() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let mut costs = ahash::AHashMap::new();
        costs.insert(a, 2.0);
        costs.insert(b, 3.0);
        costs.insert(c, 1.0);
        costs.insert(d, 4.0);

        let bl = compute_bottom_levels(&g, &costs);
        assert_eq!(bl[&d], 4.0);
        assert_eq!(bl[&b], 7.0); // 3 + 4
        assert_eq!(bl[&c], 5.0); // 1 + 4
        assert_eq!(bl[&a], 9.0); // 2 + max(7, 5) = 2 + 7
    }

    #[test]
    fn test_critical_path_diamond() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let mut costs = ahash::AHashMap::new();
        costs.insert(b, 5.0);
        costs.insert(c, 1.0);

        let (path, total) = critical_path(&g, &costs).unwrap();
        let names: Vec<&str> = path.iter().map(|&idx| g[idx].name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "d"]);
        assert_eq!(total, 7.0); // 1 + 5 + 1
    }

    #[test]
    fn test_critical_path_empty() {
        let g: InternalGraph = StableGraph::default();
        let costs = ahash::AHashMap::new();
        let (path, total) = critical_path(&g, &costs).unwrap();
        assert!(path.is_empty());
        assert_eq!(total, 0.0);
    }

    #[test]
    fn test_max_parallelism_schedule_diamond() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let costs = ahash::AHashMap::new();
        let plan = max_parallelism_schedule(&g, &costs).unwrap();
        assert_eq!(plan.steps.len(), 3);
        assert_eq!(plan.total_nodes, 4);
        assert_eq!(plan.max_parallelism, 2);
        // Step 0: [a], Step 1: [b, c], Step 2: [d]
        assert_eq!(plan.steps[0].nodes.len(), 1);
        assert_eq!(plan.steps[1].nodes.len(), 2);
        assert_eq!(plan.steps[2].nodes.len(), 1);
    }

    #[test]
    fn test_resource_constrained_1_worker() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let costs = ahash::AHashMap::new();
        let constraints = ScheduleConstraints { max_workers: 1 };
        let plan = resource_constrained_schedule(&g, &costs, &constraints).unwrap();
        assert_eq!(plan.total_nodes, 4);
        // With 1 worker, max parallelism is 1
        assert_eq!(plan.max_parallelism, 1);
    }

    #[test]
    fn test_resource_constrained_unlimited() {
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        let d = g.add_node(make_node("d"));
        g.add_edge(a, b, make_edge());
        g.add_edge(a, c, make_edge());
        g.add_edge(b, d, make_edge());
        g.add_edge(c, d, make_edge());

        let costs = ahash::AHashMap::new();
        let constraints = ScheduleConstraints { max_workers: 100 };
        let plan = resource_constrained_schedule(&g, &costs, &constraints).unwrap();
        assert_eq!(plan.total_nodes, 4);
        assert_eq!(plan.max_parallelism, 2);
    }

    #[test]
    fn test_resource_constrained_empty() {
        let g: InternalGraph = StableGraph::default();
        let costs = ahash::AHashMap::new();
        let constraints = ScheduleConstraints { max_workers: 4 };
        let plan = resource_constrained_schedule(&g, &costs, &constraints).unwrap();
        assert_eq!(plan.total_nodes, 0);
        assert!(plan.steps.is_empty());
    }

    #[test]
    fn test_max_parallelism_schedule_timing() {
        // a(cost=2) -> c(cost=1)
        // b(cost=1) -> c(cost=1)
        let mut g: InternalGraph = StableGraph::default();
        let a = g.add_node(make_node("a"));
        let b = g.add_node(make_node("b"));
        let c = g.add_node(make_node("c"));
        g.add_edge(a, c, make_edge());
        g.add_edge(b, c, make_edge());

        let mut costs = ahash::AHashMap::new();
        costs.insert(a, 2.0);
        costs.insert(b, 1.0);
        costs.insert(c, 1.0);

        let plan = max_parallelism_schedule(&g, &costs).unwrap();
        // Step 0: a (start=0, dur=2), b (start=0, dur=1)
        assert_eq!(plan.steps[0].nodes.len(), 2);
        assert_eq!(plan.steps[0].nodes[0].start_time, 0.0);
        // Step 1: c (start=2, dur=1) — must wait for max duration of step 0
        assert_eq!(plan.steps[1].nodes[0].start_time, 2.0);
        assert_eq!(plan.estimated_makespan, 3.0); // 2 + 1
    }
}
