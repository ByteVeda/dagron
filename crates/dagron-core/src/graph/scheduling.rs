use crate::algorithms;
use crate::errors::DagronError;
use crate::node::NodeId;

use super::DAG;

/// A scheduled node with timing info, using public NodeId.
#[derive(Debug, Clone)]
pub struct ScheduledNodeId {
    pub node: NodeId,
    pub start_time: f64,
    pub duration: f64,
}

/// A step in the execution plan, using public NodeId.
#[derive(Debug, Clone)]
pub struct ExecutionStepResult {
    pub step_index: usize,
    pub nodes: Vec<ScheduledNodeId>,
}

/// A complete execution plan result, using public NodeId.
#[derive(Debug, Clone)]
pub struct ExecutionPlanResult {
    pub steps: Vec<ExecutionStepResult>,
    pub total_nodes: usize,
    pub max_parallelism: usize,
    pub estimated_makespan: f64,
    pub critical_path: Option<Vec<NodeId>>,
}

impl<P> DAG<P> {
    /// Return nodes in topological order, preferring higher-priority nodes first.
    /// Priorities are passed as a map of node name → priority value.
    /// Missing nodes default to priority 0.0.
    pub fn topological_sort_priority(
        &self,
        priorities: &ahash::AHashMap<String, f64>,
    ) -> Result<Vec<NodeId>, DagronError> {
        let idx_priorities = self.names_to_indices(priorities)?;
        let indices = algorithms::topological_sort_priority(&self.graph, &idx_priorities)
            .map_err(|e| DagronError::Cycle(e.message))?;

        Ok(indices
            .iter()
            .map(|&idx| NodeId {
                index: idx.index() as u32,
                name: self.graph[idx].name.clone(),
            })
            .collect())
    }

    /// Return nodes grouped by topological level, sorted within each level by priority.
    /// Higher priority nodes appear first within their level.
    /// Missing nodes default to priority 0.0.
    pub fn topological_levels_priority(
        &self,
        priorities: &ahash::AHashMap<String, f64>,
    ) -> Result<Vec<Vec<NodeId>>, DagronError> {
        let idx_priorities = self.names_to_indices(priorities)?;
        let levels = algorithms::topological_levels_priority(&self.graph, &idx_priorities)
            .map_err(|e| DagronError::Cycle(e.message))?;

        Ok(levels
            .iter()
            .map(|level| {
                level
                    .iter()
                    .map(|&idx| NodeId {
                        index: idx.index() as u32,
                        name: self.graph[idx].name.clone(),
                    })
                    .collect()
            })
            .collect())
    }

    /// Compute an execution plan with unlimited parallelism.
    /// Costs are passed as a map of node name → duration.
    /// Missing nodes default to cost 1.0.
    pub fn execution_plan(
        &self,
        costs: &ahash::AHashMap<String, f64>,
    ) -> Result<ExecutionPlanResult, DagronError> {
        let idx_costs = self.names_to_indices(costs)?;
        let plan = algorithms::max_parallelism_schedule(&self.graph, &idx_costs)
            .map_err(|e| DagronError::Cycle(e.message))?;
        Ok(self.convert_plan(plan))
    }

    /// Compute a resource-constrained execution plan.
    /// Uses list scheduling with bottom-level priority and a simulated worker pool.
    /// Missing nodes default to cost 1.0.
    pub fn execution_plan_constrained(
        &self,
        max_workers: usize,
        costs: &ahash::AHashMap<String, f64>,
    ) -> Result<ExecutionPlanResult, DagronError> {
        let idx_costs = self.names_to_indices(costs)?;
        let constraints = algorithms::ScheduleConstraints { max_workers };
        let plan = algorithms::resource_constrained_schedule(&self.graph, &idx_costs, &constraints)
            .map_err(|e| DagronError::Cycle(e.message))?;
        Ok(self.convert_plan(plan))
    }

    /// Find the critical path through the DAG.
    /// Returns (path as NodeIds, total cost).
    /// Missing nodes default to cost 1.0.
    pub fn critical_path(
        &self,
        costs: &ahash::AHashMap<String, f64>,
    ) -> Result<(Vec<NodeId>, f64), DagronError> {
        let idx_costs = self.names_to_indices(costs)?;
        let (path, total) = algorithms::critical_path(&self.graph, &idx_costs)
            .map_err(|e| DagronError::Cycle(e.message))?;

        let node_ids = path
            .iter()
            .map(|&idx| NodeId {
                index: idx.index() as u32,
                name: self.graph[idx].name.clone(),
            })
            .collect();

        Ok((node_ids, total))
    }

    /// Compute bottom levels for all nodes, returning name→level map.
    /// Bottom level = longest path from the node to any leaf (inclusive of the node's own cost).
    /// Missing nodes default to cost 1.0.
    pub fn bottom_levels(
        &self,
        costs: &ahash::AHashMap<String, f64>,
    ) -> Result<ahash::AHashMap<String, f64>, DagronError> {
        let idx_costs = self.names_to_indices(costs)?;
        let bl = algorithms::compute_bottom_levels(&self.graph, &idx_costs);
        let mut result = ahash::AHashMap::with_capacity(bl.len());
        for (idx, level) in bl {
            result.insert(self.graph[idx].name.clone(), level);
        }
        Ok(result)
    }

    /// Convert a name→value map to an index→value map.
    /// Ignores names not found in the graph (they just won't be in the result).
    pub(crate) fn names_to_indices(
        &self,
        name_map: &ahash::AHashMap<String, f64>,
    ) -> Result<ahash::AHashMap<crate::types::InternalNodeIndex, f64>, DagronError> {
        let mut idx_map = ahash::AHashMap::with_capacity(name_map.len());
        for (name, &value) in name_map {
            if let Some(&idx) = self.name_to_index.get(name) {
                idx_map.insert(idx, value);
            }
            // Silently skip unknown names — they just use the default
        }
        Ok(idx_map)
    }

    /// Convert an internal ExecutionPlan to the public ExecutionPlanResult.
    fn convert_plan(&self, plan: algorithms::ExecutionPlan) -> ExecutionPlanResult {
        let critical_path = plan.critical_path.map(|cp| {
            cp.iter()
                .map(|&idx| NodeId {
                    index: idx.index() as u32,
                    name: self.graph[idx].name.clone(),
                })
                .collect()
        });

        let steps = plan
            .steps
            .into_iter()
            .map(|step| ExecutionStepResult {
                step_index: step.step_index,
                nodes: step
                    .nodes
                    .into_iter()
                    .map(|sn| ScheduledNodeId {
                        node: NodeId {
                            index: sn.node.index() as u32,
                            name: self.graph[sn.node].name.clone(),
                        },
                        start_time: sn.start_time,
                        duration: sn.duration,
                    })
                    .collect(),
            })
            .collect();

        ExecutionPlanResult {
            steps,
            total_nodes: plan.total_nodes,
            max_parallelism: plan.max_parallelism,
            estimated_makespan: plan.estimated_makespan,
            critical_path,
        }
    }
}
