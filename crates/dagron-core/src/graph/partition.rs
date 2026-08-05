//! Graph-level partitioning methods.

use crate::algorithms::partition::{self, PartitionResult as AlgoPartitionResult};
use crate::errors::DagronError;

use super::DAG;

/// Public partition info with node names instead of indices.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub partition_id: usize,
    pub node_names: Vec<String>,
    pub total_cost: f64,
    pub incoming_cross_edges: usize,
    pub outgoing_cross_edges: usize,
}

/// Public partition result with node names.
#[derive(Debug, Clone)]
pub struct PartitionResult {
    pub partitions: Vec<PartitionInfo>,
    pub cross_edge_count: usize,
    pub partition_order: Vec<Vec<usize>>,
}

impl<P: Clone> DAG<P> {
    /// Partition the DAG using level-based grouping.
    pub fn partition_level_based(
        &self,
        k: usize,
        costs: &ahash::AHashMap<String, f64>,
    ) -> Result<PartitionResult, DagronError> {
        let idx_costs = self.names_to_indices(costs)?;
        let result = partition::partition_level_based(&self.graph, k, &idx_costs)
            .map_err(|e| DagronError::Cycle(e.message))?;
        Ok(self.convert_partition_result(result))
    }

    /// Partition the DAG with balanced cost per partition.
    pub fn partition_balanced(
        &self,
        k: usize,
        costs: &ahash::AHashMap<String, f64>,
    ) -> Result<PartitionResult, DagronError> {
        let idx_costs = self.names_to_indices(costs)?;
        let result = partition::partition_balanced(&self.graph, k, &idx_costs)
            .map_err(|e| DagronError::Cycle(e.message))?;
        Ok(self.convert_partition_result(result))
    }

    /// Partition the DAG minimizing cross-partition communication.
    pub fn partition_communication_min(
        &self,
        k: usize,
        costs: &ahash::AHashMap<String, f64>,
        max_iterations: usize,
        max_imbalance: f64,
    ) -> Result<PartitionResult, DagronError> {
        let idx_costs = self.names_to_indices(costs)?;
        let result = partition::partition_communication_min(
            &self.graph,
            k,
            &idx_costs,
            max_iterations,
            max_imbalance,
        )
        .map_err(|e| DagronError::Cycle(e.message))?;
        Ok(self.convert_partition_result(result))
    }

    /// Extract a sub-DAG for a given partition.
    pub fn extract_partition(&self, info: &PartitionInfo) -> Result<DAG<P>, DagronError> {
        let names: Vec<&str> = info.node_names.iter().map(|s| s.as_str()).collect();
        self.subgraph(&names)
    }

    fn convert_partition_result(&self, result: AlgoPartitionResult) -> PartitionResult {
        let partitions = result
            .partitions
            .into_iter()
            .map(|p| PartitionInfo {
                partition_id: p.partition_id,
                node_names: p
                    .node_indices
                    .iter()
                    .map(|&idx| self.graph[idx].name.clone())
                    .collect(),
                total_cost: p.total_cost,
                incoming_cross_edges: p.incoming_cross_edges,
                outgoing_cross_edges: p.outgoing_cross_edges,
            })
            .collect();

        PartitionResult {
            partitions,
            cross_edge_count: result.cross_edge_count,
            partition_order: result.partition_order,
        }
    }
}
