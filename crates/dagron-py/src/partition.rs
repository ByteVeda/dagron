use pyo3::prelude::*;
use std::collections::HashMap;

use crate::dag::PyDAG;
use crate::errors;

#[pyclass(frozen, name = "PartitionInfo")]
pub struct PyPartitionInfo {
    #[pyo3(get)]
    pub partition_id: usize,
    #[pyo3(get)]
    pub node_names: Vec<String>,
    #[pyo3(get)]
    pub total_cost: f64,
    #[pyo3(get)]
    pub incoming_cross_edges: usize,
    #[pyo3(get)]
    pub outgoing_cross_edges: usize,
}

#[pymethods]
impl PyPartitionInfo {
    fn __repr__(&self) -> String {
        format!(
            "PartitionInfo(id={}, nodes={}, cost={:.2})",
            self.partition_id,
            self.node_names.len(),
            self.total_cost
        )
    }
}

#[pyclass(frozen, name = "PartitionResult")]
pub struct PyPartitionResult {
    #[pyo3(get)]
    pub partitions: Py<pyo3::types::PyList>,
    #[pyo3(get)]
    pub cross_edge_count: usize,
    #[pyo3(get)]
    pub partition_order: Vec<Vec<usize>>,
}

#[pymethods]
impl PyPartitionResult {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let list = self.partitions.bind(py);
        Ok(format!(
            "PartitionResult(partitions={}, cross_edges={})",
            list.len(),
            self.cross_edge_count
        ))
    }
}

fn to_ahash(map: Option<HashMap<String, f64>>) -> ahash::AHashMap<String, f64> {
    map.map(|m| m.into_iter().collect()).unwrap_or_default()
}

fn convert_partition_result(
    py: Python<'_>,
    result: dagron_core::GraphPartitionResult,
) -> PyResult<PyPartitionResult> {
    let partitions_list = pyo3::types::PyList::empty(py);
    for p in result.partitions {
        let info = PyPartitionInfo {
            partition_id: p.partition_id,
            node_names: p.node_names,
            total_cost: p.total_cost,
            incoming_cross_edges: p.incoming_cross_edges,
            outgoing_cross_edges: p.outgoing_cross_edges,
        };
        partitions_list.append(Py::new(py, info)?)?;
    }

    Ok(PyPartitionResult {
        partitions: partitions_list.into(),
        cross_edge_count: result.cross_edge_count,
        partition_order: result.partition_order,
    })
}

#[pymethods]
impl PyDAG {
    /// Partition the DAG using level-based grouping.
    ///
    /// Groups contiguous topological levels into k partitions.
    ///
    /// Args:
    ///     k: Number of target partitions.
    ///     costs: Optional dict mapping node names to cost values.
    ///
    /// Returns:
    ///     PartitionResult with partition info and cross-edge statistics.
    #[pyo3(signature = (k, costs=None))]
    pub fn partition_level_based(
        &self,
        py: Python<'_>,
        k: usize,
        costs: Option<HashMap<String, f64>>,
    ) -> PyResult<PyPartitionResult> {
        let cost_map = to_ahash(costs);
        let inner_ref = &self.inner;
        let result = py
            .allow_threads(|| inner_ref.partition_level_based(k, &cost_map))
            .map_err(errors::into_pyerr)?;
        convert_partition_result(py, result)
    }

    /// Partition the DAG with balanced cost per partition.
    ///
    /// Greedily assigns nodes in topological order to balance total cost.
    ///
    /// Args:
    ///     k: Number of target partitions.
    ///     costs: Optional dict mapping node names to cost values.
    ///
    /// Returns:
    ///     PartitionResult with partition info and cross-edge statistics.
    #[pyo3(signature = (k, costs=None))]
    pub fn partition_balanced(
        &self,
        py: Python<'_>,
        k: usize,
        costs: Option<HashMap<String, f64>>,
    ) -> PyResult<PyPartitionResult> {
        let cost_map = to_ahash(costs);
        let inner_ref = &self.inner;
        let result = py
            .allow_threads(|| inner_ref.partition_balanced(k, &cost_map))
            .map_err(errors::into_pyerr)?;
        convert_partition_result(py, result)
    }

    /// Partition the DAG minimizing cross-partition communication.
    ///
    /// Starts from level-based partitioning and iteratively refines to reduce
    /// cross-edges while maintaining topological ordering and balance.
    ///
    /// Args:
    ///     k: Number of target partitions.
    ///     costs: Optional dict mapping node names to cost values.
    ///     max_iterations: Maximum refinement iterations. Default 10.
    ///     max_imbalance: Maximum allowed cost imbalance ratio. Default 0.3.
    ///
    /// Returns:
    ///     PartitionResult with partition info and cross-edge statistics.
    #[pyo3(signature = (k, costs=None, max_iterations=10, max_imbalance=0.3))]
    pub fn partition_communication_min(
        &self,
        py: Python<'_>,
        k: usize,
        costs: Option<HashMap<String, f64>>,
        max_iterations: usize,
        max_imbalance: f64,
    ) -> PyResult<PyPartitionResult> {
        let cost_map = to_ahash(costs);
        let inner_ref = &self.inner;
        let result = py
            .allow_threads(|| {
                inner_ref.partition_communication_min(k, &cost_map, max_iterations, max_imbalance)
            })
            .map_err(errors::into_pyerr)?;
        convert_partition_result(py, result)
    }
}
