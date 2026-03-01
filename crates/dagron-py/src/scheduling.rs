use pyo3::prelude::*;
use std::collections::HashMap;

use crate::dag::PyDAG;
use crate::errors;
use crate::node::PyNodeId;

#[pyclass(frozen, name = "ScheduledNode")]
pub struct PyScheduledNode {
    #[pyo3(get)]
    pub node: PyNodeId,
    #[pyo3(get)]
    pub start_time: f64,
    #[pyo3(get)]
    pub duration: f64,
}

#[pymethods]
impl PyScheduledNode {
    fn __repr__(&self) -> String {
        format!(
            "ScheduledNode(node={:?}, start_time={}, duration={})",
            self.node.name, self.start_time, self.duration
        )
    }
}

#[pyclass(frozen, name = "ExecutionStep")]
pub struct PyExecutionStep {
    #[pyo3(get)]
    pub step_index: usize,
    #[pyo3(get)]
    pub nodes: Py<pyo3::types::PyList>,
}

#[pymethods]
impl PyExecutionStep {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let list = self.nodes.bind(py);
        Ok(format!(
            "ExecutionStep(step_index={}, nodes_count={})",
            self.step_index,
            list.len()
        ))
    }
}

#[pyclass(frozen, name = "ExecutionPlan")]
pub struct PyExecutionPlan {
    #[pyo3(get)]
    pub steps: Py<pyo3::types::PyList>,
    #[pyo3(get)]
    pub total_nodes: usize,
    #[pyo3(get)]
    pub max_parallelism: usize,
    #[pyo3(get)]
    pub estimated_makespan: f64,
    #[pyo3(get)]
    pub critical_path: Option<Py<pyo3::types::PyList>>,
}

#[pymethods]
impl PyExecutionPlan {
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let steps_list = self.steps.bind(py);
        Ok(format!(
            "ExecutionPlan(steps={}, total_nodes={}, max_parallelism={}, estimated_makespan={})",
            steps_list.len(),
            self.total_nodes,
            self.max_parallelism,
            self.estimated_makespan
        ))
    }
}

fn to_ahash(map: Option<HashMap<String, f64>>) -> ahash::AHashMap<String, f64> {
    map.map(|m| m.into_iter().collect()).unwrap_or_default()
}

fn convert_plan(
    py: Python<'_>,
    plan: dagron_core::ExecutionPlanResult,
) -> PyResult<PyExecutionPlan> {
    let steps_list = pyo3::types::PyList::empty(py);
    for step in plan.steps {
        let nodes_list = pyo3::types::PyList::empty(py);
        for sn in step.nodes {
            let py_sn = PyScheduledNode {
                node: PyNodeId::from(sn.node),
                start_time: sn.start_time,
                duration: sn.duration,
            };
            nodes_list.append(Py::new(py, py_sn)?)?;
        }
        let py_step = PyExecutionStep {
            step_index: step.step_index,
            nodes: nodes_list.into(),
        };
        steps_list.append(Py::new(py, py_step)?)?;
    }

    let critical_path = match plan.critical_path {
        Some(cp) => {
            let cp_list = pyo3::types::PyList::empty(py);
            for node_id in cp {
                cp_list.append(Py::new(py, PyNodeId::from(node_id))?)?;
            }
            Some(cp_list.into())
        }
        None => None,
    };

    Ok(PyExecutionPlan {
        steps: steps_list.into(),
        total_nodes: plan.total_nodes,
        max_parallelism: plan.max_parallelism,
        estimated_makespan: plan.estimated_makespan,
        critical_path,
    })
}

#[pymethods]
impl PyDAG {
    /// Return nodes in topological order, preferring higher-priority nodes first.
    ///
    /// Args:
    ///     priorities: Optional dict mapping node names to priority values.
    ///         Higher priority = earlier in output. Missing nodes default to 0.0.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    #[pyo3(signature = (priorities=None))]
    pub fn topological_sort_priority(
        &self,
        py: Python<'_>,
        priorities: Option<HashMap<String, f64>>,
    ) -> PyResult<Vec<PyNodeId>> {
        let prio = to_ahash(priorities);
        let inner_ref = &self.inner;
        let nodes = py
            .allow_threads(|| inner_ref.topological_sort_priority(&prio))
            .map_err(errors::into_pyerr)?;
        Ok(nodes.into_iter().map(PyNodeId::from).collect())
    }

    /// Return nodes grouped by topological level, sorted within each level by priority.
    ///
    /// Args:
    ///     priorities: Optional dict mapping node names to priority values.
    ///         Higher priority = earlier within each level. Missing nodes default to 0.0.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    #[pyo3(signature = (priorities=None))]
    pub fn topological_levels_priority(
        &self,
        py: Python<'_>,
        priorities: Option<HashMap<String, f64>>,
    ) -> PyResult<Vec<Vec<PyNodeId>>> {
        let prio = to_ahash(priorities);
        let inner_ref = &self.inner;
        let levels = py
            .allow_threads(|| inner_ref.topological_levels_priority(&prio))
            .map_err(errors::into_pyerr)?;
        Ok(levels
            .into_iter()
            .map(|level| level.into_iter().map(PyNodeId::from).collect())
            .collect())
    }

    /// Compute an execution plan with unlimited parallelism.
    ///
    /// Args:
    ///     costs: Optional dict mapping node names to duration/cost values.
    ///         Missing nodes default to 1.0.
    ///
    /// Returns:
    ///     ExecutionPlan with steps, timing, and critical path information.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    #[pyo3(signature = (costs=None))]
    pub fn execution_plan(
        &self,
        py: Python<'_>,
        costs: Option<HashMap<String, f64>>,
    ) -> PyResult<PyExecutionPlan> {
        let cost_map = to_ahash(costs);
        let inner_ref = &self.inner;
        let plan = py
            .allow_threads(|| inner_ref.execution_plan(&cost_map))
            .map_err(errors::into_pyerr)?;
        convert_plan(py, plan)
    }

    /// Compute a resource-constrained execution plan.
    ///
    /// Args:
    ///     max_workers: Maximum number of concurrent workers.
    ///     costs: Optional dict mapping node names to duration/cost values.
    ///         Missing nodes default to 1.0.
    ///
    /// Returns:
    ///     ExecutionPlan with steps, timing, and critical path information.
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    #[pyo3(signature = (max_workers, costs=None))]
    pub fn execution_plan_constrained(
        &self,
        py: Python<'_>,
        max_workers: usize,
        costs: Option<HashMap<String, f64>>,
    ) -> PyResult<PyExecutionPlan> {
        let cost_map = to_ahash(costs);
        let inner_ref = &self.inner;
        let plan = py
            .allow_threads(|| inner_ref.execution_plan_constrained(max_workers, &cost_map))
            .map_err(errors::into_pyerr)?;
        convert_plan(py, plan)
    }

    /// Find the critical path through the DAG.
    ///
    /// Args:
    ///     costs: Optional dict mapping node names to duration/cost values.
    ///         Missing nodes default to 1.0.
    ///
    /// Returns:
    ///     Tuple of (list of NodeId on the critical path, total cost).
    ///
    /// Raises:
    ///     CycleError: If the graph contains cycles.
    #[pyo3(signature = (costs=None))]
    pub fn critical_path(
        &self,
        py: Python<'_>,
        costs: Option<HashMap<String, f64>>,
    ) -> PyResult<(Vec<PyNodeId>, f64)> {
        let cost_map = to_ahash(costs);
        let inner_ref = &self.inner;
        let (path, total) = py
            .allow_threads(|| inner_ref.critical_path(&cost_map))
            .map_err(errors::into_pyerr)?;
        let py_path = path.into_iter().map(PyNodeId::from).collect();
        Ok((py_path, total))
    }
}
