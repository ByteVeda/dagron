use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;

#[pymethods]
impl PyDAG {
    /// Validate the graph has no cycles.
    ///
    /// Returns:
    ///     True if the graph is a valid DAG.
    ///
    /// Raises:
    ///     CycleError: If cycles are detected, with details about cycle members.
    pub fn validate(&self, py: Python<'_>) -> PyResult<bool> {
        let inner_ref = &self.inner;
        py.allow_threads(|| inner_ref.validate())
            .map_err(errors::into_pyerr)
    }
}
