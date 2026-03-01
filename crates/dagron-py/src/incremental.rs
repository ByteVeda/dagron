use std::collections::HashMap;

use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;

#[pymethods]
impl PyDAG {
    /// Compute the dirty set: changed nodes plus their transitive descendants.
    ///
    /// Args:
    ///     changed: List of node names that have changed.
    ///
    /// Returns:
    ///     List of node names that need recomputation.
    pub fn dirty_set(&self, py: Python<'_>, changed: Vec<String>) -> PyResult<Vec<String>> {
        let refs: Vec<&str> = changed.iter().map(|s| s.as_str()).collect();
        py.allow_threads(|| self.inner.dirty_set(&refs).map_err(errors::into_pyerr))
    }

    /// For each dirty node, determine which changed nodes are its ancestors.
    ///
    /// Args:
    ///     changed: List of node names that have changed.
    ///
    /// Returns:
    ///     Dict mapping dirty node name to list of changed ancestor names.
    pub fn change_provenance(
        &self,
        py: Python<'_>,
        changed: Vec<String>,
    ) -> PyResult<HashMap<String, Vec<String>>> {
        let refs: Vec<&str> = changed.iter().map(|s| s.as_str()).collect();
        py.allow_threads(|| {
            self.inner
                .change_provenance(&refs)
                .map_err(errors::into_pyerr)
        })
    }
}
