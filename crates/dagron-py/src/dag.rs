use std::collections::HashMap;

use pyo3::prelude::*;

use crate::payload::PyNodePayload;

#[pyclass(name = "DAG")]
pub struct PyDAG {
    pub(crate) inner: dagron_core::DAG<PyNodePayload>,
}

impl Default for PyDAG {
    fn default() -> Self {
        Self::new()
    }
}

#[pymethods]
impl PyDAG {
    #[new]
    pub fn new() -> Self {
        PyDAG {
            inner: dagron_core::DAG::new(),
        }
    }

    /// The current generation counter. Increments on every structural mutation.
    #[getter]
    pub fn generation(&self) -> u64 {
        self.inner.generation()
    }

    /// Return cache statistics: {"hits": N, "misses": N, "size": N}.
    pub fn cache_info(&self) -> HashMap<&str, u64> {
        let mut info = HashMap::new();
        info.insert("hits", self.inner.cache_hits());
        info.insert("misses", self.inner.cache_misses());
        info.insert("size", self.inner.cache_size() as u64);
        info
    }

    /// Clear all cached computation results.
    pub fn clear_cache(&self) {
        self.inner.clear_cache();
    }
}
