use pyo3::prelude::*;

use crate::payload::PyNodePayload;

#[pyclass(name = "DAG")]
pub struct PyDAG {
    pub(crate) inner: dagron_core::DAG<PyNodePayload>,
}

#[pymethods]
impl PyDAG {
    #[new]
    pub fn new() -> Self {
        PyDAG {
            inner: dagron_core::DAG::new(),
        }
    }
}
