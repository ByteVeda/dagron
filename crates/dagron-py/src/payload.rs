use pyo3::{Py, PyAny};

pub struct PyNodePayload {
    pub payload: Option<Py<PyAny>>,
    pub metadata: Option<Py<PyAny>>,
}

impl Default for PyNodePayload {
    fn default() -> Self {
        PyNodePayload {
            payload: None,
            metadata: None,
        }
    }
}
