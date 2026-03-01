use pyo3::{Py, PyAny};

#[derive(Default)]
pub struct PyNodePayload {
    pub payload: Option<Py<PyAny>>,
    pub metadata: Option<Py<PyAny>>,
}
