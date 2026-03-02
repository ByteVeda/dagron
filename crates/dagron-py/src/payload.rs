use pyo3::{Py, PyAny, Python};

#[derive(Default)]
pub struct PyNodePayload {
    pub payload: Option<Py<PyAny>>,
    pub metadata: Option<Py<PyAny>>,
}

impl Clone for PyNodePayload {
    fn clone(&self) -> Self {
        Python::with_gil(|py| PyNodePayload {
            payload: self.payload.as_ref().map(|p| p.clone_ref(py)),
            metadata: self.metadata.as_ref().map(|m| m.clone_ref(py)),
        })
    }
}
