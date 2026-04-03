use std::sync::Arc;

use dagron_ui::server::GateCallback;
use dagron_ui::DashboardHandle;
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// PyGateCallback — wraps three Python callables
// ---------------------------------------------------------------------------

struct PyGateCallback {
    approve_fn: PyObject,
    reject_fn: PyObject,
    has_gate_fn: PyObject,
}

impl GateCallback for PyGateCallback {
    fn approve(&self, name: &str) -> Result<(), String> {
        Python::with_gil(|py| {
            self.approve_fn
                .call1(py, (name,))
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }

    fn reject(&self, name: &str, reason: &str) -> Result<(), String> {
        Python::with_gil(|py| {
            self.reject_fn
                .call1(py, (name, reason))
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }

    fn has_gate(&self, name: &str) -> bool {
        Python::with_gil(|py| {
            self.has_gate_fn
                .call1(py, (name,))
                .and_then(|r| r.extract::<bool>(py))
                .unwrap_or(false)
        })
    }
}

// ---------------------------------------------------------------------------
// RustDashboardServer — PyO3 wrapper around DashboardHandle
// ---------------------------------------------------------------------------

#[pyclass(name = "RustDashboardServer")]
pub struct PyDashboardHandle {
    handle: Option<DashboardHandle>,
}

#[pymethods]
impl PyDashboardHandle {
    #[new]
    fn new(host: &str, port: u16) -> PyResult<Self> {
        let handle = DashboardHandle::start(host, port)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self {
            handle: Some(handle),
        })
    }

    /// The actual port the server is listening on.
    #[getter]
    fn port(&self) -> PyResult<u16> {
        self.handle
            .as_ref()
            .map(|h| h.port())
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("server already stopped"))
    }

    fn stop(&mut self) {
        if let Some(mut h) = self.handle.take() {
            h.stop();
        }
    }

    fn reset(
        &self,
        dag_dot: &str,
        node_names: Vec<String>,
        edges: Vec<(String, String)>,
    ) -> PyResult<()> {
        self.with_handle(|h| h.reset(dag_dot.to_string(), node_names, edges))
    }

    fn node_started(&self, name: &str) -> PyResult<()> {
        self.with_handle(|h| h.node_started(name))
    }

    #[pyo3(signature = (name, status, error=None))]
    fn node_finished(&self, name: &str, status: &str, error: Option<&str>) -> PyResult<()> {
        self.with_handle(|h| h.node_finished(name, status, error))
    }

    #[pyo3(signature = (total_duration, succeeded, failed, skipped=0, timed_out=0, cancelled=0))]
    fn execution_finished(
        &self,
        total_duration: f64,
        succeeded: u32,
        failed: u32,
        skipped: u32,
        timed_out: u32,
        cancelled: u32,
    ) -> PyResult<()> {
        self.with_handle(|h| {
            h.execution_finished(
                total_duration,
                succeeded,
                failed,
                skipped,
                timed_out,
                cancelled,
            )
        })
    }

    fn set_gate_callback(
        &self,
        approve_fn: PyObject,
        reject_fn: PyObject,
        has_gate_fn: PyObject,
    ) -> PyResult<()> {
        let cb = Arc::new(PyGateCallback {
            approve_fn,
            reject_fn,
            has_gate_fn,
        });
        self.with_handle(|h| h.set_gate_callback(cb))
    }

    fn set_waiting_gates(&self, gates: Vec<String>) -> PyResult<()> {
        self.with_handle(|h| h.set_waiting_gates(gates))
    }
}

impl PyDashboardHandle {
    fn with_handle<F, R>(&self, f: F) -> PyResult<R>
    where
        F: FnOnce(&DashboardHandle) -> R,
    {
        self.handle
            .as_ref()
            .map(f)
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("server already stopped"))
    }
}

impl Drop for PyDashboardHandle {
    fn drop(&mut self) {
        self.stop();
    }
}
