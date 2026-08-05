//! Helper for accepting either `str` or `NodeRef` as a node identifier
//! at the public API boundary.

use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::errors;
use crate::node::PyNodeRef;
use crate::payload::PyNodePayload;

/// A node identifier passed in from Python — either a plain string name
/// or a [`PyNodeRef`]. Use [`NodeArg::into_name`] to resolve to a `&str`,
/// validating any embedded NodeRef against the DAG.
pub enum NodeArg {
    Name(String),
    Ref(dagron_core::NodeRef),
}

impl<'py> FromPyObject<'py> for NodeArg {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(s) = ob.downcast::<PyString>() {
            return Ok(NodeArg::Name(s.to_string()));
        }
        if let Ok(r) = ob.extract::<PyRef<PyNodeRef>>() {
            return Ok(NodeArg::Ref(r.inner.clone()));
        }
        Err(pyo3::exceptions::PyTypeError::new_err(
            "expected str or NodeRef for node identifier",
        ))
    }
}

impl NodeArg {
    /// Resolve to an owned name. If the arg is a `NodeRef`, validates it
    /// against the DAG first (so a stale ref errors instead of silently
    /// resolving by name).
    pub fn into_name(self, dag: &dagron_core::DAG<PyNodePayload>) -> PyResult<String> {
        match self {
            NodeArg::Name(s) => Ok(s),
            NodeArg::Ref(r) => {
                dag.resolve_ref(&r).map_err(errors::into_pyerr)?;
                Ok(r.name.to_string())
            }
        }
    }

    /// Borrow the name without consuming. NodeRef variants are NOT validated
    /// here — use `into_name` if validation is required.
    pub fn name_str(&self) -> &str {
        match self {
            NodeArg::Name(s) => s.as_str(),
            NodeArg::Ref(r) => r.name(),
        }
    }
}
