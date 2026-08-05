use pyo3::prelude::*;

/// A snapshot identifier for a node, returned by enumeration methods
/// (`nodes()`, `successors()`, `roots()`, …). Carries the node's `name`
/// plus its current internal `index`. The `index` is a snapshot — after
/// a node is removed it may be reused. For a stable handle, use
/// [`PyNodeRef`].
#[pyclass(frozen, eq, hash, name = "NodeId")]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyNodeId {
    #[pyo3(get)]
    pub index: u32,
    #[pyo3(get)]
    pub name: String,
}

#[pymethods]
impl PyNodeId {
    fn __repr__(&self) -> String {
        format!("NodeId(name={:?}, index={})", self.name, self.index)
    }

    fn __str__(&self) -> &str {
        &self.name
    }
}

impl From<dagron_core::NodeId> for PyNodeId {
    fn from(node: dagron_core::NodeId) -> Self {
        PyNodeId {
            index: node.index,
            name: node.name,
        }
    }
}

/// A stable, persistent handle to a node, returned by `add_node`.
///
/// `NodeRef` survives unrelated mutations and is invalidated only when its
/// own node is removed (or removed-and-readded with the same name, which
/// produces a fresh handle with a different `epoch`). Pass it anywhere a
/// `str` name is accepted — both forms are interchangeable in dagron's API.
#[pyclass(frozen, eq, hash, name = "NodeRef")]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyNodeRef {
    pub inner: dagron_core::NodeRef,
}

#[pymethods]
impl PyNodeRef {
    /// The node's name.
    #[getter]
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// The creation epoch this ref was minted with.
    #[getter]
    pub fn epoch(&self) -> u64 {
        self.inner.epoch()
    }

    fn __repr__(&self) -> String {
        format!(
            "NodeRef(name={:?}, epoch={})",
            self.inner.name(),
            self.inner.epoch()
        )
    }

    fn __str__(&self) -> &str {
        self.inner.name()
    }
}

impl From<dagron_core::NodeRef> for PyNodeRef {
    fn from(r: dagron_core::NodeRef) -> Self {
        PyNodeRef { inner: r }
    }
}
