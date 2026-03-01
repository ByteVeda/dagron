use pyo3::prelude::*;

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
