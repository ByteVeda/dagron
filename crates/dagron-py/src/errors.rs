use pyo3::create_exception;
use pyo3::exceptions::PyException;

create_exception!(
    dagron,
    DagronError,
    PyException,
    "Base exception for all dagron errors."
);
create_exception!(
    dagron,
    CycleError,
    DagronError,
    "Raised when an operation would create a cycle in the DAG."
);
create_exception!(
    dagron,
    NodeNotFoundError,
    DagronError,
    "Raised when a referenced node does not exist."
);
create_exception!(
    dagron,
    DuplicateNodeError,
    DagronError,
    "Raised when adding a node with a name that already exists."
);
create_exception!(
    dagron,
    EdgeNotFoundError,
    DagronError,
    "Raised when a referenced edge does not exist."
);
create_exception!(
    dagron,
    GraphError,
    DagronError,
    "Raised for general graph operation errors."
);

/// Convert a DagronError into a PyErr.
pub fn into_pyerr(err: dagron_core::DagronError) -> pyo3::PyErr {
    match err {
        dagron_core::DagronError::Cycle(msg) => CycleError::new_err(msg),
        dagron_core::DagronError::NodeNotFound(msg) => NodeNotFoundError::new_err(msg),
        dagron_core::DagronError::DuplicateNode(msg) => DuplicateNodeError::new_err(msg),
        dagron_core::DagronError::EdgeNotFound(from, to) => {
            EdgeNotFoundError::new_err(format!("{from} -> {to}"))
        }
        dagron_core::DagronError::Graph(msg) => GraphError::new_err(msg),
    }
}
