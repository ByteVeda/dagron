pub mod construction;
pub mod dag;
pub mod errors;
pub mod incremental;
pub mod introspection;
pub mod node;
pub mod payload;
pub mod protocols;
pub mod scheduling;
pub mod serialization;
pub mod toposort;
pub mod transforms;
pub mod validation;

use pyo3::prelude::*;

#[pymodule]
#[pyo3(name = "_internal")]
fn dagron(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<dag::PyDAG>()?;
    m.add_class::<node::PyNodeId>()?;
    m.add_class::<scheduling::PyScheduledNode>()?;
    m.add_class::<scheduling::PyExecutionStep>()?;
    m.add_class::<scheduling::PyExecutionPlan>()?;

    // Register exception hierarchy
    m.add("DagronError", m.py().get_type::<errors::DagronError>())?;
    m.add("CycleError", m.py().get_type::<errors::CycleError>())?;
    m.add(
        "NodeNotFoundError",
        m.py().get_type::<errors::NodeNotFoundError>(),
    )?;
    m.add(
        "DuplicateNodeError",
        m.py().get_type::<errors::DuplicateNodeError>(),
    )?;
    m.add(
        "EdgeNotFoundError",
        m.py().get_type::<errors::EdgeNotFoundError>(),
    )?;
    m.add("GraphError", m.py().get_type::<errors::GraphError>())?;

    Ok(())
}
