use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;

#[pyclass(frozen, name = "GraphStats")]
#[derive(Clone)]
pub struct PyGraphStats {
    #[pyo3(get)]
    pub node_count: usize,
    #[pyo3(get)]
    pub edge_count: usize,
    #[pyo3(get)]
    pub depth: usize,
    #[pyo3(get)]
    pub width: usize,
    #[pyo3(get)]
    pub density: f64,
    #[pyo3(get)]
    pub longest_path_length: usize,
    #[pyo3(get)]
    pub avg_in_degree: f64,
    #[pyo3(get)]
    pub avg_out_degree: f64,
    #[pyo3(get)]
    pub max_in_degree: usize,
    #[pyo3(get)]
    pub max_out_degree: usize,
    #[pyo3(get)]
    pub root_count: usize,
    #[pyo3(get)]
    pub leaf_count: usize,
    #[pyo3(get)]
    pub is_weakly_connected: bool,
    #[pyo3(get)]
    pub component_count: usize,
}

#[pymethods]
impl PyGraphStats {
    fn __repr__(&self) -> String {
        format!(
            "GraphStats(nodes={}, edges={}, depth={}, width={}, density={:.4}, \
             longest_path={}, roots={}, leaves={}, components={})",
            self.node_count,
            self.edge_count,
            self.depth,
            self.width,
            self.density,
            self.longest_path_length,
            self.root_count,
            self.leaf_count,
            self.component_count,
        )
    }

    fn to_dict(&self, py: Python<'_>) -> std::collections::HashMap<&str, PyObject> {
        let mut map = std::collections::HashMap::new();
        map.insert(
            "node_count",
            self.node_count
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "edge_count",
            self.edge_count
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "depth",
            self.depth.into_pyobject(py).unwrap().into_any().unbind(),
        );
        map.insert(
            "width",
            self.width.into_pyobject(py).unwrap().into_any().unbind(),
        );
        map.insert(
            "density",
            self.density.into_pyobject(py).unwrap().into_any().unbind(),
        );
        map.insert(
            "longest_path_length",
            self.longest_path_length
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "avg_in_degree",
            self.avg_in_degree
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "avg_out_degree",
            self.avg_out_degree
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "max_in_degree",
            self.max_in_degree
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "max_out_degree",
            self.max_out_degree
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "root_count",
            self.root_count
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "leaf_count",
            self.leaf_count
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map.insert(
            "is_weakly_connected",
            self.is_weakly_connected
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any()
                .unbind(),
        );
        map.insert(
            "component_count",
            self.component_count
                .into_pyobject(py)
                .unwrap()
                .into_any()
                .unbind(),
        );
        map
    }
}

#[pymethods]
impl PyDAG {
    /// Compute comprehensive graph statistics.
    ///
    /// Returns a GraphStats object with metrics including node/edge counts,
    /// depth, width, density, degree statistics, and connectivity info.
    pub fn stats(&self, py: Python<'_>) -> PyResult<PyGraphStats> {
        let inner_ref = &self.inner;
        let stats = py
            .allow_threads(|| inner_ref.stats())
            .map_err(errors::into_pyerr)?;

        Ok(PyGraphStats {
            node_count: stats.node_count,
            edge_count: stats.edge_count,
            depth: stats.depth,
            width: stats.width,
            density: stats.density,
            longest_path_length: stats.longest_path_length,
            avg_in_degree: stats.avg_in_degree,
            avg_out_degree: stats.avg_out_degree,
            max_in_degree: stats.max_in_degree,
            max_out_degree: stats.max_out_degree,
            root_count: stats.root_count,
            leaf_count: stats.leaf_count,
            is_weakly_connected: stats.is_weakly_connected,
            component_count: stats.component_count,
        })
    }
}
