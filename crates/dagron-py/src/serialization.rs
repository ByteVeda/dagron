use std::path::Path;

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString};

use crate::dag::PyDAG;
use crate::errors;
use crate::payload::PyNodePayload;

/// Convert a Python object to a serde_json::Value.
fn py_to_json_value(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Ok(b) = obj.downcast::<PyBool>() {
        Ok(serde_json::Value::Bool(b.is_true()))
    } else if obj.is_instance_of::<PyInt>() {
        let i: i64 = obj.extract()?;
        Ok(serde_json::Value::Number(i.into()))
    } else if obj.is_instance_of::<PyFloat>() {
        let f: f64 = obj.extract()?;
        match serde_json::Number::from_f64(f) {
            Some(n) => Ok(serde_json::Value::Number(n)),
            None => Ok(serde_json::Value::Null),
        }
    } else if obj.is_instance_of::<PyString>() {
        let s: String = obj.extract()?;
        Ok(serde_json::Value::String(s))
    } else if let Ok(list) = obj.downcast::<PyList>() {
        let items: PyResult<Vec<serde_json::Value>> =
            list.iter().map(|item| py_to_json_value(&item)).collect();
        Ok(serde_json::Value::Array(items?))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_json_value(&v)?);
        }
        Ok(serde_json::Value::Object(map))
    } else {
        Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "Cannot convert {} to JSON",
            obj.get_type().name()?
        )))
    }
}

/// Convert a serde_json::Value to a Python object.
fn json_value_to_py(py: Python<'_>, val: &serde_json::Value) -> PyResult<PyObject> {
    match val {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_value_to_py(py, item)?)?;
            }
            Ok(list.into_pyobject(py)?.into_any().unbind())
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_value_to_py(py, v)?)?;
            }
            Ok(dict.into_pyobject(py)?.into_any().unbind())
        }
    }
}

#[pymethods]
impl PyDAG {
    /// Serialize the DAG to a JSON string.
    ///
    /// Args:
    ///     payload_serializer: Optional callable that converts a node's payload
    ///         to a JSON-compatible value (dict, list, str, int, float, bool, None).
    ///         If not provided, payloads are omitted.
    ///
    /// Returns:
    ///     A pretty-printed JSON string representing the DAG.
    #[pyo3(signature = (payload_serializer=None))]
    pub fn to_json(
        &self,
        py: Python<'_>,
        payload_serializer: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        match payload_serializer {
            Some(cb) => {
                // We need to call the Python callback for each node, so we can't release the GIL.
                // Build a SerializableGraph manually.
                let sg = self.inner.to_serializable(|p| {
                    if let Some(ref payload_obj) = p.payload {
                        let result = cb.call1((payload_obj.clone_ref(py),));
                        match result {
                            Ok(val) => py_to_json_value(&val).ok(),
                            Err(_) => None,
                        }
                    } else {
                        None
                    }
                });
                serde_json::to_string_pretty(&sg).map_err(|e| {
                    errors::into_pyerr(dagron_core::DagronError::Graph(format!(
                        "JSON serialization failed: {e}"
                    )))
                })
            }
            None => {
                let sg = self.inner.to_serializable(|_| None);
                serde_json::to_string_pretty(&sg).map_err(|e| {
                    errors::into_pyerr(dagron_core::DagronError::Graph(format!(
                        "JSON serialization failed: {e}"
                    )))
                })
            }
        }
    }

    /// Deserialize a DAG from a JSON string.
    ///
    /// Args:
    ///     json_str: A JSON string previously produced by `to_json()`.
    ///     payload_deserializer: Optional callable that converts a JSON-compatible
    ///         value back to the node's payload. If not provided, payloads are set to None.
    ///
    /// Returns:
    ///     A new DAG instance reconstructed from the JSON.
    #[classmethod]
    #[pyo3(signature = (json_str, payload_deserializer=None))]
    pub fn from_json(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        json_str: &str,
        payload_deserializer: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let sg: dagron_core::SerializableGraph = serde_json::from_str(json_str).map_err(|e| {
            errors::into_pyerr(dagron_core::DagronError::Graph(format!(
                "JSON deserialization failed: {e}"
            )))
        })?;

        let inner = dagron_core::DAG::from_serializable(sg, |json_val| {
            let payload = match (payload_deserializer, json_val) {
                (Some(cb), Some(val)) => {
                    let py_val = json_value_to_py(py, val).ok();
                    py_val.and_then(|v| cb.call1((v,)).ok().map(|r| r.unbind()))
                }
                _ => None,
            };
            PyNodePayload {
                payload,
                metadata: None,
            }
        })
        .map_err(errors::into_pyerr)?;

        Ok(PyDAG { inner })
    }

    /// Export the DAG in Graphviz DOT format.
    ///
    /// Args:
    ///     node_attrs: Optional callable that receives (name, payload) and returns
    ///         an attribute string (e.g. "shape=box, color=red") or None.
    ///
    /// Returns:
    ///     A DOT format string.
    #[pyo3(signature = (node_attrs=None))]
    pub fn to_dot(
        &self,
        py: Python<'_>,
        node_attrs: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<String> {
        match node_attrs {
            Some(cb) => Ok(self.inner.to_dot_with(|name, p| {
                let payload_obj = p
                    .payload
                    .as_ref()
                    .map(|obj| obj.clone_ref(py))
                    .unwrap_or_else(|| py.None());
                cb.call1((name, payload_obj)).ok().and_then(|result| {
                    if result.is_none() {
                        None
                    } else {
                        result.extract::<String>().ok()
                    }
                })
            })),
            None => Ok(self.inner.to_dot()),
        }
    }

    /// Export the DAG in Mermaid diagram format.
    ///
    /// Returns:
    ///     A Mermaid format string.
    pub fn to_mermaid(&self) -> String {
        self.inner.to_mermaid()
    }

    /// Serialize the DAG to a binary (bincode) byte string.
    ///
    /// Args:
    ///     payload_serializer: Optional callable that converts a node's payload
    ///         to a JSON-compatible value. If not provided, payloads are omitted.
    ///
    /// Returns:
    ///     A bytes object containing the binary representation.
    #[pyo3(signature = (payload_serializer=None))]
    pub fn to_bytes<'py>(
        &self,
        py: Python<'py>,
        payload_serializer: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Py<PyBytes>> {
        match payload_serializer {
            Some(cb) => {
                // With serializer: can't use zero-copy (callback would be called twice)
                let bytes = self
                    .inner
                    .to_bincode(|p| {
                        if let Some(ref payload_obj) = p.payload {
                            let result = cb.call1((payload_obj.clone_ref(py),));
                            match result {
                                Ok(val) => py_to_json_value(&val).ok(),
                                Err(_) => None,
                            }
                        } else {
                            None
                        }
                    })
                    .map_err(errors::into_pyerr)?;
                Ok(PyBytes::new(py, &bytes).unbind())
            }
            None => {
                // Zero-copy: two-pass approach
                // Pass 1: count the size
                let size = self
                    .inner
                    .bincode_size(|_| None)
                    .map_err(errors::into_pyerr)?;

                // Pass 2: write directly into Python-allocated buffer
                let py_bytes = PyBytes::new_with(py, size, |buf| {
                    let mut cursor = std::io::Cursor::new(buf);
                    self.inner
                        .to_bincode_writer(&mut cursor, |_| None)
                        .map_err(|e| {
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Bincode write error: {e}"
                            ))
                        })?;
                    Ok(())
                })?;
                Ok(py_bytes.unbind())
            }
        }
    }

    /// Deserialize a DAG from a binary (bincode) byte string.
    ///
    /// Args:
    ///     data: A bytes object previously produced by `to_bytes()`.
    ///     payload_deserializer: Optional callable that converts a JSON-compatible
    ///         value back to the node's payload. If not provided, payloads are set to None.
    ///
    /// Returns:
    ///     A new DAG instance reconstructed from the binary data.
    #[classmethod]
    #[pyo3(signature = (data, payload_deserializer=None))]
    pub fn from_bytes(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        data: &[u8],
        payload_deserializer: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let inner = dagron_core::DAG::from_bincode(data, |json_val| {
            let payload = match (payload_deserializer, json_val) {
                (Some(cb), Some(val)) => {
                    let py_val = json_value_to_py(py, val).ok();
                    py_val.and_then(|v| cb.call1((v,)).ok().map(|r| r.unbind()))
                }
                _ => None,
            };
            PyNodePayload {
                payload,
                metadata: None,
            }
        })
        .map_err(errors::into_pyerr)?;

        Ok(PyDAG { inner })
    }

    /// Save the DAG to a file in binary format.
    ///
    /// Args:
    ///     path: File path to write to.
    ///     payload_serializer: Optional callable that converts a node's payload
    ///         to a JSON-compatible value. If not provided, payloads are omitted.
    #[pyo3(signature = (path, payload_serializer=None))]
    pub fn save(
        &self,
        py: Python<'_>,
        path: &str,
        payload_serializer: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let file_path = Path::new(path);
        match payload_serializer {
            Some(cb) => {
                self.inner
                    .to_bincode_file(file_path, |p| {
                        if let Some(ref payload_obj) = p.payload {
                            let result = cb.call1((payload_obj.clone_ref(py),));
                            match result {
                                Ok(val) => py_to_json_value(&val).ok(),
                                Err(_) => None,
                            }
                        } else {
                            None
                        }
                    })
                    .map_err(errors::into_pyerr)?;
            }
            None => {
                let inner_ref = &self.inner;
                let path_owned = file_path.to_path_buf();
                py.allow_threads(|| inner_ref.to_bincode_file(&path_owned, |_| None))
                    .map_err(errors::into_pyerr)?;
            }
        }
        Ok(())
    }

    /// Load a DAG from a binary file using memory-mapped I/O.
    ///
    /// Args:
    ///     path: File path to read from.
    ///     payload_deserializer: Optional callable that converts a JSON-compatible
    ///         value back to the node's payload. If not provided, payloads are set to None.
    ///
    /// Returns:
    ///     A new DAG instance loaded from the file.
    #[classmethod]
    #[pyo3(signature = (path, payload_deserializer=None))]
    pub fn load(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        path: &str,
        payload_deserializer: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let file_path = Path::new(path);
        let inner = dagron_core::DAG::from_bincode_file(file_path, |json_val| {
            let payload = match (payload_deserializer, json_val) {
                (Some(cb), Some(val)) => {
                    let py_val = json_value_to_py(py, val).ok();
                    py_val.and_then(|v| cb.call1((v,)).ok().map(|r| r.unbind()))
                }
                _ => None,
            };
            PyNodePayload {
                payload,
                metadata: None,
            }
        })
        .map_err(errors::into_pyerr)?;

        Ok(PyDAG { inner })
    }
}
