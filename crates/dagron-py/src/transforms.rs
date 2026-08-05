use pyo3::prelude::*;

use crate::dag::PyDAG;
use crate::errors;
use crate::noderef::NodeArg;
use crate::payload::PyNodePayload;

/// Clone a PyNodePayload, using the GIL to clone Py<PyAny> references.
pub(crate) fn clone_payload(py: Python<'_>, p: &PyNodePayload) -> PyNodePayload {
    PyNodePayload {
        payload: p.payload.as_ref().map(|obj| obj.clone_ref(py)),
        metadata: p.metadata.as_ref().map(|obj| obj.clone_ref(py)),
    }
}

/// Clone all nodes from a source DAG into a new DAG.
pub(crate) fn clone_nodes(
    py: Python<'_>,
    src: &dagron_core::DAG<PyNodePayload>,
    dst: &mut dagron_core::DAG<PyNodePayload>,
) -> PyResult<()> {
    for name in src.node_names() {
        let payload = src.get_payload(&name).map_err(errors::into_pyerr)?;
        let cloned = clone_payload(py, payload);
        dst.add_node(name, cloned).map_err(errors::into_pyerr)?;
    }
    Ok(())
}

/// Clone all edges from a source DAG into a destination DAG.
/// Only copies edges where both endpoints exist in dst.
pub(crate) fn clone_edges(
    src: &dagron_core::DAG<PyNodePayload>,
    dst: &mut dagron_core::DAG<PyNodePayload>,
) -> PyResult<()> {
    let sg = src.to_serializable(|_| None);
    for edge in &sg.edges {
        if dst.has_node(&edge.from) && dst.has_node(&edge.to) {
            dst.add_edge(&edge.from, &edge.to, Some(edge.weight), edge.label.clone())
                .map_err(errors::into_pyerr)?;
        }
    }
    Ok(())
}

#[pymethods]
impl PyDAG {
    /// Return a new DAG with all edges reversed.
    ///
    /// Same nodes, all edges flipped (A->B becomes B->A).
    /// Edge weights and labels are preserved on the reversed edge.
    ///
    /// Returns:
    ///     A new DAG with reversed edges.
    pub fn reverse(&self, py: Python<'_>) -> PyResult<PyDAG> {
        let mut new_dag = dagron_core::DAG::new();
        clone_nodes(py, &self.inner, &mut new_dag)?;

        // Add reversed edges
        let sg = self.inner.to_serializable(|_| None);
        for edge in &sg.edges {
            new_dag
                .add_edge(&edge.to, &edge.from, Some(edge.weight), edge.label.clone())
                .map_err(errors::into_pyerr)?;
        }

        Ok(PyDAG { inner: new_dag })
    }

    /// Collapse a set of nodes into a single summary node.
    ///
    /// Internal edges (both endpoints in the collapse set) are dropped.
    /// External edges are redirected to/from the collapsed node.
    ///
    /// Args:
    ///     nodes: List of node names to collapse.
    ///     collapsed_name: Name of the new collapsed node.
    ///     payload: Optional payload for the collapsed node.
    ///
    /// Returns:
    ///     A new DAG with the nodes collapsed.
    ///
    /// Raises:
    ///     NodeNotFoundError: If any node in the list doesn't exist.
    ///     DuplicateNodeError: If collapsed_name collides with a surviving node.
    #[pyo3(signature = (nodes, collapsed_name, payload=None))]
    pub fn collapse(
        &self,
        py: Python<'_>,
        nodes: Vec<NodeArg>,
        collapsed_name: String,
        payload: Option<PyObject>,
    ) -> PyResult<PyDAG> {
        let node_names: Vec<String> = nodes
            .into_iter()
            .map(|n| n.into_name(&self.inner))
            .collect::<PyResult<_>>()?;
        let node_refs: Vec<&str> = node_names.iter().map(|s| s.as_str()).collect();
        let collapse_set: std::collections::HashSet<&str> = node_refs.iter().copied().collect();

        // Validate all nodes exist
        for &n in &node_refs {
            if !self.inner.has_node(n) {
                return Err(errors::into_pyerr(dagron_core::DagronError::NodeNotFound(
                    n.to_string(),
                )));
            }
        }

        let mut new_dag = dagron_core::DAG::new();

        // Add surviving nodes
        for name in self.inner.node_names() {
            if !collapse_set.contains(name.as_str()) {
                let p = self.inner.get_payload(&name).map_err(errors::into_pyerr)?;
                let cloned = clone_payload(py, p);
                new_dag.add_node(name, cloned).map_err(errors::into_pyerr)?;
            }
        }

        // Add collapsed node
        let collapsed_payload = PyNodePayload {
            payload,
            metadata: None,
        };
        new_dag
            .add_node(collapsed_name.clone(), collapsed_payload)
            .map_err(errors::into_pyerr)?;

        // Process edges
        let sg = self.inner.to_serializable(|_| None);
        let mut added_edges: std::collections::HashSet<(String, String)> =
            std::collections::HashSet::new();

        for edge in &sg.edges {
            let src_in = collapse_set.contains(edge.from.as_str());
            let tgt_in = collapse_set.contains(edge.to.as_str());

            if src_in && tgt_in {
                continue; // internal edge
            }

            let actual_src = if src_in { &collapsed_name } else { &edge.from };
            let actual_tgt = if tgt_in { &collapsed_name } else { &edge.to };

            if actual_src == actual_tgt {
                continue; // skip self-loops
            }

            let edge_key = (actual_src.clone(), actual_tgt.clone());
            if added_edges.contains(&edge_key) {
                continue;
            }

            new_dag
                .add_edge(
                    actual_src,
                    actual_tgt,
                    Some(edge.weight),
                    edge.label.clone(),
                )
                .map_err(errors::into_pyerr)?;
            added_edges.insert(edge_key);
        }

        Ok(PyDAG { inner: new_dag })
    }

    /// Compute the dominator tree rooted at the given node.
    ///
    /// Args:
    ///     root: The root node name.
    ///
    /// Returns:
    ///     A list of (node, immediate_dominator) tuples.
    pub fn dominator_tree(&self, py: Python<'_>, root: NodeArg) -> PyResult<Vec<(String, String)>> {
        let root_name = root.into_name(&self.inner)?;
        py.allow_threads(|| {
            self.inner
                .dominator_tree(&root_name)
                .map_err(errors::into_pyerr)
        })
    }

    /// Create an independent snapshot (deep clone) of this DAG.
    ///
    /// The snapshot copies all nodes (including payloads and metadata),
    /// edges, and the generation counter. It starts with a fresh cache.
    /// Mutations to the snapshot do not affect the original and vice versa.
    ///
    /// Returns:
    ///     A new independent DAG with the same structure and data.
    pub fn snapshot(&self, py: Python<'_>) -> PyResult<PyDAG> {
        let mut new_dag = dagron_core::DAG::new();
        clone_nodes(py, &self.inner, &mut new_dag)?;
        clone_edges(&self.inner, &mut new_dag)?;
        new_dag.set_generation(self.inner.generation());
        Ok(PyDAG { inner: new_dag })
    }

    /// Return a new DAG that is the transitive reduction of this one.
    ///
    /// Removes all redundant edges (edges implied by longer paths).
    /// Preserves edge weights and labels for non-redundant edges.
    ///
    /// Returns:
    ///     A new DAG with redundant edges removed.
    pub fn transitive_reduction(&self, py: Python<'_>) -> PyResult<PyDAG> {
        let redundant = py.allow_threads(|| {
            dagron_core::algorithms::transitive_reduction_redundant_edges(self.inner.inner_graph())
        });

        let mut new_dag = dagron_core::DAG::new();
        clone_nodes(py, &self.inner, &mut new_dag)?;

        // Add non-redundant edges
        let sg = self.inner.to_serializable(|_| None);
        for edge in &sg.edges {
            let from_idx = self
                .inner
                .resolve_name(&edge.from)
                .map_err(errors::into_pyerr)?;
            let to_idx = self
                .inner
                .resolve_name(&edge.to)
                .map_err(errors::into_pyerr)?;
            if !redundant.contains(&(from_idx, to_idx)) {
                new_dag
                    .add_edge(&edge.from, &edge.to, Some(edge.weight), edge.label.clone())
                    .map_err(errors::into_pyerr)?;
            }
        }

        Ok(PyDAG { inner: new_dag })
    }

    /// Return a new DAG that is the transitive closure of this one.
    ///
    /// Adds direct edges for all reachable pairs. New edges get weight 1.0
    /// and no label. Existing edges keep their weights and labels.
    ///
    /// Returns:
    ///     A new DAG with all transitive edges added.
    pub fn transitive_closure(&self, py: Python<'_>) -> PyResult<PyDAG> {
        let new_edges = py.allow_threads(|| {
            dagron_core::algorithms::transitive_closure_new_edges(self.inner.inner_graph())
        });

        let mut new_dag = dagron_core::DAG::new();
        clone_nodes(py, &self.inner, &mut new_dag)?;
        clone_edges(&self.inner, &mut new_dag)?;

        // Add closure edges
        for (src, tgt) in new_edges {
            let from_name = &self.inner.inner_graph()[src].name;
            let to_name = &self.inner.inner_graph()[tgt].name;
            new_dag
                .add_edge(from_name, to_name, None, None)
                .map_err(errors::into_pyerr)?;
        }

        Ok(PyDAG { inner: new_dag })
    }

    /// Return a new DAG containing only nodes that satisfy the predicate.
    ///
    /// The predicate is called with (name, payload) for each node.
    /// Edges between surviving nodes are preserved.
    ///
    /// Args:
    ///     predicate: Callable that takes (name: str, payload) and returns bool.
    ///
    /// Returns:
    ///     A new DAG containing only matching nodes and their interconnecting edges.
    pub fn filter(&self, py: Python<'_>, predicate: &Bound<'_, PyAny>) -> PyResult<PyDAG> {
        let mut new_dag = dagron_core::DAG::new();

        // Add nodes that pass the predicate
        for name in self.inner.node_names() {
            let payload = self.inner.get_payload(&name).map_err(errors::into_pyerr)?;
            let payload_obj = payload
                .payload
                .as_ref()
                .map(|obj| obj.clone_ref(py))
                .unwrap_or_else(|| py.None());
            let keep: bool = predicate.call1((&name, payload_obj))?.extract()?;
            if keep {
                let cloned = clone_payload(py, payload);
                new_dag.add_node(name, cloned).map_err(errors::into_pyerr)?;
            }
        }

        // Add edges where both endpoints survive
        clone_edges(&self.inner, &mut new_dag)?;

        Ok(PyDAG { inner: new_dag })
    }

    /// Merge this DAG with another, returning a new combined DAG.
    ///
    /// Args:
    ///     other: The other DAG to merge with.
    ///     conflict: Strategy for conflicting node names:
    ///         "keep_first" (default) — keep this DAG's payload.
    ///         "keep_second" — keep the other DAG's payload.
    ///         "error" — raise DuplicateNodeError on conflicts.
    ///     conflict_resolver: Optional callable(name, payload1, payload2) -> payload
    ///         for custom conflict resolution. If provided, overrides the `conflict` parameter.
    ///
    /// Returns:
    ///     A new merged DAG.
    ///
    /// Raises:
    ///     CycleError: If the combined edges would create a cycle.
    ///     DuplicateNodeError: If conflict="error" and node names overlap.
    #[pyo3(signature = (other, conflict="keep_first", conflict_resolver=None))]
    pub fn merge(
        &self,
        py: Python<'_>,
        other: &PyDAG,
        conflict: &str,
        conflict_resolver: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyDAG> {
        // If a custom resolver is provided, use merge_with logic
        if let Some(resolver) = conflict_resolver {
            return self.merge_with_resolver(py, other, resolver);
        }

        let strategy = match conflict {
            "keep_first" => dagron_core::MergeConflict::KeepFirst,
            "keep_second" => dagron_core::MergeConflict::KeepSecond,
            "error" => dagron_core::MergeConflict::Error,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid conflict strategy: '{conflict}'. Expected 'keep_first', 'keep_second', or 'error'."
                )))
            }
        };

        let mut new_dag = dagron_core::DAG::new();

        // Add all nodes from self
        clone_nodes(py, &self.inner, &mut new_dag)?;

        // Add nodes from other, handling conflicts
        for name in other.inner.node_names() {
            let payload = other.inner.get_payload(&name).map_err(errors::into_pyerr)?;
            if new_dag.has_node(&name) {
                match strategy {
                    dagron_core::MergeConflict::KeepFirst => {}
                    dagron_core::MergeConflict::KeepSecond => {
                        let cloned = clone_payload(py, payload);
                        let existing =
                            new_dag.get_payload_mut(&name).map_err(errors::into_pyerr)?;
                        *existing = cloned;
                    }
                    dagron_core::MergeConflict::Error => {
                        return Err(errors::into_pyerr(dagron_core::DagronError::DuplicateNode(
                            name,
                        )));
                    }
                }
            } else {
                let cloned = clone_payload(py, payload);
                new_dag.add_node(name, cloned).map_err(errors::into_pyerr)?;
            }
        }

        // Add all edges from self
        clone_edges(&self.inner, &mut new_dag)?;

        // Add edges from other (skip duplicates)
        let other_sg = other.inner.to_serializable(|_| None);
        for edge in &other_sg.edges {
            if new_dag
                .has_edge(&edge.from, &edge.to)
                .map_err(errors::into_pyerr)?
            {
                continue;
            }
            new_dag
                .add_edge(&edge.from, &edge.to, Some(edge.weight), edge.label.clone())
                .map_err(errors::into_pyerr)?;
        }

        Ok(PyDAG { inner: new_dag })
    }
}

impl PyDAG {
    fn merge_with_resolver(
        &self,
        py: Python<'_>,
        other: &PyDAG,
        resolver: &Bound<'_, PyAny>,
    ) -> PyResult<PyDAG> {
        let mut new_dag = dagron_core::DAG::new();

        // Add all nodes from self
        clone_nodes(py, &self.inner, &mut new_dag)?;

        // Add/merge nodes from other
        for name in other.inner.node_names() {
            let other_payload = other.inner.get_payload(&name).map_err(errors::into_pyerr)?;
            if new_dag.has_node(&name) {
                // Call resolver(name, payload1, payload2)
                let p1 = new_dag.get_payload(&name).map_err(errors::into_pyerr)?;
                let p1_obj = p1
                    .payload
                    .as_ref()
                    .map(|obj| obj.clone_ref(py))
                    .unwrap_or_else(|| py.None());
                let p2_obj = other_payload
                    .payload
                    .as_ref()
                    .map(|obj| obj.clone_ref(py))
                    .unwrap_or_else(|| py.None());

                let result = resolver.call1((&name, p1_obj, p2_obj))?;
                let resolved_payload = if result.is_none() {
                    None
                } else {
                    Some(result.unbind())
                };

                let existing = new_dag.get_payload_mut(&name).map_err(errors::into_pyerr)?;
                existing.payload = resolved_payload;
            } else {
                let cloned = clone_payload(py, other_payload);
                new_dag.add_node(name, cloned).map_err(errors::into_pyerr)?;
            }
        }

        // Add all edges from self
        clone_edges(&self.inner, &mut new_dag)?;

        // Add edges from other (skip duplicates)
        let other_sg = other.inner.to_serializable(|_| None);
        for edge in &other_sg.edges {
            if new_dag
                .has_edge(&edge.from, &edge.to)
                .map_err(errors::into_pyerr)?
            {
                continue;
            }
            new_dag
                .add_edge(&edge.from, &edge.to, Some(edge.weight), edge.label.clone())
                .map_err(errors::into_pyerr)?;
        }

        Ok(PyDAG { inner: new_dag })
    }
}
