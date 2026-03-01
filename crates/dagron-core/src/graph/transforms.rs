use ahash::AHashSet;
use petgraph::visit::{EdgeRef, IntoEdgeReferences, IntoNodeIdentifiers};

use crate::algorithms;
use crate::errors::DagronError;

use super::DAG;

/// Strategy for resolving node conflicts during merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflict {
    /// Keep the payload from the first (self) DAG.
    KeepFirst,
    /// Keep the payload from the second (other) DAG.
    KeepSecond,
    /// Return an error if any node names overlap.
    Error,
}

impl<P: Clone> DAG<P> {
    /// Return a new DAG that is the transitive reduction of this one.
    ///
    /// Removes all edges that are implied by other paths. Preserves
    /// edge weights and labels for non-redundant edges.
    pub fn transitive_reduction(&self) -> DAG<P> {
        let redundant = algorithms::transitive_reduction_redundant_edges(&self.graph);

        let mut new_dag = DAG::new();

        // Clone all nodes
        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            // Ignore error — names are unique in source, so no duplicates
            let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
        }

        // Clone non-redundant edges
        for edge in self.graph.edge_references() {
            let src = edge.source();
            let tgt = edge.target();
            if !redundant.contains(&(src, tgt)) {
                let from_name = &self.graph[src].name;
                let to_name = &self.graph[tgt].name;
                let data = edge.weight();
                let _ = new_dag.add_edge(from_name, to_name, Some(data.weight), data.label.clone());
            }
        }

        new_dag
    }

    /// Return a new DAG that is the transitive closure of this one.
    ///
    /// Adds edges for all reachable pairs. New edges get weight 1.0
    /// and no label. Existing edges keep their weights and labels.
    pub fn transitive_closure(&self) -> DAG<P> {
        let new_edges = algorithms::transitive_closure_new_edges(&self.graph);

        let mut new_dag = DAG::new();

        // Clone all nodes
        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
        }

        // Clone all existing edges
        for edge in self.graph.edge_references() {
            let from_name = &self.graph[edge.source()].name;
            let to_name = &self.graph[edge.target()].name;
            let data = edge.weight();
            let _ = new_dag.add_edge(from_name, to_name, Some(data.weight), data.label.clone());
        }

        // Add new closure edges (weight 1.0, no label)
        for (src, tgt) in new_edges {
            let from_name = &self.graph[src].name;
            let to_name = &self.graph[tgt].name;
            let _ = new_dag.add_edge(from_name, to_name, None, None);
        }

        new_dag
    }

    /// Return a new DAG containing only nodes that satisfy the predicate.
    ///
    /// Edges between surviving nodes are preserved with their weights and labels.
    /// Edges involving removed nodes are dropped.
    pub fn filter<F>(&self, predicate: F) -> DAG<P>
    where
        F: Fn(&str, &P) -> bool,
    {
        let mut new_dag = DAG::new();

        // Add nodes that pass the predicate
        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            if predicate(&node.name, &node.payload) {
                let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
            }
        }

        // Add edges where both endpoints survive
        for edge in self.graph.edge_references() {
            let from_name = &self.graph[edge.source()].name;
            let to_name = &self.graph[edge.target()].name;
            if new_dag.has_node(from_name) && new_dag.has_node(to_name) {
                let data = edge.weight();
                let _ =
                    new_dag.add_edge(from_name, to_name, Some(data.weight), data.label.clone());
            }
        }

        new_dag
    }

    /// Merge two DAGs into a new one using the given conflict strategy.
    ///
    /// For overlapping node names:
    /// - `KeepFirst`: keep self's payload
    /// - `KeepSecond`: keep other's payload
    /// - `Error`: return an error
    ///
    /// All edges from both DAGs are included. The merged graph is validated
    /// for cycles (returns an error if the merge would create one).
    pub fn merge(
        &self,
        other: &DAG<P>,
        conflict: MergeConflict,
    ) -> Result<DAG<P>, DagronError> {
        let mut new_dag = DAG::new();

        // Add all nodes from self
        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
        }

        // Add nodes from other, handling conflicts
        for idx in other.graph.node_identifiers() {
            let node = &other.graph[idx];
            if new_dag.has_node(&node.name) {
                match conflict {
                    MergeConflict::KeepFirst => {
                        // Already have self's version, skip
                    }
                    MergeConflict::KeepSecond => {
                        // Replace payload with other's
                        let payload_mut = new_dag.get_payload_mut(&node.name)?;
                        *payload_mut = node.payload.clone();
                    }
                    MergeConflict::Error => {
                        return Err(DagronError::DuplicateNode(node.name.clone()));
                    }
                }
            } else {
                let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
            }
        }

        // Add all edges from self
        for edge in self.graph.edge_references() {
            let from_name = &self.graph[edge.source()].name;
            let to_name = &self.graph[edge.target()].name;
            let data = edge.weight();
            new_dag.add_edge(from_name, to_name, Some(data.weight), data.label.clone())?;
        }

        // Add edges from other (skip duplicates — same from→to already added from self)
        for edge in other.graph.edge_references() {
            let from_name = &other.graph[edge.source()].name;
            let to_name = &other.graph[edge.target()].name;
            if new_dag.has_edge(from_name, to_name)? {
                continue; // self's version wins for duplicate edges
            }
            let data = edge.weight();
            new_dag.add_edge(from_name, to_name, Some(data.weight), data.label.clone())?;
        }

        Ok(new_dag)
    }

    /// Merge two DAGs using a custom resolver function for conflicting nodes.
    ///
    /// When both DAGs contain a node with the same name, `resolver(name, &self_payload, &other_payload)`
    /// is called to produce the merged payload.
    ///
    /// The merged graph is validated for cycles.
    pub fn merge_with<F>(
        &self,
        other: &DAG<P>,
        resolver: F,
    ) -> Result<DAG<P>, DagronError>
    where
        F: Fn(&str, &P, &P) -> P,
    {
        let mut new_dag = DAG::new();

        // Add all nodes from self
        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
        }

        // Add/merge nodes from other
        for idx in other.graph.node_identifiers() {
            let node = &other.graph[idx];
            if new_dag.has_node(&node.name) {
                let self_payload = new_dag.get_payload(&node.name)?;
                let merged = resolver(&node.name, self_payload, &node.payload);
                let payload_mut = new_dag.get_payload_mut(&node.name)?;
                *payload_mut = merged;
            } else {
                let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
            }
        }

        // Add all edges from self
        for edge in self.graph.edge_references() {
            let from_name = &self.graph[edge.source()].name;
            let to_name = &self.graph[edge.target()].name;
            let data = edge.weight();
            new_dag.add_edge(from_name, to_name, Some(data.weight), data.label.clone())?;
        }

        // Add edges from other (skip duplicates)
        for edge in other.graph.edge_references() {
            let from_name = &other.graph[edge.source()].name;
            let to_name = &other.graph[edge.target()].name;
            if new_dag.has_edge(from_name, to_name)? {
                continue;
            }
            let data = edge.weight();
            new_dag.add_edge(from_name, to_name, Some(data.weight), data.label.clone())?;
        }

        Ok(new_dag)
    }

    /// Return a new DAG with all edges reversed.
    ///
    /// Same nodes, all edges flipped (A→B becomes B→A).
    /// Edge weights and labels are preserved on the reversed edge.
    pub fn reverse(&self) -> DAG<P> {
        let mut new_dag = DAG::new();

        // Clone all nodes
        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
        }

        // Add reversed edges
        for edge in self.graph.edge_references() {
            let from_name = &self.graph[edge.target()].name; // flipped
            let to_name = &self.graph[edge.source()].name; // flipped
            let data = edge.weight();
            let _ = new_dag.add_edge(from_name, to_name, Some(data.weight), data.label.clone());
        }

        new_dag
    }

    /// Collapse a set of nodes into a single summary node.
    ///
    /// Internal edges (both endpoints in the collapse set) are dropped.
    /// External edges are redirected to/from the collapsed node (duplicates and
    /// self-loops are skipped).
    pub fn collapse(
        &self,
        nodes: &[&str],
        name: &str,
        payload: P,
    ) -> Result<DAG<P>, DagronError> {
        let collapse_set: AHashSet<&str> = nodes.iter().copied().collect();

        // Validate all nodes exist
        for &n in nodes {
            if !self.has_node(n) {
                return Err(DagronError::NodeNotFound(n.to_string()));
            }
        }

        // Check collapsed name doesn't collide with surviving nodes
        let surviving_names: AHashSet<&str> = self
            .graph
            .node_identifiers()
            .map(|idx| self.graph[idx].name.as_str())
            .filter(|n| !collapse_set.contains(n))
            .collect();

        if surviving_names.contains(name) {
            return Err(DagronError::DuplicateNode(name.to_string()));
        }

        let mut new_dag = DAG::new();

        // Add surviving nodes
        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            if !collapse_set.contains(node.name.as_str()) {
                let _ = new_dag.add_node(node.name.clone(), node.payload.clone());
            }
        }

        // Add the collapsed node
        new_dag.add_node(name.to_string(), payload)?;

        // Track edges we've already added to avoid duplicates
        let mut added_edges: AHashSet<(String, String)> = AHashSet::new();

        // Process edges
        for edge in self.graph.edge_references() {
            let src_name = &self.graph[edge.source()].name;
            let tgt_name = &self.graph[edge.target()].name;
            let src_in = collapse_set.contains(src_name.as_str());
            let tgt_in = collapse_set.contains(tgt_name.as_str());
            let data = edge.weight();

            if src_in && tgt_in {
                // Internal edge — drop
                continue;
            }

            let actual_src = if src_in { name } else { src_name.as_str() };
            let actual_tgt = if tgt_in { name } else { tgt_name.as_str() };

            // Skip self-loops
            if actual_src == actual_tgt {
                continue;
            }

            let edge_key = (actual_src.to_string(), actual_tgt.to_string());
            if added_edges.contains(&edge_key) {
                continue;
            }

            new_dag.add_edge(actual_src, actual_tgt, Some(data.weight), data.label.clone())?;
            added_edges.insert(edge_key);
        }

        Ok(new_dag)
    }
}

impl<P> DAG<P> {
    /// Compute the dominator tree rooted at the given node.
    ///
    /// Returns a list of (node, immediate_dominator) pairs.
    /// The root node maps to itself.
    pub fn dominator_tree(&self, root: &str) -> Result<Vec<(String, String)>, DagronError> {
        let root_idx = self.resolve_name(root)?;
        let topo_order = algorithms::topological_sort_kahn(&self.graph)
            .map_err(|e| DagronError::Graph(e.message))?;
        let idom = algorithms::immediate_dominators(&self.graph, root_idx, &topo_order);

        let mut result: Vec<(String, String)> = idom
            .iter()
            .map(|(&node, &dom)| {
                (
                    self.graph[node].name.clone(),
                    self.graph[dom].name.clone(),
                )
            })
            .collect();

        // Sort for deterministic output
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }
}
