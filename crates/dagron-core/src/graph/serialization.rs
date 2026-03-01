use std::fmt::Write;

use petgraph::visit::{EdgeRef, IntoEdgeReferences, IntoNodeIdentifiers};
use serde::{Deserialize, Serialize};

use crate::errors::DagronError;

use super::DAG;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableNode {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEdge {
    pub from: String,
    pub to: String,
    pub weight: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableGraph {
    pub nodes: Vec<SerializableNode>,
    pub edges: Vec<SerializableEdge>,
}

impl<P> DAG<P> {
    /// Convert the DAG to a serializable intermediate representation.
    ///
    /// The `payload_fn` closure converts each node's payload to an optional JSON value.
    /// Pass `|_| None` to omit payloads.
    pub fn to_serializable<F>(&self, payload_fn: F) -> SerializableGraph
    where
        F: Fn(&P) -> Option<serde_json::Value>,
    {
        let nodes: Vec<SerializableNode> = self
            .graph
            .node_identifiers()
            .map(|idx| {
                let node = &self.graph[idx];
                SerializableNode {
                    name: node.name.clone(),
                    payload: payload_fn(&node.payload),
                }
            })
            .collect();

        let edges: Vec<SerializableEdge> = self
            .graph
            .edge_references()
            .map(|e| {
                let data = e.weight();
                SerializableEdge {
                    from: self.graph[e.source()].name.clone(),
                    to: self.graph[e.target()].name.clone(),
                    weight: data.weight,
                    label: data.label.clone(),
                }
            })
            .collect();

        SerializableGraph { nodes, edges }
    }

    /// Reconstruct a DAG from a serializable intermediate representation.
    ///
    /// The `payload_fn` closure converts each node's optional JSON payload to the payload type `P`.
    pub fn from_serializable<F>(
        desc: SerializableGraph,
        payload_fn: F,
    ) -> Result<Self, DagronError>
    where
        F: Fn(Option<&serde_json::Value>) -> P,
    {
        let mut dag = DAG::new();
        for node in &desc.nodes {
            let payload = payload_fn(node.payload.as_ref());
            dag.add_node(node.name.clone(), payload)?;
        }
        for edge in &desc.edges {
            dag.add_edge(&edge.from, &edge.to, Some(edge.weight), edge.label.clone())?;
        }
        Ok(dag)
    }

    /// Serialize the DAG to a pretty-printed JSON string.
    pub fn to_json<F>(&self, payload_fn: F) -> Result<String, DagronError>
    where
        F: Fn(&P) -> Option<serde_json::Value>,
    {
        let sg = self.to_serializable(payload_fn);
        serde_json::to_string_pretty(&sg)
            .map_err(|e| DagronError::Graph(format!("JSON serialization failed: {e}")))
    }

    /// Deserialize a DAG from a JSON string.
    pub fn from_json<F>(json: &str, payload_fn: F) -> Result<Self, DagronError>
    where
        F: Fn(Option<&serde_json::Value>) -> P,
    {
        let sg: SerializableGraph = serde_json::from_str(json)
            .map_err(|e| DagronError::Graph(format!("JSON deserialization failed: {e}")))?;
        Self::from_serializable(sg, payload_fn)
    }

    /// Export the DAG in Graphviz DOT format.
    pub fn to_dot(&self) -> String {
        self.to_dot_with(|_, _| None)
    }

    /// Export the DAG in Graphviz DOT format with custom node attributes.
    ///
    /// The `node_attrs` closure receives the node name and payload, and returns
    /// an optional attribute string (e.g. `"shape=box, color=red"`).
    pub fn to_dot_with<F>(&self, node_attrs: F) -> String
    where
        F: Fn(&str, &P) -> Option<String>,
    {
        let mut out = String::from("digraph {\n");
        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            let name = &node.name;
            if let Some(attrs) = node_attrs(name, &node.payload) {
                let _ = writeln!(out, "    \"{name}\" [{attrs}];");
            } else {
                let _ = writeln!(out, "    \"{name}\";");
            }
        }
        for e in self.graph.edge_references() {
            let from = &self.graph[e.source()].name;
            let to = &self.graph[e.target()].name;
            let data = e.weight();
            if let Some(ref label) = data.label {
                let _ = writeln!(out, "    \"{from}\" -> \"{to}\" [label=\"{label}\"];");
            } else if (data.weight - 1.0).abs() > f64::EPSILON {
                let _ = writeln!(out, "    \"{from}\" -> \"{to}\" [label=\"{}\"];", data.weight);
            } else {
                let _ = writeln!(out, "    \"{from}\" -> \"{to}\";");
            }
        }
        out.push('}');
        out
    }

    /// Export the DAG in Mermaid diagram format.
    pub fn to_mermaid(&self) -> String {
        let mut out = String::from("graph TD\n");
        // Emit all nodes
        for idx in self.graph.node_identifiers() {
            let name = &self.graph[idx].name;
            let _ = writeln!(out, "    {name}[\"{name}\"]");
        }
        // Emit all edges
        for e in self.graph.edge_references() {
            let from = &self.graph[e.source()].name;
            let to = &self.graph[e.target()].name;
            let data = e.weight();
            if let Some(ref label) = data.label {
                let _ = writeln!(out, "    {from} -->|\"{label}\"| {to}");
            } else if (data.weight - 1.0).abs() > f64::EPSILON {
                let _ = writeln!(out, "    {from} -->|\"{}\"| {to}", data.weight);
            } else {
                let _ = writeln!(out, "    {from} --> {to}");
            }
        }
        out
    }
}
