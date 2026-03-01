use std::fmt::Write;
use std::io;

use petgraph::visit::{EdgeRef, IntoEdgeReferences, IntoNodeIdentifiers};
use serde::{Deserialize, Serialize};

use crate::errors::DagronError;

use super::DAG;

/// A writer that counts bytes without storing them.
struct CountingWriter {
    count: usize,
}

impl CountingWriter {
    fn new() -> Self {
        CountingWriter { count: 0 }
    }
}

impl io::Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.count += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

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

/// Bincode-friendly node: payload stored as a JSON string to avoid
/// bincode's limitation with `serde_json::Value::deserialize_any`.
#[derive(Serialize, Deserialize)]
struct BincodeNode {
    name: String,
    payload_json: Option<String>,
}

/// Bincode-friendly edge.
#[derive(Serialize, Deserialize)]
struct BincodeEdge {
    from: String,
    to: String,
    weight: f64,
    label: Option<String>,
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
    pub fn from_serializable<F>(desc: SerializableGraph, payload_fn: F) -> Result<Self, DagronError>
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
                let _ = writeln!(
                    out,
                    "    \"{from}\" -> \"{to}\" [label=\"{}\"];",
                    data.weight
                );
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

    /// Serialize the DAG to a streaming binary format.
    ///
    /// Format: node_count (u64), then each node (name + payload as JSON string),
    /// then edge_count (u64), then each edge — all encoded via bincode individually.
    /// Payloads are stored as `Option<String>` (JSON-encoded) to avoid bincode's
    /// limitation with `serde_json::Value::deserialize_any`.
    pub fn to_bincode_writer<W, F>(&self, mut writer: W, payload_fn: F) -> Result<(), DagronError>
    where
        W: io::Write,
        F: Fn(&P) -> Option<serde_json::Value>,
    {
        // Stream nodes directly — no intermediate Vec
        let node_count = self.graph.node_count() as u64;
        bincode::serialize_into(&mut writer, &node_count)
            .map_err(|e| DagronError::Graph(format!("Bincode write error: {e}")))?;

        for idx in self.graph.node_identifiers() {
            let node = &self.graph[idx];
            let bc_node = BincodeNode {
                name: node.name.clone(),
                payload_json: payload_fn(&node.payload)
                    .as_ref()
                    .map(|v| serde_json::to_string(v).unwrap_or_default()),
            };
            bincode::serialize_into(&mut writer, &bc_node)
                .map_err(|e| DagronError::Graph(format!("Bincode write error: {e}")))?;
        }

        // Stream edges directly — no intermediate Vec
        let edge_count = self.graph.edge_count() as u64;
        bincode::serialize_into(&mut writer, &edge_count)
            .map_err(|e| DagronError::Graph(format!("Bincode write error: {e}")))?;

        for e in self.graph.edge_references() {
            let data = e.weight();
            let bc_edge = BincodeEdge {
                from: self.graph[e.source()].name.clone(),
                to: self.graph[e.target()].name.clone(),
                weight: data.weight,
                label: data.label.clone(),
            };
            bincode::serialize_into(&mut writer, &bc_edge)
                .map_err(|e| DagronError::Graph(format!("Bincode write error: {e}")))?;
        }

        Ok(())
    }

    /// Deserialize a DAG from a streaming binary format.
    ///
    /// Reads nodes and edges one-by-one, building the DAG incrementally
    /// without materializing a full intermediate SerializableGraph.
    pub fn from_bincode_reader<R, F>(mut reader: R, payload_fn: F) -> Result<Self, DagronError>
    where
        R: io::Read,
        F: Fn(Option<&serde_json::Value>) -> P,
    {
        let node_count: u64 = bincode::deserialize_from(&mut reader)
            .map_err(|e| DagronError::Graph(format!("Bincode read error: {e}")))?;

        let mut dag = DAG::new();

        for _ in 0..node_count {
            let bc_node: BincodeNode = bincode::deserialize_from(&mut reader)
                .map_err(|e| DagronError::Graph(format!("Bincode read error: {e}")))?;
            let json_val: Option<serde_json::Value> = bc_node
                .payload_json
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok());
            let payload = payload_fn(json_val.as_ref());
            dag.add_node(bc_node.name, payload)?;
        }

        let edge_count: u64 = bincode::deserialize_from(&mut reader)
            .map_err(|e| DagronError::Graph(format!("Bincode read error: {e}")))?;

        for _ in 0..edge_count {
            let bc_edge: BincodeEdge = bincode::deserialize_from(&mut reader)
                .map_err(|e| DagronError::Graph(format!("Bincode read error: {e}")))?;
            dag.add_edge(
                &bc_edge.from,
                &bc_edge.to,
                Some(bc_edge.weight),
                bc_edge.label,
            )?;
        }

        Ok(dag)
    }

    /// Calculate the serialized bincode size without allocating.
    /// Useful for pre-allocating a buffer of the exact size.
    pub fn bincode_size<F>(&self, payload_fn: F) -> Result<usize, DagronError>
    where
        F: Fn(&P) -> Option<serde_json::Value>,
    {
        let mut counter = CountingWriter::new();
        self.to_bincode_writer(&mut counter, payload_fn)?;
        Ok(counter.count)
    }

    /// Serialize the DAG to a binary (bincode) byte vector.
    ///
    /// Delegates to the streaming writer with a `Vec<u8>` buffer.
    pub fn to_bincode<F>(&self, payload_fn: F) -> Result<Vec<u8>, DagronError>
    where
        F: Fn(&P) -> Option<serde_json::Value>,
    {
        let mut buf = Vec::new();
        self.to_bincode_writer(&mut buf, payload_fn)?;
        Ok(buf)
    }

    /// Deserialize a DAG from a binary (bincode) byte slice.
    ///
    /// Delegates to the streaming reader with a cursor.
    pub fn from_bincode<F>(bytes: &[u8], payload_fn: F) -> Result<Self, DagronError>
    where
        F: Fn(Option<&serde_json::Value>) -> P,
    {
        Self::from_bincode_reader(io::Cursor::new(bytes), payload_fn)
    }

    /// Write the DAG to a file in bincode format using BufWriter.
    pub fn to_bincode_file<F>(
        &self,
        path: &std::path::Path,
        payload_fn: F,
    ) -> Result<(), DagronError>
    where
        F: Fn(&P) -> Option<serde_json::Value>,
    {
        let file = std::fs::File::create(path).map_err(|e| {
            DagronError::Graph(format!("Failed to create file {}: {e}", path.display()))
        })?;
        let writer = io::BufWriter::new(file);
        self.to_bincode_writer(writer, payload_fn)
    }

    /// Load a DAG from a bincode file using memory-mapped I/O.
    ///
    /// The file is memory-mapped for efficient reading, then the DAG is
    /// fully deserialized (data copied). The mmap is dropped after loading.
    pub fn from_bincode_file<F>(path: &std::path::Path, payload_fn: F) -> Result<Self, DagronError>
    where
        F: Fn(Option<&serde_json::Value>) -> P,
    {
        let file = std::fs::File::open(path).map_err(|e| {
            DagronError::Graph(format!("Failed to open file {}: {e}", path.display()))
        })?;
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            DagronError::Graph(format!("Failed to mmap file {}: {e}", path.display()))
        })?;
        Self::from_bincode_reader(io::Cursor::new(&mmap[..]), payload_fn)
    }
}
