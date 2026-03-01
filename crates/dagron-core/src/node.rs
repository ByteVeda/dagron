use std::fmt;

/// A unique identifier for a node in a DAG.
///
/// **Note:** The `index` field corresponds to the internal `petgraph` node index
/// and is only valid for the lifetime of that node in the graph. After a node is
/// removed, its index may be reused by a subsequently added node. Do not persist
/// or compare `index` values across graph mutations that involve removals.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId {
    pub index: u32,
    pub name: String,
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
