use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A snapshot identifier for a node in a DAG.
///
/// `NodeId` is returned by enumeration methods (`nodes()`, `successors()`, …).
/// It carries the node's `name` plus its current `petgraph` index. The `index`
/// field is a *snapshot*: after a node is removed, its index may be reused by
/// a subsequently added node, so do not persist or compare `index` values
/// across graph mutations that involve removals.
///
/// For a stable, persistent handle that survives mutations, use [`NodeRef`].
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

/// A stable, persistent handle to a node.
///
/// `NodeRef` is returned by [`DAG::add_node`] and remains valid as long as the
/// node it points to has not been removed (or removed-and-readded with the
/// same name, which produces a fresh `epoch`). Use it anywhere a `&str` name
/// is accepted; resolution is O(1) and detects stale references.
///
/// `NodeRef` clones cheaply: the name is reference-counted via `Arc<str>`.
#[derive(Debug, Clone)]
pub struct NodeRef {
    pub name: Arc<str>,
    pub epoch: u64,
}

impl NodeRef {
    /// Construct a `NodeRef` directly. Prefer obtaining one from
    /// [`DAG::add_node`] or [`DAG::node_ref`].
    pub fn new(name: impl Into<Arc<str>>, epoch: u64) -> Self {
        NodeRef {
            name: name.into(),
            epoch,
        }
    }

    /// Borrow the name as a string slice.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The creation epoch this ref was minted with.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl PartialEq for NodeRef {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.name == other.name
    }
}

impl Eq for NodeRef {}

impl Hash for NodeRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.epoch.hash(state);
    }
}

impl fmt::Display for NodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
