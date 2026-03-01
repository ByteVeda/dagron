#[derive(Debug, Clone, thiserror::Error)]
pub enum DagronError {
    #[error("Cycle detected: {0}")]
    Cycle(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Duplicate node: {0}")]
    DuplicateNode(String),

    #[error("Edge not found: {0} -> {1}")]
    EdgeNotFound(String, String),

    #[error("Graph error: {0}")]
    Graph(String),
}
