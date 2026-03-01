use petgraph::stable_graph::StableGraph;
use petgraph::Directed;

pub type Ix = u32;
pub type InternalNodeIndex = petgraph::graph::NodeIndex<Ix>;
pub type InternalEdgeIndex = petgraph::graph::EdgeIndex<Ix>;

pub struct NodeData<P = ()> {
    pub name: String,
    pub payload: P,
}

pub struct EdgeData {
    pub weight: f64,
    pub label: Option<String>,
}

pub type InternalGraph<P = ()> = StableGraph<NodeData<P>, EdgeData, Directed, Ix>;
