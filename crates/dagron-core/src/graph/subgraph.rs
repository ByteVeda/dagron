use crate::algorithms::subgraph::{depth_neighborhood, SubgraphDirection};
use crate::errors::DagronError;

use super::DAG;

impl<P: Clone> DAG<P> {
    /// Induced subgraph: only the named nodes + edges between them.
    pub fn subgraph(&self, nodes: &[&str]) -> Result<DAG<P>, DagronError> {
        let mut new_dag = DAG::new();

        for &name in nodes {
            let idx = self.resolve_name(name)?;
            let payload = self.graph[idx].payload.clone();
            new_dag.add_node(name.to_string(), payload)?;
        }

        // Add edges where both endpoints are in the subgraph
        use petgraph::visit::EdgeRef;
        for &name in nodes {
            let idx = self.resolve_name(name)?;
            for edge in self.graph.edges(idx) {
                let target = edge.target();
                let target_name = &self.graph[target].name;
                if new_dag.has_node(target_name) {
                    let edge_data = edge.weight();
                    new_dag.add_edge(
                        name,
                        target_name,
                        Some(edge_data.weight),
                        edge_data.label.clone(),
                    )?;
                }
            }
        }

        Ok(new_dag)
    }

    /// Depth-based subgraph: nodes within `depth` hops of `root`.
    pub fn subgraph_by_depth(
        &self,
        root: &str,
        depth: usize,
        direction: SubgraphDirection,
    ) -> Result<DAG<P>, DagronError> {
        let root_idx = self.resolve_name(root)?;
        let neighborhood = depth_neighborhood(&self.graph, root_idx, depth, direction);

        // Collect names
        let names: Vec<String> = neighborhood
            .iter()
            .map(|&idx| self.graph[idx].name.clone())
            .collect();
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();

        self.subgraph(&name_refs)
    }
}
