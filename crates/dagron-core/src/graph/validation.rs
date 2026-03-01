use crate::algorithms;
use crate::errors::DagronError;

use super::DAG;

impl<P> DAG<P> {
    /// Validate the graph has no cycles.
    ///
    /// Returns true if valid, or DagronError::Cycle with details.
    pub fn validate(&self) -> Result<bool, DagronError> {
        let cycles = algorithms::find_cycles(&self.graph);

        if cycles.is_empty() {
            Ok(true)
        } else {
            let cycle_descriptions: Vec<String> = cycles
                .iter()
                .map(|cycle| {
                    let names: Vec<String> = cycle
                        .iter()
                        .map(|&idx| self.graph[idx].name.clone())
                        .collect();
                    names.join(" -> ")
                })
                .collect();
            Err(DagronError::Cycle(format!(
                "Graph contains {} cycle(s): [{}]",
                cycles.len(),
                cycle_descriptions.join(", ")
            )))
        }
    }
}
