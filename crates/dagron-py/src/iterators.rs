use pyo3::prelude::*;

use crate::node::PyNodeId;

/// A lazy iterator over nodes that defers PyNodeId creation to __next__.
#[pyclass(name = "NodeIterator")]
pub struct PyNodeIterator {
    items: Vec<(u32, String)>,
    index: usize,
}

impl PyNodeIterator {
    pub fn new(items: Vec<(u32, String)>) -> Self {
        PyNodeIterator { items, index: 0 }
    }
}

#[pymethods]
impl PyNodeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyNodeId> {
        if self.index < self.items.len() {
            let (idx, name) = &self.items[self.index];
            self.index += 1;
            Some(PyNodeId {
                index: *idx,
                name: name.clone(),
            })
        } else {
            None
        }
    }

    fn __len__(&self) -> usize {
        self.items.len()
    }

    /// Collect all remaining items into a list.
    fn collect(&mut self) -> Vec<PyNodeId> {
        let remaining: Vec<PyNodeId> = self.items[self.index..]
            .iter()
            .map(|(idx, name)| PyNodeId {
                index: *idx,
                name: name.clone(),
            })
            .collect();
        self.index = self.items.len();
        remaining
    }

    fn __repr__(&self) -> String {
        format!(
            "NodeIterator(total={}, remaining={})",
            self.items.len(),
            self.items.len() - self.index,
        )
    }
}

/// A lazy iterator over topological levels.
/// Each call to __next__ returns a list of PyNodeId for one level.
#[pyclass(name = "NodeLevelIterator")]
pub struct PyNodeLevelIterator {
    levels: Vec<Vec<(u32, String)>>,
    index: usize,
}

impl PyNodeLevelIterator {
    pub fn new(levels: Vec<Vec<(u32, String)>>) -> Self {
        PyNodeLevelIterator { levels, index: 0 }
    }
}

#[pymethods]
impl PyNodeLevelIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<Vec<PyNodeId>> {
        if self.index < self.levels.len() {
            let level = &self.levels[self.index];
            self.index += 1;
            Some(
                level
                    .iter()
                    .map(|(idx, name)| PyNodeId {
                        index: *idx,
                        name: name.clone(),
                    })
                    .collect(),
            )
        } else {
            None
        }
    }

    fn __len__(&self) -> usize {
        self.levels.len()
    }

    /// Collect all remaining levels into a list of lists.
    fn collect(&mut self) -> Vec<Vec<PyNodeId>> {
        let remaining: Vec<Vec<PyNodeId>> = self.levels[self.index..]
            .iter()
            .map(|level| {
                level
                    .iter()
                    .map(|(idx, name)| PyNodeId {
                        index: *idx,
                        name: name.clone(),
                    })
                    .collect()
            })
            .collect();
        self.index = self.levels.len();
        remaining
    }

    fn __repr__(&self) -> String {
        format!(
            "NodeLevelIterator(total_levels={}, remaining={})",
            self.levels.len(),
            self.levels.len() - self.index,
        )
    }
}
