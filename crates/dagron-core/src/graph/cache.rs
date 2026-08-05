use crate::node::NodeId;

pub(crate) struct DagCache {
    gen: u64,
    hits: u64,
    misses: u64,
    roots: Option<Vec<NodeId>>,
    leaves: Option<Vec<NodeId>>,
    topo_sort: Option<Vec<NodeId>>,
    topo_sort_dfs: Option<Vec<NodeId>>,
    topo_levels: Option<Vec<Vec<NodeId>>>,
}

impl DagCache {
    pub fn new() -> Self {
        DagCache {
            gen: 0,
            hits: 0,
            misses: 0,
            roots: None,
            leaves: None,
            topo_sort: None,
            topo_sort_dfs: None,
            topo_levels: None,
        }
    }

    pub fn invalidate(&mut self) {
        self.roots = None;
        self.leaves = None;
        self.topo_sort = None;
        self.topo_sort_dfs = None;
        self.topo_levels = None;
    }

    pub fn gen(&self) -> u64 {
        self.gen
    }

    pub fn set_gen(&mut self, gen: u64) {
        self.gen = gen;
    }

    pub fn hits(&self) -> u64 {
        self.hits
    }

    pub fn misses(&self) -> u64 {
        self.misses
    }

    pub fn record_hit(&mut self) {
        self.hits += 1;
    }

    pub fn record_miss(&mut self) {
        self.misses += 1;
    }

    pub fn size(&self) -> usize {
        let mut count = 0;
        if self.roots.is_some() {
            count += 1;
        }
        if self.leaves.is_some() {
            count += 1;
        }
        if self.topo_sort.is_some() {
            count += 1;
        }
        if self.topo_sort_dfs.is_some() {
            count += 1;
        }
        if self.topo_levels.is_some() {
            count += 1;
        }
        count
    }

    pub fn roots(&self) -> Option<&Vec<NodeId>> {
        self.roots.as_ref()
    }

    pub fn set_roots(&mut self, roots: Vec<NodeId>) {
        self.roots = Some(roots);
    }

    pub fn leaves(&self) -> Option<&Vec<NodeId>> {
        self.leaves.as_ref()
    }

    pub fn set_leaves(&mut self, leaves: Vec<NodeId>) {
        self.leaves = Some(leaves);
    }

    pub fn topo_sort(&self) -> Option<&Vec<NodeId>> {
        self.topo_sort.as_ref()
    }

    pub fn set_topo_sort(&mut self, result: Vec<NodeId>) {
        self.topo_sort = Some(result);
    }

    pub fn topo_sort_dfs(&self) -> Option<&Vec<NodeId>> {
        self.topo_sort_dfs.as_ref()
    }

    pub fn set_topo_sort_dfs(&mut self, result: Vec<NodeId>) {
        self.topo_sort_dfs = Some(result);
    }

    pub fn topo_levels(&self) -> Option<&Vec<Vec<NodeId>>> {
        self.topo_levels.as_ref()
    }

    pub fn set_topo_levels(&mut self, result: Vec<Vec<NodeId>>) {
        self.topo_levels = Some(result);
    }
}
