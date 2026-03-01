"""Benchmarks: dagron vs networkx on equivalent DAG operations."""

from __future__ import annotations

import json

import networkx as nx
import pytest

from dagron import DAG

# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------


def _dagron_chain(n: int) -> DAG:
    dag = DAG()
    names = [f"node_{i}" for i in range(n)]
    dag.add_nodes(names)
    dag.add_edges([(names[i], names[i + 1]) for i in range(n - 1)])
    return dag


def _nx_chain(n: int) -> nx.DiGraph:
    g = nx.DiGraph()
    names = [f"node_{i}" for i in range(n)]
    g.add_nodes_from(names)
    g.add_edges_from((names[i], names[i + 1]) for i in range(n - 1))
    return g


def _dagron_wide(roots: int, depth: int) -> DAG:
    dag = DAG()
    counter = 0
    current = []
    for _ in range(roots):
        name = f"node_{counter}"
        counter += 1
        dag.add_node(name)
        current.append(name)
    for _ in range(1, depth):
        nxt = []
        for parent in current:
            name = f"node_{counter}"
            counter += 1
            dag.add_node(name)
            dag.add_edge(parent, name)
            nxt.append(name)
        current = nxt
    return dag


def _nx_wide(roots: int, depth: int) -> nx.DiGraph:
    g = nx.DiGraph()
    counter = 0
    current = []
    for _ in range(roots):
        name = f"node_{counter}"
        counter += 1
        g.add_node(name)
        current.append(name)
    for _ in range(1, depth):
        nxt = []
        for parent in current:
            name = f"node_{counter}"
            counter += 1
            g.add_node(name)
            g.add_edge(parent, name)
            nxt.append(name)
        current = nxt
    return g


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestBuildChain:
    def test_dagron_chain_10k(self, benchmark):
        benchmark(_dagron_chain, 10_000)

    def test_nx_chain_10k(self, benchmark):
        benchmark(_nx_chain, 10_000)


class TestBuildWide:
    def test_dagron_wide_10k(self, benchmark):
        benchmark(_dagron_wide, 1_000, 10)

    def test_nx_wide_10k(self, benchmark):
        benchmark(_nx_wide, 1_000, 10)


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------


class TestToposort:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self.dagron_dag = _dagron_chain(10_000)
        self.nx_dag = _nx_chain(10_000)

    def test_dagron_toposort(self, benchmark):
        benchmark(self.dagron_dag.topological_sort)

    def test_nx_toposort(self, benchmark):
        def _run():
            list(nx.topological_sort(self.nx_dag))

        benchmark(_run)


# ---------------------------------------------------------------------------
# Ancestors / descendants
# ---------------------------------------------------------------------------


class TestAncestors:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self.dagron_dag = _dagron_chain(10_000)
        self.nx_dag = _nx_chain(10_000)

    def test_dagron_ancestors(self, benchmark):
        benchmark(self.dagron_dag.ancestors, "node_5000")

    def test_nx_ancestors(self, benchmark):
        def _run():
            nx.ancestors(self.nx_dag, "node_5000")

        benchmark(_run)

    def test_dagron_descendants(self, benchmark):
        benchmark(self.dagron_dag.descendants, "node_5000")

    def test_nx_descendants(self, benchmark):
        def _run():
            nx.descendants(self.nx_dag, "node_5000")

        benchmark(_run)


# ---------------------------------------------------------------------------
# Cycle detection / validation
# ---------------------------------------------------------------------------


class TestCycleDetection:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self.dagron_dag = _dagron_chain(10_000)
        self.nx_dag = _nx_chain(10_000)

    def test_dagron_validate(self, benchmark):
        benchmark(self.dagron_dag.validate)

    def test_nx_is_dag(self, benchmark):
        benchmark(nx.is_directed_acyclic_graph, self.nx_dag)


# ---------------------------------------------------------------------------
# Serialization (JSON)
# ---------------------------------------------------------------------------


class TestSerialization:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self.dagron_dag = _dagron_chain(1_000)
        self.nx_dag = _nx_chain(1_000)

    def test_dagron_to_json(self, benchmark):
        benchmark(self.dagron_dag.to_json)

    def test_nx_to_json(self, benchmark):
        def _run():
            data = nx.node_link_data(self.nx_dag)
            json.dumps(data)

        benchmark(_run)


# ---------------------------------------------------------------------------
# Reachability (index build + batch queries)
# ---------------------------------------------------------------------------


class TestReachability:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self.dagron_dag = _dagron_chain(5_000)
        self.nx_dag = _nx_chain(5_000)

    def test_dagron_build_index(self, benchmark):
        benchmark(self.dagron_dag.build_reachability_index)

    def test_dagron_query_batch(self, benchmark):
        idx = self.dagron_dag.build_reachability_index()

        def _run():
            for i in range(0, 5_000, 500):
                idx.can_reach(f"node_{i}", "node_4999")

        benchmark(_run)

    def test_nx_has_path_batch(self, benchmark):
        def _run():
            for i in range(0, 5_000, 500):
                nx.has_path(self.nx_dag, f"node_{i}", "node_4999")

        benchmark(_run)


# ---------------------------------------------------------------------------
# BFS levels (topological levels)
# ---------------------------------------------------------------------------


class TestBFSLevels:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self.dagron_dag = _dagron_chain(10_000)
        self.nx_dag = _nx_chain(10_000)

    def test_dagron_topological_levels(self, benchmark):
        benchmark(self.dagron_dag.topological_levels)

    def test_nx_bfs_levels(self, benchmark):
        def _run():
            # Compute BFS layers from roots
            roots = [n for n, d in self.nx_dag.in_degree() if d == 0]
            if roots:
                list(nx.bfs_layers(self.nx_dag, roots))

        benchmark(_run)
