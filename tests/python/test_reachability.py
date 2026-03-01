"""Tests for reachability queries."""

import pytest

from dagron import DAG, NodeNotFoundError, ReachabilityIndex


class TestReachabilityIndex:
    def test_build(self, diamond_dag):
        idx = diamond_dag.build_reachability_index()
        assert isinstance(idx, ReachabilityIndex)
        assert idx.node_count() == 4

    def test_can_reach_forward(self, diamond_dag):
        idx = diamond_dag.build_reachability_index()
        assert idx.can_reach("a", "d")
        assert idx.can_reach("a", "b")
        assert idx.can_reach("b", "d")

    def test_can_reach_reverse_false(self, diamond_dag):
        idx = diamond_dag.build_reachability_index()
        assert not idx.can_reach("d", "a")
        assert not idx.can_reach("b", "c")

    def test_can_reach_self(self, diamond_dag):
        idx = diamond_dag.build_reachability_index()
        assert idx.can_reach("a", "a")

    def test_reachable_from(self, diamond_dag):
        idx = diamond_dag.build_reachability_index()
        reachable = idx.reachable_from("a")
        assert set(reachable) == {"b", "c", "d"}

    def test_ancestors_of(self, diamond_dag):
        idx = diamond_dag.build_reachability_index()
        ancestors = idx.ancestors_of("d")
        assert set(ancestors) == {"a", "b", "c"}

    def test_nonexistent_node(self, diamond_dag):
        idx = diamond_dag.build_reachability_index()
        with pytest.raises(NodeNotFoundError):
            idx.can_reach("z", "a")

    def test_empty_graph(self):
        dag = DAG()
        idx = dag.build_reachability_index()
        assert idx.node_count() == 0


class TestIsAncestor:
    def test_true(self, diamond_dag):
        assert diamond_dag.is_ancestor("a", "d")
        assert diamond_dag.is_ancestor("b", "d")

    def test_false(self, diamond_dag):
        assert not diamond_dag.is_ancestor("d", "a")
        assert not diamond_dag.is_ancestor("b", "c")

    def test_self(self, diamond_dag):
        assert diamond_dag.is_ancestor("a", "a")

    def test_nonexistent(self, diamond_dag):
        with pytest.raises(NodeNotFoundError):
            diamond_dag.is_ancestor("z", "a")
