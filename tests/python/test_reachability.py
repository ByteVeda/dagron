"""Tests for reachability queries."""

import pytest

from dagron import DAG, NodeNotFoundError, ReachabilityIndex


def diamond_dag():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_node("d")
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    return dag


class TestReachabilityIndex:
    def test_build(self):
        dag = diamond_dag()
        idx = dag.build_reachability_index()
        assert isinstance(idx, ReachabilityIndex)
        assert idx.node_count() == 4

    def test_can_reach_forward(self):
        dag = diamond_dag()
        idx = dag.build_reachability_index()
        assert idx.can_reach("a", "d")
        assert idx.can_reach("a", "b")
        assert idx.can_reach("b", "d")

    def test_can_reach_reverse_false(self):
        dag = diamond_dag()
        idx = dag.build_reachability_index()
        assert not idx.can_reach("d", "a")
        assert not idx.can_reach("b", "c")

    def test_can_reach_self(self):
        dag = diamond_dag()
        idx = dag.build_reachability_index()
        assert idx.can_reach("a", "a")

    def test_reachable_from(self):
        dag = diamond_dag()
        idx = dag.build_reachability_index()
        reachable = idx.reachable_from("a")
        assert set(reachable) == {"b", "c", "d"}

    def test_ancestors_of(self):
        dag = diamond_dag()
        idx = dag.build_reachability_index()
        ancestors = idx.ancestors_of("d")
        assert set(ancestors) == {"a", "b", "c"}

    def test_nonexistent_node(self):
        dag = diamond_dag()
        idx = dag.build_reachability_index()
        with pytest.raises(NodeNotFoundError):
            idx.can_reach("z", "a")

    def test_empty_graph(self):
        dag = DAG()
        idx = dag.build_reachability_index()
        assert idx.node_count() == 0


class TestIsAncestor:
    def test_true(self):
        dag = diamond_dag()
        assert dag.is_ancestor("a", "d")
        assert dag.is_ancestor("b", "d")

    def test_false(self):
        dag = diamond_dag()
        assert not dag.is_ancestor("d", "a")
        assert not dag.is_ancestor("b", "c")

    def test_self(self):
        dag = diamond_dag()
        assert dag.is_ancestor("a", "a")

    def test_nonexistent(self):
        dag = diamond_dag()
        with pytest.raises(NodeNotFoundError):
            dag.is_ancestor("z", "a")
