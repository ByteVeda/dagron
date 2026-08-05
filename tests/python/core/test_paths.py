"""Tests for path queries."""

import pytest

from dagron import DAG, NodeNotFoundError


class TestAllPaths:
    def test_linear(self, linear_dag):
        paths = linear_dag.all_paths("a", "c")
        assert len(paths) == 1
        names = [n.name for n in paths[0]]
        assert names == ["a", "b", "c"]

    def test_diamond(self, diamond_dag):
        paths = diamond_dag.all_paths("a", "d")
        assert len(paths) == 2

    def test_with_limit(self, diamond_dag):
        paths = diamond_dag.all_paths("a", "d", limit=1)
        assert len(paths) == 1

    def test_no_path(self, linear_dag):
        paths = linear_dag.all_paths("c", "a")
        assert len(paths) == 0

    def test_same_node(self, linear_dag):
        paths = linear_dag.all_paths("b", "b")
        assert len(paths) == 1
        assert len(paths[0]) == 1

    def test_nonexistent_node(self, linear_dag):
        with pytest.raises(NodeNotFoundError):
            linear_dag.all_paths("a", "z")


class TestShortestPath:
    def test_linear(self, linear_dag):
        path = linear_dag.shortest_path("a", "c")
        assert path is not None
        names = [n.name for n in path]
        assert names == ["a", "b", "c"]

    def test_diamond_with_shortcut(self, diamond_dag):
        diamond_dag.add_edge("a", "d")
        path = diamond_dag.shortest_path("a", "d")
        assert path is not None
        assert len(path) == 2

    def test_no_path(self, linear_dag):
        assert linear_dag.shortest_path("c", "a") is None

    def test_same_node(self, linear_dag):
        path = linear_dag.shortest_path("b", "b")
        assert path is not None
        assert len(path) == 1

    def test_disconnected(self):
        dag = DAG()
        dag.add_node("a")
        dag.add_node("b")
        assert dag.shortest_path("a", "b") is None


class TestLongestPath:
    def test_linear(self, linear_dag):
        result = linear_dag.longest_path("a", "c")
        assert result is not None
        path, cost = result
        names = [n.name for n in path]
        assert names == ["a", "b", "c"]
        assert cost == 3.0

    def test_diamond_weighted(self, diamond_dag):
        costs = {"a": 1.0, "b": 10.0, "c": 2.0, "d": 1.0}
        result = diamond_dag.longest_path("a", "d", costs=costs)
        assert result is not None
        path, cost = result
        names = [n.name for n in path]
        assert names == ["a", "b", "d"]
        assert cost == 12.0

    def test_no_path(self, linear_dag):
        assert linear_dag.longest_path("c", "a") is None

    def test_default_costs(self, linear_dag):
        result = linear_dag.longest_path("a", "c")
        assert result is not None
        _, cost = result
        assert cost == 3.0
