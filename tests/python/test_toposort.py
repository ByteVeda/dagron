import pytest
from dagron import DAG


def is_valid_topological_order(dag, order):
    """Verify that every edge (u, v) has u before v in the order."""
    names = [n.name for n in order]
    pos = {name: i for i, name in enumerate(names)}
    for from_name, to_name in dag.edges():
        if pos[from_name] >= pos[to_name]:
            return False
    return True


class TestTopologicalSort:
    def test_linear(self, linear_dag):
        order = linear_dag.topological_sort()
        names = [n.name for n in order]
        assert names == ["a", "b", "c"]

    def test_diamond(self, diamond_dag):
        order = diamond_dag.topological_sort()
        assert is_valid_topological_order(diamond_dag, order)
        assert len(order) == 4

    def test_empty(self, empty_dag):
        assert empty_dag.topological_sort() == []

    def test_single_node(self, empty_dag):
        empty_dag.add_node("a")
        order = empty_dag.topological_sort()
        assert len(order) == 1
        assert order[0].name == "a"

    def test_disconnected(self, empty_dag):
        empty_dag.add_nodes(["a", "b", "c"])
        order = empty_dag.topological_sort()
        assert len(order) == 3


class TestTopologicalSortDFS:
    def test_linear(self, linear_dag):
        order = linear_dag.topological_sort_dfs()
        names = [n.name for n in order]
        assert names == ["a", "b", "c"]

    def test_diamond(self, diamond_dag):
        order = diamond_dag.topological_sort_dfs()
        assert is_valid_topological_order(diamond_dag, order)
        assert len(order) == 4

    def test_empty(self, empty_dag):
        assert empty_dag.topological_sort_dfs() == []


class TestTopologicalLevels:
    def test_linear(self, linear_dag):
        levels = linear_dag.topological_levels()
        assert len(levels) == 3
        assert [n.name for n in levels[0]] == ["a"]
        assert [n.name for n in levels[1]] == ["b"]
        assert [n.name for n in levels[2]] == ["c"]

    def test_diamond(self, diamond_dag):
        levels = diamond_dag.topological_levels()
        assert len(levels) == 3
        assert [n.name for n in levels[0]] == ["a"]
        level1_names = {n.name for n in levels[1]}
        assert level1_names == {"b", "c"}
        assert [n.name for n in levels[2]] == ["d"]

    def test_empty(self, empty_dag):
        assert empty_dag.topological_levels() == []

    def test_wide(self, empty_dag):
        """Many roots, one sink."""
        empty_dag.add_nodes(["r1", "r2", "r3", "sink"])
        empty_dag.add_edges([("r1", "sink"), ("r2", "sink"), ("r3", "sink")])
        levels = empty_dag.topological_levels()
        assert len(levels) == 2
        assert len(levels[0]) == 3
        assert len(levels[1]) == 1
