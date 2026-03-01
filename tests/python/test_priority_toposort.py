from dagron import DAG


def is_valid_topological_order(dag, order):
    """Verify that every edge (u, v) has u before v in the order."""
    names = [n.name for n in order]
    pos = {name: i for i, name in enumerate(names)}
    for from_name, to_name in dag.edges():
        if pos[from_name] >= pos[to_name]:
            return False
    return True


class TestTopologicalSortPriority:
    def test_no_priorities(self, diamond_dag):
        order = diamond_dag.topological_sort_priority()
        assert is_valid_topological_order(diamond_dag, order)
        assert len(order) == 4

    def test_higher_priority_first(self, diamond_dag):
        order = diamond_dag.topological_sort_priority({"c": 10.0, "b": 1.0})
        names = [n.name for n in order]
        assert names[0] == "a"
        c_pos = names.index("c")
        b_pos = names.index("b")
        assert c_pos < b_pos

    def test_respects_dependencies(self, linear_dag):
        order = linear_dag.topological_sort_priority({"c": 100.0})
        names = [n.name for n in order]
        assert names == ["a", "b", "c"]

    def test_empty(self, empty_dag):
        order = empty_dag.topological_sort_priority()
        assert order == []

    def test_multiple_roots(self, empty_dag):
        empty_dag.add_nodes(["x", "y", "z", "sink"])
        empty_dag.add_edges([("x", "sink"), ("y", "sink"), ("z", "sink")])
        order = empty_dag.topological_sort_priority({"z": 5.0, "x": 3.0, "y": 1.0})
        names = [n.name for n in order]
        assert names[0] == "z"
        assert names[1] == "x"
        assert names[2] == "y"

    def test_negative_priorities(self, empty_dag):
        empty_dag.add_nodes(["a", "b", "c"])
        order = empty_dag.topological_sort_priority({"a": -10.0, "b": 0.0, "c": 10.0})
        names = [n.name for n in order]
        assert names == ["c", "b", "a"]

    def test_equal_priority_alphabetical(self, diamond_dag):
        order = diamond_dag.topological_sort_priority({"b": 5.0, "c": 5.0})
        names = [n.name for n in order]
        # Equal priority: alphabetical tiebreak
        b_pos = names.index("b")
        c_pos = names.index("c")
        assert b_pos < c_pos


class TestTopologicalLevelsPriority:
    def test_sorted_within_level(self, diamond_dag):
        levels = diamond_dag.topological_levels_priority({"c": 10.0, "b": 1.0})
        assert len(levels) == 3
        assert [n.name for n in levels[0]] == ["a"]
        assert [n.name for n in levels[1]] == ["c", "b"]
        assert [n.name for n in levels[2]] == ["d"]

    def test_default_alphabetical(self, diamond_dag):
        levels = diamond_dag.topological_levels_priority()
        assert [n.name for n in levels[1]] == ["b", "c"]

    def test_empty(self, empty_dag):
        assert empty_dag.topological_levels_priority() == []
