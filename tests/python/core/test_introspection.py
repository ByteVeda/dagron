import pytest

from dagron import NodeNotFoundError


class TestNodeQueries:
    def test_has_node(self, linear_dag):
        assert linear_dag.has_node("a")
        assert not linear_dag.has_node("z")

    def test_has_edge(self, linear_dag):
        assert linear_dag.has_edge("a", "b")
        assert not linear_dag.has_edge("a", "c")
        assert not linear_dag.has_edge("b", "a")

    def test_node_count(self, diamond_dag):
        assert diamond_dag.node_count() == 4

    def test_edge_count(self, diamond_dag):
        assert diamond_dag.edge_count() == 4


class TestPayloadAndMetadata:
    def test_get_set_payload(self, empty_dag):
        empty_dag.add_node("a", payload="original")
        assert empty_dag.get_payload("a") == "original"
        empty_dag.set_payload("a", "updated")
        assert empty_dag.get_payload("a") == "updated"

    def test_payload_none(self, empty_dag):
        empty_dag.add_node("a")
        assert empty_dag.get_payload("a") is None

    def test_get_payload_missing_node(self, empty_dag):
        with pytest.raises(NodeNotFoundError):
            empty_dag.get_payload("z")


class TestPredecessorsSuccessors:
    def test_predecessors(self, diamond_dag):
        preds = diamond_dag.predecessors("d")
        names = {n.name for n in preds}
        assert names == {"b", "c"}

    def test_successors(self, diamond_dag):
        succs = diamond_dag.successors("a")
        names = {n.name for n in succs}
        assert names == {"b", "c"}

    def test_predecessors_root(self, diamond_dag):
        assert diamond_dag.predecessors("a") == []

    def test_successors_leaf(self, diamond_dag):
        assert diamond_dag.successors("d") == []

    def test_predecessors_missing_node(self, empty_dag):
        with pytest.raises(NodeNotFoundError):
            empty_dag.predecessors("z")


class TestAncestorsDescendants:
    def test_ancestors(self, diamond_dag):
        anc = diamond_dag.ancestors("d")
        names = {n.name for n in anc}
        assert names == {"a", "b", "c"}

    def test_descendants(self, diamond_dag):
        desc = diamond_dag.descendants("a")
        names = {n.name for n in desc}
        assert names == {"b", "c", "d"}

    def test_ancestors_of_root(self, diamond_dag):
        assert diamond_dag.ancestors("a") == []

    def test_descendants_of_leaf(self, diamond_dag):
        assert diamond_dag.descendants("d") == []


class TestDegrees:
    def test_in_degree(self, diamond_dag):
        assert diamond_dag.in_degree("a") == 0
        assert diamond_dag.in_degree("d") == 2

    def test_out_degree(self, diamond_dag):
        assert diamond_dag.out_degree("a") == 2
        assert diamond_dag.out_degree("d") == 0

    def test_degree_missing_node(self, empty_dag):
        with pytest.raises(NodeNotFoundError):
            empty_dag.in_degree("z")


class TestRootsAndLeaves:
    def test_roots(self, diamond_dag):
        roots = diamond_dag.roots()
        names = {n.name for n in roots}
        assert names == {"a"}

    def test_leaves(self, diamond_dag):
        leaves = diamond_dag.leaves()
        names = {n.name for n in leaves}
        assert names == {"d"}

    def test_multiple_roots(self, empty_dag):
        empty_dag.add_nodes(["a", "b", "c"])
        empty_dag.add_edge("a", "c")
        empty_dag.add_edge("b", "c")
        roots = empty_dag.roots()
        names = {n.name for n in roots}
        assert names == {"a", "b"}

    def test_nodes_list(self, diamond_dag):
        nodes = diamond_dag.nodes()
        names = {n.name for n in nodes}
        assert names == {"a", "b", "c", "d"}
