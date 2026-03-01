import pytest

from dagron import (
    CycleError,
    DuplicateNodeError,
    EdgeNotFoundError,
    NodeId,
    NodeNotFoundError,
)


class TestAddNode:
    def test_add_single_node(self, empty_dag):
        node = empty_dag.add_node("a")
        assert isinstance(node, NodeId)
        assert node.name == "a"
        assert empty_dag.node_count() == 1

    def test_add_node_with_payload(self, empty_dag):
        payload = {"key": "value"}
        empty_dag.add_node("a", payload=payload)
        assert empty_dag.get_payload("a") == payload

    def test_add_node_with_metadata(self, empty_dag):
        meta = {"version": 1}
        empty_dag.add_node("a", metadata=meta)
        assert empty_dag.get_metadata("a") == meta

    def test_duplicate_node_raises(self, empty_dag):
        empty_dag.add_node("a")
        with pytest.raises(DuplicateNodeError):
            empty_dag.add_node("a")

    def test_add_multiple_nodes(self, empty_dag):
        nodes = empty_dag.add_nodes(["a", "b", "c"])
        assert len(nodes) == 3
        assert empty_dag.node_count() == 3
        assert all(isinstance(n, NodeId) for n in nodes)

    def test_add_nodes_with_payloads(self, empty_dag):
        nodes = empty_dag.add_nodes([("a", 1), ("b", 2)])
        assert len(nodes) == 2
        assert empty_dag.get_payload("a") == 1
        assert empty_dag.get_payload("b") == 2

    def test_add_nodes_with_metadata(self, empty_dag):
        nodes = empty_dag.add_nodes([("a", 1, {"m": True})])
        assert len(nodes) == 1
        assert empty_dag.get_metadata("a") == {"m": True}

    def test_add_nodes_duplicate_raises(self, empty_dag):
        empty_dag.add_node("a")
        with pytest.raises(DuplicateNodeError):
            empty_dag.add_nodes(["b", "a"])


class TestAddEdge:
    def test_add_edge(self, empty_dag):
        empty_dag.add_node("a")
        empty_dag.add_node("b")
        empty_dag.add_edge("a", "b")
        assert empty_dag.edge_count() == 1
        assert empty_dag.has_edge("a", "b")

    def test_add_edge_missing_node(self, empty_dag):
        empty_dag.add_node("a")
        with pytest.raises(NodeNotFoundError):
            empty_dag.add_edge("a", "nonexistent")

    def test_add_edge_cycle_raises(self, linear_dag):
        with pytest.raises(CycleError):
            linear_dag.add_edge("c", "a")

    def test_add_edge_self_loop_raises(self, empty_dag):
        empty_dag.add_node("a")
        with pytest.raises(CycleError):
            empty_dag.add_edge("a", "a")

    def test_add_edges_bulk(self, empty_dag):
        empty_dag.add_nodes(["a", "b", "c"])
        empty_dag.add_edges([("a", "b"), ("b", "c")])
        assert empty_dag.edge_count() == 2

    def test_add_edges_with_weight(self, empty_dag):
        empty_dag.add_nodes(["a", "b"])
        empty_dag.add_edges([("a", "b", 2.5)])
        assert empty_dag.edge_count() == 1


class TestRemoveNode:
    def test_remove_node(self, linear_dag):
        linear_dag.remove_node("b")
        assert not linear_dag.has_node("b")
        assert linear_dag.node_count() == 2
        assert linear_dag.edge_count() == 0  # Both edges involving b removed

    def test_remove_missing_node_raises(self, empty_dag):
        with pytest.raises(NodeNotFoundError):
            empty_dag.remove_node("nonexistent")


class TestRemoveEdge:
    def test_remove_edge(self, linear_dag):
        linear_dag.remove_edge("a", "b")
        assert not linear_dag.has_edge("a", "b")
        assert linear_dag.edge_count() == 1

    def test_remove_missing_edge_raises(self, linear_dag):
        with pytest.raises(EdgeNotFoundError):
            linear_dag.remove_edge("a", "c")

    def test_remove_edge_missing_node_raises(self, empty_dag):
        with pytest.raises(NodeNotFoundError):
            empty_dag.remove_edge("a", "b")
