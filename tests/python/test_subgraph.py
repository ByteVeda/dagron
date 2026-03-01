"""Tests for subgraph extraction."""

import pytest

from dagron import DAG, NodeNotFoundError


def diamond_dag():
    dag = DAG()
    dag.add_node("a", payload=1)
    dag.add_node("b", payload=2)
    dag.add_node("c", payload=3)
    dag.add_node("d", payload=4)
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    return dag


def linear_dag():
    dag = DAG()
    dag.add_node("a", payload=1)
    dag.add_node("b", payload=2)
    dag.add_node("c", payload=3)
    dag.add_node("d", payload=4)
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    dag.add_edge("c", "d")
    return dag


# --- Induced subgraph ---


class TestSubgraph:
    def test_all_nodes(self):
        dag = diamond_dag()
        sub = dag.subgraph(["a", "b", "c", "d"])
        assert sub.node_count() == 4
        assert sub.edge_count() == 4

    def test_subset(self):
        dag = diamond_dag()
        sub = dag.subgraph(["a", "b"])
        assert sub.node_count() == 2
        assert sub.edge_count() == 1
        assert sub.has_edge("a", "b")

    def test_single_node(self):
        dag = diamond_dag()
        sub = dag.subgraph(["b"])
        assert sub.node_count() == 1
        assert sub.edge_count() == 0

    def test_empty(self):
        dag = diamond_dag()
        sub = dag.subgraph([])
        assert sub.node_count() == 0

    def test_nonexistent_node(self):
        dag = diamond_dag()
        with pytest.raises(NodeNotFoundError):
            dag.subgraph(["a", "z"])

    def test_payload_preserved(self):
        dag = diamond_dag()
        sub = dag.subgraph(["a", "b"])
        assert sub.get_payload("a") == 1
        assert sub.get_payload("b") == 2


# --- Depth-based subgraph ---


class TestSubgraphByDepth:
    def test_depth_zero(self):
        dag = linear_dag()
        sub = dag.subgraph_by_depth("b", 0)
        assert sub.node_count() == 1
        assert sub.has_node("b")

    def test_forward(self):
        dag = linear_dag()
        sub = dag.subgraph_by_depth("a", 1, direction="forward")
        assert sub.node_count() == 2
        assert sub.has_node("a")
        assert sub.has_node("b")
        assert not sub.has_node("c")

    def test_backward(self):
        dag = linear_dag()
        sub = dag.subgraph_by_depth("d", 1, direction="backward")
        assert sub.node_count() == 2
        assert sub.has_node("d")
        assert sub.has_node("c")

    def test_both(self):
        dag = linear_dag()
        sub = dag.subgraph_by_depth("b", 1, direction="both")
        assert sub.node_count() == 3
        assert sub.has_node("a")
        assert sub.has_node("b")
        assert sub.has_node("c")

    def test_large_depth(self):
        dag = linear_dag()
        sub = dag.subgraph_by_depth("a", 100, direction="forward")
        assert sub.node_count() == 4

    def test_nonexistent_root(self):
        dag = linear_dag()
        with pytest.raises(NodeNotFoundError):
            dag.subgraph_by_depth("z", 1)

    def test_invalid_direction(self):
        dag = linear_dag()
        with pytest.raises(ValueError):
            dag.subgraph_by_depth("a", 1, direction="invalid")

    def test_payload_preserved(self):
        dag = linear_dag()
        sub = dag.subgraph_by_depth("a", 1, direction="forward")
        assert sub.get_payload("a") == 1
        assert sub.get_payload("b") == 2
