"""Tests for graph algorithms: reverse, collapse, dominator tree, binary serialization."""

import pytest

import dagron
from dagron import DAG

# --- Reverse ---


def test_reverse_diamond(diamond_dag):
    rev = diamond_dag.reverse()
    assert rev.node_count() == 4
    assert rev.edge_count() == 4
    assert rev.has_edge("b", "a")
    assert rev.has_edge("c", "a")
    assert rev.has_edge("d", "b")
    assert rev.has_edge("d", "c")
    assert not rev.has_edge("a", "b")


def test_reverse_preserves_payloads():
    dag = DAG()
    dag.add_node("a", payload=42)
    dag.add_node("b", payload="hello")
    dag.add_edge("a", "b")

    rev = dag.reverse()
    assert rev.get_payload("a") == 42
    assert rev.get_payload("b") == "hello"
    assert rev.has_edge("b", "a")


def test_reverse_preserves_edges():
    dag = DAG()
    dag.add_node("x")
    dag.add_node("y")
    dag.add_edge("x", "y", weight=3.5, label="dep")

    rev = dag.reverse()
    edges = rev.edges()
    assert len(edges) == 1
    assert edges[0] == ("y", "x")


def test_reverse_empty():
    dag = DAG()
    rev = dag.reverse()
    assert rev.node_count() == 0
    assert rev.edge_count() == 0


# --- Collapse ---


def test_collapse_pair_in_diamond(diamond_dag):
    collapsed = diamond_dag.collapse(["b", "c"], "bc")
    assert collapsed.node_count() == 3
    assert collapsed.has_node("a")
    assert collapsed.has_node("bc")
    assert collapsed.has_node("d")
    assert not collapsed.has_node("b")
    assert not collapsed.has_node("c")
    assert collapsed.has_edge("a", "bc")
    assert collapsed.has_edge("bc", "d")


def test_collapse_preserves_payloads():
    dag = DAG()
    dag.add_node("a", payload=10)
    dag.add_node("b", payload=20)
    dag.add_node("c", payload=30)
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    collapsed = dag.collapse(["b"], "merged", payload=99)
    assert collapsed.get_payload("a") == 10
    assert collapsed.get_payload("merged") == 99
    assert collapsed.get_payload("c") == 30


def test_collapse_error_on_missing_node(diamond_dag):
    with pytest.raises(dagron.NodeNotFoundError):
        diamond_dag.collapse(["b", "nonexistent"], "bc")


def test_collapse_all():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_edge("a", "b")

    collapsed = dag.collapse(["a", "b"], "all")
    assert collapsed.node_count() == 1
    assert collapsed.has_node("all")
    assert collapsed.edge_count() == 0


# --- Dominator tree ---


def test_dominator_tree_diamond(diamond_dag):
    dom = diamond_dag.dominator_tree("a")
    dom_map = dict(dom)
    assert dom_map["a"] == "a"
    assert dom_map["b"] == "a"
    assert dom_map["c"] == "a"
    assert dom_map["d"] == "a"


def test_dominator_tree_linear():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")

    dom = dag.dominator_tree("a")
    dom_map = dict(dom)
    assert dom_map["a"] == "a"
    assert dom_map["b"] == "a"
    assert dom_map["c"] == "b"


def test_dominator_tree_converging():
    dag = DAG()
    dag.add_node("a")
    dag.add_node("b")
    dag.add_node("c")
    dag.add_node("d")
    dag.add_node("e")
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    dag.add_edge("b", "e")
    dag.add_edge("d", "e")

    dom = dag.dominator_tree("a")
    dom_map = dict(dom)
    assert dom_map["d"] == "a"
    assert dom_map["e"] == "a"


# --- Binary serialization ---


def test_to_bytes_from_bytes_empty():
    dag = DAG()
    data = dag.to_bytes()
    dag2 = DAG.from_bytes(data)
    assert dag2.node_count() == 0
    assert dag2.edge_count() == 0


def test_to_bytes_from_bytes_diamond(diamond_dag):
    data = diamond_dag.to_bytes()
    dag2 = DAG.from_bytes(data)
    assert dag2.node_count() == 4
    assert dag2.edge_count() == 4
    assert dag2.has_edge("a", "b")
    assert dag2.has_edge("c", "d")


def test_to_bytes_from_bytes_with_payloads():
    dag = DAG()
    dag.add_node("a", payload={"x": 1})
    dag.add_node("b", payload={"y": 2})
    dag.add_edge("a", "b")

    data = dag.to_bytes(payload_serializer=lambda p: p)
    dag2 = DAG.from_bytes(data, payload_deserializer=lambda p: p)
    assert dag2.get_payload("a") == {"x": 1}
    assert dag2.get_payload("b") == {"y": 2}
