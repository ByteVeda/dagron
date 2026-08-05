"""Tests for graph diff."""

from dagron import DAG, GraphDiff


def test_identical_graphs():
    dag1 = DAG()
    dag1.add_nodes(["a", "b"])
    dag1.add_edge("a", "b")

    dag2 = DAG()
    dag2.add_nodes(["a", "b"])
    dag2.add_edge("a", "b")

    diff = dag1.diff(dag2)
    assert isinstance(diff, GraphDiff)
    assert diff.added_nodes == []
    assert diff.removed_nodes == []
    assert diff.changed_nodes == []
    assert diff.added_edges == []
    assert diff.removed_edges == []
    assert diff.changed_edges == []


def test_added_node():
    dag1 = DAG()
    dag1.add_node("a")

    dag2 = DAG()
    dag2.add_nodes(["a", "b"])

    diff = dag1.diff(dag2)
    assert diff.added_nodes == ["b"]
    assert diff.removed_nodes == []


def test_removed_node():
    dag1 = DAG()
    dag1.add_nodes(["a", "b"])

    dag2 = DAG()
    dag2.add_node("a")

    diff = dag1.diff(dag2)
    assert diff.added_nodes == []
    assert diff.removed_nodes == ["b"]


def test_added_edge():
    dag1 = DAG()
    dag1.add_nodes(["a", "b"])

    dag2 = DAG()
    dag2.add_nodes(["a", "b"])
    dag2.add_edge("a", "b")

    diff = dag1.diff(dag2)
    assert diff.added_edges == [("a", "b")]
    assert diff.removed_edges == []


def test_removed_edge():
    dag1 = DAG()
    dag1.add_nodes(["a", "b"])
    dag1.add_edge("a", "b")

    dag2 = DAG()
    dag2.add_nodes(["a", "b"])

    diff = dag1.diff(dag2)
    assert diff.added_edges == []
    assert diff.removed_edges == [("a", "b")]


def test_changed_payload():
    dag1 = DAG()
    dag1.add_node("a", payload=1)
    dag1.add_node("b", payload=2)

    dag2 = DAG()
    dag2.add_node("a", payload=1)
    dag2.add_node("b", payload=99)

    diff = dag1.diff(dag2)
    assert diff.changed_nodes == ["b"]
    assert diff.added_nodes == []
    assert diff.removed_nodes == []


def test_changed_edge_weight():
    dag1 = DAG()
    dag1.add_nodes(["a", "b"])
    dag1.add_edge("a", "b", weight=1.0)

    dag2 = DAG()
    dag2.add_nodes(["a", "b"])
    dag2.add_edge("a", "b", weight=5.0)

    diff = dag1.diff(dag2)
    assert diff.changed_edges == [("a", "b")]


def test_changed_edge_label():
    dag1 = DAG()
    dag1.add_nodes(["a", "b"])
    dag1.add_edge("a", "b", label="old")

    dag2 = DAG()
    dag2.add_nodes(["a", "b"])
    dag2.add_edge("a", "b", label="new")

    diff = dag1.diff(dag2)
    assert diff.changed_edges == [("a", "b")]


def test_empty_graphs_diff():
    dag1 = DAG()
    dag2 = DAG()
    diff = dag1.diff(dag2)
    assert diff.added_nodes == []
    assert diff.removed_nodes == []


def test_diff_repr():
    dag1 = DAG()
    dag1.add_node("a")

    dag2 = DAG()
    dag2.add_node("b")

    diff = dag1.diff(dag2)
    r = repr(diff)
    assert "GraphDiff" in r


def test_diff_to_dict():
    dag1 = DAG()
    dag1.add_node("a")

    dag2 = DAG()
    dag2.add_nodes(["a", "b"])

    diff = dag1.diff(dag2)
    d = diff.to_dict()
    assert d["added_nodes"] == ["b"]
    assert d["removed_nodes"] == []


def test_complex_diff():
    dag1 = DAG()
    dag1.add_nodes(["a", "b", "c"])
    dag1.add_edge("a", "b")
    dag1.add_edge("b", "c")

    dag2 = DAG()
    dag2.add_nodes(["a", "b", "d"])
    dag2.add_edge("a", "b")
    dag2.add_edge("b", "d")

    diff = dag1.diff(dag2)
    assert diff.removed_nodes == ["c"]
    assert diff.added_nodes == ["d"]
    assert diff.removed_edges == [("b", "c")]
    assert diff.added_edges == [("b", "d")]


def test_none_vs_payload():
    dag1 = DAG()
    dag1.add_node("a")  # payload is None

    dag2 = DAG()
    dag2.add_node("a", payload=42)

    diff = dag1.diff(dag2)
    assert diff.changed_nodes == ["a"]
