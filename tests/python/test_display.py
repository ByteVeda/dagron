"""Tests for pretty-print / ASCII rendering and Jupyter SVG repr."""

from unittest.mock import patch

import pytest

from dagron import DAG, pretty_print
from dagron.display import _repr_svg_


def test_empty_graph():
    dag = DAG()
    output = pretty_print(dag)
    assert output == "(empty graph)"


def test_single_node():
    dag = DAG()
    dag.add_node("a")
    output = pretty_print(dag)
    assert "[ a ]" in output


def test_linear_dag_vertical():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    output = pretty_print(dag, layout="vertical")
    assert "[ a ]" in output
    assert "[ b ]" in output
    assert "[ c ]" in output


def test_diamond_dag_vertical(diamond_dag):
    output = pretty_print(diamond_dag, layout="vertical")
    assert "[ a ]" in output
    assert "[ d ]" in output


def test_horizontal_layout():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    output = pretty_print(dag, layout="horizontal")
    assert "[ a ]" in output
    assert "[ b ]" in output
    assert "[ c ]" in output


def test_max_nodes_exceeded():
    dag = DAG()
    for i in range(10):
        dag.add_node(f"n{i}")
    with pytest.raises(ValueError, match="exceeding max_nodes"):
        pretty_print(dag, max_nodes=5)


def test_show_payloads():
    dag = DAG()
    dag.add_node("x", payload=42)
    output = pretty_print(dag, show_payloads=True)
    assert "x=42" in output


def test_node_formatter():
    dag = DAG()
    dag.add_node("x", payload=42)
    output = pretty_print(dag, node_formatter=lambda name, payload: f"{name}:{payload}")
    assert "x:42" in output


def test_dag_method():
    dag = DAG()
    dag.add_nodes(["a", "b"])
    dag.add_edge("a", "b")
    # Test that the monkey-patched method works
    output = dag.pretty_print(layout="vertical")
    assert "[ a ]" in output
    assert "[ b ]" in output


def test_complex_dag_display(complex_dag):
    output = pretty_print(complex_dag)
    assert "[ a ]" in output
    assert "[ f ]" in output


def test_disconnected_nodes():
    dag = DAG()
    dag.add_nodes(["a", "b", "c"])
    # No edges
    output = pretty_print(dag)
    assert "[ a ]" in output
    assert "[ b ]" in output
    assert "[ c ]" in output


# ---------------------------------------------------------------------------
# Jupyter _repr_svg_ tests
# ---------------------------------------------------------------------------


class TestReprSvg:
    def test_empty_graph(self):
        dag = DAG()
        svg = dag._repr_svg_()
        assert "<svg" in svg
        assert "(empty graph)" in svg

    def test_small_graph_returns_svg(self):
        dag = DAG()
        dag.add_nodes(["a", "b", "c"])
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        svg = dag._repr_svg_()
        assert "<svg" in svg or "<?xml" in svg

    def test_large_graph_summary(self):
        dag = DAG()
        dag.add_nodes([f"n{i}" for i in range(150)])
        svg = _repr_svg_(dag, max_nodes=100)
        assert "<svg" in svg
        assert "too large to render" in svg
        assert "nodes=150" in svg

    def test_fallback_without_graphviz(self):
        dag = DAG()
        dag.add_nodes(["x", "y"])
        dag.add_edge("x", "y")

        # Patch out both graphviz approaches to force ASCII fallback
        with (
            patch("dagron.display.subprocess.run", side_effect=FileNotFoundError),
            patch.dict("sys.modules", {"graphviz": None}),
        ):
            svg = _repr_svg_(dag)
            assert "<svg" in svg

    def test_standalone_function(self):
        dag = DAG()
        dag.add_nodes(["a", "b"])
        dag.add_edge("a", "b")
        svg = _repr_svg_(dag)
        assert "<svg" in svg or "<?xml" in svg
