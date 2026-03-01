"""Tests for Graph Query Language (mini-DSL)."""

import pytest

from dagron import DAG, DAGBuilder


@pytest.fixture
def pipeline_dag():
    """A realistic pipeline DAG."""
    dag = DAG()
    dag.add_nodes([
        "input_raw", "input_config",
        "extract", "validate",
        "transform_a", "transform_b",
        "test_a", "test_b",
        "merge",
        "output_final",
    ])
    dag.add_edges([
        ("input_raw", "extract"),
        ("input_config", "extract"),
        ("extract", "validate"),
        ("validate", "transform_a"),
        ("validate", "transform_b"),
        ("transform_a", "test_a"),
        ("transform_b", "test_b"),
        ("test_a", "merge"),
        ("test_b", "merge"),
        ("merge", "output_final"),
    ])
    return dag


class TestQuery:
    def test_roots(self, pipeline_dag):
        result = pipeline_dag.query("roots")
        assert set(result) == {"input_raw", "input_config"}

    def test_leaves(self, pipeline_dag):
        result = pipeline_dag.query("leaves")
        assert result == ["output_final"]

    def test_critical_path(self, pipeline_dag):
        result = pipeline_dag.query("critical_path")
        assert len(result) > 0
        assert "output_final" in result

    def test_ancestors(self, pipeline_dag):
        result = pipeline_dag.query("ancestors(merge)")
        assert "transform_a" in result
        assert "transform_b" in result
        assert "validate" in result
        assert "output_final" not in result

    def test_descendants(self, pipeline_dag):
        result = pipeline_dag.query("descendants(validate)")
        assert "transform_a" in result
        assert "transform_b" in result
        assert "merge" in result
        assert "output_final" in result

    def test_predecessors(self, pipeline_dag):
        result = pipeline_dag.query("predecessors(merge)")
        assert set(result) == {"test_a", "test_b"}

    def test_successors(self, pipeline_dag):
        result = pipeline_dag.query("successors(validate)")
        assert set(result) == {"transform_a", "transform_b"}

    def test_depth_filter(self, pipeline_dag):
        result = pipeline_dag.query("depth <= 1")
        assert "input_raw" in result
        assert "input_config" in result
        assert "extract" in result
        assert "output_final" not in result

    def test_name_pattern(self, pipeline_dag):
        result = pipeline_dag.query("name:test_*")
        assert set(result) == {"test_a", "test_b"}

    def test_name_pattern_input(self, pipeline_dag):
        result = pipeline_dag.query("name:input_*")
        assert set(result) == {"input_raw", "input_config"}

    def test_intersection(self, pipeline_dag):
        result = pipeline_dag.query("ancestors(merge) & depth <= 2")
        # ancestors of merge that are at depth 0, 1, or 2
        for node in result:
            assert node in {"input_raw", "input_config", "extract", "validate"}

    def test_union(self, pipeline_dag):
        result = pipeline_dag.query("roots | leaves")
        assert set(result) == {"input_raw", "input_config", "output_final"}

    def test_difference(self, pipeline_dag):
        result = pipeline_dag.query("ancestors(output_final) - roots")
        assert "input_raw" not in result
        assert "input_config" not in result
        assert "merge" in result

    def test_in_degree(self, pipeline_dag):
        result = pipeline_dag.query("in_degree >= 2")
        assert "merge" in result
        assert "extract" in result

    def test_out_degree(self, pipeline_dag):
        result = pipeline_dag.query("out_degree >= 2")
        assert "validate" in result

    def test_complex_query(self, pipeline_dag):
        result = pipeline_dag.query("descendants(validate) & name:test_*")
        assert set(result) == {"test_a", "test_b"}

    def test_unknown_expression(self, pipeline_dag):
        with pytest.raises(ValueError, match="Unknown query"):
            pipeline_dag.query("foobar(xyz)")

    def test_single_node(self, pipeline_dag):
        result = pipeline_dag.query("merge")
        assert result == ["merge"]
