"""Tests for Multi-DAG Composition with Namespacing."""

import pytest

from dagron import DAGBuilder, compose


@pytest.fixture
def etl_dag():
    return (
        DAGBuilder()
        .add_node("extract")
        .add_node("transform")
        .add_node("load")
        .add_edge("extract", "transform")
        .add_edge("transform", "load")
        .build()
    )


@pytest.fixture
def ml_dag():
    return (
        DAGBuilder()
        .add_node("train")
        .add_node("evaluate")
        .add_node("deploy")
        .add_edge("train", "evaluate")
        .add_edge("evaluate", "deploy")
        .build()
    )


class TestCompose:
    def test_basic_composition(self, etl_dag, ml_dag):
        combined = compose({"etl": etl_dag, "ml": ml_dag})
        assert combined.node_count() == 6
        assert combined.has_node("etl/extract")
        assert combined.has_node("ml/train")

    def test_namespaced_edges(self, etl_dag, ml_dag):
        combined = compose({"etl": etl_dag, "ml": ml_dag})
        assert combined.has_edge("etl/extract", "etl/transform")
        assert combined.has_edge("ml/train", "ml/evaluate")

    def test_cross_namespace_connections(self, etl_dag, ml_dag):
        combined = compose(
            {"etl": etl_dag, "ml": ml_dag},
            connections=[("etl/load", "ml/train")],
        )
        assert combined.has_edge("etl/load", "ml/train")
        assert combined.edge_count() == 5  # 2 + 2 + 1 cross

    def test_custom_separator(self, etl_dag, ml_dag):
        combined = compose(
            {"etl": etl_dag, "ml": ml_dag},
            separator=".",
        )
        assert combined.has_node("etl.extract")
        assert combined.has_node("ml.train")

    def test_preserves_payloads(self):
        dag = DAGBuilder().add_node("a", payload=42).build()
        combined = compose({"ns": dag})
        assert combined.get_payload("ns/a") == 42

    def test_stats(self, etl_dag, ml_dag):
        combined = compose(
            {"etl": etl_dag, "ml": ml_dag},
            connections=[("etl/load", "ml/train")],
        )
        stats = combined.stats()
        assert stats.node_count == 6
        assert stats.root_count == 1  # etl/extract
        assert stats.leaf_count == 1  # ml/deploy

    def test_analysis_on_composed(self, etl_dag, ml_dag):
        combined = compose(
            {"etl": etl_dag, "ml": ml_dag},
            connections=[("etl/load", "ml/train")],
        )
        # Full dagron analysis should work
        cp_nodes, _cp_cost = combined.critical_path()
        assert len(cp_nodes) == 6

    def test_single_dag(self, etl_dag):
        combined = compose({"pipeline": etl_dag})
        assert combined.node_count() == 3
        assert combined.has_node("pipeline/extract")
