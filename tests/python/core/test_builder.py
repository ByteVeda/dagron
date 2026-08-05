"""Tests for DAGBuilder fluent API."""

import pytest

from dagron import DAG, CycleError, DAGBuilder, DuplicateNodeError, NodeNotFoundError


class TestDAGBuilder:
    def test_basic(self):
        dag = DAGBuilder().add_node("a").add_node("b").add_edge("a", "b").build()
        assert isinstance(dag, DAG)
        assert dag.node_count() == 2
        assert dag.edge_count() == 1

    def test_with_payload(self):
        dag = (
            DAGBuilder()
            .add_node("a", payload=1)
            .add_node("b", payload=2)
            .add_edge("a", "b")
            .build()
        )
        assert dag.get_payload("a") == 1
        assert dag.get_payload("b") == 2

    def test_with_metadata(self):
        dag = DAGBuilder().add_node("a", metadata={"key": "val"}).build()
        assert dag.get_metadata("a") == {"key": "val"}

    def test_with_weight_and_label(self):
        dag = (
            DAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_edge("a", "b", weight=2.5, label="dep")
            .build()
        )
        assert dag.node_count() == 2
        assert dag.edge_count() == 1

    def test_diamond(self):
        dag = (
            DAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_node("c")
            .add_node("d")
            .add_edge("a", "b")
            .add_edge("a", "c")
            .add_edge("b", "d")
            .add_edge("c", "d")
            .build()
        )
        assert dag.node_count() == 4
        assert dag.edge_count() == 4

    def test_empty(self):
        dag = DAGBuilder().build()
        assert dag.node_count() == 0

    def test_duplicate_node_error(self):
        with pytest.raises(DuplicateNodeError):
            DAGBuilder().add_node("a").add_node("a").build()

    def test_missing_node_error(self):
        with pytest.raises(NodeNotFoundError):
            DAGBuilder().add_node("a").add_edge("a", "b").build()

    def test_cycle_error(self):
        with pytest.raises(CycleError):
            (
                DAGBuilder()
                .add_node("a")
                .add_node("b")
                .add_edge("a", "b")
                .add_edge("b", "a")
                .build()
            )

    def test_chaining_returns_self(self):
        builder = DAGBuilder()
        result = builder.add_node("a")
        assert result is builder
        result = builder.add_edge(
            "a", "a"
        )  # This will fail on build with cycle, but chaining works

    def test_builder_staticmethod(self):
        builder = DAG.builder()
        assert isinstance(builder, DAGBuilder)
        dag = builder.add_node("x").build()
        assert dag.node_count() == 1
