"""Tests for Conditional Edges / Branching."""

import pytest

from dagron import ConditionalDAGBuilder, ConditionalExecutor, NodeStatus


class TestConditionalDAGBuilder:
    def test_build(self):
        dag, conditions = (
            ConditionalDAGBuilder()
            .add_node("validate")
            .add_node("process")
            .add_node("error_handler")
            .add_edge("validate", "process",
                       condition=lambda r: r.get("valid", False))
            .add_edge("validate", "error_handler",
                       condition=lambda r: not r.get("valid", False))
            .build()
        )
        assert dag.node_count() == 3
        assert dag.edge_count() == 2
        assert len(conditions) == 2

    def test_unconditional_edges(self):
        dag, conditions = (
            ConditionalDAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_edge("a", "b")
            .build()
        )
        assert len(conditions) == 0


class TestConditionalExecutor:
    def test_condition_true_path(self):
        dag, conditions = (
            ConditionalDAGBuilder()
            .add_node("validate")
            .add_node("process")
            .add_node("error_handler")
            .add_edge("validate", "process",
                       condition=lambda r: r.get("valid", False))
            .add_edge("validate", "error_handler",
                       condition=lambda r: not r.get("valid", False))
            .build()
        )

        tasks = {
            "validate": lambda: {"valid": True, "data": [1, 2, 3]},
            "process": lambda: "processed",
            "error_handler": lambda: "error handled",
        }

        executor = ConditionalExecutor(dag, conditions)
        result = executor.execute(tasks)

        assert result.node_results["validate"].status == NodeStatus.COMPLETED
        assert result.node_results["process"].status == NodeStatus.COMPLETED
        assert result.node_results["error_handler"].status == NodeStatus.SKIPPED

    def test_condition_false_path(self):
        dag, conditions = (
            ConditionalDAGBuilder()
            .add_node("validate")
            .add_node("process")
            .add_node("error_handler")
            .add_edge("validate", "process",
                       condition=lambda r: r.get("valid", False))
            .add_edge("validate", "error_handler",
                       condition=lambda r: not r.get("valid", False))
            .build()
        )

        tasks = {
            "validate": lambda: {"valid": False},
            "process": lambda: "processed",
            "error_handler": lambda: "error handled",
        }

        executor = ConditionalExecutor(dag, conditions)
        result = executor.execute(tasks)

        assert result.node_results["validate"].status == NodeStatus.COMPLETED
        assert result.node_results["process"].status == NodeStatus.SKIPPED
        assert result.node_results["error_handler"].status == NodeStatus.COMPLETED

    def test_unconditional_always_runs(self):
        dag, conditions = (
            ConditionalDAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_node("c")
            .add_edge("a", "b")
            .add_edge("b", "c")
            .build()
        )

        tasks = {
            "a": lambda: "a",
            "b": lambda: "b",
            "c": lambda: "c",
        }

        executor = ConditionalExecutor(dag, conditions)
        result = executor.execute(tasks)

        assert result.succeeded == 3

    def test_fail_fast(self):
        dag, conditions = (
            ConditionalDAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_edge("a", "b")
            .build()
        )

        def fail():
            raise ValueError("boom")

        tasks = {"a": fail, "b": lambda: "b"}
        executor = ConditionalExecutor(dag, conditions, fail_fast=True)
        result = executor.execute(tasks)

        assert result.node_results["a"].status == NodeStatus.FAILED
        assert result.node_results["b"].status == NodeStatus.SKIPPED

    def test_cascading_skip(self):
        """Skipped nodes should cause their dependents to skip too."""
        dag, conditions = (
            ConditionalDAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_node("c")
            .add_edge("a", "b", condition=lambda r: False)
            .add_edge("b", "c")
            .build()
        )

        tasks = {
            "a": lambda: "a",
            "b": lambda: "b",
            "c": lambda: "c",
        }

        executor = ConditionalExecutor(dag, conditions)
        result = executor.execute(tasks)

        assert result.node_results["a"].status == NodeStatus.COMPLETED
        assert result.node_results["b"].status == NodeStatus.SKIPPED
        assert result.node_results["c"].status == NodeStatus.SKIPPED

    def test_with_tracing(self):
        dag, conditions = (
            ConditionalDAGBuilder()
            .add_node("a")
            .add_node("b")
            .add_edge("a", "b")
            .build()
        )

        tasks = {"a": lambda: "a", "b": lambda: "b"}
        executor = ConditionalExecutor(dag, conditions, enable_tracing=True)
        result = executor.execute(tasks)

        assert result.trace is not None
        assert len(result.trace.events) > 0
