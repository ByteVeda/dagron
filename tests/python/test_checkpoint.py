"""Tests for Checkpoint/Resume Execution."""

import tempfile

import pytest

from dagron import DAG, DAGBuilder, NodeStatus
from dagron.execution.checkpoint import CheckpointExecutor


@pytest.fixture
def chain_dag():
    return (
        DAGBuilder()
        .add_node("a")
        .add_node("b")
        .add_node("c")
        .add_node("d")
        .add_edge("a", "b")
        .add_edge("b", "c")
        .add_edge("c", "d")
        .build()
    )


class TestCheckpointExecutor:
    def test_basic_execution(self, chain_dag):
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = CheckpointExecutor(chain_dag, checkpoint_dir=tmpdir)
            tasks = {
                "a": lambda: "a",
                "b": lambda: "b",
                "c": lambda: "c",
                "d": lambda: "d",
            }
            result = executor.execute(tasks)
            assert result.succeeded == 4
            assert result.failed == 0

    def test_checkpoint_info_after_execution(self, chain_dag):
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = CheckpointExecutor(chain_dag, checkpoint_dir=tmpdir)
            tasks = {
                "a": lambda: "a",
                "b": lambda: "b",
                "c": lambda: "c",
                "d": lambda: "d",
            }
            executor.execute(tasks)
            info = executor.checkpoint_info()
            assert info is not None
            assert set(info.completed_nodes) == {"a", "b", "c", "d"}
            assert info.failed_nodes == []

    def test_resume_after_failure(self, chain_dag):
        with tempfile.TemporaryDirectory() as tmpdir:
            call_count = {"c": 0}

            def fail_c():
                call_count["c"] += 1
                if call_count["c"] == 1:
                    raise ValueError("first attempt fails")
                return "c_fixed"

            tasks = {
                "a": lambda: "a",
                "b": lambda: "b",
                "c": fail_c,
                "d": lambda: "d",
            }

            executor = CheckpointExecutor(chain_dag, checkpoint_dir=tmpdir)
            result = executor.execute(tasks)
            assert result.failed == 1
            assert result.node_results["c"].status == NodeStatus.FAILED

            # Resume — a and b should be loaded from checkpoint
            result2 = executor.resume(tasks)
            assert result2.succeeded == 4
            assert result2.node_results["a"].result == "a"
            assert result2.node_results["c"].result == "c_fixed"

    def test_clear_checkpoint(self, chain_dag):
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = CheckpointExecutor(chain_dag, checkpoint_dir=tmpdir)
            tasks = {
                "a": lambda: "a",
                "b": lambda: "b",
                "c": lambda: "c",
                "d": lambda: "d",
            }
            executor.execute(tasks)
            assert executor.checkpoint_info() is not None
            executor.clear_checkpoint()
            assert executor.checkpoint_info() is None

    def test_fail_fast(self, chain_dag):
        with tempfile.TemporaryDirectory() as tmpdir:

            def fail():
                raise ValueError("boom")

            tasks = {
                "a": fail,
                "b": lambda: "b",
                "c": lambda: "c",
                "d": lambda: "d",
            }

            executor = CheckpointExecutor(chain_dag, checkpoint_dir=tmpdir, fail_fast=True)
            result = executor.execute(tasks)
            assert result.failed == 1
            assert result.skipped == 3

    def test_no_checkpoint_returns_none(self, chain_dag):
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = CheckpointExecutor(chain_dag, checkpoint_dir=tmpdir)
            assert executor.checkpoint_info() is None
