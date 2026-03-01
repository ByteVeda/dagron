"""Tests for Phase 6: Concurrency — snapshots, timeouts, cancellation."""

import asyncio
import threading
import time

import pytest

import dagron


# ── Helpers ──────────────────────────────────────────────────────────


def diamond_dag(with_payloads=False):
    """Create a diamond DAG: A → B, A → C, B → D, C → D."""
    dag = dagron.DAG()
    dag.add_node("A", payload={"val": 10} if with_payloads else None)
    dag.add_node("B", payload={"val": 20} if with_payloads else None)
    dag.add_node("C", payload={"val": 30} if with_payloads else None)
    dag.add_node("D", payload={"val": 40} if with_payloads else None)
    dag.add_edge("A", "B")
    dag.add_edge("A", "C")
    dag.add_edge("B", "D")
    dag.add_edge("C", "D")
    return dag


# ── Snapshot tests ───────────────────────────────────────────────────


class TestSnapshot:
    def test_snapshot_isolated_from_mutations(self):
        dag = diamond_dag()
        snap = dag.snapshot()

        dag.add_node("E")
        dag.add_edge("D", "E")

        assert snap.node_count() == 4
        assert dag.node_count() == 5
        assert not snap.has_node("E")

    def test_snapshot_preserves_payloads_and_metadata(self):
        dag = dagron.DAG()
        dag.add_node("A", payload=42, metadata={"key": "val"})
        snap = dag.snapshot()

        assert snap.get_payload("A") == 42
        assert snap.get_metadata("A") == {"key": "val"}

    def test_snapshot_empty_dag(self):
        dag = dagron.DAG()
        snap = dag.snapshot()
        assert snap.node_count() == 0
        assert snap.edge_count() == 0

    def test_snapshot_preserves_edges(self):
        dag = diamond_dag()
        snap = dag.snapshot()

        assert snap.edge_count() == 4
        assert snap.has_edge("A", "B")
        assert snap.has_edge("A", "C")
        assert snap.has_edge("B", "D")
        assert snap.has_edge("C", "D")

    def test_snapshot_independent_mutate_snapshot(self):
        """Mutating the snapshot doesn't affect the original."""
        dag = diamond_dag(with_payloads=True)
        snap = dag.snapshot()

        snap.add_node("E", payload={"val": 50})
        snap.add_edge("D", "E")

        assert dag.node_count() == 4
        assert not dag.has_node("E")
        assert snap.node_count() == 5


# ── DAGExecutor timeout tests ───────────────────────────────────────


class TestDAGExecutorTimeout:
    def test_timed_out_node_gets_timed_out_status(self):
        dag = dagron.DAG()
        dag.add_node("slow")

        def slow_task():
            time.sleep(5)
            return "done"

        executor = dagron.DAGExecutor(dag)
        result = executor.execute({"slow": slow_task}, timeout=0.1)

        assert result.node_results["slow"].status == dagron.NodeStatus.TIMED_OUT
        assert result.timed_out == 1

    def test_downstream_of_timed_out_skipped(self):
        dag = dagron.DAG()
        dag.add_node("slow")
        dag.add_node("downstream")
        dag.add_edge("slow", "downstream")

        def slow_task():
            time.sleep(5)
            return "done"

        executor = dagron.DAGExecutor(dag, fail_fast=True)
        result = executor.execute(
            {"slow": slow_task, "downstream": lambda: "ok"},
            timeout=0.1,
        )

        assert result.node_results["slow"].status == dagron.NodeStatus.TIMED_OUT
        assert result.node_results["downstream"].status == dagron.NodeStatus.SKIPPED
        assert result.timed_out == 1
        assert result.skipped == 1

    def test_successful_within_timeout(self):
        dag = dagron.DAG()
        dag.add_node("fast")

        executor = dagron.DAGExecutor(dag)
        result = executor.execute({"fast": lambda: "quick"}, timeout=5.0)

        assert result.node_results["fast"].status == dagron.NodeStatus.COMPLETED
        assert result.succeeded == 1
        assert result.timed_out == 0


# ── DAGExecutor cancel tests ────────────────────────────────────────


class TestDAGExecutorCancel:
    def test_cancel_event_stops_between_steps(self):
        dag = dagron.DAG()
        dag.add_node("A")
        dag.add_node("B")
        dag.add_edge("A", "B")

        cancel = threading.Event()

        def task_a():
            cancel.set()  # Cancel after first step
            return "a_done"

        executor = dagron.DAGExecutor(dag)
        result = executor.execute(
            {"A": task_a, "B": lambda: "b_done"},
            cancel_event=cancel,
        )

        assert result.node_results["A"].status == dagron.NodeStatus.COMPLETED
        assert result.node_results["B"].status == dagron.NodeStatus.CANCELLED
        assert result.succeeded == 1
        assert result.cancelled == 1

    def test_completed_nodes_preserved(self):
        dag = dagron.DAG()
        dag.add_node("A")
        dag.add_node("B")
        dag.add_node("C")
        dag.add_edge("A", "B")
        dag.add_edge("B", "C")

        cancel = threading.Event()

        def task_a():
            return "a_result"

        def task_b():
            cancel.set()
            return "b_result"

        executor = dagron.DAGExecutor(dag)
        result = executor.execute(
            {"A": task_a, "B": task_b, "C": lambda: "c_result"},
            cancel_event=cancel,
        )

        assert result.node_results["A"].status == dagron.NodeStatus.COMPLETED
        assert result.node_results["A"].result == "a_result"
        assert result.node_results["B"].status == dagron.NodeStatus.COMPLETED
        assert result.node_results["B"].result == "b_result"
        assert result.node_results["C"].status == dagron.NodeStatus.CANCELLED


# ── AsyncDAGExecutor timeout tests ──────────────────────────────────


class TestAsyncDAGExecutorTimeout:
    def test_timed_out_node_gets_timed_out_status(self):
        dag = dagron.DAG()
        dag.add_node("slow")

        async def slow_task():
            await asyncio.sleep(5)
            return "done"

        executor = dagron.AsyncDAGExecutor(dag)
        result = asyncio.run(
            executor.execute({"slow": slow_task}, timeout=0.1)
        )

        assert result.node_results["slow"].status == dagron.NodeStatus.TIMED_OUT
        assert result.timed_out == 1


# ── AsyncDAGExecutor cancel tests ───────────────────────────────────


class TestAsyncDAGExecutorCancel:
    def test_cancel_stops_between_steps(self):
        dag = dagron.DAG()
        dag.add_node("A")
        dag.add_node("B")
        dag.add_edge("A", "B")

        cancel = asyncio.Event()

        async def task_a():
            cancel.set()
            return "a_done"

        async def task_b():
            return "b_done"

        executor = dagron.AsyncDAGExecutor(dag)
        result = asyncio.run(
            executor.execute(
                {"A": task_a, "B": task_b},
                cancel_event=cancel,
            )
        )

        assert result.node_results["A"].status == dagron.NodeStatus.COMPLETED
        assert result.node_results["B"].status == dagron.NodeStatus.CANCELLED
        assert result.succeeded == 1
        assert result.cancelled == 1


# ── Snapshot + executor integration ─────────────────────────────────


class TestSnapshotExecutor:
    def test_execute_on_snapshot_while_original_mutated(self):
        """Execute on a snapshot while the original is concurrently modified."""
        dag = diamond_dag()
        snap = dag.snapshot()

        # Mutate the original in a background thread
        def mutate():
            time.sleep(0.05)
            dag.add_node("E")
            dag.add_edge("D", "E")

        t = threading.Thread(target=mutate)
        t.start()

        # Execute on the snapshot
        tasks = {
            "A": lambda: "a",
            "B": lambda: "b",
            "C": lambda: "c",
            "D": lambda: "d",
        }
        executor = dagron.DAGExecutor(snap)
        result = executor.execute(tasks)

        t.join()

        # Snapshot execution unaffected
        assert result.succeeded == 4
        assert result.failed == 0
        assert snap.node_count() == 4

        # Original was mutated
        assert dag.node_count() == 5
