"""Distributed execution via graph partitioning."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG

from dagron.execution._types import (
    ExecutionCallbacks,
    ExecutionResult,
    NodeResult,
    NodeStatus,
)
from dagron.execution.executor import DAGExecutor


class PartitionedDAGExecutor:
    """Execute a DAG by partitioning it and running partitions in dependency order.

    Each partition is executed internally using a ``DAGExecutor``.
    Partitions are run in topological order of the partition dependency graph.

    Args:
        dag: The DAG to execute.
        k: Number of target partitions.
        strategy: Partitioning strategy: "level_based", "balanced", or "communication_min".
        costs: Optional node cost estimates.
        max_workers: Maximum concurrent workers per partition.
        callbacks: Optional execution callbacks.
        fail_fast: If True, skip downstream partitions when a partition fails.
        enable_tracing: If True, record execution trace.
        partition_kwargs: Extra kwargs for the partitioning strategy
            (e.g., max_iterations, max_imbalance for communication_min).
    """

    def __init__(
        self,
        dag: DAG,
        k: int = 2,
        strategy: str = "level_based",
        costs: dict[str, float] | None = None,
        max_workers: int | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
        **partition_kwargs: Any,
    ) -> None:
        self._dag = dag
        self._k = k
        self._strategy = strategy
        self._costs = costs
        self._max_workers = max_workers
        self._callbacks = callbacks
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing
        self._partition_kwargs = partition_kwargs

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
    ) -> ExecutionResult:
        """Execute tasks by partitioning the DAG.

        Returns:
            Aggregated ExecutionResult from all partitions.
        """
        from dagron.execution.tracing import ExecutionTrace, TraceEventType

        # Partition the DAG
        partition_result = self._partition()

        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        start_time = time.monotonic()
        failed_partitions: set[int] = set()

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        # Execute partitions in dependency order
        for level in partition_result.partition_order:
            for pid in level:
                partition_info = None
                for p in partition_result.partitions:
                    if p.partition_id == pid:
                        partition_info = p
                        break

                if partition_info is None:
                    continue

                # Skip if any upstream partition failed (fail_fast)
                if self._fail_fast and failed_partitions:
                    # Check if any partition this one depends on has failed
                    has_failed_dep = False
                    for node_name in partition_info.node_names:
                        preds = [n.name for n in self._dag.predecessors(node_name)]
                        for pred in preds:
                            if pred in result.node_results:
                                nr = result.node_results[pred]
                                if nr.status == NodeStatus.FAILED:
                                    has_failed_dep = True
                                    break
                        if has_failed_dep:
                            break

                    if has_failed_dep:
                        # Skip all nodes in this partition
                        for node_name in partition_info.node_names:
                            if node_name not in result.node_results:
                                nr = NodeResult(
                                    name=node_name, status=NodeStatus.SKIPPED
                                )
                                result.node_results[node_name] = nr
                                result.skipped += 1
                        failed_partitions.add(pid)
                        continue

                # Extract sub-DAG and tasks for this partition
                node_names = partition_info.node_names
                sub_dag = self._dag.subgraph(node_names)
                sub_tasks = {
                    name: tasks[name]
                    for name in node_names
                    if name in tasks
                }

                # Inject results from previous partitions as pre-completed
                # (handled naturally by DAGExecutor since only partition nodes are in sub_dag)

                sub_executor = DAGExecutor(
                    sub_dag,
                    max_workers=self._max_workers,
                    costs=self._costs,
                    callbacks=self._callbacks,
                    fail_fast=self._fail_fast,
                    enable_tracing=self._enable_tracing,
                )
                sub_result = sub_executor.execute(sub_tasks)

                # Merge results
                for name, nr in sub_result.node_results.items():
                    result.node_results[name] = nr
                result.succeeded += sub_result.succeeded
                result.failed += sub_result.failed
                result.skipped += sub_result.skipped
                result.timed_out += sub_result.timed_out
                result.cancelled += sub_result.cancelled

                if sub_result.failed > 0:
                    failed_partitions.add(pid)

        if trace:
            trace.record(TraceEventType.EXECUTION_COMPLETED)

        result.total_duration_seconds = time.monotonic() - start_time
        result.trace = trace
        return result

    def _partition(self) -> Any:
        """Run the partitioning algorithm."""
        if self._strategy == "level_based":
            return self._dag.partition_level_based(self._k, self._costs)  # type: ignore[attr-defined]
        elif self._strategy == "balanced":
            return self._dag.partition_balanced(self._k, self._costs)  # type: ignore[attr-defined]
        elif self._strategy == "communication_min":
            return self._dag.partition_communication_min(  # type: ignore[attr-defined]
                self._k,
                self._costs,
                self._partition_kwargs.get("max_iterations", 10),
                self._partition_kwargs.get("max_imbalance", 0.3),
            )
        else:
            raise ValueError(f"Unknown partitioning strategy: '{self._strategy}'")
