"""Conditional edges and branching for DAG execution."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG
    from dagron.execution._types import ExecutionCallbacks, ExecutionResult


@dataclass(frozen=True)
class ConditionalEdge:
    """An edge with a predicate controlling downstream execution."""

    from_node: str
    to_node: str
    condition: Callable[[Any], bool]
    label: str | None = None


class ConditionalDAGBuilder:
    """Build a DAG with conditional edges.

    Conditional edges have predicates that are evaluated at execution time
    against the upstream node's result. If the predicate returns False,
    the downstream node is skipped.

    Example::

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

        executor = ConditionalExecutor(dag, conditions)
        result = executor.execute(tasks)
    """

    def __init__(self) -> None:
        self._nodes: list[tuple[str, object, object]] = []
        self._edges: list[tuple[str, str, float | None, str | None]] = []
        self._conditions: dict[tuple[str, str], Callable[[Any], bool]] = {}

    def add_node(
        self,
        name: str,
        payload: object = None,
        metadata: object = None,
    ) -> ConditionalDAGBuilder:
        self._nodes.append((name, payload, metadata))
        return self

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        *,
        weight: float | None = None,
        label: str | None = None,
        condition: Callable[[Any], bool] | None = None,
    ) -> ConditionalDAGBuilder:
        self._edges.append((from_node, to_node, weight, label))
        if condition is not None:
            self._conditions[(from_node, to_node)] = condition
        return self

    def build(self) -> tuple[DAG, dict[tuple[str, str], Callable[[Any], bool]]]:
        """Build the DAG and return it with the condition map.

        Returns:
            Tuple of (DAG, conditions_dict).
        """
        from dagron._internal import DAG

        dag = DAG()
        for name, payload, metadata in self._nodes:
            dag.add_node(name, payload=payload, metadata=metadata)
        for from_node, to_node, weight, label in self._edges:
            dag.add_edge(from_node, to_node, weight=weight, label=label)
        return dag, dict(self._conditions)


class ConditionalExecutor:
    """Execute a DAG with conditional edge evaluation.

    At each step, before executing a node, checks whether all incoming
    conditional edges are satisfied. If any incoming conditional edge
    evaluates to False, the node is skipped.

    Args:
        dag: The DAG to execute.
        conditions: Dict mapping (from, to) edge tuples to predicate functions.
        fail_fast: Skip downstream nodes on failure.
        enable_tracing: Record execution trace events.
    """

    def __init__(
        self,
        dag: DAG,
        conditions: dict[tuple[str, str], Callable[[Any], bool]],
        *,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
    ) -> None:
        self._dag = dag
        self._conditions = conditions
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing

        from dagron.execution._types import ExecutionCallbacks

        self._callbacks = callbacks or ExecutionCallbacks()

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
    ) -> ExecutionResult:
        """Execute tasks with conditional edge evaluation.

        Args:
            tasks: Dict mapping node names to callables.

        Returns:
            ExecutionResult with per-node results.
        """
        from dagron.execution._helpers import _run_sync_task
        from dagron.execution._types import (
            ExecutionResult,
            NodeResult,
            NodeStatus,
        )
        from dagron.execution.tracing import ExecutionTrace, TraceEventType

        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        results_map: dict[str, Any] = {}
        failed_nodes: set[str] = set()
        skipped_nodes: set[str] = set()
        start_time = time.monotonic()

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        topo_order = [n.name for n in self._dag.topological_sort()]

        for name in topo_order:
            # Check fail-fast: skip if any ancestor failed
            if self._fail_fast and failed_nodes:
                ancestors = {n.name for n in self._dag.ancestors(name)}
                if ancestors & failed_nodes:
                    nr: NodeResult[Any] = NodeResult(name=name, status=NodeStatus.SKIPPED)
                    result.node_results[name] = nr
                    result.skipped += 1
                    skipped_nodes.add(name)
                    if trace:
                        trace.record(TraceEventType.NODE_SKIPPED, node_name=name)
                    continue

            # Check conditional edges
            preds = self._dag.predecessors(name)
            condition_failed = False
            for pred in preds:
                edge_key = (pred.name, name)
                if edge_key in self._conditions:
                    pred_result = results_map.get(pred.name)
                    cond_fn = self._conditions[edge_key]
                    try:
                        if not cond_fn(pred_result):
                            condition_failed = True
                            break
                    except Exception:
                        condition_failed = True
                        break
                elif pred.name in skipped_nodes:
                    # Predecessor was skipped (e.g. due to condition),
                    # skip this node too
                    condition_failed = True
                    break

            if condition_failed:
                nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                result.node_results[name] = nr
                result.skipped += 1
                skipped_nodes.add(name)
                if self._callbacks.on_skip:
                    self._callbacks.on_skip(name)
                if trace:
                    trace.record(TraceEventType.NODE_SKIPPED, node_name=name)
                continue

            # Execute the task
            task_fn = tasks.get(name)
            if task_fn is None:
                nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                result.node_results[name] = nr
                result.skipped += 1
                skipped_nodes.add(name)
                if trace:
                    trace.record(TraceEventType.NODE_SKIPPED, node_name=name)
                continue

            if trace:
                trace.record(TraceEventType.NODE_STARTED, node_name=name)

            nr = _run_sync_task(name, task_fn, self._callbacks)
            result.node_results[name] = nr

            if nr.status == NodeStatus.COMPLETED:
                result.succeeded += 1
                results_map[name] = nr.result
                if trace:
                    trace.record(
                        TraceEventType.NODE_COMPLETED,
                        node_name=name,
                        duration=nr.duration_seconds,
                    )
            elif nr.status == NodeStatus.FAILED:
                result.failed += 1
                failed_nodes.add(name)
                if trace:
                    trace.record(
                        TraceEventType.NODE_FAILED,
                        node_name=name,
                        duration=nr.duration_seconds,
                        error=str(nr.error) if nr.error else None,
                    )

        if trace:
            trace.record(TraceEventType.EXECUTION_COMPLETED)

        result.total_duration_seconds = time.monotonic() - start_time
        result.trace = trace
        return result
