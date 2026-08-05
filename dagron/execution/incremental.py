"""Incremental DAG execution with early-cutoff recomputation."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG
    from dagron.execution.tracing import ExecutionTrace

from dagron.execution._helpers import _run_sync_task
from dagron.execution._types import ExecutionCallbacks, NodeResult, NodeStatus


@dataclass
class IncrementalResult:
    """Result of an incremental DAG execution."""

    node_results: dict[str, NodeResult[Any]] = field(default_factory=dict)
    recomputed: list[str] = field(default_factory=list)
    early_cutoff: list[str] = field(default_factory=list)
    reused: list[str] = field(default_factory=list)
    provenance: dict[str, list[str]] = field(default_factory=dict)
    total_duration_seconds: float = 0.0
    trace: ExecutionTrace | None = None


class IncrementalExecutor:
    """Execute DAG tasks with early-cutoff incremental recomputation.

    Maintains a cache of previous results across calls. On subsequent
    executions with `changed_nodes`, only the dirty set is re-evaluated,
    and propagation stops when a node produces the same result as before.

    **Thread safety:** This class is *not* thread-safe. Do not call
    ``execute()`` concurrently from multiple threads on the same instance.

    Args:
        dag: A dagron.DAG instance.
        callbacks: Optional ExecutionCallbacks for lifecycle events.
        fail_fast: If True, skip downstream nodes when a dependency fails.
    """

    def __init__(
        self,
        dag: DAG,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
    ):
        self._dag = dag
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing
        self._cache: dict[str, Any] = {}

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
        changed_nodes: list[str] | None = None,
    ) -> IncrementalResult:
        """Execute tasks, reusing cached results where possible.

        Args:
            tasks: Dict mapping node names to callables.
            changed_nodes: Nodes that have changed since the last run.
                If None or cache is empty, all nodes are executed.

        Returns:
            IncrementalResult with recomputed, early_cutoff, and reused lists.
        """
        from dagron.execution.tracing import ExecutionTrace, TraceEventType

        start_time = time.monotonic()
        trace = ExecutionTrace() if self._enable_tracing else None
        result = IncrementalResult()

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        # Get topological order for deterministic processing
        topo_order = [n.name for n in self._dag.topological_sort()]

        # First run or no changed_nodes: execute everything
        if not self._cache or changed_nodes is None:
            for name in topo_order:
                task_fn = tasks.get(name)
                if task_fn is None:
                    continue
                if trace:
                    trace.record(TraceEventType.NODE_STARTED, node_name=name)
                nr = _run_sync_task(name, task_fn, self._callbacks)
                result.node_results[name] = nr
                if nr.status == NodeStatus.COMPLETED:
                    self._cache[name] = nr.result
                    result.recomputed.append(name)
                    if trace:
                        trace.record(
                            TraceEventType.NODE_COMPLETED,
                            node_name=name,
                            duration=nr.duration_seconds,
                        )
                elif trace:
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

        # Incremental execution
        dirty = set(self._dag.dirty_set(changed_nodes))
        provenance = self._dag.change_provenance(changed_nodes)
        result.provenance = provenance

        # Build propagation set -- starts with the changed nodes themselves
        propagation_set: set[str] = set(changed_nodes)

        # Get predecessors for each node (for checking propagation)
        predecessors_cache: dict[str, set[str]] = {}

        def get_predecessors(name: str) -> set[str]:
            if name not in predecessors_cache:
                preds = {n.name for n in self._dag.predecessors(name)}
                predecessors_cache[name] = preds
            return predecessors_cache[name]

        for name in topo_order:
            if name not in dirty:
                # Not dirty -- reuse cached result
                if name in self._cache:
                    result.node_results[name] = NodeResult(
                        name=name,
                        status=NodeStatus.COMPLETED,
                        result=self._cache[name],
                    )
                    result.reused.append(name)
                continue

            # Node is dirty -- check if any predecessor is in propagation set
            preds = get_predecessors(name)
            has_propagating_pred = bool(preds & propagation_set)

            # If this node is a changed node itself, always re-execute
            is_changed = name in changed_nodes

            if not is_changed and not has_propagating_pred:
                # Early cutoff from upstream -- no predecessor is propagating
                if name in self._cache:
                    result.node_results[name] = NodeResult(
                        name=name,
                        status=NodeStatus.COMPLETED,
                        result=self._cache[name],
                    )
                    result.reused.append(name)
                continue

            # Execute this node
            task_fn = tasks.get(name)
            if task_fn is None:
                continue

            if trace:
                trace.record(TraceEventType.NODE_STARTED, node_name=name)
            nr = _run_sync_task(name, task_fn, self._callbacks)
            result.node_results[name] = nr

            if nr.status == NodeStatus.COMPLETED:
                old_result = self._cache.get(name)
                result.recomputed.append(name)
                if trace:
                    trace.record(
                        TraceEventType.NODE_COMPLETED,
                        node_name=name,
                        duration=nr.duration_seconds,
                    )

                try:
                    same = old_result == nr.result
                except Exception:
                    same = False

                if same:
                    # Early cutoff -- result unchanged, don't propagate
                    result.early_cutoff.append(name)
                else:
                    # Result changed -- propagate to downstream
                    self._cache[name] = nr.result
                    propagation_set.add(name)
            else:
                # Failed -- propagate (downstream may need to react)
                propagation_set.add(name)
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
