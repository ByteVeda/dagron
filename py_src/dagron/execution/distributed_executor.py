"""Distributed executor — run DAG nodes across processes/machines."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG
    from dagron.execution.backends.base import DistributedBackend

from dagron.execution._helpers import _record_skip
from dagron.execution._types import (
    ExecutionCallbacks,
    ExecutionResult,
    NodeResult,
    NodeStatus,
)


@dataclass
class DistributedExecutionResult:
    """Execution result with additional distributed-execution metadata."""

    execution_result: ExecutionResult = field(default_factory=ExecutionResult)
    backend_name: str = ""
    dispatch_info: dict[str, Any] = field(default_factory=dict)


class DistributedExecutor:
    """Execute DAG tasks using a pluggable :class:`DistributedBackend`.

    Dispatches tasks by topological level: all nodes in a level are
    submitted to the backend concurrently, and results are collected
    before advancing to the next level.

    Supports context-manager usage for automatic shutdown::

        with DistributedExecutor(dag, backend) as ex:
            result = ex.execute(tasks)

    Args:
        dag: The DAG to execute.
        backend: A :class:`DistributedBackend` instance.
        callbacks: Optional execution callbacks.
        fail_fast: If True, skip downstream nodes when a dependency fails.
        enable_tracing: If True, record an execution trace.
        node_timeout: Optional per-node timeout in seconds.
    """

    def __init__(
        self,
        dag: DAG,
        backend: DistributedBackend,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
        node_timeout: float | None = None,
    ) -> None:
        self._dag = dag
        self._backend = backend
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing
        self._node_timeout = node_timeout

    def __enter__(self) -> DistributedExecutor:
        return self

    def __exit__(self, *exc: Any) -> None:
        self._backend.shutdown(wait=True)

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
    ) -> DistributedExecutionResult:
        """Execute tasks via the distributed backend.

        Args:
            tasks: Mapping of node names to zero-argument callables.

        Returns:
            A :class:`DistributedExecutionResult` containing the aggregate
            result and backend metadata.
        """
        from dagron.execution.tracing import ExecutionTrace, TraceEventType

        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        failed_nodes: set[str] = set()
        start_time = time.monotonic()
        dispatch_info: dict[str, Any] = {}

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        ancestors_cache: dict[str, set[str]] = {}

        def get_ancestors(name: str) -> set[str]:
            if name not in ancestors_cache:
                ancestors_cache[name] = {n.name for n in self._dag.ancestors(name)}
            return ancestors_cache[name]

        levels = self._dag.topological_levels()

        for level_idx, level in enumerate(levels):
            if trace:
                trace.record(TraceEventType.STEP_STARTED, step_index=level_idx)

            futures: dict[Any, str] = {}
            for node_id in level:
                name = node_id.name

                if (
                    self._fail_fast
                    and failed_nodes
                    and get_ancestors(name) & failed_nodes
                ):
                    _record_skip(name, result, self._callbacks, trace)
                    continue

                task_fn = tasks.get(name)
                if task_fn is None:
                    _record_skip(name, result, self._callbacks, trace)
                    continue

                if self._callbacks.on_start:
                    self._callbacks.on_start(name)

                if trace:
                    trace.record(TraceEventType.NODE_STARTED, node_name=name)

                future = self._backend.submit(task_fn)
                futures[future] = name

            for future, name in futures.items():
                t0 = time.monotonic()
                try:
                    value = self._backend.result(future, timeout=self._node_timeout)
                    duration = time.monotonic() - t0
                    nr = NodeResult(
                        name=name,
                        status=NodeStatus.COMPLETED,
                        result=value,
                        duration_seconds=duration,
                    )
                    result.node_results[name] = nr
                    result.succeeded += 1
                    dispatch_info[name] = {"backend": self._backend.name}

                    if self._callbacks.on_complete:
                        self._callbacks.on_complete(name, value)
                    if trace:
                        trace.record(
                            TraceEventType.NODE_COMPLETED,
                            node_name=name,
                            duration=duration,
                        )

                except TimeoutError:
                    duration = time.monotonic() - t0
                    nr = NodeResult(
                        name=name,
                        status=NodeStatus.TIMED_OUT,
                        duration_seconds=duration,
                    )
                    result.node_results[name] = nr
                    result.timed_out += 1
                    failed_nodes.add(name)
                    if trace:
                        trace.record(
                            TraceEventType.NODE_TIMED_OUT,
                            node_name=name,
                            duration=duration,
                        )

                except Exception as exc:
                    duration = time.monotonic() - t0
                    nr = NodeResult(
                        name=name,
                        status=NodeStatus.FAILED,
                        error=exc,
                        duration_seconds=duration,
                    )
                    result.node_results[name] = nr
                    result.failed += 1
                    failed_nodes.add(name)

                    if self._callbacks.on_failure:
                        self._callbacks.on_failure(name, exc)
                    if trace:
                        trace.record(
                            TraceEventType.NODE_FAILED,
                            node_name=name,
                            duration=duration,
                            error=str(exc),
                        )

            if trace:
                trace.record(TraceEventType.STEP_COMPLETED, step_index=level_idx)

        if trace:
            trace.record(TraceEventType.EXECUTION_COMPLETED)

        result.total_duration_seconds = time.monotonic() - start_time
        result.trace = trace

        return DistributedExecutionResult(
            execution_result=result,
            backend_name=self._backend.name,
            dispatch_info=dispatch_info,
        )
