"""Shared helper functions for DAG executors."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron.execution._types import ExecutionCallbacks, ExecutionResult, NodeResult
    from dagron.execution.tracing import ExecutionTrace


def _run_sync_task(
    name: str,
    task_fn: Callable[[], Any],
    callbacks: ExecutionCallbacks,
) -> NodeResult[Any]:
    """Execute a synchronous task function with callbacks."""
    from dagron.execution._types import NodeResult, NodeStatus

    if callbacks.on_start:
        callbacks.on_start(name)

    t0 = time.monotonic()
    try:
        value = task_fn()
        duration = time.monotonic() - t0
        if callbacks.on_complete:
            callbacks.on_complete(name, value)
        return NodeResult(
            name=name,
            status=NodeStatus.COMPLETED,
            result=value,
            duration_seconds=duration,
        )
    except Exception as exc:
        duration = time.monotonic() - t0
        if callbacks.on_failure:
            callbacks.on_failure(name, exc)
        return NodeResult(
            name=name,
            status=NodeStatus.FAILED,
            error=exc,
            duration_seconds=duration,
        )


def _record_skip(
    name: str,
    result: ExecutionResult,
    callbacks: ExecutionCallbacks,
    trace: ExecutionTrace | None,
) -> None:
    """Record a skipped node in the result, fire callback, and trace."""
    from dagron.execution._types import NodeResult, NodeStatus
    from dagron.execution.tracing import TraceEventType

    nr: NodeResult[Any] = NodeResult(name=name, status=NodeStatus.SKIPPED)
    result.node_results[name] = nr
    result.skipped += 1
    if callbacks.on_skip:
        callbacks.on_skip(name)
    if trace:
        trace.record(TraceEventType.NODE_SKIPPED, node_name=name)
