"""Pure Python execution harness for DAG task scheduling."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import threading
    from collections.abc import Awaitable, Callable

    from dagron._internal import DAG
    from dagron.plugins.hooks import HookRegistry

from dagron.execution._helpers import _record_skip, _run_sync_task
from dagron.execution._types import (
    ExecutionCallbacks,
    ExecutionResult,
    NodeResult,
    NodeStatus,
)


def _fire_hook(hooks: HookRegistry | None, **kwargs: Any) -> None:
    """Fire a hook if the registry is available."""
    if hooks is not None:
        from dagron.plugins.hooks import HookContext

        hooks.fire(HookContext(**kwargs))


class DAGExecutor:
    """Execute DAG tasks using a thread pool.

    Uses the DAG's execution plan to determine execution order and parallelism.
    Tasks within the same step are dispatched concurrently up to max_workers.

    Args:
        dag: A dagron.DAG instance.
        max_workers: Maximum concurrent workers. None = unlimited parallelism.
        costs: Optional dict mapping node names to duration estimates.
        callbacks: Optional ExecutionCallbacks for lifecycle events.
        fail_fast: If True, skip downstream nodes when a dependency fails.
    """

    def __init__(
        self,
        dag: DAG,
        max_workers: int | None = None,
        costs: dict[str, float] | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
        hooks: HookRegistry | None = None,
    ):
        self._dag = dag
        self._max_workers = max_workers
        self._costs = costs
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing
        self._hooks = hooks

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
        timeout: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> ExecutionResult:
        """Execute tasks according to the DAG's dependency order.

        Args:
            tasks: Dict mapping node names to callables.
                Each callable takes no arguments and returns a result.
            timeout: Optional per-node timeout in seconds. Nodes that exceed
                this duration get TIMED_OUT status.
            cancel_event: Optional threading.Event. When set, remaining
                unstarted nodes get CANCELLED status.

        Returns:
            ExecutionResult with per-node results and aggregate counts.
        """
        from dagron.execution.tracing import ExecutionTrace, TraceEventType
        from dagron.plugins.hooks import HookEvent

        if self._max_workers is not None:
            plan = self._dag.execution_plan_constrained(self._max_workers, self._costs)
        else:
            plan = self._dag.execution_plan(self._costs)

        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        failed_nodes: set[str] = set()
        start_time = time.monotonic()

        _fire_hook(self._hooks, event=HookEvent.PRE_EXECUTE, dag=self._dag)

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        # Get all ancestors for fail-fast skip detection
        ancestors_cache: dict[str, set[str]] = {}

        def get_ancestors(name: str) -> set[str]:
            if name not in ancestors_cache:
                anc = {n.name for n in self._dag.ancestors(name)}
                ancestors_cache[name] = anc
            return ancestors_cache[name]

        pool_workers = self._max_workers or plan.max_parallelism or 1
        with ThreadPoolExecutor(max_workers=pool_workers) as pool:
            for step in plan.steps:
                if trace:
                    trace.record(TraceEventType.STEP_STARTED, step_index=step.step_index)

                # Check cancellation between steps
                if cancel_event is not None and cancel_event.is_set():
                    for scheduled_node in step.nodes:
                        name = scheduled_node.node.name
                        if name not in result.node_results:
                            nr = NodeResult(name=name, status=NodeStatus.CANCELLED)
                            result.node_results[name] = nr
                            result.cancelled += 1
                            if trace:
                                trace.record(TraceEventType.NODE_CANCELLED, node_name=name)
                    if trace:
                        trace.record(TraceEventType.STEP_COMPLETED, step_index=step.step_index)
                    continue

                futures = {}
                for scheduled_node in step.nodes:
                    name = scheduled_node.node.name

                    # Skip if a dependency has failed
                    if self._fail_fast and failed_nodes and get_ancestors(name) & failed_nodes:
                        _record_skip(name, result, self._callbacks, trace)
                        continue

                    task_fn = tasks.get(name)
                    if task_fn is None:
                        _record_skip(name, result, self._callbacks, trace)
                        continue

                    if trace:
                        trace.record(TraceEventType.NODE_STARTED, node_name=name)
                    _fire_hook(
                        self._hooks, event=HookEvent.PRE_NODE, dag=self._dag, node_name=name
                    )
                    futures[pool.submit(_run_sync_task, name, task_fn, self._callbacks)] = name

                # Wait for all futures in this step
                for future in futures:
                    name = futures[future]
                    try:
                        nr = future.result(timeout=timeout)
                    except TimeoutError:
                        nr = NodeResult(
                            name=name,
                            status=NodeStatus.TIMED_OUT,
                            duration_seconds=timeout if timeout else 0.0,
                        )
                    result.node_results[name] = nr
                    if nr.status == NodeStatus.COMPLETED:
                        result.succeeded += 1
                        _fire_hook(
                            self._hooks,
                            event=HookEvent.POST_NODE,
                            dag=self._dag,
                            node_name=name,
                            node_result=nr.result,
                        )
                        if trace:
                            trace.record(
                                TraceEventType.NODE_COMPLETED,
                                node_name=name,
                                duration=nr.duration_seconds,
                            )
                    elif nr.status == NodeStatus.FAILED:
                        result.failed += 1
                        failed_nodes.add(name)
                        _fire_hook(
                            self._hooks,
                            event=HookEvent.ON_ERROR,
                            dag=self._dag,
                            node_name=name,
                            error=nr.error,
                        )
                        if trace:
                            trace.record(
                                TraceEventType.NODE_FAILED,
                                node_name=name,
                                duration=nr.duration_seconds,
                                error=str(nr.error) if nr.error else None,
                            )
                    elif nr.status == NodeStatus.TIMED_OUT:
                        result.timed_out += 1
                        failed_nodes.add(name)
                        _fire_hook(
                            self._hooks,
                            event=HookEvent.ON_ERROR,
                            dag=self._dag,
                            node_name=name,
                        )
                        if trace:
                            trace.record(
                                TraceEventType.NODE_TIMED_OUT,
                                node_name=name,
                                duration=nr.duration_seconds,
                            )

                if trace:
                    trace.record(TraceEventType.STEP_COMPLETED, step_index=step.step_index)

        if trace:
            trace.record(TraceEventType.EXECUTION_COMPLETED)

        result.total_duration_seconds = time.monotonic() - start_time
        result.trace = trace
        _fire_hook(
            self._hooks,
            event=HookEvent.POST_EXECUTE,
            dag=self._dag,
            execution_result=result,
        )
        return result


class AsyncDAGExecutor:
    """Execute DAG tasks using asyncio.

    Uses the DAG's execution plan to determine execution order and parallelism.
    Tasks within the same step are dispatched concurrently using asyncio.gather,
    with an optional semaphore for limiting concurrency.

    Args:
        dag: A dagron.DAG instance.
        max_workers: Maximum concurrent workers. None = unlimited parallelism.
        costs: Optional dict mapping node names to duration estimates.
        callbacks: Optional ExecutionCallbacks for lifecycle events.
        fail_fast: If True, skip downstream nodes when a dependency fails.
    """

    def __init__(
        self,
        dag: DAG,
        max_workers: int | None = None,
        costs: dict[str, float] | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
        hooks: HookRegistry | None = None,
    ):
        self._dag = dag
        self._max_workers = max_workers
        self._costs = costs
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing
        self._hooks = hooks

    async def execute(
        self,
        tasks: dict[str, Callable[[], Awaitable[Any]]],
        timeout: float | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> ExecutionResult:
        """Execute async tasks according to the DAG's dependency order.

        Args:
            tasks: Dict mapping node names to async callables.
                Each callable takes no arguments and returns an awaitable.
            timeout: Optional per-node timeout in seconds. Nodes that exceed
                this duration get TIMED_OUT status.
            cancel_event: Optional asyncio.Event. When set, remaining
                unstarted nodes get CANCELLED status.

        Returns:
            ExecutionResult with per-node results and aggregate counts.
        """
        from dagron.execution.tracing import ExecutionTrace, TraceEventType
        from dagron.plugins.hooks import HookEvent

        if self._max_workers is not None:
            plan = self._dag.execution_plan_constrained(self._max_workers, self._costs)
        else:
            plan = self._dag.execution_plan(self._costs)

        semaphore = asyncio.Semaphore(self._max_workers) if self._max_workers else None
        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        failed_nodes: set[str] = set()
        start_time = time.monotonic()

        _fire_hook(self._hooks, event=HookEvent.PRE_EXECUTE, dag=self._dag)

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        ancestors_cache: dict[str, set[str]] = {}

        def get_ancestors(name: str) -> set[str]:
            if name not in ancestors_cache:
                anc = {n.name for n in self._dag.ancestors(name)}
                ancestors_cache[name] = anc
            return ancestors_cache[name]

        for step in plan.steps:
            if trace:
                trace.record(TraceEventType.STEP_STARTED, step_index=step.step_index)

            # Check cancellation between steps
            if cancel_event is not None and cancel_event.is_set():
                for scheduled_node in step.nodes:
                    name = scheduled_node.node.name
                    if name not in result.node_results:
                        nr = NodeResult(name=name, status=NodeStatus.CANCELLED)
                        result.node_results[name] = nr
                        result.cancelled += 1
                        if trace:
                            trace.record(TraceEventType.NODE_CANCELLED, node_name=name)
                if trace:
                    trace.record(TraceEventType.STEP_COMPLETED, step_index=step.step_index)
                continue

            coros = []
            names = []

            for scheduled_node in step.nodes:
                name = scheduled_node.node.name

                if self._fail_fast and failed_nodes and get_ancestors(name) & failed_nodes:
                    _record_skip(name, result, self._callbacks, trace)
                    continue

                task_fn = tasks.get(name)
                if task_fn is None:
                    _record_skip(name, result, self._callbacks, trace)
                    continue

                if trace:
                    trace.record(TraceEventType.NODE_STARTED, node_name=name)
                _fire_hook(self._hooks, event=HookEvent.PRE_NODE, dag=self._dag, node_name=name)
                coros.append(self._run_task(name, task_fn, semaphore, timeout))
                names.append(name)

            if coros:
                node_results = await asyncio.gather(*coros)
                for name, nr in zip(names, node_results, strict=True):
                    result.node_results[name] = nr
                    if nr.status == NodeStatus.COMPLETED:
                        result.succeeded += 1
                        _fire_hook(
                            self._hooks,
                            event=HookEvent.POST_NODE,
                            dag=self._dag,
                            node_name=name,
                            node_result=nr.result,
                        )
                        if trace:
                            trace.record(
                                TraceEventType.NODE_COMPLETED,
                                node_name=name,
                                duration=nr.duration_seconds,
                            )
                    elif nr.status == NodeStatus.FAILED:
                        result.failed += 1
                        failed_nodes.add(name)
                        _fire_hook(
                            self._hooks,
                            event=HookEvent.ON_ERROR,
                            dag=self._dag,
                            node_name=name,
                            error=nr.error,
                        )
                        if trace:
                            trace.record(
                                TraceEventType.NODE_FAILED,
                                node_name=name,
                                duration=nr.duration_seconds,
                                error=str(nr.error) if nr.error else None,
                            )
                    elif nr.status == NodeStatus.TIMED_OUT:
                        result.timed_out += 1
                        failed_nodes.add(name)
                        _fire_hook(
                            self._hooks,
                            event=HookEvent.ON_ERROR,
                            dag=self._dag,
                            node_name=name,
                        )
                        if trace:
                            trace.record(
                                TraceEventType.NODE_TIMED_OUT,
                                node_name=name,
                                duration=nr.duration_seconds,
                            )

            if trace:
                trace.record(TraceEventType.STEP_COMPLETED, step_index=step.step_index)

        if trace:
            trace.record(TraceEventType.EXECUTION_COMPLETED)

        result.total_duration_seconds = time.monotonic() - start_time
        result.trace = trace
        _fire_hook(
            self._hooks,
            event=HookEvent.POST_EXECUTE,
            dag=self._dag,
            execution_result=result,
        )
        return result

    async def _run_task(
        self,
        name: str,
        task_fn: Callable[[], Awaitable[Any]],
        semaphore: asyncio.Semaphore | None,
        timeout: float | None = None,
    ) -> NodeResult:
        if self._callbacks.on_start:
            self._callbacks.on_start(name)

        t0 = time.monotonic()
        try:
            if semaphore:
                async with semaphore:
                    if timeout is not None:
                        value = await asyncio.wait_for(task_fn(), timeout=timeout)
                    else:
                        value = await task_fn()
            else:
                if timeout is not None:
                    value = await asyncio.wait_for(task_fn(), timeout=timeout)
                else:
                    value = await task_fn()
            duration = time.monotonic() - t0
            if self._callbacks.on_complete:
                self._callbacks.on_complete(name, value)
            return NodeResult(
                name=name,
                status=NodeStatus.COMPLETED,
                result=value,
                duration_seconds=duration,
            )
        except TimeoutError:
            duration = time.monotonic() - t0
            return NodeResult(
                name=name,
                status=NodeStatus.TIMED_OUT,
                duration_seconds=duration,
            )
        except Exception as exc:
            duration = time.monotonic() - t0
            if self._callbacks.on_failure:
                self._callbacks.on_failure(name, exc)
            return NodeResult(
                name=name,
                status=NodeStatus.FAILED,
                error=exc,
                duration_seconds=duration,
            )
