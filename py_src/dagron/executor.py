"""Pure Python execution harness for DAG task scheduling."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable


class NodeStatus(Enum):
    """Status of a node during execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class NodeResult:
    """Result of executing a single node."""

    name: str
    status: NodeStatus
    result: Any = None
    error: Exception | None = None
    duration_seconds: float = 0.0


@dataclass
class ExecutionCallbacks:
    """Optional callbacks for execution events."""

    on_start: Callable[[str], None] | None = None
    on_complete: Callable[[str, Any], None] | None = None
    on_failure: Callable[[str, Exception], None] | None = None
    on_skip: Callable[[str], None] | None = None


@dataclass
class ExecutionResult:
    """Aggregate result of executing an entire DAG."""

    node_results: dict[str, NodeResult] = field(default_factory=dict)
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    total_duration_seconds: float = 0.0


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
        dag,
        max_workers: int | None = None,
        costs: dict[str, float] | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
    ):
        self._dag = dag
        self._max_workers = max_workers
        self._costs = costs
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast

    def execute(self, tasks: dict[str, Callable]) -> ExecutionResult:
        """Execute tasks according to the DAG's dependency order.

        Args:
            tasks: Dict mapping node names to callables.
                Each callable takes no arguments and returns a result.

        Returns:
            ExecutionResult with per-node results and aggregate counts.
        """
        if self._max_workers is not None:
            plan = self._dag.execution_plan_constrained(self._max_workers, self._costs)
        else:
            plan = self._dag.execution_plan(self._costs)

        result = ExecutionResult()
        failed_nodes: set[str] = set()
        start_time = time.monotonic()

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
                futures = {}
                for scheduled_node in step.nodes:
                    name = scheduled_node.node.name

                    # Skip if a dependency has failed
                    if self._fail_fast and failed_nodes and get_ancestors(name) & failed_nodes:
                        nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                        result.node_results[name] = nr
                        result.skipped += 1
                        if self._callbacks.on_skip:
                            self._callbacks.on_skip(name)
                        continue

                    task_fn = tasks.get(name)
                    if task_fn is None:
                        nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                        result.node_results[name] = nr
                        result.skipped += 1
                        if self._callbacks.on_skip:
                            self._callbacks.on_skip(name)
                        continue

                    futures[pool.submit(self._run_task, name, task_fn)] = name

                # Wait for all futures in this step
                for future in futures:
                    name = futures[future]
                    nr = future.result()
                    result.node_results[name] = nr
                    if nr.status == NodeStatus.COMPLETED:
                        result.succeeded += 1
                    elif nr.status == NodeStatus.FAILED:
                        result.failed += 1
                        failed_nodes.add(name)

        result.total_duration_seconds = time.monotonic() - start_time
        return result

    def _run_task(self, name: str, task_fn: Callable) -> NodeResult:
        if self._callbacks.on_start:
            self._callbacks.on_start(name)

        t0 = time.monotonic()
        try:
            value = task_fn()
            duration = time.monotonic() - t0
            if self._callbacks.on_complete:
                self._callbacks.on_complete(name, value)
            return NodeResult(
                name=name,
                status=NodeStatus.COMPLETED,
                result=value,
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
        dag,
        max_workers: int | None = None,
        costs: dict[str, float] | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
    ):
        self._dag = dag
        self._max_workers = max_workers
        self._costs = costs
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast

    async def execute(self, tasks: dict[str, Callable[[], Awaitable]]) -> ExecutionResult:
        """Execute async tasks according to the DAG's dependency order.

        Args:
            tasks: Dict mapping node names to async callables.
                Each callable takes no arguments and returns an awaitable.

        Returns:
            ExecutionResult with per-node results and aggregate counts.
        """
        if self._max_workers is not None:
            plan = self._dag.execution_plan_constrained(self._max_workers, self._costs)
        else:
            plan = self._dag.execution_plan(self._costs)

        semaphore = asyncio.Semaphore(self._max_workers) if self._max_workers else None
        result = ExecutionResult()
        failed_nodes: set[str] = set()
        start_time = time.monotonic()

        ancestors_cache: dict[str, set[str]] = {}

        def get_ancestors(name: str) -> set[str]:
            if name not in ancestors_cache:
                anc = {n.name for n in self._dag.ancestors(name)}
                ancestors_cache[name] = anc
            return ancestors_cache[name]

        for step in plan.steps:
            coros = []
            names = []

            for scheduled_node in step.nodes:
                name = scheduled_node.node.name

                if self._fail_fast and failed_nodes and get_ancestors(name) & failed_nodes:
                    nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                    result.node_results[name] = nr
                    result.skipped += 1
                    if self._callbacks.on_skip:
                        self._callbacks.on_skip(name)
                    continue

                task_fn = tasks.get(name)
                if task_fn is None:
                    nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                    result.node_results[name] = nr
                    result.skipped += 1
                    if self._callbacks.on_skip:
                        self._callbacks.on_skip(name)
                    continue

                coros.append(self._run_task(name, task_fn, semaphore))
                names.append(name)

            if coros:
                node_results = await asyncio.gather(*coros)
                for name, nr in zip(names, node_results):
                    result.node_results[name] = nr
                    if nr.status == NodeStatus.COMPLETED:
                        result.succeeded += 1
                    elif nr.status == NodeStatus.FAILED:
                        result.failed += 1
                        failed_nodes.add(name)

        result.total_duration_seconds = time.monotonic() - start_time
        return result

    async def _run_task(
        self,
        name: str,
        task_fn: Callable[[], Awaitable],
        semaphore: asyncio.Semaphore | None,
    ) -> NodeResult:
        if self._callbacks.on_start:
            self._callbacks.on_start(name)

        t0 = time.monotonic()
        try:
            if semaphore:
                async with semaphore:
                    value = await task_fn()
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
