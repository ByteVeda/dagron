"""Pure Python execution harness for DAG task scheduling."""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:
    from dagron._internal import DAG


class NodeStatus(Enum):
    """Status of a node during execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"


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
    timed_out: int = 0
    cancelled: int = 0
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

    def execute(
        self,
        tasks: dict[str, Callable],
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
                # Check cancellation between steps
                if cancel_event is not None and cancel_event.is_set():
                    for scheduled_node in step.nodes:
                        name = scheduled_node.node.name
                        if name not in result.node_results:
                            nr = NodeResult(name=name, status=NodeStatus.CANCELLED)
                            result.node_results[name] = nr
                            result.cancelled += 1
                    continue

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
                    elif nr.status == NodeStatus.FAILED:
                        result.failed += 1
                        failed_nodes.add(name)
                    elif nr.status == NodeStatus.TIMED_OUT:
                        result.timed_out += 1
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

    async def execute(
        self,
        tasks: dict[str, Callable[[], Awaitable]],
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
            # Check cancellation between steps
            if cancel_event is not None and cancel_event.is_set():
                for scheduled_node in step.nodes:
                    name = scheduled_node.node.name
                    if name not in result.node_results:
                        nr = NodeResult(name=name, status=NodeStatus.CANCELLED)
                        result.node_results[name] = nr
                        result.cancelled += 1
                continue

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

                coros.append(self._run_task(name, task_fn, semaphore, timeout))
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
                    elif nr.status == NodeStatus.TIMED_OUT:
                        result.timed_out += 1
                        failed_nodes.add(name)

        result.total_duration_seconds = time.monotonic() - start_time
        return result

    async def _run_task(
        self,
        name: str,
        task_fn: Callable[[], Awaitable],
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
        except asyncio.TimeoutError:
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


@dataclass
class IncrementalResult:
    """Result of an incremental DAG execution."""

    node_results: dict[str, NodeResult] = field(default_factory=dict)
    recomputed: list[str] = field(default_factory=list)
    early_cutoff: list[str] = field(default_factory=list)
    reused: list[str] = field(default_factory=list)
    provenance: dict[str, list[str]] = field(default_factory=dict)
    total_duration_seconds: float = 0.0


class IncrementalExecutor:
    """Execute DAG tasks with early-cutoff incremental recomputation.

    Maintains a cache of previous results across calls. On subsequent
    executions with `changed_nodes`, only the dirty set is re-evaluated,
    and propagation stops when a node produces the same result as before.

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
    ):
        self._dag = dag
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._cache: dict[str, Any] = {}

    def execute(
        self,
        tasks: dict[str, Callable],
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
        start_time = time.monotonic()
        result = IncrementalResult()

        # Get topological order for deterministic processing
        topo_order = [n.name for n in self._dag.topological_sort()]

        # First run or no changed_nodes: execute everything
        if not self._cache or changed_nodes is None:
            for name in topo_order:
                task_fn = tasks.get(name)
                if task_fn is None:
                    continue
                nr = self._run_task(name, task_fn)
                result.node_results[name] = nr
                if nr.status == NodeStatus.COMPLETED:
                    self._cache[name] = nr.result
                    result.recomputed.append(name)

            result.total_duration_seconds = time.monotonic() - start_time
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

            nr = self._run_task(name, task_fn)
            result.node_results[name] = nr

            if nr.status == NodeStatus.COMPLETED:
                old_result = self._cache.get(name)
                result.recomputed.append(name)

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
