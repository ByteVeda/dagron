"""Resource-aware DAG execution with heterogeneous resource constraints."""

from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from dagron._internal import DAG

from dagron.execution._helpers import _record_skip, _run_sync_task
from dagron.execution._types import (
    ExecutionCallbacks,
    ExecutionResult,
    NodeResult,
    NodeStatus,
)


@dataclass(frozen=True)
class ResourceRequirements:
    """Resource requirements for a node.

    Example::

        req = ResourceRequirements(resources={"gpu": 2, "memory_mb": 4096})
        # or use shorthand:
        req = ResourceRequirements.gpu(2)
    """

    resources: dict[str, int] = field(default_factory=dict)

    @staticmethod
    def gpu(count: int = 1) -> ResourceRequirements:
        return ResourceRequirements(resources={"gpu": count})

    @staticmethod
    def cpu(count: int = 1) -> ResourceRequirements:
        return ResourceRequirements(resources={"cpu_slots": count})

    @staticmethod
    def memory(mb: int) -> ResourceRequirements:
        return ResourceRequirements(resources={"memory_mb": mb})

    def fits(self, available: dict[str, int]) -> bool:
        """Check if these requirements fit within available resources."""
        for resource, needed in self.resources.items():
            if available.get(resource, 0) < needed:
                return False
        return True


@dataclass
class ResourceSnapshot:
    """Point-in-time snapshot of resource utilization."""

    timestamp: float
    allocated: dict[str, int]
    available: dict[str, int]
    node_name: str | None = None
    event: str = ""  # "acquired" or "released"


class ResourceTimeline:
    """Records resource utilization snapshots over time."""

    def __init__(self) -> None:
        self._snapshots: list[ResourceSnapshot] = []
        self._start_time: float | None = None

    def record(
        self,
        allocated: dict[str, int],
        available: dict[str, int],
        node_name: str | None = None,
        event: str = "",
    ) -> None:
        if self._start_time is None:
            self._start_time = time.monotonic()
        self._snapshots.append(
            ResourceSnapshot(
                timestamp=time.monotonic() - self._start_time,
                allocated=dict(allocated),
                available=dict(available),
                node_name=node_name,
                event=event,
            )
        )

    @property
    def snapshots(self) -> list[ResourceSnapshot]:
        return list(self._snapshots)

    def peak_utilization(self) -> dict[str, int]:
        """Return peak allocation for each resource."""
        peaks: dict[str, int] = {}
        for snap in self._snapshots:
            for resource, amount in snap.allocated.items():
                peaks[resource] = max(peaks.get(resource, 0), amount)
        return peaks


class ResourcePool:
    """Thread-safe pool of resources with blocking acquire/release.

    Args:
        capacities: Dict mapping resource names to total capacity.
    """

    def __init__(self, capacities: dict[str, int]) -> None:
        self._capacities = dict(capacities)
        self._available = dict(capacities)
        self._allocated: dict[str, int] = dict.fromkeys(capacities, 0)
        self._condition = threading.Condition()
        self._timeline = ResourceTimeline()

    @property
    def capacities(self) -> dict[str, int]:
        return dict(self._capacities)

    @property
    def available(self) -> dict[str, int]:
        with self._condition:
            return dict(self._available)

    @property
    def allocated(self) -> dict[str, int]:
        with self._condition:
            return dict(self._allocated)

    @property
    def timeline(self) -> ResourceTimeline:
        return self._timeline

    def can_satisfy(self, requirements: ResourceRequirements) -> bool:
        """Check if the pool can ever satisfy these requirements (capacity check)."""
        for resource, needed in requirements.resources.items():
            if self._capacities.get(resource, 0) < needed:
                return False
        return True

    def try_acquire(
        self, requirements: ResourceRequirements, node_name: str | None = None
    ) -> bool:
        """Try to acquire resources without blocking. Returns True if acquired."""
        with self._condition:
            if requirements.fits(self._available):
                for resource, needed in requirements.resources.items():
                    self._available[resource] -= needed
                    self._allocated[resource] += needed
                self._timeline.record(
                    self._allocated,
                    self._available,
                    node_name=node_name,
                    event="acquired",
                )
                return True
            return False

    def acquire(
        self,
        requirements: ResourceRequirements,
        node_name: str | None = None,
        timeout: float | None = None,
    ) -> bool:
        """Acquire resources, blocking until available. Returns True if acquired."""
        with self._condition:
            deadline = time.monotonic() + timeout if timeout is not None else None
            while not requirements.fits(self._available):
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                self._condition.wait(timeout=remaining)

            for resource, needed in requirements.resources.items():
                self._available[resource] -= needed
                self._allocated[resource] += needed
            self._timeline.record(
                self._allocated,
                self._available,
                node_name=node_name,
                event="acquired",
            )
            return True

    def release(self, requirements: ResourceRequirements, node_name: str | None = None) -> None:
        """Release previously acquired resources."""
        with self._condition:
            for resource, needed in requirements.resources.items():
                self._available[resource] = min(
                    self._available.get(resource, 0) + needed,
                    self._capacities.get(resource, needed),
                )
                self._allocated[resource] = max(
                    self._allocated.get(resource, 0) - needed,
                    0,
                )
            self._timeline.record(
                self._allocated,
                self._available,
                node_name=node_name,
                event="released",
            )
            self._condition.notify_all()


class ResourceAwareExecutor:
    """Execute DAG tasks respecting heterogeneous resource constraints.

    Uses a custom event-driven scheduler that dispatches nodes based on
    bottom-level priority and resource availability.

    Args:
        dag: The DAG to execute.
        resource_pool: Pool of available resources.
        requirements: Dict mapping node names to their resource requirements.
        costs: Optional node cost estimates for priority scheduling.
        callbacks: Optional execution callbacks.
        fail_fast: If True, skip downstream nodes when a dependency fails.
        enable_tracing: If True, record execution trace.
    """

    def __init__(
        self,
        dag: DAG,
        resource_pool: ResourcePool,
        requirements: dict[str, ResourceRequirements] | None = None,
        costs: dict[str, float] | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
    ) -> None:
        self._dag = dag
        self._pool = resource_pool
        self._requirements = requirements or {}
        self._costs = costs
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
    ) -> ExecutionResult:
        """Execute tasks respecting resource constraints.

        Pre-validates that all requirements can be satisfied by pool capacity.
        Dispatches nodes in bottom-level priority order when resources are available.

        Returns:
            ExecutionResult with per-node results and resource timeline.
        """
        from dagron.execution.tracing import ExecutionTrace, TraceEventType

        # Pre-validation
        for name, req in self._requirements.items():
            if not self._pool.can_satisfy(req):
                raise ValueError(
                    f"Node '{name}' requires {req.resources} but pool capacity "
                    f"is {self._pool.capacities}"
                )

        # Compute bottom levels for priority
        bottom_levels = self._dag.bottom_levels(self._costs)  # type: ignore[attr-defined]

        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        failed_nodes: set[str] = set()
        completed_nodes: set[str] = set()
        start_time = time.monotonic()

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        # Compute in-degrees
        in_degree: dict[str, int] = {}
        for node in self._dag.topological_sort():
            name = node.name
            preds = list(self._dag.predecessors(name))
            in_degree[name] = len(preds)

        # Successors map
        successors: dict[str, list[str]] = {}
        for node in self._dag.topological_sort():
            name = node.name
            succs = list(self._dag.successors(name))
            successors[name] = [s.name for s in succs]

        # Ancestors cache for fail-fast
        ancestors_cache: dict[str, set[str]] = {}

        def get_ancestors(name: str) -> set[str]:
            if name not in ancestors_cache:
                ancestors_cache[name] = {n.name for n in self._dag.ancestors(name)}
            return ancestors_cache[name]

        # Initialize ready queue (nodes with in-degree 0)
        ready: list[str] = sorted(
            [name for name, deg in in_degree.items() if deg == 0],
            key=lambda n: -bottom_levels.get(n, 0.0),
        )

        active_futures: dict[Future[NodeResult], tuple[str, ResourceRequirements]] = {}

        pool_workers = max(len(tasks), 1)
        with ThreadPoolExecutor(max_workers=pool_workers) as pool:
            while completed_nodes != set(in_degree.keys()):
                # Try to dispatch ready nodes
                still_ready: list[str] = []
                for name in ready:
                    if name in completed_nodes or name in result.node_results:
                        continue

                    # Skip if ancestor failed
                    if self._fail_fast and failed_nodes and get_ancestors(name) & failed_nodes:
                        _record_skip(name, result, self._callbacks, trace)
                        completed_nodes.add(name)
                        self._update_successors(
                            name,
                            in_degree,
                            successors,
                            bottom_levels,
                            still_ready,
                            completed_nodes,
                        )
                        continue

                    task_fn = tasks.get(name)
                    if task_fn is None:
                        _record_skip(name, result, self._callbacks, trace)
                        completed_nodes.add(name)
                        self._update_successors(
                            name,
                            in_degree,
                            successors,
                            bottom_levels,
                            still_ready,
                            completed_nodes,
                        )
                        continue

                    req = self._requirements.get(name, ResourceRequirements())
                    if self._pool.try_acquire(req, node_name=name):
                        if trace:
                            trace.record(TraceEventType.NODE_STARTED, node_name=name)
                            trace.record(
                                TraceEventType.RESOURCE_ACQUIRED,
                                node_name=name,
                                metadata={"resources": req.resources},
                            )
                        if self._callbacks.on_resource_acquired:
                            self._callbacks.on_resource_acquired(name, req.resources)

                        future = pool.submit(_run_sync_task, name, task_fn, self._callbacks)
                        active_futures[future] = (name, req)
                    else:
                        still_ready.append(name)

                ready = still_ready

                if not active_futures:
                    if ready:
                        # All ready nodes need resources — wait briefly
                        time.sleep(0.001)
                        continue
                    else:
                        break

                # Wait for any future to complete
                import concurrent.futures

                done, _ = concurrent.futures.wait(
                    active_futures.keys(),
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )

                for future in done:
                    name, req = active_futures.pop(future)
                    nr = future.result()
                    result.node_results[name] = nr
                    completed_nodes.add(name)

                    # Release resources
                    self._pool.release(req, node_name=name)
                    if trace:
                        trace.record(
                            TraceEventType.RESOURCE_RELEASED,
                            node_name=name,
                            metadata={"resources": req.resources},
                        )
                    if self._callbacks.on_resource_released:
                        self._callbacks.on_resource_released(name, req.resources)

                    if nr.status == NodeStatus.COMPLETED:
                        result.succeeded += 1
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

                    # Update successors
                    self._update_successors(
                        name,
                        in_degree,
                        successors,
                        bottom_levels,
                        ready,
                        completed_nodes,
                    )

        if trace:
            trace.record(TraceEventType.EXECUTION_COMPLETED)

        result.total_duration_seconds = time.monotonic() - start_time
        result.trace = trace
        return result

    def _update_successors(
        self,
        name: str,
        in_degree: dict[str, int],
        successors: dict[str, list[str]],
        bottom_levels: dict[str, float],
        ready: list[str],
        completed: set[str],
    ) -> None:
        """Update successor in-degrees and add newly-ready nodes."""
        for succ in successors.get(name, []):
            in_degree[succ] -= 1
            if in_degree[succ] == 0 and succ not in completed:
                ready.append(succ)
        # Re-sort ready queue by bottom level
        ready.sort(key=lambda n: -bottom_levels.get(n, 0.0))


class AsyncResourceAwareExecutor:
    """Async version of ResourceAwareExecutor.

    Uses asyncio for concurrency while respecting resource constraints.
    """

    def __init__(
        self,
        dag: DAG,
        resource_pool: ResourcePool,
        requirements: dict[str, ResourceRequirements] | None = None,
        costs: dict[str, float] | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
    ) -> None:
        self._dag = dag
        self._pool = resource_pool
        self._requirements = requirements or {}
        self._costs = costs
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing

    async def execute(
        self,
        tasks: dict[str, Callable[[], Awaitable[Any]]],
    ) -> ExecutionResult:
        """Execute async tasks respecting resource constraints."""
        from dagron.execution.tracing import ExecutionTrace, TraceEventType

        for name, req in self._requirements.items():
            if not self._pool.can_satisfy(req):
                raise ValueError(
                    f"Node '{name}' requires {req.resources} but pool capacity "
                    f"is {self._pool.capacities}"
                )

        bottom_levels = self._dag.bottom_levels(self._costs)  # type: ignore[attr-defined]

        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        failed_nodes: set[str] = set()
        completed_nodes: set[str] = set()
        start_time = time.monotonic()

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        in_degree: dict[str, int] = {}
        successors: dict[str, list[str]] = {}
        for node in self._dag.topological_sort():
            name = node.name
            preds = list(self._dag.predecessors(name))
            in_degree[name] = len(preds)
            succs = list(self._dag.successors(name))
            successors[name] = [s.name for s in succs]

        ancestors_cache: dict[str, set[str]] = {}

        def get_ancestors(name: str) -> set[str]:
            if name not in ancestors_cache:
                ancestors_cache[name] = {n.name for n in self._dag.ancestors(name)}
            return ancestors_cache[name]

        ready = sorted(
            [name for name, deg in in_degree.items() if deg == 0],
            key=lambda n: -bottom_levels.get(n, 0.0),
        )

        active_tasks: dict[asyncio.Task[NodeResult], tuple[str, ResourceRequirements]] = {}
        all_nodes = set(in_degree.keys())

        while completed_nodes != all_nodes:
            still_ready = []
            for name in ready:
                if name in completed_nodes or name in result.node_results:
                    continue

                if self._fail_fast and failed_nodes and get_ancestors(name) & failed_nodes:
                    nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                    result.node_results[name] = nr
                    result.skipped += 1
                    completed_nodes.add(name)
                    for succ in successors.get(name, []):
                        in_degree[succ] -= 1
                        if in_degree[succ] == 0 and succ not in completed_nodes:
                            still_ready.append(succ)
                    continue

                task_fn = tasks.get(name)
                if task_fn is None:
                    nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                    result.node_results[name] = nr
                    result.skipped += 1
                    completed_nodes.add(name)
                    for succ in successors.get(name, []):
                        in_degree[succ] -= 1
                        if in_degree[succ] == 0 and succ not in completed_nodes:
                            still_ready.append(succ)
                    continue

                req = self._requirements.get(name, ResourceRequirements())
                if self._pool.try_acquire(req, node_name=name):
                    if trace:
                        trace.record(TraceEventType.NODE_STARTED, node_name=name)

                    async_task = asyncio.create_task(self._run_task(name, task_fn))
                    active_tasks[async_task] = (name, req)
                else:
                    still_ready.append(name)

            ready = still_ready

            if not active_tasks:
                if ready:
                    await asyncio.sleep(0.001)
                    continue
                else:
                    break

            done, _ = await asyncio.wait(
                active_tasks.keys(),
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                name, req = active_tasks.pop(task)
                nr = task.result()
                result.node_results[name] = nr
                completed_nodes.add(name)

                self._pool.release(req, node_name=name)

                if nr.status == NodeStatus.COMPLETED:
                    result.succeeded += 1
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
                        )

                for succ in successors.get(name, []):
                    in_degree[succ] -= 1
                    if in_degree[succ] == 0 and succ not in completed_nodes:
                        ready.append(succ)
                ready.sort(key=lambda n: -bottom_levels.get(n, 0.0))

        if trace:
            trace.record(TraceEventType.EXECUTION_COMPLETED)

        result.total_duration_seconds = time.monotonic() - start_time
        result.trace = trace
        return result

    async def _run_task(
        self,
        name: str,
        task_fn: Callable[[], Awaitable[Any]],
    ) -> NodeResult:
        if self._callbacks.on_start:
            self._callbacks.on_start(name)
        t0 = time.monotonic()
        try:
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
