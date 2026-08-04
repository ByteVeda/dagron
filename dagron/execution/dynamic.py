"""Dynamic DAG modification during execution."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG

from dagron.execution._helpers import _run_sync_task
from dagron.execution._types import (
    ExecutionCallbacks,
    ExecutionResult,
    NodeResult,
    NodeStatus,
)


@dataclass
class DynamicNodeSpec:
    """Specification for a dynamically-added node."""

    name: str
    task: Callable[[], Any]
    dependencies: list[str] = field(default_factory=list)
    dependents: list[str] = field(default_factory=list)


@dataclass
class DynamicModification:
    """A batch of dynamic modifications to apply to a running DAG."""

    add_nodes: list[DynamicNodeSpec] = field(default_factory=list)
    remove_nodes: list[str] = field(default_factory=list)


class DynamicExecutor:
    """Execute a DAG with support for dynamic modifications during execution.

    Expanders are called after a node completes. They receive the node name and
    its result, and may return a ``DynamicModification`` describing new nodes
    to add or existing nodes to remove. Modifications are applied to a runtime
    snapshot — the original DAG is never mutated.

    Args:
        dag: The base DAG.
        expanders: Dict mapping node names to expander functions.
            Each expander is ``(node_name, result) -> DynamicModification | None``.
        max_workers: Maximum concurrent workers.
        callbacks: Optional execution callbacks.
        fail_fast: If True, skip downstream nodes when a dependency fails.
        enable_tracing: If True, record execution trace.
    """

    def __init__(
        self,
        dag: DAG,
        expanders: dict[str, Callable[[str, Any], DynamicModification | None]] | None = None,
        max_workers: int | None = None,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
    ) -> None:
        self._dag = dag
        self._expanders = expanders or {}
        self._max_workers = max_workers
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing
        self._dynamic_origin: dict[str, str] = {}

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
    ) -> ExecutionResult:
        """Execute tasks with dynamic expansion support.

        Processes nodes in topological order. After each node completes,
        checks if an expander exists for that node and applies any
        modifications to the runtime DAG.

        Args:
            tasks: Dict mapping node names to callables. Dynamically-added
                nodes get their tasks from DynamicNodeSpec.task.

        Returns:
            ExecutionResult with per-node results.
        """
        from dagron.execution.tracing import ExecutionTrace, TraceEventType

        self._runtime_dag = self._dag.snapshot()
        self._runtime_tasks = dict(tasks)
        trace = ExecutionTrace() if self._enable_tracing else None
        self._result = ExecutionResult()
        failed_nodes: set[str] = set()
        self._completed_nodes: set[str] = set()
        start_time = time.monotonic()
        self._dynamic_origin.clear()

        # Local aliases for readability
        runtime_dag = self._runtime_dag
        runtime_tasks = self._runtime_tasks
        result = self._result
        completed_nodes = self._completed_nodes

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        self._ancestors_cache: dict[str, set[str]] = {}
        ancestors_cache = self._ancestors_cache

        def get_ancestors(name: str) -> set[str]:
            if name not in ancestors_cache:
                anc = {n.name for n in runtime_dag.ancestors(name)}
                ancestors_cache[name] = anc
            return ancestors_cache[name]

        def get_ready_nodes() -> list[str]:
            """Get nodes whose dependencies have all completed."""
            ready = []
            all_nodes = [n.name for n in runtime_dag.topological_sort()]
            for name in all_nodes:
                if name in completed_nodes or name in result.node_results:
                    continue
                preds = {n.name for n in runtime_dag.predecessors(name)}
                if preds <= completed_nodes:
                    ready.append(name)
            return ready

        while True:
            ready = get_ready_nodes()
            if not ready:
                break

            for name in ready:
                # Skip if a dependency has failed
                if self._fail_fast and failed_nodes and get_ancestors(name) & failed_nodes:
                    nr: NodeResult[Any] = NodeResult(name=name, status=NodeStatus.SKIPPED)
                    result.node_results[name] = nr
                    result.skipped += 1
                    completed_nodes.add(name)
                    if self._callbacks.on_skip:
                        self._callbacks.on_skip(name)
                    if trace:
                        trace.record(TraceEventType.NODE_SKIPPED, node_name=name)
                    continue

                task_fn = runtime_tasks.get(name)
                if task_fn is None:
                    nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                    result.node_results[name] = nr
                    result.skipped += 1
                    completed_nodes.add(name)
                    if trace:
                        trace.record(TraceEventType.NODE_SKIPPED, node_name=name)
                    continue

                if trace:
                    trace.record(TraceEventType.NODE_STARTED, node_name=name)

                nr = _run_sync_task(name, task_fn, self._callbacks)
                result.node_results[name] = nr
                completed_nodes.add(name)

                if nr.status == NodeStatus.COMPLETED:
                    result.succeeded += 1
                    if trace:
                        trace.record(
                            TraceEventType.NODE_COMPLETED,
                            node_name=name,
                            duration=nr.duration_seconds,
                        )

                    # Check expanders
                    expander = self._expanders.get(name)
                    if expander is not None:
                        self._cleanup_orphans(name)
                        mod = expander(name, nr.result)
                        if mod is not None:
                            self._apply_modification(
                                runtime_dag,
                                runtime_tasks,
                                mod,
                                ancestors_cache,
                                parent_name=name,
                            )
                            if self._callbacks.on_dynamic_expand:
                                self._callbacks.on_dynamic_expand(name, mod)

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

    def _cleanup_orphans(self, parent_name: str) -> None:
        """Remove all dynamic nodes previously spawned by parent_name, recursively."""
        # Collect direct children of this parent
        to_remove: list[str] = []
        for node, origin in list(self._dynamic_origin.items()):
            if origin == parent_name:
                to_remove.append(node)

        # Recursively collect transitive dynamic descendants
        all_orphans: set[str] = set()
        stack = list(to_remove)
        while stack:
            node = stack.pop()
            if node in all_orphans:
                continue
            all_orphans.add(node)
            # Find children spawned by this node
            for child, origin in list(self._dynamic_origin.items()):
                if origin == node and child not in all_orphans:
                    stack.append(child)

        # Remove orphans from runtime state
        for node in all_orphans:
            if self._runtime_dag.has_node(node):
                self._runtime_dag.remove_node(node)
            self._runtime_tasks.pop(node, None)
            self._ancestors_cache.pop(node, None)
            self._completed_nodes.discard(node)
            self._result.node_results.pop(node, None)
            del self._dynamic_origin[node]

    def _apply_modification(
        self,
        dag: DAG,
        tasks: dict[str, Callable[[], Any]],
        mod: DynamicModification,
        ancestors_cache: dict[str, set[str]],
        parent_name: str = "",
    ) -> None:
        """Apply a dynamic modification to the runtime DAG."""
        # Remove nodes first
        for name in mod.remove_nodes:
            if dag.has_node(name):
                dag.remove_node(name)
                tasks.pop(name, None)
                ancestors_cache.pop(name, None)

        # Add new nodes
        for spec in mod.add_nodes:
            if not dag.has_node(spec.name):
                dag.add_node(spec.name)
            tasks[spec.name] = spec.task
            if parent_name:
                self._dynamic_origin[spec.name] = parent_name

            # Add dependency edges (dep -> spec.name)
            for dep in spec.dependencies:
                if dag.has_node(dep) and not dag.has_edge(dep, spec.name):
                    dag.add_edge(dep, spec.name)

            # Add dependent edges (spec.name -> dependent)
            for dependent in spec.dependents:
                if dag.has_node(dependent) and not dag.has_edge(spec.name, dependent):
                    dag.add_edge(spec.name, dependent)

            # Invalidate ancestors cache for affected nodes
            ancestors_cache.pop(spec.name, None)
            for dependent in spec.dependents:
                ancestors_cache.pop(dependent, None)
