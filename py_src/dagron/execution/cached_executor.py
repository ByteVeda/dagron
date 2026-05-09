"""Cached DAG executor using content-addressable caching."""

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
from dagron.execution.content_cache import (
    CacheKeyBuilder,
    ContentAddressableCache,
)


@dataclass
class CachedExecutionResult:
    """Extended execution result with cache statistics."""

    execution_result: ExecutionResult
    cache_hits: int = 0
    cache_misses: int = 0
    nodes_executed: list[str] = field(default_factory=list)
    nodes_cached: list[str] = field(default_factory=list)


class CachedDAGExecutor:
    """Execute a DAG with content-addressable caching.

    Processes nodes in topological order. For each node, computes a Merkle-tree
    cache key from the node name, task source, and predecessor result hashes.
    On cache hit, returns the cached result without executing the task.
    On cache miss, executes the task and stores the result.

    This subsumes ``IncrementalExecutor`` for cross-run scenarios — no need
    to specify ``changed_nodes`` since the cache key automatically changes
    when any upstream changes.

    Args:
        dag: The DAG to execute.
        cache: ContentAddressableCache instance.
        callbacks: Optional execution callbacks.
        fail_fast: If True, skip downstream nodes when a dependency fails.
        enable_tracing: If True, record execution trace.
    """

    def __init__(
        self,
        dag: DAG,
        cache: ContentAddressableCache,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
    ) -> None:
        self._dag = dag
        self._cache = cache
        self._callbacks = callbacks or ExecutionCallbacks()
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
    ) -> CachedExecutionResult:
        """Execute tasks with content-addressable caching.

        Returns:
            CachedExecutionResult with execution results and cache statistics.
        """
        from dagron.execution.tracing import ExecutionTrace, TraceEventType

        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        cached_result = CachedExecutionResult(execution_result=result)
        failed_nodes: set[str] = set()
        start_time = time.monotonic()

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        # Process in topological order for deterministic cache key computation
        topo_order = [n.name for n in self._dag.topological_sort()]

        # Track result hashes for Merkle-tree key propagation
        result_hashes: dict[str, str] = {}
        key_builder = CacheKeyBuilder()

        ancestors_cache: dict[str, set[str]] = {}

        def get_ancestors(name: str) -> set[str]:
            if name not in ancestors_cache:
                ancestors_cache[name] = {n.name for n in self._dag.ancestors(name)}
            return ancestors_cache[name]

        for name in topo_order:
            # Skip if a dependency has failed
            if self._fail_fast and failed_nodes and get_ancestors(name) & failed_nodes:
                nr: NodeResult[Any] = NodeResult(name=name, status=NodeStatus.SKIPPED)
                result.node_results[name] = nr
                result.skipped += 1
                if self._callbacks.on_skip:
                    self._callbacks.on_skip(name)
                if trace:
                    trace.record(TraceEventType.NODE_SKIPPED, node_name=name)
                continue

            task_fn = tasks.get(name)
            if task_fn is None:
                nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                result.node_results[name] = nr
                result.skipped += 1
                if trace:
                    trace.record(TraceEventType.NODE_SKIPPED, node_name=name)
                continue

            # Build predecessor result hashes
            pred_hashes: dict[str, str] = {}
            for pred in self._dag.predecessors(name):
                pred_name = pred.name
                if pred_name in result_hashes:
                    pred_hashes[pred_name] = result_hashes[pred_name]

            # Compute cache key
            cache_key = self._cache.compute_key(name, task_fn, pred_hashes)

            # Check cache
            cached_value, found = self._cache.get(cache_key)
            if found:
                nr = NodeResult(
                    name=name,
                    status=NodeStatus.CACHE_HIT,
                    result=cached_value,
                    duration_seconds=0.0,
                )
                result.node_results[name] = nr
                result.succeeded += 1
                cached_result.cache_hits += 1
                cached_result.nodes_cached.append(name)
                result_hashes[name] = key_builder.hash_value(cached_value)

                if trace:
                    trace.record(
                        TraceEventType.NODE_CACHE_HIT,
                        node_name=name,
                        metadata={"cache_key": cache_key},
                    )
                continue

            # Cache miss — execute the task
            cached_result.cache_misses += 1

            if trace:
                trace.record(TraceEventType.NODE_CACHE_MISS, node_name=name)
                trace.record(TraceEventType.NODE_STARTED, node_name=name)

            nr = _run_sync_task(name, task_fn, self._callbacks)
            result.node_results[name] = nr

            if nr.status == NodeStatus.COMPLETED:
                result.succeeded += 1
                cached_result.nodes_executed.append(name)
                result_hashes[name] = key_builder.hash_value(nr.result)

                # Store in cache
                self._cache.put(cache_key, nr.result, name)

                if trace:
                    trace.record(
                        TraceEventType.NODE_COMPLETED,
                        node_name=name,
                        duration=nr.duration_seconds,
                    )
            elif nr.status == NodeStatus.FAILED:
                result.failed += 1
                failed_nodes.add(name)
                cached_result.nodes_executed.append(name)
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
        return cached_result
