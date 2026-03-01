"""Checkpoint and resume execution for DAG tasks."""

from __future__ import annotations

import json
import pickle
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import DAG
    from dagron.executor import ExecutionCallbacks, ExecutionResult


@dataclass(frozen=True)
class CheckpointInfo:
    """Information about a saved checkpoint."""

    checkpoint_dir: str
    completed_nodes: list[str]
    failed_nodes: list[str]
    total_nodes: int
    timestamp: float


class CheckpointExecutor:
    """Execute DAG tasks with checkpointing and resume capability.

    Saves node results to disk as they complete. On failure, execution
    can be resumed from the last checkpoint, skipping already-completed
    nodes.

    Args:
        dag: The DAG to execute.
        checkpoint_dir: Directory to store checkpoint files.
        callbacks: Optional execution callbacks.
        fail_fast: Skip downstream nodes on failure.
        enable_tracing: Record execution trace events.

    Example::

        executor = CheckpointExecutor(dag, checkpoint_dir="./checkpoints")
        result = executor.execute(tasks)       # fails at node 47
        result = executor.resume(tasks)        # resumes from node 47
    """

    def __init__(
        self,
        dag: DAG,
        checkpoint_dir: str | Path,
        *,
        callbacks: ExecutionCallbacks | None = None,
        fail_fast: bool = True,
        enable_tracing: bool = False,
    ) -> None:
        self._dag = dag
        self._checkpoint_dir = Path(checkpoint_dir)
        self._fail_fast = fail_fast
        self._enable_tracing = enable_tracing

        from dagron.executor import ExecutionCallbacks

        self._callbacks = callbacks or ExecutionCallbacks()

    def _ensure_dir(self) -> None:
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _node_path(self, name: str) -> Path:
        # Sanitize node name for filesystem
        safe_name = name.replace("/", "_").replace("\\", "_")
        return self._checkpoint_dir / f"{safe_name}.pkl"

    def _meta_path(self) -> Path:
        return self._checkpoint_dir / "_checkpoint_meta.json"

    def _save_node_result(self, name: str, result: Any, status: str, duration: float) -> None:
        data = {
            "result": result,
            "status": status,
            "duration": duration,
        }
        with open(self._node_path(name), "wb") as f:
            pickle.dump(data, f)

    def _load_node_result(self, name: str) -> dict[str, Any] | None:
        path = self._node_path(name)
        if not path.exists():
            return None
        with open(path, "rb") as f:
            result: dict[str, Any] = pickle.load(f)
            return result

    def _save_meta(self, completed: list[str], failed: list[str]) -> None:
        meta = {
            "completed": completed,
            "failed": failed,
            "timestamp": time.time(),
            "node_count": self._dag.node_count(),
        }
        with open(self._meta_path(), "w") as f:
            json.dump(meta, f)

    def _load_meta(self) -> dict[str, Any] | None:
        path = self._meta_path()
        if not path.exists():
            return None
        with open(path) as f:
            result: dict[str, Any] = json.load(f)
            return result

    def checkpoint_info(self) -> CheckpointInfo | None:
        """Get information about the current checkpoint state.

        Returns:
            CheckpointInfo if a checkpoint exists, None otherwise.
        """
        meta = self._load_meta()
        if meta is None:
            return None
        return CheckpointInfo(
            checkpoint_dir=str(self._checkpoint_dir),
            completed_nodes=meta["completed"],
            failed_nodes=meta["failed"],
            total_nodes=meta["node_count"],
            timestamp=meta["timestamp"],
        )

    def clear_checkpoint(self) -> None:
        """Remove all checkpoint files."""
        if self._checkpoint_dir.exists():
            for f in self._checkpoint_dir.iterdir():
                f.unlink()

    def execute(
        self,
        tasks: dict[str, Callable[[], Any]],
    ) -> ExecutionResult:
        """Execute tasks with checkpointing. Starts fresh.

        Args:
            tasks: Dict mapping node names to callables.

        Returns:
            ExecutionResult with per-node results.
        """
        self.clear_checkpoint()
        return self._run(tasks, resume_from=None)

    def resume(
        self,
        tasks: dict[str, Callable[[], Any]],
    ) -> ExecutionResult:
        """Resume execution from the last checkpoint.

        Completed nodes are loaded from checkpoint files.
        Failed and pending nodes are re-executed.

        Args:
            tasks: Dict mapping node names to callables.

        Returns:
            ExecutionResult with per-node results.
        """
        meta = self._load_meta()
        completed = set(meta["completed"]) if meta else set()
        return self._run(tasks, resume_from=completed)

    def _run(
        self,
        tasks: dict[str, Callable[[], Any]],
        resume_from: set[str] | None,
    ) -> ExecutionResult:
        from dagron.executor import (
            ExecutionResult,
            NodeResult,
            NodeStatus,
            _run_sync_task,
        )
        from dagron.tracing import ExecutionTrace, TraceEventType

        self._ensure_dir()
        trace = ExecutionTrace() if self._enable_tracing else None
        result = ExecutionResult()
        failed_nodes: set[str] = set()
        completed_nodes: list[str] = []
        failed_list: list[str] = []
        start_time = time.monotonic()

        if trace:
            trace.record(TraceEventType.EXECUTION_STARTED)

        topo_order = [n.name for n in self._dag.topological_sort()]

        for name in topo_order:
            # If resuming and node was already completed, load from checkpoint
            if resume_from and name in resume_from:
                saved = self._load_node_result(name)
                if saved and saved["status"] == "completed":
                    nr = NodeResult(
                        name=name,
                        status=NodeStatus.COMPLETED,
                        result=saved["result"],
                        duration_seconds=saved["duration"],
                    )
                    result.node_results[name] = nr
                    result.succeeded += 1
                    completed_nodes.append(name)
                    if trace:
                        trace.record(
                            TraceEventType.NODE_COMPLETED,
                            node_name=name,
                            duration=nr.duration_seconds,
                        )
                    continue

            # Check fail-fast
            if self._fail_fast and failed_nodes:
                ancestors = {n.name for n in self._dag.ancestors(name)}
                if ancestors & failed_nodes:
                    nr = NodeResult(name=name, status=NodeStatus.SKIPPED)
                    result.node_results[name] = nr
                    result.skipped += 1
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

            if trace:
                trace.record(TraceEventType.NODE_STARTED, node_name=name)

            nr = _run_sync_task(name, task_fn, self._callbacks)
            result.node_results[name] = nr

            if nr.status == NodeStatus.COMPLETED:
                result.succeeded += 1
                completed_nodes.append(name)
                self._save_node_result(name, nr.result, "completed", nr.duration_seconds)
                self._save_meta(completed_nodes, failed_list)
                if trace:
                    trace.record(
                        TraceEventType.NODE_COMPLETED,
                        node_name=name,
                        duration=nr.duration_seconds,
                    )
            elif nr.status == NodeStatus.FAILED:
                result.failed += 1
                failed_nodes.add(name)
                failed_list.append(name)
                self._save_meta(completed_nodes, failed_list)
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
