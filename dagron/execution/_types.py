"""Core data types for DAG task execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar, overload

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron._internal import NodeRef
    from dagron.execution.tracing import ExecutionTrace
    from dagron.flow import FlowFuture


# Type variable used for `__getitem__` overloads (PEP 695 generic syntax is
# used directly on `NodeResult` below, so this TypeVar is method-scoped).
T = TypeVar("T")


class NodeStatus(Enum):
    """Status of a node during execution."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    CACHE_HIT = "cache_hit"


@dataclass
class NodeResult[T]:
    """Result of executing a single node.

    Generic in the wrapped value type so typed lookups
    (e.g. `result[my_flow_future]`) preserve the value's type at the
    *class* level. The `result` field is typed `Any` for backwards
    compat with code that subscripts it directly (`result["k"].result["x"]`).
    """

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
    on_gate_waiting: Callable[[str], None] | None = None
    on_gate_resolved: Callable[[str, str], None] | None = None
    on_dynamic_expand: Callable[[str, Any], None] | None = None
    on_resource_acquired: Callable[[str, dict[str, int]], None] | None = None
    on_resource_released: Callable[[str, dict[str, int]], None] | None = None


@dataclass
class ExecutionResult:
    """Aggregate result of executing an entire DAG."""

    node_results: dict[str, NodeResult[Any]] = field(default_factory=dict)
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    timed_out: int = 0
    cancelled: int = 0
    total_duration_seconds: float = 0.0
    trace: ExecutionTrace | None = None

    @overload
    def __getitem__(self, node: FlowFuture[T]) -> NodeResult[T]: ...
    @overload
    def __getitem__(self, node: str | NodeRef) -> NodeResult[Any]: ...
    def __getitem__(self, node: str | NodeRef | FlowFuture[Any]) -> NodeResult[Any]:
        """Look up a node's result by string name, NodeRef, or FlowFuture."""
        # Both NodeRef and FlowFuture expose `.name`.
        key = node if isinstance(node, str) else node.name
        return self.node_results[key]

    def __contains__(self, node: object) -> bool:
        if isinstance(node, str):
            return node in self.node_results
        # NodeRef and FlowFuture both expose `.name`.
        name = getattr(node, "name", None)
        return name in self.node_results if isinstance(name, str) else False
