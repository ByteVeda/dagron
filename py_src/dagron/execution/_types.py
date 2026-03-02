"""Core data types for DAG task execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from dagron.execution.tracing import ExecutionTrace


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
    on_gate_waiting: Callable[[str], None] | None = None
    on_gate_resolved: Callable[[str, str], None] | None = None
    on_dynamic_expand: Callable[[str, Any], None] | None = None
    on_resource_acquired: Callable[[str, dict[str, int]], None] | None = None
    on_resource_released: Callable[[str, dict[str, int]], None] | None = None


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
    trace: ExecutionTrace | None = None
