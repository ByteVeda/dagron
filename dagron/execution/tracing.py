"""Execution tracing for DAG task execution."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TraceEventType(Enum):
    """Types of trace events recorded during execution."""

    EXECUTION_STARTED = "execution_started"
    STEP_STARTED = "step_started"
    NODE_STARTED = "node_started"
    NODE_COMPLETED = "node_completed"
    NODE_FAILED = "node_failed"
    NODE_SKIPPED = "node_skipped"
    NODE_TIMED_OUT = "node_timed_out"
    NODE_CANCELLED = "node_cancelled"
    STEP_COMPLETED = "step_completed"
    EXECUTION_COMPLETED = "execution_completed"
    # Approval gates
    NODE_GATE_WAITING = "node_gate_waiting"
    NODE_GATE_RESOLVED = "node_gate_resolved"
    # Resource management
    RESOURCE_ACQUIRED = "resource_acquired"
    RESOURCE_RELEASED = "resource_released"
    # Content-addressable caching
    NODE_CACHE_HIT = "node_cache_hit"
    NODE_CACHE_MISS = "node_cache_miss"


@dataclass
class TraceEvent:
    """A single trace event."""

    event_type: TraceEventType
    timestamp: float
    node_name: str | None = None
    step_index: int | None = None
    duration: float | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class ExecutionTrace:
    """Structured timeline log of execution events."""

    def __init__(self) -> None:
        self._events: list[TraceEvent] = []
        self._start_time: float | None = None

    def record(
        self,
        event_type: TraceEventType,
        *,
        node_name: str | None = None,
        step_index: int | None = None,
        duration: float | None = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a trace event."""
        if self._start_time is None:
            self._start_time = time.monotonic()
        event = TraceEvent(
            event_type=event_type,
            timestamp=time.monotonic() - self._start_time,
            node_name=node_name,
            step_index=step_index,
            duration=duration,
            error=error,
            metadata=metadata or {},
        )
        self._events.append(event)

    @property
    def events(self) -> list[TraceEvent]:
        """All recorded events."""
        return list(self._events)

    def events_for_node(self, name: str) -> list[TraceEvent]:
        """Filter events for a specific node."""
        return [e for e in self._events if e.node_name == name]

    def to_json(self) -> str:
        """Export trace as JSON."""
        records: list[dict[str, Any]] = []
        for e in self._events:
            record: dict[str, Any] = {
                "event_type": e.event_type.value,
                "timestamp": e.timestamp,
            }
            if e.node_name is not None:
                record["node_name"] = e.node_name
            if e.step_index is not None:
                record["step_index"] = e.step_index
            if e.duration is not None:
                record["duration"] = e.duration
            if e.error is not None:
                record["error"] = e.error
            if e.metadata:
                record["metadata"] = e.metadata
            records.append(record)
        return json.dumps(records, indent=2)

    def to_chrome_trace(self) -> str:
        """Export trace in Chrome Tracing format (chrome://tracing).

        Returns a JSON string compatible with the Chrome trace viewer.
        Each node execution becomes a Duration event (B/E pair).
        """
        events: list[dict[str, Any]] = []
        pid = 1
        tid_map: dict[str, int] = {}
        next_tid = 1

        for e in self._events:
            if e.node_name is not None:
                if e.node_name not in tid_map:
                    tid_map[e.node_name] = next_tid
                    next_tid += 1
                tid = tid_map[e.node_name]
            else:
                tid = 0

            ts = e.timestamp * 1_000_000  # Convert to microseconds

            if e.event_type == TraceEventType.NODE_STARTED:
                events.append(
                    {
                        "name": e.node_name,
                        "cat": "node",
                        "ph": "B",
                        "ts": ts,
                        "pid": pid,
                        "tid": tid,
                    }
                )
            elif e.event_type in (
                TraceEventType.NODE_COMPLETED,
                TraceEventType.NODE_FAILED,
                TraceEventType.NODE_TIMED_OUT,
            ):
                args: dict[str, Any] = {"status": e.event_type.value}
                if e.error:
                    args["error"] = e.error
                events.append(
                    {
                        "name": e.node_name,
                        "cat": "node",
                        "ph": "E",
                        "ts": ts,
                        "pid": pid,
                        "tid": tid,
                        "args": args,
                    }
                )
            elif e.event_type == TraceEventType.EXECUTION_STARTED:
                events.append(
                    {
                        "name": "execution",
                        "cat": "execution",
                        "ph": "B",
                        "ts": ts,
                        "pid": pid,
                        "tid": 0,
                    }
                )
            elif e.event_type == TraceEventType.EXECUTION_COMPLETED:
                events.append(
                    {
                        "name": "execution",
                        "cat": "execution",
                        "ph": "E",
                        "ts": ts,
                        "pid": pid,
                        "tid": 0,
                    }
                )

        return json.dumps({"traceEvents": events}, indent=2)

    def summary(self) -> str:
        """Return a human-readable summary of the trace."""
        total = len(self._events)
        node_events = [e for e in self._events if e.node_name is not None]
        unique_nodes = {e.node_name for e in node_events}
        completed = sum(1 for e in self._events if e.event_type == TraceEventType.NODE_COMPLETED)
        failed = sum(1 for e in self._events if e.event_type == TraceEventType.NODE_FAILED)
        skipped = sum(1 for e in self._events if e.event_type == TraceEventType.NODE_SKIPPED)
        timed_out = sum(1 for e in self._events if e.event_type == TraceEventType.NODE_TIMED_OUT)
        cancelled = sum(1 for e in self._events if e.event_type == TraceEventType.NODE_CANCELLED)

        # Total execution duration
        duration = 0.0
        if self._events:
            duration = self._events[-1].timestamp - self._events[0].timestamp

        lines = [
            "Execution Trace Summary",
            f"  Total events: {total}",
            f"  Unique nodes: {len(unique_nodes)}",
            f"  Completed: {completed}",
            f"  Failed: {failed}",
            f"  Skipped: {skipped}",
            f"  Timed out: {timed_out}",
            f"  Cancelled: {cancelled}",
            f"  Duration: {duration:.4f}s",
        ]
        return "\n".join(lines)
