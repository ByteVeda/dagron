"""dagron — A fast Rust-backed DAG engine for Python."""

from dagron._internal import (
    DAG,
    CycleError,
    DagronError,
    DuplicateNodeError,
    EdgeNotFoundError,
    ExecutionPlan,
    ExecutionStep,
    GraphDiff,
    GraphError,
    GraphStats,
    NodeId,
    NodeIterator,
    NodeLevelIterator,
    NodeNotFoundError,
    ReachabilityIndex,
    ScheduledNode,
)
from dagron.builder import DAGBuilder
from dagron.display import _repr_svg_, pretty_print
from dagron.executor import (
    AsyncDAGExecutor,
    DAGExecutor,
    ExecutionCallbacks,
    ExecutionResult,
    IncrementalExecutor,
    IncrementalResult,
    NodeResult,
    NodeStatus,
)
from dagron.integration import from_records
from dagron.profiling import NodeProfile, ProfileReport, profile_execution
from dagron.tracing import ExecutionTrace, TraceEvent, TraceEventType

# Monkey-patch for convenience
DAG.from_records = staticmethod(from_records)  # type: ignore[attr-defined]
DAG.pretty_print = lambda self, **kw: pretty_print(self, **kw)  # type: ignore[attr-defined]
DAG._repr_svg_ = lambda self: _repr_svg_(self)  # type: ignore[attr-defined]

__version__ = "0.1.0"

__all__ = [
    "DAG",
    "AsyncDAGExecutor",
    "CycleError",
    "DAGBuilder",
    "DAGExecutor",
    "DagronError",
    "DuplicateNodeError",
    "EdgeNotFoundError",
    "ExecutionCallbacks",
    "ExecutionPlan",
    "ExecutionResult",
    "ExecutionStep",
    "ExecutionTrace",
    "GraphDiff",
    "GraphError",
    "GraphStats",
    "IncrementalExecutor",
    "IncrementalResult",
    "NodeId",
    "NodeIterator",
    "NodeLevelIterator",
    "NodeNotFoundError",
    "NodeProfile",
    "NodeResult",
    "NodeStatus",
    "ProfileReport",
    "ReachabilityIndex",
    "ScheduledNode",
    "TraceEvent",
    "TraceEventType",
    "from_records",
    "pretty_print",
    "profile_execution",
]
