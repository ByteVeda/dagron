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
DAG.from_records = staticmethod(from_records)
DAG.pretty_print = lambda self, **kw: pretty_print(self, **kw)
DAG._repr_svg_ = lambda self: _repr_svg_(self)

__version__ = "0.1.0"

__all__ = [
    "DAG",
    "DAGBuilder",
    "NodeId",
    "ReachabilityIndex",
    "DagronError",
    "CycleError",
    "NodeNotFoundError",
    "DuplicateNodeError",
    "EdgeNotFoundError",
    "GraphError",
    "GraphDiff",
    "GraphStats",
    "ScheduledNode",
    "ExecutionStep",
    "ExecutionPlan",
    "DAGExecutor",
    "AsyncDAGExecutor",
    "ExecutionCallbacks",
    "ExecutionResult",
    "IncrementalExecutor",
    "IncrementalResult",
    "NodeResult",
    "NodeStatus",
    "ExecutionTrace",
    "TraceEvent",
    "TraceEventType",
    "NodeProfile",
    "ProfileReport",
    "profile_execution",
    "NodeIterator",
    "NodeLevelIterator",
    "pretty_print",
    "from_records",
]
