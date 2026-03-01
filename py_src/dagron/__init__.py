"""dagron — A fast Rust-backed DAG engine for Python."""

from dagron._internal import (
    DAG,
    CycleError,
    DagronError,
    DuplicateNodeError,
    EdgeNotFoundError,
    ExecutionPlan,
    ExecutionStep,
    GraphError,
    NodeId,
    NodeNotFoundError,
    ReachabilityIndex,
    ScheduledNode,
)
from dagron.builder import DAGBuilder
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

# Monkey-patch for convenience
DAG.from_records = staticmethod(from_records)

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
    "from_records",
]
