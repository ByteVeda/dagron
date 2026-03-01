"""Type stubs for dagron package re-exports."""

from typing import Any, Callable, Sequence

from dagron._internal import (
    DAG as DAG,
    CycleError as CycleError,
    DagronError as DagronError,
    DuplicateNodeError as DuplicateNodeError,
    EdgeNotFoundError as EdgeNotFoundError,
    ExecutionPlan as ExecutionPlan,
    ExecutionStep as ExecutionStep,
    GraphError as GraphError,
    NodeId as NodeId,
    NodeNotFoundError as NodeNotFoundError,
    ReachabilityIndex as ReachabilityIndex,
    ScheduledNode as ScheduledNode,
)
from dagron.builder import DAGBuilder as DAGBuilder
from dagron.executor import (
    AsyncDAGExecutor as AsyncDAGExecutor,
    DAGExecutor as DAGExecutor,
    ExecutionCallbacks as ExecutionCallbacks,
    ExecutionResult as ExecutionResult,
    IncrementalExecutor as IncrementalExecutor,
    IncrementalResult as IncrementalResult,
    NodeResult as NodeResult,
    NodeStatus as NodeStatus,
)

__version__: str

def from_records(
    records: Sequence[Any],
    *,
    name_field: str = "name",
    edge_fn: Callable[[Any], list[str]] | None = None,
    payload_fn: Callable[[Any], Any] | None = None,
) -> DAG: ...
