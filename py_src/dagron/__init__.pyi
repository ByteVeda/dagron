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
    GraphDiff as GraphDiff,
    GraphError as GraphError,
    GraphStats as GraphStats,
    NodeId as NodeId,
    NodeIterator as NodeIterator,
    NodeLevelIterator as NodeLevelIterator,
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
from dagron.profiling import (
    NodeProfile as NodeProfile,
    ProfileReport as ProfileReport,
    profile_execution as profile_execution,
)
from dagron.tracing import (
    ExecutionTrace as ExecutionTrace,
    TraceEvent as TraceEvent,
    TraceEventType as TraceEventType,
)

__version__: str

def from_records(
    records: Sequence[Any],
    *,
    name_field: str = "name",
    edge_fn: Callable[[Any], list[str]] | None = None,
    payload_fn: Callable[[Any], Any] | None = None,
) -> DAG: ...
def pretty_print(
    dag: DAG,
    *,
    layout: str = "vertical",
    max_nodes: int = 50,
    show_payloads: bool = False,
    node_formatter: Callable[[str, Any], str] | None = None,
) -> str: ...
def _repr_svg_(dag: DAG, *, max_nodes: int = 100) -> str: ...
