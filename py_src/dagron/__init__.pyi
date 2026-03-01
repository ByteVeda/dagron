"""Type stubs for dagron package re-exports."""

from collections.abc import Callable, Sequence
from typing import Any, Literal

from dagron._internal import (
    DAG as DAG,
)
from dagron._internal import (
    CycleError as CycleError,
)
from dagron._internal import (
    DagronError as DagronError,
)
from dagron._internal import (
    DuplicateNodeError as DuplicateNodeError,
)
from dagron._internal import (
    EdgeNotFoundError as EdgeNotFoundError,
)
from dagron._internal import (
    ExecutionPlan as ExecutionPlan,
)
from dagron._internal import (
    ExecutionStep as ExecutionStep,
)
from dagron._internal import (
    GraphDiff as GraphDiff,
)
from dagron._internal import (
    GraphError as GraphError,
)
from dagron._internal import (
    GraphStats as GraphStats,
)
from dagron._internal import (
    NodeId as NodeId,
)
from dagron._internal import (
    NodeIterator as NodeIterator,
)
from dagron._internal import (
    NodeLevelIterator as NodeLevelIterator,
)
from dagron._internal import (
    NodeNotFoundError as NodeNotFoundError,
)
from dagron._internal import (
    ReachabilityIndex as ReachabilityIndex,
)
from dagron._internal import (
    ScheduledNode as ScheduledNode,
)
from dagron.builder import DAGBuilder as DAGBuilder
from dagron.executor import (
    AsyncDAGExecutor as AsyncDAGExecutor,
)
from dagron.executor import (
    DAGExecutor as DAGExecutor,
)
from dagron.executor import (
    ExecutionCallbacks as ExecutionCallbacks,
)
from dagron.executor import (
    ExecutionResult as ExecutionResult,
)
from dagron.executor import (
    IncrementalExecutor as IncrementalExecutor,
)
from dagron.executor import (
    IncrementalResult as IncrementalResult,
)
from dagron.executor import (
    NodeResult as NodeResult,
)
from dagron.executor import (
    NodeStatus as NodeStatus,
)
from dagron.profiling import (
    NodeProfile as NodeProfile,
)
from dagron.profiling import (
    ProfileReport as ProfileReport,
)
from dagron.profiling import (
    profile_execution as profile_execution,
)
from dagron.tracing import (
    ExecutionTrace as ExecutionTrace,
)
from dagron.tracing import (
    TraceEvent as TraceEvent,
)
from dagron.tracing import (
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
    layout: Literal["vertical", "horizontal"] = "vertical",
    max_nodes: int = 50,
    show_payloads: bool = False,
    node_formatter: Callable[[str, Any], str] | None = None,
) -> str: ...
def _repr_svg_(dag: DAG, *, max_nodes: int = 100) -> str: ...
